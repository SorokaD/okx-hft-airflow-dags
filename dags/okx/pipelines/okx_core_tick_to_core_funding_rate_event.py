from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from okx.common.etl_common import get_logical_run_date, log_diagnostics, day_start_utc


CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"
 
DAG_ID = "okx_core_tick_to_core_funding_rate_event"
SCHEDULE = None  # запускается мастер-DAG'ом раз в сутки (t-1)
TAGS = ["okx", "etl", "core", "timescaledb", "funding", "event"]

SQL_SELECT_1 = "SELECT 1;"
SQL_CURRENT_DB = "SELECT current_database();"


@dataclass(frozen=True)
class EtlConfig:
    tick_table_fq: str = "okx_core.fact_funding_rate_tick"
    event_table_fq: str = "okx_core.fact_funding_rate_event"

    overlap_minutes: int = 5

    # statement timeout (ms)
    statement_timeout_ms: int = 30 * 60 * 1000

    execution_timeout_sec: int = 2 * 60 * 60
    retries: int = 1
    retry_delay_sec: int = 120


CFG = EtlConfig()


def _db_sanity_checks(hook: PostgresHook) -> str:
    v = hook.get_first(SQL_SELECT_1)
    if not v or v[0] != 1:
        raise RuntimeError(f"DB ping failed: {v}")

    row = hook.get_first(SQL_CURRENT_DB)
    dbname = row[0] if row else None
    if DB_NAME_EXPECTED and dbname != DB_NAME_EXPECTED:
        raise RuntimeError(f"Connected to unexpected database: {dbname} (expected {DB_NAME_EXPECTED})")
    return dbname or "UNKNOWN"


def _get_event_watermark_dt(cursor) -> datetime | None:
    sql = f"SELECT max(ts_ingest) FROM {CFG.event_table_fq};"
    cursor.execute(sql)
    row = cursor.fetchone()
    return row[0] if row and row[0] is not None else None

def _get_tick_max_ts_ingest(cursor) -> datetime | None:
    sql = f"SELECT max(ts_ingest) FROM {CFG.tick_table_fq};"
    cursor.execute(sql)
    row = cursor.fetchone()
    return row[0] if row and row[0] is not None else None


def _sql_insert_window(from_dt: datetime, to_dt: datetime) -> str:
    # Делаем "event" из тиков: последний тик на каждое funding_time
    # Ограничиваемся тиками по ts_ingest в окне.
    return f"""
    WITH latest AS (
      SELECT DISTINCT ON (t.inst_id, t.funding_time)
        t.inst_id,
        t.funding_time,
        t.ts_ingest,
        t.funding_rate,
        t.next_funding_time
      FROM {CFG.tick_table_fq} t
      WHERE t.ts_ingest >= '{from_dt.isoformat()}'
        AND t.ts_ingest <  '{to_dt.isoformat()}'
      ORDER BY t.inst_id, t.funding_time, t.ts_ingest DESC
    ),
    ins AS (
      INSERT INTO {CFG.event_table_fq}
        (inst_id, ts_event, ts_ingest, funding_rate, funding_time, next_funding_time)
      SELECT
        l.inst_id,
        l.funding_time AS ts_event,
        l.ts_ingest,
        l.funding_rate,
        l.funding_time,
        l.next_funding_time
      FROM latest l
      ON CONFLICT (ts_event, inst_id, funding_time) DO NOTHING
      RETURNING 1
    )
    SELECT count(*)::bigint AS inserted_rows FROM ins;
    """


def run_sync() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    dbname = _db_sanity_checks(hook)

    conn = hook.get_conn()
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("SET statement_timeout = %s", (CFG.statement_timeout_ms,))
    log_diagnostics(cursor, [CFG.tick_table_fq, CFG.event_table_fq])

    run_dt = get_logical_run_date()
    to_dt = day_start_utc(run_dt)

    wm = _get_event_watermark_dt(cursor)
    from_dt = wm - timedelta(minutes=CFG.overlap_minutes) if wm else None

    tick_max = _get_tick_max_ts_ingest(cursor)
    if tick_max is None or (from_dt is not None and tick_max < from_dt):
        print(
            f"[{DAG_ID}] SKIP: tick empty or older than window "
            f"tick_max={tick_max} window_from={from_dt}"
        )
        return

    if from_dt is None:
        from_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)

    print(f"[{DAG_ID}] db={dbname} window=[{from_dt}..{to_dt})")
    row = hook.get_first(_sql_insert_window(from_dt, to_dt))
    inserted_rows = int(row[0]) if row and row[0] is not None else 0
    print(f"[{DAG_ID}] DONE inserted_total={inserted_rows}")


default_args: dict[str, Any] = {
    "owner": "okx-data",
    "retries": CFG.retries,
    "retry_delay": timedelta(seconds=CFG.retry_delay_sec),
    "execution_timeout": timedelta(seconds=CFG.execution_timeout_sec),
}

with DAG(
    dag_id=DAG_ID,
    description="OKX ETL: core tick -> core funding rate event (latest tick per funding_time)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=TAGS,
) as dag:
    PythonOperator(task_id="sync", python_callable=run_sync)
