from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"
 
DAG_ID = "okx_core_tick_to_core_funding_rate_event"
SCHEDULE = "30 1,7,13,19 * * *"  # чуть после tick DAG
TAGS = ["okx", "etl", "core", "timescaledb", "funding", "event"]

SQL_SELECT_1 = "SELECT 1;"
SQL_CURRENT_DB = "SELECT current_database();"


@dataclass(frozen=True)
class EtlConfig:
    tick_table_fq: str = "okx_core.fact_funding_rate_tick"
    event_table_fq: str = "okx_core.fact_funding_rate_event"

    mode: str = "backfill"
    window_hours: int = 24  # события редкие, можно шире

    max_windows_per_run: int = 144  # 24 часа при step=10m
    step_minutes: int = 60
    overlap_minutes: int = 5

    execution_timeout_sec: int = 2 * 60 * 60
    retries: int = 1
    retry_delay_sec: int = 120


CFG = EtlConfig()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _floor_to_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def _db_sanity_checks(hook: PostgresHook) -> str:
    v = hook.get_first(SQL_SELECT_1)
    if not v or v[0] != 1:
        raise RuntimeError(f"DB ping failed: {v}")

    row = hook.get_first(SQL_CURRENT_DB)
    dbname = row[0] if row else None
    if DB_NAME_EXPECTED and dbname != DB_NAME_EXPECTED:
        raise RuntimeError(f"Connected to unexpected database: {dbname} (expected {DB_NAME_EXPECTED})")
    return dbname or "UNKNOWN"


def _get_event_watermark_dt(hook: PostgresHook) -> datetime | None:
    sql = f"SELECT max(ts_ingest) FROM {CFG.event_table_fq};"
    row = hook.get_first(sql)
    return row[0] if row and row[0] is not None else None


def _window_bounds_rolling(now: datetime) -> Tuple[datetime, datetime]:
    to_dt = _floor_to_minute(now)
    from_dt = to_dt - timedelta(hours=CFG.window_hours) - timedelta(minutes=CFG.overlap_minutes)
    return from_dt, to_dt


def _window_bounds_backfill(hook: PostgresHook, now: datetime) -> Tuple[datetime, datetime]:
    to_dt = _floor_to_minute(now)
    wm = _get_event_watermark_dt(hook)
    if wm is None:
        from_dt = to_dt - timedelta(hours=CFG.window_hours)
    else:
        from_dt = wm - timedelta(minutes=CFG.overlap_minutes)
    return _floor_to_minute(from_dt), to_dt


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
    now = _now_utc()

    if CFG.mode not in ("rolling", "backfill"):
        raise ValueError(f"CFG.mode must be 'rolling' or 'backfill', got: {CFG.mode}")

    if CFG.mode == "rolling":
        from_dt, to_dt = _window_bounds_rolling(now)
        windows_budget = 10**9
    else:
        from_dt, to_dt = _window_bounds_backfill(hook, now)
        windows_budget = CFG.max_windows_per_run

    step = timedelta(minutes=CFG.step_minutes)
    t = from_dt

    inserted_total = 0
    windows_done = 0

    print(f"[{DAG_ID}] mode={CFG.mode} db={dbname} window=[{from_dt}..{to_dt}) step_min={CFG.step_minutes}")

    while t < to_dt and windows_done < windows_budget:
        w_from = t
        w_to = min(t + step, to_dt)

        row = hook.get_first(_sql_insert_window(w_from, w_to))
        inserted_rows = int(row[0]) if row and row[0] is not None else 0

        inserted_total += inserted_rows
        windows_done += 1
        print(f"[{DAG_ID}] window [{w_from.isoformat()}..{w_to.isoformat()}) inserted={inserted_rows}")
        t = w_to

    print(f"[{DAG_ID}] DONE inserted_total={inserted_total} windows_done={windows_done} stopped_at={t.isoformat()}")


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
