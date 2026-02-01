from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Sequence, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from okx.common.etl_common import (
    batch_iter,
    day_start_utc,
    get_logical_run_date,
    log_diagnostics,
    ms,
)


# ============================================================
# 0) Project-wide constants (единый стандарт для всех DAG)
# ============================================================

CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"

DAG_ID = "okx_raw_to_core_mark_price_tick"
SCHEDULE = None  # запускается мастер-DAG'ом раз в сутки (t-1)
TAGS = ["okx", "etl", "raw-to-core", "timescaledb", "mark-price"]

SQL_SELECT_1 = "SELECT 1;"
SQL_CURRENT_DB = "SELECT current_database();"

  
# ============================================================
# 1) Config (всё настраиваемое — только тут)
# ============================================================

@dataclass(frozen=True)
class EtlConfig:
    raw_table_fq: str = "okx_raw.mark_prices"
    core_table_fq: str = "okx_core.fact_mark_price_tick"

    # "rolling" / "backfill"
    mode: str = "backfill"

    window_hours: int = 6

    # backfill controls
    max_windows_per_run: int = 144  # 24 часа при step=10m

    # batching by time
    step_minutes: int = 10
    overlap_minutes: int = 2

    # batching by instrument
    batch_size: int = 20
    max_instruments_per_run: int | None = None

    # statement timeout (ms)
    statement_timeout_ms: int = 30 * 60 * 1000

    # safety/ops
    execution_timeout_sec: int = 2 * 60 * 60
    retries: int = 1
    retry_delay_sec: int = 120


CFG = EtlConfig()


# ============================================================
# 2) Helpers
# ============================================================

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _day_start_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


def _floor_to_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def _ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _db_sanity_checks(hook: PostgresHook) -> str:
    v = hook.get_first(SQL_SELECT_1)
    if not v or v[0] != 1:
        raise RuntimeError(f"DB ping failed: {v}")

    row = hook.get_first(SQL_CURRENT_DB)
    dbname = row[0] if row else None
    if DB_NAME_EXPECTED and dbname != DB_NAME_EXPECTED:
        raise RuntimeError(
            f"Connected to unexpected database: {dbname} (expected {DB_NAME_EXPECTED})"
        )
    return dbname or "UNKNOWN"


def _get_core_watermark_dt(cursor) -> datetime | None:
    sql = f"SELECT max(ts_ingest) FROM {CFG.core_table_fq};"
    cursor.execute(sql)
    row = cursor.fetchone()
    return row[0] if row and row[0] is not None else None

def _get_raw_max_ts_ingest_ms(cursor) -> int | None:
    sql = f"SELECT max(ts_ingest_ms) FROM {CFG.raw_table_fq};"
    cursor.execute(sql)
    row = cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def _get_distinct_instids(cursor, where_sql: str, alias: str) -> list[str]:
    cursor.execute(
        f"SELECT DISTINCT {alias}.instid FROM {CFG.raw_table_fq} {alias} WHERE {where_sql};"
    )
    return [r[0] for r in cursor.fetchall() if r and r[0] is not None]


def _window_bounds_rolling(now: datetime) -> Tuple[datetime, datetime]:
    to_dt = _day_start_utc(now)
    from_dt = to_dt - timedelta(hours=CFG.window_hours) - \
        timedelta(minutes=CFG.overlap_minutes)
    return from_dt, to_dt


def _window_bounds_backfill(hook: PostgresHook, now: datetime) -> Tuple[datetime, datetime]:
    to_dt = _day_start_utc(now)
    wm = _get_core_watermark_dt(hook)

    if wm is None:
        from_dt = to_dt - timedelta(days=1)
    else:
        from_dt = wm - timedelta(minutes=CFG.overlap_minutes)

    return _floor_to_minute(from_dt), to_dt


def _sql_insert_window(where_sql: str, instids: Sequence[str] | None) -> tuple[str, dict | None]:
    """
    RAW okx_raw.mark_prices:
      instid varchar(50)
      markpx float8
      idxpx float8 (IGNORED)
      idxts int8 (IGNORED)
      ts_event_ms int8
      ts_ingest_ms int8

    CORE okx_core.fact_mark_price_tick:
      inst_id text
      ts_event timestamptz
      ts_ingest timestamptz
      mark_px float8

    Dedup key: (ts_event, inst_id)  <-- unique index должен существовать
    """
    return f"""
    WITH ins AS (
      INSERT INTO {CFG.core_table_fq}
        (inst_id, ts_event, ts_ingest, mark_px)
      SELECT
        r.instid::text AS inst_id,
        (to_timestamp(r.ts_event_ms / 1000.0) AT TIME ZONE 'UTC')::timestamptz AS ts_event,
        (to_timestamp(r.ts_ingest_ms / 1000.0) AT TIME ZONE 'UTC')::timestamptz AS ts_ingest,
        r.markpx::float8 AS mark_px
      FROM {CFG.raw_table_fq} r
      WHERE {where_sql}
        {f"AND r.instid = ANY(%(instids)s)" if instids is not None else ""}
      ON CONFLICT (ts_event, inst_id) DO NOTHING
      RETURNING 1
    )
    SELECT count(*)::bigint AS inserted_rows FROM ins;
    """, ({"instids": list(instids)} if instids is not None else None)


# ============================================================
# 3) Main callable
# ============================================================

def run_sync() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    _db_sanity_checks(hook)

    conn = hook.get_conn()
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("SET statement_timeout = %s", (CFG.statement_timeout_ms,))
    log_diagnostics(cursor, [CFG.raw_table_fq, CFG.core_table_fq])

    run_dt = get_logical_run_date()
    to_dt = day_start_utc(run_dt)

    wm = _get_core_watermark_dt(cursor)
    from_dt = wm - timedelta(minutes=CFG.overlap_minutes) if wm else None

    to_ms = ms(to_dt)
    where_sql = f"r.ts_ingest_ms < {to_ms}"
    if from_dt is not None:
        where_sql = f"r.ts_ingest_ms >= {ms(from_dt)} AND " + where_sql

    raw_max_ms = _get_raw_max_ts_ingest_ms(cursor)
    if raw_max_ms is None or (from_dt is not None and raw_max_ms < ms(from_dt)):
        print(f"[{DAG_ID}] SKIP: raw empty or older than window raw_max_ms={raw_max_ms}")
        return

    instids = _get_distinct_instids(cursor, where_sql, "r")
    if CFG.max_instruments_per_run is not None:
        instids = instids[: CFG.max_instruments_per_run]

    inserted_total = 0
    if not instids or len(instids) <= CFG.batch_size:
        sql, params = _sql_insert_window(where_sql, None)
        cursor.execute(sql, params)
        row = cursor.fetchone()
        inserted_total = int(row[0]) if row and row[0] is not None else 0
        print(f"[{DAG_ID}] inserted_total={inserted_total}")
        return

    print(f"[{DAG_ID}] batching instid count={len(instids)} batch_size={CFG.batch_size}")
    for batch in batch_iter(instids, CFG.batch_size):
        sql, params = _sql_insert_window(where_sql, batch)
        cursor.execute(sql, params)
        row = cursor.fetchone()
        inserted = int(row[0]) if row and row[0] is not None else 0
        inserted_total += inserted
        print(f"[{DAG_ID}] batch inserted={inserted}")

    print(f"[{DAG_ID}] DONE inserted_total={inserted_total}")


# ============================================================
# 4) DAG definition
# ============================================================

default_args: dict[str, Any] = {
    "owner": "okx-data",
    "retries": CFG.retries,
    "retry_delay": timedelta(seconds=CFG.retry_delay_sec),
    "execution_timeout": timedelta(seconds=CFG.execution_timeout_sec),
}

with DAG(
    dag_id=DAG_ID,
    description="OKX ETL: windowed raw->core for mark prices (rolling/backfill; dedup via ON CONFLICT)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=TAGS,
) as dag:
    PythonOperator(
        task_id="sync",
        python_callable=run_sync,
    )
