from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"

DAG_ID = "okx_raw_to_core_funding_rate_tick"
SCHEDULE = "15 1,7,13,19 * * *"  # как раньше (UTC)
TAGS = ["okx", "etl", "raw-to-core", "timescaledb", "funding"]
  
SQL_SELECT_1 = "SELECT 1;"
SQL_CURRENT_DB = "SELECT current_database();"


@dataclass(frozen=True)
class EtlConfig:
    raw_table_fq: str = "okx_raw.funding_rates"
    core_table_fq: str = "okx_core.fact_funding_rate_tick"

    mode: str = "backfill"  # rolling/backfill
    window_hours: int = 6

    max_windows_per_run: int = 144  # 24 часа при step=10m

    step_minutes: int = 60
    overlap_minutes: int = 2

    execution_timeout_sec: int = 2 * 60 * 60
    retries: int = 1
    retry_delay_sec: int = 120


CFG = EtlConfig()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


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
        raise RuntimeError(f"Connected to unexpected database: {dbname} (expected {DB_NAME_EXPECTED})")
    return dbname or "UNKNOWN"


def _get_core_watermark_dt(hook: PostgresHook) -> datetime | None:
    sql = f"SELECT max(ts_ingest) FROM {CFG.core_table_fq};"
    row = hook.get_first(sql)
    return row[0] if row and row[0] is not None else None


def _window_bounds_rolling(now: datetime) -> Tuple[datetime, datetime]:
    to_dt = _floor_to_minute(now)
    from_dt = to_dt - timedelta(hours=CFG.window_hours) - timedelta(minutes=CFG.overlap_minutes)
    return from_dt, to_dt


def _window_bounds_backfill(hook: PostgresHook, now: datetime) -> Tuple[datetime, datetime]:
    to_dt = _floor_to_minute(now)
    wm = _get_core_watermark_dt(hook)
    if wm is None:
        from_dt = to_dt - timedelta(hours=CFG.window_hours)
    else:
        from_dt = wm - timedelta(minutes=CFG.overlap_minutes)
    return _floor_to_minute(from_dt), to_dt


def _sql_insert_window(from_ms: int, to_ms: int) -> str:
    # raw: instid, fundingrate, fundingtime(ms), nextfundingtime(ms), ts_event_ms, ts_ingest_ms
    # core: inst_id, ts_event, ts_ingest, funding_rate, funding_time, next_funding_time
    return f"""
    WITH ins AS (
      INSERT INTO {CFG.core_table_fq}
        (inst_id, ts_event, ts_ingest, funding_rate, funding_time, next_funding_time)
      SELECT
        r.instid::text AS inst_id,
        (to_timestamp(r.ts_event_ms / 1000.0) AT TIME ZONE 'UTC')::timestamptz AS ts_event,
        (to_timestamp(r.ts_ingest_ms / 1000.0) AT TIME ZONE 'UTC')::timestamptz AS ts_ingest,
        r.fundingrate::float8 AS funding_rate,
        (to_timestamp(r.fundingtime / 1000.0) AT TIME ZONE 'UTC')::timestamptz AS funding_time,
        (to_timestamp(r.nextfundingtime / 1000.0) AT TIME ZONE 'UTC')::timestamptz AS next_funding_time
      FROM {CFG.raw_table_fq} r
      WHERE r.ts_ingest_ms >= {from_ms}
        AND r.ts_ingest_ms <  {to_ms}
      ON CONFLICT (ts_event, inst_id) DO NOTHING
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

        row = hook.get_first(_sql_insert_window(_ms(w_from), _ms(w_to)))
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
    description="OKX ETL: raw->core funding rate tick (rolling/backfill; dedup via ON CONFLICT)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=TAGS,
) as dag:
    PythonOperator(task_id="sync", python_callable=run_sync)
