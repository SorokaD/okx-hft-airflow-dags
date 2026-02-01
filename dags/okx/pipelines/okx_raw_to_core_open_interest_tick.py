from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ============================================================
# 0) Project-wide constants (единый стандарт для всех DAG)
# ============================================================

CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"

DAG_ID = "okx_raw_to_core_open_interest_tick"
SCHEDULE = None  # запускается мастер-DAG'ом раз в сутки (t-1)
TAGS = ["okx", "etl", "raw-to-core", "timescaledb", "open-interest"]

SQL_SELECT_1 = "SELECT 1;"
SQL_CURRENT_DB = "SELECT current_database();"

 
# ============================================================
# 1) Config (всё настраиваемое — только тут)
# ============================================================

@dataclass(frozen=True)
class EtlConfig:
    raw_table_fq: str = "okx_raw.open_interest"
    core_table_fq: str = "okx_core.fact_open_interest_tick"

    # "rolling" / "backfill"
    mode: str = "backfill"

    window_hours: int = 6

    # backfill controls
    max_windows_per_run: int = 144  # 24 часа при step=10m

    # batching by time
    step_minutes: int = 10
    overlap_minutes: int = 2

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


def _get_core_watermark_dt(hook: PostgresHook) -> datetime | None:
    sql = f"SELECT max(ts_ingest) FROM {CFG.core_table_fq};"
    row = hook.get_first(sql)
    return row[0] if row and row[0] is not None else None

def _get_raw_max_ts_ingest_ms(hook: PostgresHook) -> int | None:
    sql = f"SELECT max(ts_ingest_ms) FROM {CFG.raw_table_fq};"
    row = hook.get_first(sql)
    return int(row[0]) if row and row[0] is not None else None


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


def _sql_insert_window(from_ms: int, to_ms: int) -> str:
    """
    RAW okx_raw.open_interest:
      instid varchar(50)
      oi float8
      oiccy float8
      ts_event_ms int8
      ts_ingest_ms int8

    CORE okx_core.fact_open_interest_tick:
      inst_id text
      ts_event timestamptz
      ts_ingest timestamptz
      open_interest float8
      open_interest_ccy float8 (nullable)

    Dedup key: (ts_event, inst_id)  <-- unique index должен существовать
    """
    return f"""
    WITH ins AS (
      INSERT INTO {CFG.core_table_fq}
        (inst_id, ts_event, ts_ingest, open_interest, open_interest_ccy)
      SELECT
        r.instid::text AS inst_id,
        (to_timestamp(r.ts_event_ms / 1000.0) AT TIME ZONE 'UTC')::timestamptz AS ts_event,
        (to_timestamp(r.ts_ingest_ms / 1000.0) AT TIME ZONE 'UTC')::timestamptz AS ts_ingest,
        r.oi::float8 AS open_interest,
        r.oiccy::float8 AS open_interest_ccy
      FROM {CFG.raw_table_fq} r
      WHERE r.ts_ingest_ms >= {from_ms}
        AND r.ts_ingest_ms <  {to_ms}
      ON CONFLICT (ts_event, inst_id) DO NOTHING
      RETURNING 1
    )
    SELECT count(*)::bigint AS inserted_rows FROM ins;
    """


# ============================================================
# 3) Main callable
# ============================================================

def run_sync() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    dbname = _db_sanity_checks(hook)
    now = _now_utc()

    if CFG.mode not in ("rolling", "backfill"):
        raise ValueError(
            f"CFG.mode must be 'rolling' or 'backfill', got: {CFG.mode}")

    if CFG.mode == "rolling":
        from_dt, to_dt = _window_bounds_rolling(now)
        windows_budget = 10**9
    else:
        from_dt, to_dt = _window_bounds_backfill(hook, now)
        windows_budget = CFG.max_windows_per_run

    # быстрый выход: если в raw нет данных до окна
    raw_max_ms = _get_raw_max_ts_ingest_ms(hook)
    if raw_max_ms is None or raw_max_ms < _ms(from_dt):
        print(
            f"[{DAG_ID}] SKIP: raw empty or older than window "
            f"raw_max_ms={raw_max_ms} window_from_ms={_ms(from_dt)}"
        )
        return

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
        print(
            f"[{DAG_ID}] window [{w_from.isoformat()}..{w_to.isoformat()}) inserted={inserted_rows}")

        t = w_to

    remaining = to_dt - t
    print(
        f"[{DAG_ID}] DONE mode={CFG.mode} windows_done={windows_done} inserted_total={inserted_total} "
        f"stopped_at={t.isoformat()} remaining={remaining}"
    )


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
    description="OKX ETL: windowed raw->core for open interest (rolling/backfill; dedup via ON CONFLICT)",
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
