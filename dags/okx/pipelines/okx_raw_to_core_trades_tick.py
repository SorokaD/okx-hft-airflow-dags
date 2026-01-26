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

DAG_ID = "okx_raw_to_core_trades_tick"
SCHEDULE = "45 0,6,12,18 * * *"  # 00:00, 06:00, 12:00, 18:00 UTC

TAGS = ["okx", "etl", "raw-to-core", "timescaledb", "trades"]

SQL_SELECT_1 = "SELECT 1;"
SQL_CURRENT_DB = "SELECT current_database();"


# ============================================================
# 1) Config (всё настраиваемое — только тут)
# ============================================================

@dataclass(frozen=True)
class EtlConfig:
    raw_table_fq: str = "okx_raw.trades"
    core_table_fq: str = "okx_core.fact_trades_tick"

    # --- MODE SWITCH ---
    # "rolling"  -> грузим последние window_hours (поддержка)
    # "backfill" -> догоняем от watermark в core до now (но ограниченно)
    mode: str = "backfill"  # <<< ВОТ ТУТ ПЕРЕКЛЮЧАЕШЬ

    # rolling window
    window_hours: int = 6

    # backfill controls
    max_windows_per_run: int = 144  # 144*10min = 24 часа данных за 1 запуск

    # batching by time
    step_minutes: int = 10
    overlap_minutes: int = 2

    # safety/ops
    execution_timeout_sec: int = 2 * 60 * 60  # 2 часа
    retries: int = 1
    retry_delay_sec: int = 120


CFG = EtlConfig()


# ============================================================
# 2) Helpers
# ============================================================

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
        raise RuntimeError(
            f"Connected to unexpected database: {dbname} (expected {DB_NAME_EXPECTED})"
        )
    return dbname or "UNKNOWN"


def _get_core_watermark_dt(hook: PostgresHook) -> datetime | None:
    sql = f"SELECT max(ts_ingest) FROM {CFG.core_table_fq};"
    row = hook.get_first(sql)
    return row[0] if row and row[0] is not None else None


def _window_bounds_rolling(now: datetime) -> Tuple[datetime, datetime]:
    to_dt = _floor_to_minute(now)
    from_dt = to_dt - timedelta(hours=CFG.window_hours) - \
        timedelta(minutes=CFG.overlap_minutes)
    return from_dt, to_dt


def _window_bounds_backfill(hook: PostgresHook, now: datetime) -> Tuple[datetime, datetime]:
    to_dt = _floor_to_minute(now)

    wm = _get_core_watermark_dt(hook)
    if wm is None:
        from_dt = to_dt - timedelta(hours=CFG.window_hours)
    else:
        from_dt = wm - timedelta(minutes=CFG.overlap_minutes)

    from_dt = _floor_to_minute(from_dt)
    return from_dt, to_dt


def _sql_insert_window(from_ms: int, to_ms: int) -> str:
    # dedup по твоему unique index: (inst_id, ts_event, trade_id)
    return f"""
    WITH ins AS (
      INSERT INTO {CFG.core_table_fq}
        (
          inst_id,
          ts_event,
          ts_ingest,
          trade_id,
          trade_px,
          trade_sz,
          side
        )
      SELECT
          t.instid::text AS inst_id,
          (to_timestamp(t.ts_event_ms / 1000.0) AT TIME ZONE 'UTC')::timestamptz AS ts_event,
          (to_timestamp(t.ts_ingest_ms / 1000.0) AT TIME ZONE 'UTC')::timestamptz AS ts_ingest,
          t.tradeid::text AS trade_id,
          t.px::float8 AS trade_px,
          t.sz::float8 AS trade_sz,
          t.side::text AS side
      FROM {CFG.raw_table_fq} t
      WHERE t.ts_ingest_ms >= {from_ms}
        AND t.ts_ingest_ms <  {to_ms}
      ON CONFLICT (inst_id, ts_event, trade_id) DO NOTHING
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
        windows_budget = 10**9  # без ограничения
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

        sql = _sql_insert_window(_ms(w_from), _ms(w_to))
        row = hook.get_first(sql)
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
    description="OKX ETL: windowed raw->core for trades (rolling/backfill; dedup via unique index ON CONFLICT)",
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
