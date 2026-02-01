from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ============================================================
# 0) Project-wide constants
# ============================================================

CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"

DAG_ID = "okx_core_orderbook_update_level"
SCHEDULE = None  # запускается мастер-DAG'ом раз в сутки (t-1)

TAGS = ["okx", "etl", "core", "timescaledb", "orderbook"]

SQL_SELECT_1 = "SELECT 1;"
SQL_CURRENT_DB = "SELECT current_database();" 

 
# ============================================================
# 1) Config
# ============================================================

@dataclass(frozen=True)
class EtlConfig:
    # sources/targets
    src_table_fq: str = "okx_core.fact_orderbook_update"
    dst_table_fq: str = "okx_core.fact_orderbook_update_level"

    # explode params
    top_n: int = 20

    # --- MODE SWITCH ---
    mode: str = "backfill"  # "rolling" | "backfill"

    # rolling window
    window_hours: int = 6

    # backfill controls
    max_windows_per_run: int = 144  # 144*10min = 24 часов данных за 1 запуск

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

def _day_start_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

def _floor_to_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)

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

def _get_dst_watermark_dt(hook: PostgresHook) -> datetime | None:
    # критично чтобы на dst был индекс по ts_ingest (иначе max(ts_ingest) будет боль)
    sql = f"SELECT max(ts_ingest) FROM {CFG.dst_table_fq};"
    row = hook.get_first(sql)
    return row[0] if row and row[0] is not None else None

def _window_bounds_rolling(now: datetime) -> Tuple[datetime, datetime]:
    to_dt = _day_start_utc(now)
    from_dt = to_dt - timedelta(hours=CFG.window_hours) - timedelta(minutes=CFG.overlap_minutes)
    return from_dt, to_dt

def _window_bounds_backfill(hook: PostgresHook, now: datetime) -> Tuple[datetime, datetime]:
    to_dt = _day_start_utc(now)

    wm = _get_dst_watermark_dt(hook)
    if wm is None:
        from_dt = to_dt - timedelta(days=1)
    else:
        from_dt = wm - timedelta(minutes=CFG.overlap_minutes)

    return _floor_to_minute(from_dt), to_dt

def _sql_upsert_levels_window(from_dt: datetime, to_dt: datetime) -> str:
    # NB: тут тяжелое место — jsonb_array_elements + row_number.
    # Мы режем по времени ts_ingest, чтобы запрос был ограниченный и предсказуемый.
    return f"""
    WITH params AS (
      SELECT
        '{from_dt.isoformat()}'::timestamptz AS v_from,
        '{to_dt.isoformat()}'::timestamptz AS v_to
    ),
    batch AS (
      SELECT
        inst_id, ts_event, ts_ingest, bids_delta, asks_delta, checksum
      FROM {CFG.src_table_fq}, params
      WHERE ts_ingest >= params.v_from
        AND ts_ingest <  params.v_to
        AND (bids_delta IS NOT NULL OR asks_delta IS NOT NULL)
    ),
    lvl AS (
      SELECT
        b.inst_id, b.ts_event, b.ts_ingest,
        z.side,
        (z.elem->>'price')::float8 AS price_px,
        (z.elem->>'size')::float8  AS size_qty,
        b.checksum,
        row_number() OVER (
          PARTITION BY b.inst_id, b.ts_event, z.side
          ORDER BY
            CASE WHEN z.side='bid' THEN (z.elem->>'price')::float8 END DESC,
            CASE WHEN z.side='ask' THEN (z.elem->>'price')::float8 END ASC
        ) AS rn
      FROM batch b
      CROSS JOIN LATERAL (
        SELECT 'bid'::text AS side, e AS elem
        FROM jsonb_array_elements(COALESCE(b.bids_delta, '[]'::jsonb)) e
        UNION ALL
        SELECT 'ask'::text AS side, e AS elem
        FROM jsonb_array_elements(COALESCE(b.asks_delta, '[]'::jsonb)) e
      ) z
    ),
    ins AS (
      INSERT INTO {CFG.dst_table_fq}
        (inst_id, ts_event, ts_ingest, side, price_px, size_qty, checksum)
      SELECT inst_id, ts_event, ts_ingest, side, price_px, size_qty, checksum
      FROM lvl
      WHERE rn <= {int(CFG.top_n)}
      ON CONFLICT (inst_id, ts_event, side, price_px)
      DO UPDATE SET
        size_qty = EXCLUDED.size_qty,
        ts_ingest = EXCLUDED.ts_ingest,
        checksum  = EXCLUDED.checksum
      RETURNING 1
    )
    SELECT count(*)::bigint AS upserted_rows FROM ins;
    """


# ============================================================
# 3) Main callable
# ============================================================

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

    upserted_total = 0
    windows_done = 0

    print(f"[{DAG_ID}] mode={CFG.mode} db={dbname} window=[{from_dt}..{to_dt}) step_min={CFG.step_minutes} top_n={CFG.top_n}")

    while t < to_dt and windows_done < windows_budget:
        w_from = t
        w_to = min(t + step, to_dt)

        sql = _sql_upsert_levels_window(w_from, w_to)
        row = hook.get_first(sql)
        upserted = int(row[0]) if row and row[0] is not None else 0

        upserted_total += upserted
        windows_done += 1

        print(f"[{DAG_ID}] window [{w_from.isoformat()}..{w_to.isoformat()}) upserted={upserted}")

        t = w_to

    remaining = to_dt - t
    print(
        f"[{DAG_ID}] DONE mode={CFG.mode} windows_done={windows_done} upserted_total={upserted_total} "
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
    description="OKX ETL: windowed core->core for orderbook levels (rolling/backfill; top-N)",
    default_args=default_args,
    start_date=datetime(2026, 1, 2, tzinfo=timezone.utc),
    schedule=SCHEDULE,
    catchup=False,      # важное отличие: мы сами делаем backfill режимом
    max_active_runs=1,
    tags=TAGS,
) as dag:
    PythonOperator(
        task_id="sync",
        python_callable=run_sync,
    )
