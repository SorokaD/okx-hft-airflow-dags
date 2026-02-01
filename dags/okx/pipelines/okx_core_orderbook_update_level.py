from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from okx.common.etl_common import (
    day_start_utc,
    get_logical_run_date,
    log_diagnostics,
)


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

    # ingestion window
    overlap_minutes: int = 2

    # statement timeout (ms)
    statement_timeout_ms: int = 30 * 60 * 1000

    # safety/ops
    execution_timeout_sec: int = 2 * 60 * 60  # 2 часа
    retries: int = 1
    retry_delay_sec: int = 120


CFG = EtlConfig()


# ============================================================
# 2) Helpers
# ============================================================

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

def _get_dst_watermark_dt(cursor) -> datetime | None:
    # критично чтобы на dst был индекс по ts_ingest (иначе max(ts_ingest) будет боль)
    sql = f"SELECT max(ts_ingest) FROM {CFG.dst_table_fq};"
    cursor.execute(sql)
    row = cursor.fetchone()
    return row[0] if row and row[0] is not None else None

def _get_src_max_ts_ingest(cursor) -> datetime | None:
    sql = f"SELECT max(ts_ingest) FROM {CFG.src_table_fq};"
    cursor.execute(sql)
    row = cursor.fetchone()
    return row[0] if row and row[0] is not None else None

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

    conn = hook.get_conn()
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("SET statement_timeout = %s", (CFG.statement_timeout_ms,))
    log_diagnostics(cursor, [CFG.src_table_fq, CFG.dst_table_fq])

    run_dt = get_logical_run_date()
    to_dt = day_start_utc(run_dt)

    wm = _get_dst_watermark_dt(cursor)
    from_dt = wm - timedelta(minutes=CFG.overlap_minutes) if wm else None

    src_max = _get_src_max_ts_ingest(cursor)
    if src_max is None or (from_dt is not None and src_max < from_dt):
        print(
            f"[{DAG_ID}] SKIP: src empty or older than window "
            f"src_max={src_max} window_from={from_dt}"
        )
        return

    if from_dt is None:
        # при первом запуске берем все до to_dt
        from_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)

    print(f"[{DAG_ID}] db={dbname} window=[{from_dt}..{to_dt}) top_n={CFG.top_n}")
    sql = _sql_upsert_levels_window(from_dt, to_dt)
    row = hook.get_first(sql)
    upserted = int(row[0]) if row and row[0] is not None else 0
    print(f"[{DAG_ID}] DONE upserted_total={upserted}")


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
