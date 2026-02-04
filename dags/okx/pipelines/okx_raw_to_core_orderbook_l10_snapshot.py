from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Sequence

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from okx.common.etl_common import (
    batch_iter,
    db_sanity_checks,
    day_start_utc,
    get_logical_run_date,
    log_diagnostics,
    ms,
)

# ============================================================
# 0) Project-wide constants
# ============================================================

CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"

DAG_ID = "okx_raw_to_core_orderbook_l10_snapshot"
SCHEDULE = None  # запускается master DAG (t-1)

TAGS = ["okx", "etl", "raw-to-core", "timescaledb", "orderbook", "l10"]


# ============================================================
# 1) Config
# ============================================================

@dataclass(frozen=True)
class EtlConfig:
    raw_table_fq: str = "okx_raw.orderbook_snapshots"
    core_table_fq: str = "okx_core.fact_orderbook_l10_snapshot"

    top_n: int = 10

    # raw encoding (подтверждено: 1=bids, 2=asks)
    bid_side_value: int = 1
    ask_side_value: int = 2

    # ingestion window
    overlap_minutes: int = 2

    # batching by instrument
    batch_size: int = 20
    max_instruments_per_run: int | None = None

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

def _get_core_watermark_dt(cursor) -> datetime | None:
    cursor.execute(f"SELECT max(ts_ingest) FROM {CFG.core_table_fq};")
    row = cursor.fetchone()
    return row[0] if row and row[0] is not None else None


def _get_raw_max_ts_ingest_ms(cursor) -> int | None:
    cursor.execute(f"SELECT max(ts_ingest_ms) FROM {CFG.raw_table_fq};")
    row = cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def _get_distinct_instids(cursor, where_sql: str) -> list[str]:
    cursor.execute(f"SELECT DISTINCT r.instid FROM {CFG.raw_table_fq} r WHERE {where_sql};")
    return [r[0] for r in cursor.fetchall() if r and r[0] is not None]


def _sql_upsert_bulk(where_sql: str, instids: Sequence[str] | None) -> tuple[str, dict | None]:
    top_n = int(CFG.top_n)
    bid = int(CFG.bid_side_value)
    ask = int(CFG.ask_side_value)

    return f"""
    WITH base AS (
      SELECT
        r.instid::text AS inst_id,
        r.snapshot_id::text AS snapshot_id,

        r.ts_event_ms::bigint AS ts_event_ms,
        r.ts_ingest_ms::bigint AS ts_ingest_ms,

        -- Важно: to_timestamp() уже timestamptz (UTC), AT TIME ZONE тут не нужен.
        to_timestamp(r.ts_event_ms / 1000.0)::timestamptz AS ts_event,
        to_timestamp(r.ts_ingest_ms / 1000.0)::timestamptz AS ts_ingest,

        (r.ts_ingest_ms - r.ts_event_ms)::int AS latency_ms,

        r.side::int  AS side,
        r.level::int AS level,
        r.price::float8 AS price,
        r.size::float8  AS size
      FROM {CFG.raw_table_fq} r
      WHERE {where_sql}
        {f"AND r.instid = ANY(%(instids)s)" if instids is not None else ""}
        AND r.level BETWEEN 1 AND {top_n}
        AND r.side IN ({bid}, {ask})
    ),
    pivot AS (
      SELECT
        inst_id,
        snapshot_id,

        min(ts_event) AS ts_event,
        min(ts_ingest) AS ts_ingest,
        min(ts_event_ms) AS ts_event_ms,
        min(ts_ingest_ms) AS ts_ingest_ms,
        min(latency_ms) AS latency_ms,

        max(price) FILTER (WHERE side={bid} AND level=1)  AS bid_px_01,
        max(size)  FILTER (WHERE side={bid} AND level=1)  AS bid_sz_01,
        max(price) FILTER (WHERE side={bid} AND level=2)  AS bid_px_02,
        max(size)  FILTER (WHERE side={bid} AND level=2)  AS bid_sz_02,
        max(price) FILTER (WHERE side={bid} AND level=3)  AS bid_px_03,
        max(size)  FILTER (WHERE side={bid} AND level=3)  AS bid_sz_03,
        max(price) FILTER (WHERE side={bid} AND level=4)  AS bid_px_04,
        max(size)  FILTER (WHERE side={bid} AND level=4)  AS bid_sz_04,
        max(price) FILTER (WHERE side={bid} AND level=5)  AS bid_px_05,
        max(size)  FILTER (WHERE side={bid} AND level=5)  AS bid_sz_05,
        max(price) FILTER (WHERE side={bid} AND level=6)  AS bid_px_06,
        max(size)  FILTER (WHERE side={bid} AND level=6)  AS bid_sz_06,
        max(price) FILTER (WHERE side={bid} AND level=7)  AS bid_px_07,
        max(size)  FILTER (WHERE side={bid} AND level=7)  AS bid_sz_07,
        max(price) FILTER (WHERE side={bid} AND level=8)  AS bid_px_08,
        max(size)  FILTER (WHERE side={bid} AND level=8)  AS bid_sz_08,
        max(price) FILTER (WHERE side={bid} AND level=9)  AS bid_px_09,
        max(size)  FILTER (WHERE side={bid} AND level=9)  AS bid_sz_09,
        max(price) FILTER (WHERE side={bid} AND level=10) AS bid_px_10,
        max(size)  FILTER (WHERE side={bid} AND level=10) AS bid_sz_10,

        max(price) FILTER (WHERE side={ask} AND level=1)  AS ask_px_01,
        max(size)  FILTER (WHERE side={ask} AND level=1)  AS ask_sz_01,
        max(price) FILTER (WHERE side={ask} AND level=2)  AS ask_px_02,
        max(size)  FILTER (WHERE side={ask} AND level=2)  AS ask_sz_02,
        max(price) FILTER (WHERE side={ask} AND level=3)  AS ask_px_03,
        max(size)  FILTER (WHERE side={ask} AND level=3)  AS ask_sz_03,
        max(price) FILTER (WHERE side={ask} AND level=4)  AS ask_px_04,
        max(size)  FILTER (WHERE side={ask} AND level=4)  AS ask_sz_04,
        max(price) FILTER (WHERE side={ask} AND level=5)  AS ask_px_05,
        max(size)  FILTER (WHERE side={ask} AND level=5)  AS ask_sz_05,
        max(price) FILTER (WHERE side={ask} AND level=6)  AS ask_px_06,
        max(size)  FILTER (WHERE side={ask} AND level=6)  AS ask_sz_06,
        max(price) FILTER (WHERE side={ask} AND level=7)  AS ask_px_07,
        max(size)  FILTER (WHERE side={ask} AND level=7)  AS ask_sz_07,
        max(price) FILTER (WHERE side={ask} AND level=8)  AS ask_px_08,
        max(size)  FILTER (WHERE side={ask} AND level=8)  AS ask_sz_08,
        max(price) FILTER (WHERE side={ask} AND level=9)  AS ask_px_09,
        max(size)  FILTER (WHERE side={ask} AND level=9)  AS ask_sz_09,
        max(price) FILTER (WHERE side={ask} AND level=10) AS ask_px_10,
        max(size)  FILTER (WHERE side={ask} AND level=10) AS ask_sz_10

      FROM base
      GROUP BY inst_id, snapshot_id
    ),
    enrich AS (
      SELECT
        *,
        ((bid_px_01 + ask_px_01) / 2.0) AS mid_px,
        (ask_px_01 - bid_px_01)        AS spread_px
      FROM pivot
    ),
    ins AS (
      INSERT INTO {CFG.core_table_fq} (
        inst_id, snapshot_id, ts_event, ts_ingest, ts_event_ms, ts_ingest_ms, latency_ms,
        bid_px_01,bid_sz_01,bid_px_02,bid_sz_02,bid_px_03,bid_sz_03,bid_px_04,bid_sz_04,bid_px_05,bid_sz_05,
        bid_px_06,bid_sz_06,bid_px_07,bid_sz_07,bid_px_08,bid_sz_08,bid_px_09,bid_sz_09,bid_px_10,bid_sz_10,
        ask_px_01,ask_sz_01,ask_px_02,ask_sz_02,ask_px_03,ask_sz_03,ask_px_04,ask_sz_04,ask_px_05,ask_sz_05,
        ask_px_06,ask_sz_06,ask_px_07,ask_sz_07,ask_px_08,ask_sz_08,ask_px_09,ask_sz_09,ask_px_10,ask_sz_10,
        mid_px, spread_px
      )
      SELECT
        inst_id, snapshot_id, ts_event, ts_ingest, ts_event_ms, ts_ingest_ms, latency_ms,
        bid_px_01,bid_sz_01,bid_px_02,bid_sz_02,bid_px_03,bid_sz_03,bid_px_04,bid_sz_04,bid_px_05,bid_sz_05,
        bid_px_06,bid_sz_06,bid_px_07,bid_sz_07,bid_px_08,bid_sz_08,bid_px_09,bid_sz_09,bid_px_10,bid_sz_10,
        ask_px_01,ask_sz_01,ask_px_02,ask_sz_02,ask_px_03,ask_sz_03,ask_px_04,ask_sz_04,ask_px_05,ask_sz_05,
        ask_px_06,ask_sz_06,ask_px_07,ask_sz_07,ask_px_08,ask_sz_08,ask_px_09,ask_sz_09,ask_px_10,ask_sz_10,
        mid_px, spread_px
      FROM enrich
      ON CONFLICT (inst_id, snapshot_id)
      DO NOTHING
      RETURNING 1
    )
    SELECT count(*)::bigint AS inserted_rows FROM ins;
    """, ({"instids": list(instids)} if instids is not None else None)


# ============================================================
# 3) Main callable
# ============================================================

def run_sync() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = True
    cursor = conn.cursor()

    db_sanity_checks(cursor, DB_NAME_EXPECTED)
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

    instids = _get_distinct_instids(cursor, where_sql)
    if CFG.max_instruments_per_run is not None:
        instids = instids[: CFG.max_instruments_per_run]

    inserted_total = 0
    if not instids or len(instids) <= CFG.batch_size:
        sql, params = _sql_upsert_bulk(where_sql, None)
        cursor.execute(sql, params)
        row = cursor.fetchone()
        inserted_total = int(row[0]) if row and row[0] is not None else 0
        print(f"[{DAG_ID}] inserted_total={inserted_total}")
        return

    print(f"[{DAG_ID}] batching instid count={len(instids)} batch_size={CFG.batch_size}")
    for batch in batch_iter(instids, CFG.batch_size):
        sql, params = _sql_upsert_bulk(where_sql, batch)
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
    description="OKX ETL: windowed raw snapshots -> core compact L10 snapshot (no JSON)",
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
