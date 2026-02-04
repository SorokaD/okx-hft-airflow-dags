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

CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"

DAG_ID = "okx_raw_to_core_orderbook_l10_snapshot"
SCHEDULE = None  # запускается master DAG (t-1)

TAGS = ["okx", "etl", "raw-to-core", "timescaledb", "orderbook", "l10"]


@dataclass(frozen=True)
class EtlConfig:
    raw_table_fq: str = "okx_raw.orderbook_snapshots"
    core_table_fq: str = "okx_core.fact_orderbook_l10_snapshot"

    top_n: int = 10

    # raw encoding
    bid_side_value: int = 1
    ask_side_value: int = 2

    overlap_minutes: int = 2

    batch_size: int = 20
    max_instruments_per_run: int | None = None

    statement_timeout_ms: int = 30 * 60 * 1000

    execution_timeout_sec: int = 2 * 60 * 60
    retries: int = 1
    retry_delay_sec: int = 120


CFG = EtlConfig()


def _get_core_watermark_dt(cursor) -> datetime | None:
    cursor.execute(f"SELECT max(ts_ingest) FROM {CFG.core_table_fq};")
    row = cursor.fetchone()
    return row[0] if row and row[0] is not None else None


def _get_raw_max_ts_ingest_ms(cursor) -> int | None:
    cursor.execute(f"SELECT max(ts_ingest_ms) FROM {CFG.raw_table_fq};")
    row = cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def _get_distinct_instids(cursor, where_sql: str) -> list[str]:
    cursor.execute(
        f"SELECT DISTINCT r.instid FROM {CFG.raw_table_fq} r WHERE {where_sql};")
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
        (to_timestamp(r.ts_event_ms / 1000.0) AT TIME ZONE 'UTC')::timestamptz AS ts_event,
        (to_timestamp(r.ts_ingest_ms / 1000.0) AT TIME ZONE 'UTC')::timestamptz AS ts_ingest,
        (r.ts_ingest_ms - r.ts_event_ms)::int AS latency_ms,
        r.side::int AS side,
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
        inst_id, snapshot_id,
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
      ON C
