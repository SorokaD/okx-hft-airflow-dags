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

DAG_ID = "okx_raw_to_core_orderbook_updates"
SCHEDULE = None  # запускается мастер-DAG'ом раз в сутки (t-1)

TAGS = ["okx", "etl", "raw-to-core", "timescaledb", "orderbook"]

SQL_SELECT_1 = "SELECT 1;"
SQL_CURRENT_DB = "SELECT current_database();"


# ============================================================
# 1) Config
# ============================================================

@dataclass(frozen=True)
class EtlConfig:
    raw_table_fq: str = "okx_raw.orderbook_updates"
    core_table_fq: str = "okx_core.fact_orderbook_update"

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
    sql = f"SELECT max(ts_ingest) FROM {CFG.core_table_fq};"
    cursor.execute(sql)
    row = cursor.fetchone()
    return row[0] if row and row[0] is not None else None


def _get_raw_max_ts_ingest_ms(cursor) -> int | None:
    sql = f"SELECT max(ts_ingest_ms) FROM {CFG.raw_table_fq};"
    cursor.execute(sql)
    row = cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def _get_distinct_instids(cursor, where_sql: str) -> list[str]:
    cursor.execute(
        f"SELECT DISTINCT instid FROM {CFG.raw_table_fq} WHERE {where_sql};"
    )
    return [r[0] for r in cursor.fetchall() if r and r[0] is not None]


def _sql_upsert_bulk(where_sql: str, instids: Sequence[str] | None) -> tuple[str, dict | None]:
    return f"""
    WITH ins AS (
      INSERT INTO {CFG.core_table_fq}
        (inst_id, ts_event, ts_ingest, bids_delta, asks_delta, checksum)
      SELECT
        r.instid::text AS inst_id,
        (to_timestamp(r.ts_event_ms / 1000.0) AT TIME ZONE 'UTC')::timestamptz AS ts_event,
        (to_timestamp(r.ts_ingest_ms / 1000.0) AT TIME ZONE 'UTC')::timestamptz AS ts_ingest,
        r.bids_delta,
        r.asks_delta,
        r.checksum
      FROM {CFG.raw_table_fq} r
      WHERE {where_sql}
        {f"AND r.instid = ANY(%(instids)s)" if instids is not None else ""}
      ON CONFLICT (inst_id, ts_event)
      DO NOTHING
      RETURNING 1
    )
    SELECT count(*)::bigint AS upserted_rows FROM ins;
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

    upserted_total = 0
    if not instids or len(instids) <= CFG.batch_size:
        sql, params = _sql_upsert_bulk(where_sql, None)
        cursor.execute(sql, params)
        row = cursor.fetchone()
        upserted_total = int(row[0]) if row and row[0] is not None else 0
        print(f"[{DAG_ID}] inserted_total={upserted_total}")
        return

    print(f"[{DAG_ID}] batching instid count={len(instids)} batch_size={CFG.batch_size}")
    for batch in batch_iter(instids, CFG.batch_size):
        sql, params = _sql_upsert_bulk(where_sql, batch)
        cursor.execute(sql, params)
        row = cursor.fetchone()
        inserted = int(row[0]) if row and row[0] is not None else 0
        upserted_total += inserted
        print(f"[{DAG_ID}] batch inserted={inserted}")

    print(f"[{DAG_ID}] DONE inserted_total={upserted_total}")


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
    description="OKX ETL: windowed raw->core for orderbook updates (rolling/backfill; upsert by PK)",
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
