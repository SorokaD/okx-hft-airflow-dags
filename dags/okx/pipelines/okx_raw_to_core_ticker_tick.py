from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ============================================================
# 0) Project-wide constants (единый стандарт для всех DAG)
# ============================================================

CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"  # самопроверка, чтобы не залить "не туда"

# DAG identity
DAG_ID = "okx__raw_to_core__ticker_tick"
SCHEDULE = "*/1 * * * *"

# Tags (единый набор)
TAGS = ["okx", "etl", "raw-to-core", "timescaledb", "tickers"]

# SQL basics
SQL_SELECT_1 = "SELECT 1;"
SQL_CURRENT_DB = "SELECT current_database();"


# ============================================================
# 1) Config (всё настраиваемое — только тут)
# ============================================================


@dataclass(frozen=True)
class EtlConfig:
    # tables
    raw_table_fq: str = "okx_raw.tickers"  # schema.table
    core_table_fq: str = "okx_core.fact_ticker_tick"  # schema.table

    # watermark
    core_wm_col: str = "ts_ingest"  # timestamptz
    raw_wm_ms_col: str = "ts_ingest_ms"  # bigint epoch-ms

    # batching/limits
    batch_size: int = 300_000
    max_loops: int = 20

    # safety/ops
    execution_timeout_sec: int = 180
    retries: int = 2
    retry_delay_sec: int = 60

    # behavior knobs
    stop_if_inserted_zero: bool = (
        False  # для ON CONFLICT DO NOTHING лучше НЕ стопать на inserted=0
    )


CFG = EtlConfig()


# ============================================================
# 2) Helpers
# ============================================================


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _ms_from_timestamptz(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _db_sanity_checks(hook: PostgresHook) -> str:
    # ping
    v = hook.get_first(SQL_SELECT_1)
    if not v or v[0] != 1:
        raise RuntimeError(f"DB ping failed: {v}")

    # database name sanity
    row = hook.get_first(SQL_CURRENT_DB)
    dbname = row[0] if row else None
    if DB_NAME_EXPECTED and dbname != DB_NAME_EXPECTED:
        raise RuntimeError(
            f"Connected to unexpected database: {dbname} (expected {DB_NAME_EXPECTED})"
        )

    return dbname or "UNKNOWN"


def _get_core_watermark_ms(hook: PostgresHook) -> int:
    sql = f"SELECT max({CFG.core_wm_col}) FROM {CFG.core_table_fq};"
    row = hook.get_first(sql)
    max_ts = row[0] if row else None
    return 0 if max_ts is None else _ms_from_timestamptz(max_ts)


def _sql_increment_once(last_ms: int) -> str:
    # IMPORTANT:
    # - Use raw ts_ingest_ms watermark for incremental batching.
    # - Rely on core PK (inst_id, ts_event) to deduplicate via ON CONFLICT.
    # - Return new_last_ms + counters (batch/inserted).
    return f"""
    WITH batch AS (
      SELECT *
      FROM {CFG.raw_table_fq}
      WHERE {CFG.raw_wm_ms_col} > {last_ms}
      ORDER BY {CFG.raw_wm_ms_col}
      LIMIT {CFG.batch_size}
    ),
    ins AS (
      INSERT INTO {CFG.core_table_fq}
        (
          inst_id,
          ts_event,
          ts_ingest,
          last_px,
          bid_px,
          bid_sz,
          ask_px,
          ask_sz,
          open_24h,
          high_24h,
          low_24h,
          vol_24h,
          vol_ccy_24h
        )
      SELECT
          b.instid::text AS inst_id,
          (to_timestamp(b.ts_event_ms / 1000.0) AT TIME ZONE 'UTC')::timestamptz AS ts_event,
          (to_timestamp(b.ts_ingest_ms / 1000.0) AT TIME ZONE 'UTC')::timestamptz AS ts_ingest,
          b.last   AS last_px,
          b.bidpx  AS bid_px,
          b.bidsz  AS bid_sz,
          b.askpx  AS ask_px,
          b.asksz  AS ask_sz,
          b.open24h    AS open_24h,
          b.high24h    AS high_24h,
          b.low24h     AS low_24h,
          b.vol24h     AS vol_24h,
          b.volccy24h  AS vol_ccy_24h
      FROM batch b
      ON CONFLICT (inst_id, ts_event) DO NOTHING
      RETURNING 1
    )
    SELECT
      COALESCE((SELECT max({CFG.raw_wm_ms_col}) FROM batch), {last_ms}) AS new_last_ms,
      (SELECT count(*) FROM batch) AS batch_rows,
      (SELECT count(*) FROM ins) AS inserted_rows;
    """


def _log_run(
    *,
    dag_id: str,
    now: datetime,
    dbname: str,
    loops: int,
    last_ms: int,
    totals: dict[str, int],
) -> None:
    print(
        f"[{dag_id}] now_utc={now.isoformat()} db={dbname} loops={loops} "
        f"batch={totals['batch']} inserted={totals['inserted']} last_ms={last_ms}"
    )


# ============================================================
# 3) Main callable (single responsibility: sync)
# ============================================================


def run_sync() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    dbname = _db_sanity_checks(hook)

    now = _now_utc()
    last_ms = _get_core_watermark_ms(hook)

    totals = {"batch": 0, "inserted": 0}
    loops = 0

    while loops < CFG.max_loops:
        loops += 1

        sql = _sql_increment_once(last_ms)
        row = hook.get_first(sql)
        if not row:
            raise RuntimeError("ETL query returned no result row")

        new_last_ms = int(row[0])
        batch_rows = int(row[1]) if row[1] is not None else 0
        inserted_rows = int(row[2]) if row[2] is not None else 0

        totals["batch"] += batch_rows
        totals["inserted"] += inserted_rows

        last_ms = new_last_ms

        # stop conditions
        if batch_rows == 0:
            break
        if CFG.stop_if_inserted_zero and inserted_rows == 0:
            break

    _log_run(
        dag_id=DAG_ID,
        now=now,
        dbname=dbname,
        loops=loops,
        last_ms=last_ms,
        totals=totals,
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
    description="OKX ETL: incremental raw->core for tickers (batch, PK-dedup via ON CONFLICT)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,  # never overlap same DAG runs
    tags=TAGS,
) as dag:
    PythonOperator(
        task_id="sync",
        python_callable=run_sync,
    )
