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
DAG_ID = "okx__raw_to_core__<entity>"  # <-- заполни
SCHEDULE = "*/1 * * * *"  # <-- заполни (или None для manual)

# Tags (единый набор)
TAGS = ["okx", "etl", "raw-to-core", "timescaledb"]

# SQL basics
SQL_SELECT_1 = "SELECT 1;"
SQL_CURRENT_DB = "SELECT current_database();"


# ============================================================
# 1) Config (всё настраиваемое — только тут)
# ============================================================


@dataclass(frozen=True)
class EtlConfig:
    # tables
    raw_table_fq: str = "okx_raw.<raw_table>"  # schema.table
    core_table_fq: str = "okx_core.<core_table>"  # schema.table

    # watermark
    core_wm_col: str = "ts_ingest"  # timestamptz
    raw_wm_ms_col: str = "ts_ingest_ms"  # bigint epoch-ms

    # dedup within batch (partition by “semantic key”, take freshest by ts_ingest_ms)
    dedup_partition_cols_raw: tuple[str, ...] = (
        "instid",
        "ts_event_ms",
        "side",
        "level",
    )
    dedup_order_col_raw: str = "ts_ingest_ms"

    # batching/limits
    batch_size: int = 300_000
    max_loops: int = 20

    # safety/ops
    execution_timeout_sec: int = 180
    retries: int = 2
    retry_delay_sec: int = 60

    # behavior knobs
    stop_if_inserted_zero: bool = (
        True  # stop loop if inserted_rows==0 (typically duplicates only)
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
    part_cols = ", ".join([f"b.{c}" for c in CFG.dedup_partition_cols_raw])

    # IMPORTANT:
    # - Always return new_last_ms + counters (batch/dedup/inserted).
    # - No ON CONFLICT here by default; dedup is done via row_number().
    return f"""
    WITH batch AS (
      SELECT *
      FROM {CFG.raw_table_fq}
      WHERE {CFG.raw_wm_ms_col} > {last_ms}
      ORDER BY {CFG.raw_wm_ms_col}
      LIMIT {CFG.batch_size}
    ),
    dedup AS (
      SELECT *
      FROM (
        SELECT
          b.*,
          row_number() OVER (
            PARTITION BY {part_cols}
            ORDER BY b.{CFG.dedup_order_col_raw} DESC
          ) AS rn
        FROM batch b
      ) x
      WHERE x.rn = 1
    ),
    ins AS (
      INSERT INTO {CFG.core_table_fq}
        (
          -- TODO: map columns raw -> core
          -- example:
          -- snapshot_id, inst_id, ts_event, side, level_no, price_px, size_qty, ts_ingest
        )
      SELECT
          -- TODO: map values raw -> core
          -- example:
          -- d.snapshot_id,
          -- d.instid::text,
          -- to_timestamp(d.ts_event_ms / 1000.0),
          -- CASE d.side WHEN 1 THEN 'bid' WHEN 2 THEN 'ask' ELSE NULL END,
          -- d.level::int2,
          -- d.price,
          -- d.size,
          -- to_timestamp(d.ts_ingest_ms / 1000.0)
      FROM dedup d
      RETURNING 1
    )
    SELECT
      COALESCE((SELECT max({CFG.raw_wm_ms_col}) FROM batch), {last_ms}) AS new_last_ms,
      (SELECT count(*) FROM batch) AS batch_rows,
      (SELECT count(*) FROM dedup) AS dedup_rows,
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
        f"batch={totals['batch']} dedup={totals['dedup']} inserted={totals['inserted']} last_ms={last_ms}"
    )


# ============================================================
# 3) Main callable (single responsibility: sync)
# ============================================================


def run_sync() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    dbname = _db_sanity_checks(hook)

    now = _now_utc()
    last_ms = _get_core_watermark_ms(hook)

    totals = {"batch": 0, "dedup": 0, "inserted": 0}
    loops = 0

    while loops < CFG.max_loops:
        loops += 1

        sql = _sql_increment_once(last_ms)
        row = hook.get_first(sql)
        if not row:
            raise RuntimeError("ETL query returned no result row")

        new_last_ms = int(row[0])
        batch_rows = int(row[1]) if row[1] is not None else 0
        dedup_rows = int(row[2]) if row[2] is not None else 0
        inserted_rows = int(row[3]) if row[3] is not None else 0

        totals["batch"] += batch_rows
        totals["dedup"] += dedup_rows
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
    description="ETL template: incremental raw->core (batch + dedup-in-batch)",
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
