from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from okx.common.etl_common import (
    db_sanity_checks,
    log_diagnostics,
)


# ============================================================
# 0) Project-wide constants
# ============================================================

CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"

DAG_ID = "okx_mart_build_ticker_1s"
SCHEDULE = None

TAGS = ["okx", "etl", "core-to-mart", "timescaledb", "tickers", "superset", "mart"]


# ============================================================
# 1) Config
# ============================================================

@dataclass(frozen=True)
class EtlConfig:
    core_table_fq: str = "okx_core.fact_ticker_tick"
    mart_table_fq: str = "okx_mart.agg_ticker_1s"

    bucket_interval: str = "1 second"

    # Небольшой lookback для безопасного пересчета последних бакетов,
    # чтобы не терять late arrivals и пограничные записи.
    rebuild_lookback_sec: int = 1

    statement_timeout_ms: int = 30 * 60 * 1000
    execution_timeout_sec: int = 3 * 60 * 60
    retries: int = 1
    retry_delay_sec: int = 120


CFG = EtlConfig()


# ============================================================
# 2) SQL templates
# ============================================================

SQL_GET_SOURCE_BOUNDS = """
SELECT
    min(ts_event) AS min_ts_event,
    max(ts_event) AS max_ts_event
FROM {core};
"""

SQL_GET_TARGET_MAX_BUCKET = """
SELECT max(ts_bucket) AS max_ts_bucket
FROM {target};
"""

SQL_DELETE_BUCKETS = """
DELETE FROM {target}
WHERE ts_bucket >= %(from_ts)s
  AND ts_bucket <  %(to_ts)s;
"""

SQL_INSERT_AGG = """
INSERT INTO {target} (
    inst_id,
    ts_bucket,
    ticks_cnt,

    last_px_min,
    last_px_max,
    last_px_avg,

    bid_px_min,
    bid_px_max,
    bid_px_avg,

    ask_px_min,
    ask_px_max,
    ask_px_avg,

    bid_sz_min,
    bid_sz_max,
    bid_sz_avg,

    ask_sz_min,
    ask_sz_max,
    ask_sz_avg,

    open_24h_min,
    open_24h_max,
    open_24h_avg,

    high_24h_min,
    high_24h_max,
    high_24h_avg,

    low_24h_min,
    low_24h_max,
    low_24h_avg,

    vol_24h_min,
    vol_24h_max,
    vol_24h_avg,

    vol_ccy_24h_min,
    vol_ccy_24h_max,
    vol_ccy_24h_avg
)
SELECT
    inst_id,
    time_bucket(%(bucket_interval)s::interval, ts_event) AS ts_bucket,
    count(*)::int4 AS ticks_cnt,

    min(last_px) AS last_px_min,
    max(last_px) AS last_px_max,
    avg(last_px) AS last_px_avg,

    min(bid_px) AS bid_px_min,
    max(bid_px) AS bid_px_max,
    avg(bid_px) AS bid_px_avg,

    min(ask_px) AS ask_px_min,
    max(ask_px) AS ask_px_max,
    avg(ask_px) AS ask_px_avg,

    min(bid_sz) AS bid_sz_min,
    max(bid_sz) AS bid_sz_max,
    avg(bid_sz) AS bid_sz_avg,

    min(ask_sz) AS ask_sz_min,
    max(ask_sz) AS ask_sz_max,
    avg(ask_sz) AS ask_sz_avg,

    min(open_24h) AS open_24h_min,
    max(open_24h) AS open_24h_max,
    avg(open_24h) AS open_24h_avg,

    min(high_24h) AS high_24h_min,
    max(high_24h) AS high_24h_max,
    avg(high_24h) AS high_24h_avg,

    min(low_24h) AS low_24h_min,
    max(low_24h) AS low_24h_max,
    avg(low_24h) AS low_24h_avg,

    min(vol_24h) AS vol_24h_min,
    max(vol_24h) AS vol_24h_max,
    avg(vol_24h) AS vol_24h_avg,

    min(vol_ccy_24h) AS vol_ccy_24h_min,
    max(vol_ccy_24h) AS vol_ccy_24h_max,
    avg(vol_ccy_24h) AS vol_ccy_24h_avg

FROM {core}
WHERE ts_event >= %(from_ts)s
  AND ts_event <  %(to_ts)s
GROUP BY
    inst_id,
    time_bucket(%(bucket_interval)s::interval, ts_event);
"""


# ============================================================
# 3) Helpers
# ============================================================

def _floor_to_second(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.replace(microsecond=0)


def _ceil_to_next_second(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    if ts.microsecond == 0:
        return ts + timedelta(seconds=1)

    return ts.replace(microsecond=0) + timedelta(seconds=1)


def _get_source_bounds(cursor) -> tuple[datetime | None, datetime | None]:
    cursor.execute(SQL_GET_SOURCE_BOUNDS.format(core=CFG.core_table_fq))
    row = cursor.fetchone()
    return row[0], row[1]


def _get_target_max_bucket(cursor) -> datetime | None:
    cursor.execute(SQL_GET_TARGET_MAX_BUCKET.format(target=CFG.mart_table_fq))
    row = cursor.fetchone()
    return row[0]


def _resolve_rebuild_window(cursor) -> tuple[datetime | None, datetime | None]:
    source_min_ts, source_max_ts = _get_source_bounds(cursor)

    if source_min_ts is None or source_max_ts is None:
        print(f"[{DAG_ID}] source is empty: {CFG.core_table_fq}")
        return None, None

    if source_min_ts.tzinfo is None:
        source_min_ts = source_min_ts.replace(tzinfo=timezone.utc)
    if source_max_ts.tzinfo is None:
        source_max_ts = source_max_ts.replace(tzinfo=timezone.utc)

    target_max_bucket = _get_target_max_bucket(cursor)

    # Верхняя граница окна — по фактически доступным данным в source.
    # Exclusive upper bound.
    to_ts = _ceil_to_next_second(source_max_ts)

    if target_max_bucket is None:
        from_ts = _floor_to_second(source_min_ts)
        print(
            f"[{DAG_ID}] target is empty -> full bootstrap "
            f"from source bounds [{from_ts} .. {to_ts})"
        )
        return from_ts, to_ts

    if target_max_bucket.tzinfo is None:
        target_max_bucket = target_max_bucket.replace(tzinfo=timezone.utc)

    from_ts = target_max_bucket - timedelta(seconds=CFG.rebuild_lookback_sec)
    from_ts = _floor_to_second(from_ts)

    source_floor = _floor_to_second(source_min_ts)
    if from_ts < source_floor:
        from_ts = source_floor

    if from_ts >= to_ts:
        print(
            f"[{DAG_ID}] nothing to do: "
            f"resolved window [{from_ts} .. {to_ts}) is empty"
        )
        return None, None

    print(
        f"[{DAG_ID}] incremental catch-up window resolved: "
        f"target_max_bucket={target_max_bucket}, source_max_ts={source_max_ts}, "
        f"window=[{from_ts} .. {to_ts})"
    )
    return from_ts, to_ts


def _rebuild_window(cursor, target_fq: str, from_ts: datetime, to_ts: datetime) -> None:
    cursor.execute(
        SQL_DELETE_BUCKETS.format(target=target_fq),
        {"from_ts": from_ts, "to_ts": to_ts},
    )

    cursor.execute(
        SQL_INSERT_AGG.format(target=target_fq, core=CFG.core_table_fq),
        {
            "from_ts": from_ts,
            "to_ts": to_ts,
            "bucket_interval": CFG.bucket_interval,
        },
    )


# ============================================================
# 4) Main callable
# ============================================================

def run_build() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = True
    cursor = conn.cursor()

    db_sanity_checks(cursor, DB_NAME_EXPECTED)
    cursor.execute("SET statement_timeout = %s", (CFG.statement_timeout_ms,))

    log_diagnostics(
        cursor,
        [
            CFG.core_table_fq,
            CFG.mart_table_fq,
        ],
    )

    from_ts, to_ts = _resolve_rebuild_window(cursor)

    if from_ts is None or to_ts is None:
        print(f"[{DAG_ID}] SKIP: nothing to rebuild")
        return

    print(f"[{DAG_ID}] rebuild window: [{from_ts} .. {to_ts})")

    _rebuild_window(cursor, CFG.mart_table_fq, from_ts, to_ts)

    print(f"[{DAG_ID}] OK: {CFG.mart_table_fq}")
    print(f"[{DAG_ID}] DONE")


# ============================================================
# 5) DAG definition
# ============================================================

default_args: dict[str, Any] = {
    "owner": "okx-data",
    "retries": CFG.retries,
    "retry_delay": timedelta(seconds=CFG.retry_delay_sec),
    "execution_timeout": timedelta(seconds=CFG.execution_timeout_sec),
}

with DAG(
    dag_id=DAG_ID,
    description="OKX MART: source-driven incremental catch-up rebuild for 1-second ticker aggregates",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=TAGS,
) as dag:
    PythonOperator(
        task_id="build_ticker_1s",
        python_callable=run_build,
    )