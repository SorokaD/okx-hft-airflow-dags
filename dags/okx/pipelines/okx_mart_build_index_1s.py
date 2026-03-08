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

DAG_ID = "okx_mart_build_index_1s"
SCHEDULE = None

TAGS = ["okx", "etl", "core-to-mart", "timescaledb", "index", "superset", "mart"]


# ============================================================
# 1) Config
# ============================================================

@dataclass(frozen=True)
class EtlConfig:
    core_table_fq: str = "okx_core.fact_index_tick"
    mart_table_fq: str = "okx_mart.agg_index_1s"

    bucket_interval: str = "1 second"

    # Пересобираем от последнего bucket в mart назад на небольшой lookback,
    # чтобы захватывать late arrivals / пограничные эффекты.
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
SELECT max(bucket_1s) AS max_bucket_1s
FROM {target};
"""

SQL_DELETE_BUCKETS = """
DELETE FROM {target}
WHERE bucket_1s >= %(from_ts)s
  AND bucket_1s <  %(to_ts)s;
"""

SQL_INSERT_AGG = """
INSERT INTO {target} (
    bucket_1s,
    inst_id,
    open_px,
    high_px,
    low_px,
    close_px,
    avg_px,
    tick_count,
    range_px,
    range_pct,
    volatility_px,
    first_event_ts,
    last_event_ts,
    last_ingest_ts
)
SELECT
    time_bucket(%(bucket_interval)s::interval, ts_event) AS bucket_1s,
    inst_id,

    first(index_px, ts_event)                            AS open_px,
    max(index_px)                                        AS high_px,
    min(index_px)                                        AS low_px,
    last(index_px, ts_event)                             AS close_px,

    avg(index_px)                                        AS avg_px,
    count(*)::int4                                       AS tick_count,

    max(index_px) - min(index_px)                        AS range_px,
    CASE
        WHEN min(index_px) <> 0
        THEN (max(index_px) - min(index_px)) / min(index_px)
    END                                                  AS range_pct,

    stddev_pop(index_px)                                 AS volatility_px,

    min(ts_event)                                        AS first_event_ts,
    max(ts_event)                                        AS last_event_ts,
    max(ts_ingest)                                       AS last_ingest_ts

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


def _resolve_rebuild_window(
    cursor,
) -> tuple[datetime | None, datetime | None]:
    source_min_ts, source_max_ts = _get_source_bounds(cursor)

    if source_min_ts is None or source_max_ts is None:
        print(f"[{DAG_ID}] source is empty: {CFG.core_table_fq}")
        return None, None

    if source_min_ts.tzinfo is None:
        source_min_ts = source_min_ts.replace(tzinfo=timezone.utc)
    if source_max_ts.tzinfo is None:
        source_max_ts = source_max_ts.replace(tzinfo=timezone.utc)

    target_max_bucket = _get_target_max_bucket(cursor)

    # Верхняя граница — по фактически доступным данным в source.
    # Делаем exclusive upper bound.
    to_ts = _ceil_to_next_second(source_max_ts)

    if target_max_bucket is None:
        # Первая загрузка: грузим всё, что есть в source.
        from_ts = _floor_to_second(source_min_ts)
        print(
            f"[{DAG_ID}] target is empty -> full bootstrap "
            f"from source bounds [{from_ts} .. {to_ts})"
        )
        return from_ts, to_ts

    if target_max_bucket.tzinfo is None:
        target_max_bucket = target_max_bucket.replace(tzinfo=timezone.utc)

    # Перестраиваем от последнего bucket назад на lookback,
    # чтобы безопасно захватить границу и late arrivals.
    from_ts = target_max_bucket - timedelta(seconds=CFG.rebuild_lookback_sec)
    from_ts = _floor_to_second(from_ts)

    # Не уходим левее первого source-события
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
    description="OKX MART: source-driven incremental catch-up rebuild for 1-second index aggregates",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=TAGS,
) as dag:
    PythonOperator(
        task_id="build_index_1s",
        python_callable=run_build,
    )