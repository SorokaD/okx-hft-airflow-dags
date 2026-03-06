from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from okx.common.etl_common import (
    db_sanity_checks,
    day_start_utc,
    get_logical_run_date,
    log_diagnostics,
)


# ============================================================
# 0) Project-wide constants
# ============================================================

CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"

DAG_ID = "okx_mart_build_index_1s"
SCHEDULE = None  # запускается мастер-DAG'ом раз в сутки (t-1)

TAGS = ["okx", "etl", "core-to-mart",
        "timescaledb", "index", "superset", "mart"]


# ============================================================
# 1) Config
# ============================================================

@dataclass(frozen=True)
class EtlConfig:
    core_table_fq: str = "okx_core.fact_index_tick"
    mart_table_fq: str = "okx_mart.agg_index_1s"

    bucket_interval: str = "1 second"

    statement_timeout_ms: int = 30 * 60 * 1000
    execution_timeout_sec: int = 3 * 60 * 60
    retries: int = 1
    retry_delay_sec: int = 120


CFG = EtlConfig()


# ============================================================
# 2) SQL templates
# ============================================================

SQL_ENSURE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS okx_mart;"

# Удаляем по bucket_1s, чтобы чистить ровно целевые бакеты окна
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

def _ensure_target_table(cursor, target_fq: str) -> None:
    cursor.execute(SQL_ENSURE_SCHEMA)

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {target_fq} (
            bucket_1s       timestamptz NOT NULL,
            inst_id         text        NOT NULL,

            open_px         float8 NULL,
            high_px         float8 NULL,
            low_px          float8 NULL,
            close_px        float8 NULL,

            avg_px          float8 NULL,
            tick_count      int4   NULL,

            range_px        float8 NULL,
            range_pct       float8 NULL,
            volatility_px   float8 NULL,

            first_event_ts  timestamptz NULL,
            last_event_ts   timestamptz NULL,
            last_ingest_ts  timestamptz NULL,

            PRIMARY KEY (bucket_1s, inst_id)
        );
        """
    )

    cursor.execute(
        f"""
        CREATE INDEX IF NOT EXISTS ix_{target_fq.replace('.', '_')}_inst_bucket
        ON {target_fq} (inst_id, bucket_1s DESC);
        """
    )

    cursor.execute(
        f"""
        CREATE INDEX IF NOT EXISTS ix_{target_fq.replace('.', '_')}_bucket
        ON {target_fq} (bucket_1s DESC);
        """
    )


def _ensure_comments(cursor, target_fq: str) -> None:
    cursor.execute(
        f"""
        COMMENT ON TABLE {target_fq} IS
        '1-секундная агрегированная mart-таблица по индексной цене инструмента. Используется в Superset для построения базовых дашбордов: цена, OHLC, диапазон движения, волатильность и интенсивность поступления тиков.';
        """
    )

    comments = {
        "bucket_1s": "Начало 1-секундного интервала агрегации по времени события ts_event.",
        "inst_id": "Идентификатор инструмента (например, BTC-USDT-SWAP).",
        "open_px": "Первое значение индексной цены index_px внутри 1-секундного интервала.",
        "high_px": "Максимальное значение индексной цены index_px внутри 1-секундного интервала.",
        "low_px": "Минимальное значение индексной цены index_px внутри 1-секундного интервала.",
        "close_px": "Последнее значение индексной цены index_px внутри 1-секундного интервала.",
        "avg_px": "Среднее значение индексной цены index_px внутри 1-секундного интервала.",
        "tick_count": "Количество тиков index price, попавших в 1-секундный интервал.",
        "range_px": "Абсолютный диапазон движения индексной цены внутри 1-секундного интервала: high_px - low_px.",
        "range_pct": "Относительный диапазон движения индексной цены внутри 1-секундного интервала: (high_px - low_px) / low_px.",
        "volatility_px": "Стандартное отклонение индексной цены index_px внутри 1-секундного интервала.",
        "first_event_ts": "Минимальная временная метка ts_event внутри 1-секундного интервала.",
        "last_event_ts": "Максимальная временная метка ts_event внутри 1-секундного интервала.",
        "last_ingest_ts": "Максимальная временная метка ts_ingest среди записей, попавших в 1-секундный интервал.",
    }

    for col, text in comments.items():
        cursor.execute(f"COMMENT ON COLUMN {target_fq}.{col} IS %s;", (text,))


def _rebuild_window(cursor, target_fq: str, from_ts: datetime, to_ts: datetime) -> None:
    # 1) удаляем целевые бакеты окна
    cursor.execute(
        SQL_DELETE_BUCKETS.format(target=target_fq),
        {"from_ts": from_ts, "to_ts": to_ts},
    )

    # 2) вставляем заново пересчитанные агрегаты
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

    _ensure_target_table(cursor, CFG.mart_table_fq)
    _ensure_comments(cursor, CFG.mart_table_fq)

    log_diagnostics(
        cursor,
        [
            CFG.core_table_fq,
            CFG.mart_table_fq,
        ],
    )

    # окно t-1 сутки: [from_ts, to_ts)
    run_dt = get_logical_run_date()
    to_ts = day_start_utc(run_dt)
    from_ts = to_ts - timedelta(days=1)

    if from_ts.tzinfo is None:
        from_ts = from_ts.replace(tzinfo=timezone.utc)
    if to_ts.tzinfo is None:
        to_ts = to_ts.replace(tzinfo=timezone.utc)

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
    description="OKX MART: daily rebuild 1-second index aggregates for t-1 window, idempotent (delete+insert)",
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
