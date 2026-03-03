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
# 0) Project-wide constants (единый стандарт для всех DAG)
# ============================================================

CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"

DAG_ID = "okx_mart_build_ticker_aggs"
SCHEDULE = None  # запускается мастер-DAG'ом раз в сутки (t-1)

TAGS = ["okx", "etl", "core-to-mart", "timescaledb", "tickers", "superset", "mart"]


# ============================================================
# 1) Config
# ============================================================


@dataclass(frozen=True)
class EtlConfig:
    core_table_fq: str = "okx_core.fact_ticker_tick"

    mart_table_1m_fq: str = "okx_mart.agg_ticker_1m"
    mart_table_1h_fq: str = "okx_mart.agg_ticker_1h"
    mart_table_1d_fq: str = "okx_mart.agg_ticker_1d"

    statement_timeout_ms: int = 30 * 60 * 1000
    execution_timeout_sec: int = 3 * 60 * 60
    retries: int = 1
    retry_delay_sec: int = 120


CFG = EtlConfig()


# ============================================================
# 2) SQL templates
# ============================================================

SQL_ENSURE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS okx_mart;"

# Важно: удаляем по ts_bucket (а не по ts_event), чтобы чистить ровно бакеты окна.
SQL_DELETE_BUCKETS = """
DELETE FROM {target}
WHERE ts_bucket >= %(from_ts)s
  AND ts_bucket <  %(to_ts)s;
"""

# Агрегация по бакету (1m/1h/1d) внутри окна по ts_event
SQL_INSERT_AGG = """
INSERT INTO {target} (
    inst_id,
    ts_bucket,
    ticks_cnt,
    last_px_min, last_px_max, last_px_avg,
    bid_px_min,  bid_px_max,  bid_px_avg,
    ask_px_min,  ask_px_max,  ask_px_avg,
    bid_sz_min,  bid_sz_max,  bid_sz_avg,
    ask_sz_min,  ask_sz_max,  ask_sz_avg,
    open_24h_min, open_24h_max, open_24h_avg,
    high_24h_min, high_24h_max, high_24h_avg,
    low_24h_min,  low_24h_max,  low_24h_avg,
    vol_24h_min,  vol_24h_max,  vol_24h_avg,
    vol_ccy_24h_min, vol_ccy_24h_max, vol_ccy_24h_avg
)
SELECT
    inst_id,
    time_bucket(%(bucket_interval)s::interval, ts_event) AS ts_bucket,
    COUNT(*)::int4 AS ticks_cnt,

    MIN(last_px) AS last_px_min,
    MAX(last_px) AS last_px_max,
    AVG(last_px) AS last_px_avg,

    MIN(bid_px) AS bid_px_min,
    MAX(bid_px) AS bid_px_max,
    AVG(bid_px) AS bid_px_avg,

    MIN(ask_px) AS ask_px_min,
    MAX(ask_px) AS ask_px_max,
    AVG(ask_px) AS ask_px_avg,

    MIN(bid_sz) AS bid_sz_min,
    MAX(bid_sz) AS bid_sz_max,
    AVG(bid_sz) AS bid_sz_avg,

    MIN(ask_sz) AS ask_sz_min,
    MAX(ask_sz) AS ask_sz_max,
    AVG(ask_sz) AS ask_sz_avg,

    MIN(open_24h) AS open_24h_min,
    MAX(open_24h) AS open_24h_max,
    AVG(open_24h) AS open_24h_avg,

    MIN(high_24h) AS high_24h_min,
    MAX(high_24h) AS high_24h_max,
    AVG(high_24h) AS high_24h_avg,

    MIN(low_24h) AS low_24h_min,
    MAX(low_24h) AS low_24h_max,
    AVG(low_24h) AS low_24h_avg,

    MIN(vol_24h) AS vol_24h_min,
    MAX(vol_24h) AS vol_24h_max,
    AVG(vol_24h) AS vol_24h_avg,

    MIN(vol_ccy_24h) AS vol_ccy_24h_min,
    MAX(vol_ccy_24h) AS vol_ccy_24h_max,
    AVG(vol_ccy_24h) AS vol_ccy_24h_avg

FROM {core}
WHERE ts_event >= %(from_ts)s
  AND ts_event <  %(to_ts)s
GROUP BY inst_id, time_bucket(%(bucket_interval)s::interval, ts_event);
"""


# ============================================================
# 3) Helpers
# ============================================================


def _ensure_target_table(cursor, target_fq: str) -> None:
    """
    Минимально-необходимая страховка: если таблица отсутствует — создаём.
    (Если у тебя уже создано руками — просто выполнится быстро и ничего не сломает.)
    """
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {target_fq} (
            inst_id text NOT NULL,
            ts_bucket timestamptz NOT NULL,
            ticks_cnt int4 NOT NULL,

            last_px_min float8 NULL,
            last_px_max float8 NULL,
            last_px_avg float8 NULL,

            bid_px_min float8 NULL,
            bid_px_max float8 NULL,
            bid_px_avg float8 NULL,

            ask_px_min float8 NULL,
            ask_px_max float8 NULL,
            ask_px_avg float8 NULL,

            bid_sz_min float8 NULL,
            bid_sz_max float8 NULL,
            bid_sz_avg float8 NULL,

            ask_sz_min float8 NULL,
            ask_sz_max float8 NULL,
            ask_sz_avg float8 NULL,

            open_24h_min float8 NULL,
            open_24h_max float8 NULL,
            open_24h_avg float8 NULL,

            high_24h_min float8 NULL,
            high_24h_max float8 NULL,
            high_24h_avg float8 NULL,

            low_24h_min float8 NULL,
            low_24h_max float8 NULL,
            low_24h_avg float8 NULL,

            vol_24h_min float8 NULL,
            vol_24h_max float8 NULL,
            vol_24h_avg float8 NULL,

            vol_ccy_24h_min float8 NULL,
            vol_ccy_24h_max float8 NULL,
            vol_ccy_24h_avg float8 NULL,

            PRIMARY KEY (inst_id, ts_bucket)
        );
        """
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS ix_{target_fq.replace('.', '_')}_ts ON {target_fq} (ts_bucket DESC);"
    )


def _rebuild_window(
    cursor, target_fq: str, bucket_interval: str, from_ts: datetime, to_ts: datetime
) -> None:
    # 1) delete window (idempotency)
    cursor.execute(
        SQL_DELETE_BUCKETS.format(target=target_fq),
        {"from_ts": from_ts, "to_ts": to_ts},
    )

    # 2) insert recomputed aggregates for window
    params = {"from_ts": from_ts, "to_ts": to_ts, "bucket_interval": bucket_interval}
    cursor.execute(
        SQL_INSERT_AGG.format(target=target_fq, core=CFG.core_table_fq),
        params,
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
            CFG.mart_table_1m_fq,
            CFG.mart_table_1h_fq,
            CFG.mart_table_1d_fq,
        ],
    )

    # окно t-1 сутки: [from_ts, to_ts)
    run_dt = get_logical_run_date()
    to_ts = day_start_utc(run_dt)
    from_ts = to_ts - timedelta(days=1)

    # фикс TZ на всякий случай
    if from_ts.tzinfo is None:
        from_ts = from_ts.replace(tzinfo=timezone.utc)
    if to_ts.tzinfo is None:
        to_ts = to_ts.replace(tzinfo=timezone.utc)

    cursor.execute(SQL_ENSURE_SCHEMA)

    # ensure tables exist + PK for safety (inst_id, ts_bucket)
    _ensure_target_table(cursor, CFG.mart_table_1m_fq)
    _ensure_target_table(cursor, CFG.mart_table_1h_fq)
    _ensure_target_table(cursor, CFG.mart_table_1d_fq)

    print(f"[{DAG_ID}] rebuild window: [{from_ts} .. {to_ts})")

    # rebuild in order: minute -> hour -> day (не критично, но логично)
    _rebuild_window(cursor, CFG.mart_table_1m_fq, "1 minute", from_ts, to_ts)
    print(f"[{DAG_ID}] OK: {CFG.mart_table_1m_fq}")

    _rebuild_window(cursor, CFG.mart_table_1h_fq, "1 hour", from_ts, to_ts)
    print(f"[{DAG_ID}] OK: {CFG.mart_table_1h_fq}")

    _rebuild_window(cursor, CFG.mart_table_1d_fq, "1 day", from_ts, to_ts)
    print(f"[{DAG_ID}] OK: {CFG.mart_table_1d_fq}")

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
    description="OKX MART: daily rebuild ticker aggregates (1m/1h/1d) for t-1 window, idempotent (delete+insert)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=TAGS,
) as dag:
    PythonOperator(
        task_id="build_ticker_aggs",
        python_callable=run_build,
    )
