from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "timescaledb"
DAG_ID = "okx_health_table_ranges_daily"
SCHEDULE = None  # запускается master DAG (t-1)
TAGS = ["okx", "health", "timescaledb", "sanity"]


SQL_DDL = """
CREATE SCHEMA IF NOT EXISTS okx_health;

CREATE TABLE IF NOT EXISTS okx_health.table_ranges_daily (
  run_ts_utc       timestamptz NOT NULL DEFAULT now(),
  logical_date_utc timestamptz NOT NULL,
  table_name       text        NOT NULL,

  min_ts           timestamptz NULL,
  max_ts           timestamptz NULL,
  history_days     numeric(18,2) NULL,
  rows_cnt         bigint      NOT NULL,

  PRIMARY KEY (logical_date_utc, table_name)
);
"""

SQL_INSERT = """
INSERT INTO okx_health.table_ranges_daily
  (logical_date_utc, table_name, min_ts, max_ts, history_days, rows_cnt)
SELECT
  %(logical_date_utc)s::timestamptz AS logical_date_utc,
  q.table_name,
  q.min_ts,
  q.max_ts,
  q.history_days,
  q.rows_cnt
FROM (
  SELECT
    'fact_funding_rate_event' AS table_name,
    min(ts_event) AS min_ts,
    max(ts_event) AS max_ts,
    ROUND(EXTRACT(EPOCH FROM (max(ts_event) - min(ts_event))) / 86400.0, 2) AS history_days,
    COUNT(*) AS rows_cnt
  FROM okx_core.fact_funding_rate_event

  UNION ALL
  SELECT
    'fact_funding_rate_tick',
    min(ts_event),
    max(ts_event),
    ROUND(EXTRACT(EPOCH FROM (max(ts_event) - min(ts_event))) / 86400.0, 2),
    COUNT(*)
  FROM okx_core.fact_funding_rate_tick

  UNION ALL
  SELECT
    'fact_index_tick',
    min(ts_event),
    max(ts_event),
    ROUND(EXTRACT(EPOCH FROM (max(ts_event) - min(ts_event))) / 86400.0, 2),
    COUNT(*)
  FROM okx_core.fact_index_tick

  UNION ALL
  SELECT
    'fact_mark_price_tick',
    min(ts_event),
    max(ts_event),
    ROUND(EXTRACT(EPOCH FROM (max(ts_event) - min(ts_event))) / 86400.0, 2),
    COUNT(*)
  FROM okx_core.fact_mark_price_tick

  UNION ALL
  SELECT
    'fact_open_interest_tick',
    min(ts_event),
    max(ts_event),
    ROUND(EXTRACT(EPOCH FROM (max(ts_event) - min(ts_event))) / 86400.0, 2),
    COUNT(*)
  FROM okx_core.fact_open_interest_tick

  UNION ALL
  SELECT
    'fact_orderbook_l10_snapshot',
    min(ts_event),
    max(ts_event),
    ROUND(EXTRACT(EPOCH FROM (max(ts_event) - min(ts_event))) / 86400.0, 2),
    COUNT(*)
  FROM okx_core.fact_orderbook_l10_snapshot

  UNION ALL
  SELECT
    'fact_ticker_tick',
    min(ts_event),
    max(ts_event),
    ROUND(EXTRACT(EPOCH FROM (max(ts_event) - min(ts_event))) / 86400.0, 2),
    COUNT(*)
  FROM okx_core.fact_ticker_tick

  UNION ALL
  SELECT
    'fact_trades_tick',
    min(ts_event),
    max(ts_event),
    ROUND(EXTRACT(EPOCH FROM (max(ts_event) - min(ts_event))) / 86400.0, 2),
    COUNT(*)
  FROM okx_core.fact_trades_tick

  UNION ALL
  SELECT
    'feat_hybrid_10ms',
    min(ts_bucket),
    max(ts_bucket),
    ROUND(EXTRACT(EPOCH FROM (max(ts_bucket) - min(ts_bucket))) / 86400.0, 2),
    COUNT(*)
  FROM okx_feat.feat_hybrid_10ms
) q
ON CONFLICT (logical_date_utc, table_name)
DO UPDATE SET
  run_ts_utc   = now(),
  min_ts       = EXCLUDED.min_ts,
  max_ts       = EXCLUDED.max_ts,
  history_days = EXCLUDED.history_days,
  rows_cnt     = EXCLUDED.rows_cnt;
"""


def compute_and_store(**context) -> None:
    # Airflow logical_date обычно tz-aware (UTC). Приведём к UTC явно.
    logical_date = context["dag_run"].logical_date
    if logical_date.tzinfo is None:
        logical_date = logical_date.replace(tzinfo=timezone.utc)
    logical_date_utc = logical_date.astimezone(timezone.utc)

    hook = PostgresHook(postgres_conn_id=CONN_ID)

    # 1) DDL
    hook.run(SQL_DDL)

    # 2) Insert/Upsert
    hook.run(SQL_INSERT, parameters={
             "logical_date_utc": logical_date_utc.isoformat()})


with DAG(
    dag_id=DAG_ID,
    schedule=SCHEDULE,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=TAGS,
    max_active_runs=1,
) as dag:
    PythonOperator(
        task_id="compute_and_store",
        python_callable=compute_and_store,
    )
