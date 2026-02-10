from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "timescaledb"
DAG_ID = "okx_health_table_inventory_daily"
SCHEDULE = None  # запускается master DAG (t-1)
TAGS = ["okx", "health", "timescaledb", "inventory", "sanity"]


@dataclass(frozen=True)
class Config:
    retention_days: int = 30  # история для Superset (графики роста)


CFG = Config()

# ВАЖНО:
# Таблицу/индексы создай один раз админом.
# В DAG DDL НЕ выполняем, чтобы не упираться в owner/privileges.

SQL_UPSERT = """
WITH
plain_tables AS (
  SELECT
    n.nspname AS table_schema,
    c.relname AS table_name,

    pg_total_relation_size(c.oid) AS total_bytes,
    pg_relation_size(c.oid)       AS heap_bytes,
    (pg_total_relation_size(c.oid) - pg_relation_size(c.oid)) AS indexes_toast_bytes,

    NULLIF(st.n_live_tup, -1)::bigint AS approx_row_count,

    NULL::timestamptz AS min_ts,
    NULL::timestamptz AS max_ts,

    false AS is_hypertable
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN pg_stat_all_tables st ON st.relid = c.oid
  WHERE c.relkind = 'r'
    AND n.nspname NOT IN ('pg_catalog','information_schema')
    AND n.nspname NOT LIKE 'pg_toast%'
    AND n.nspname NOT LIKE '_timescaledb_%'
),

hypertables AS (
  SELECT
    ht.hypertable_schema AS table_schema,
    ht.hypertable_name   AS table_name,

    SUM(pg_total_relation_size(format('%I.%I', c.chunk_schema, c.chunk_name)::regclass)) AS total_bytes,
    SUM(pg_relation_size(format('%I.%I', c.chunk_schema, c.chunk_name)::regclass))       AS heap_bytes,
    SUM(
      pg_total_relation_size(format('%I.%I', c.chunk_schema, c.chunk_name)::regclass)
      - pg_relation_size(format('%I.%I', c.chunk_schema, c.chunk_name)::regclass)
    ) AS indexes_toast_bytes,

    NULLIF(SUM(pc.reltuples), -1)::bigint AS approx_row_count,

    MIN(c.range_start) AS min_ts,
    MAX(c.range_end)   AS max_ts,

    true AS is_hypertable
  FROM timescaledb_information.hypertables ht
  JOIN timescaledb_information.chunks c
    ON c.hypertable_schema = ht.hypertable_schema
   AND c.hypertable_name   = ht.hypertable_name
  JOIN pg_class pc
    ON pc.oid = format('%I.%I', c.chunk_schema, c.chunk_name)::regclass
  GROUP BY 1,2
),

all_tables AS (
  SELECT * FROM plain_tables
  UNION ALL
  SELECT * FROM hypertables
),

dedup AS (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY table_schema, table_name
        ORDER BY is_hypertable DESC
      ) AS rn
    FROM all_tables
  ) x
  WHERE rn = 1
)

INSERT INTO okx_health.table_inventory_daily (
  run_ts_utc,
  logical_date_utc,
  table_schema,
  table_name,
  is_hypertable,
  total_bytes,
  heap_bytes,
  indexes_toast_bytes,
  approx_row_count,
  min_ts,
  max_ts,
  coverage_days
)
SELECT
  now() AT TIME ZONE 'utc' AS run_ts_utc,
  %s::timestamptz AS logical_date_utc,
  table_schema,
  table_name,
  is_hypertable,
  total_bytes,
  heap_bytes,
  indexes_toast_bytes,
  approx_row_count,
  min_ts,
  max_ts,
  CASE
    WHEN min_ts IS NULL OR max_ts IS NULL THEN NULL
    ELSE ROUND(EXTRACT(EPOCH FROM (max_ts - min_ts)) / 86400.0, 2)
  END AS coverage_days
FROM dedup
ON CONFLICT (logical_date_utc, table_schema, table_name)
DO UPDATE SET
  run_ts_utc          = now(),
  is_hypertable       = EXCLUDED.is_hypertable,
  total_bytes         = EXCLUDED.total_bytes,
  heap_bytes          = EXCLUDED.heap_bytes,
  indexes_toast_bytes = EXCLUDED.indexes_toast_bytes,
  approx_row_count    = EXCLUDED.approx_row_count,
  min_ts              = EXCLUDED.min_ts,
  max_ts              = EXCLUDED.max_ts,
  coverage_days       = EXCLUDED.coverage_days;
"""

SQL_DELETE_OLD = """
DELETE FROM okx_health.table_inventory_daily
WHERE logical_date_utc < (%s::timestamptz - make_interval(days => %s));
"""


def compute_and_store(**context) -> None:
    logical_date = context["dag_run"].logical_date
    if logical_date.tzinfo is None:
        logical_date = logical_date.replace(tzinfo=timezone.utc)
    logical_date_utc = logical_date.astimezone(timezone.utc).isoformat()

    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            # 1) Upsert текущего среза
            cur.execute(SQL_UPSERT, (logical_date_utc,))

            # 2) Чистка старых срезов
            cur.execute(SQL_DELETE_OLD, (logical_date_utc, CFG.retention_days))

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


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
