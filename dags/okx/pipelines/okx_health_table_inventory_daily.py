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
    retention_days: int = 30  # сколько дней хранить историю


CFG = Config()

# 1) Удаляем текущий срез (на эту дату), чтобы сделать чистую перезапись
SQL_DELETE_THIS_SLICE = r"""
DELETE FROM okx_health.table_inventory_daily
WHERE logical_date_utc = %s::timestamptz;
"""

# 2) Вставляем “как на скрине”: pretty sizes + approx_row_count
SQL_INSERT_SLICE = r"""
WITH
plain_tables AS (
  SELECT
    n.nspname AS table_schema,
    c.relname AS table_name,

    pg_total_relation_size(c.oid) AS total_bytes,
    pg_relation_size(c.oid)       AS heap_bytes,
    (pg_total_relation_size(c.oid) - pg_relation_size(c.oid)) AS indexes_toast_bytes,

    NULLIF(st.n_live_tup, -1)::bigint AS approx_row_count,

    false AS is_hypertable
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN pg_stat_all_tables st ON st.relid = c.oid
  WHERE c.relkind = 'r'
    AND n.nspname NOT IN ('pg_catalog','information_schema')
    AND n.nspname NOT LIKE 'pg_toast%%'
    AND n.nspname NOT LIKE '_timescaledb_%%'
),

hypertables AS (
  SELECT
    ht.hypertable_schema AS table_schema,
    ht.hypertable_name   AS table_name,

    SUM(pg_total_relation_size(format('%%I.%%I', c.chunk_schema, c.chunk_name)::regclass)) AS total_bytes,
    SUM(pg_relation_size(format('%%I.%%I', c.chunk_schema, c.chunk_name)::regclass))       AS heap_bytes,
    SUM(
      pg_total_relation_size(format('%%I.%%I', c.chunk_schema, c.chunk_name)::regclass)
      - pg_relation_size(format('%%I.%%I', c.chunk_schema, c.chunk_name)::regclass)
    ) AS indexes_toast_bytes,

    -- оценка строк по чанкам: reltuples может быть отрицательным => оставляем как есть (как у тебя на скрине)
    SUM(pc.reltuples)::bigint AS approx_row_count,

    true AS is_hypertable
  FROM timescaledb_information.hypertables ht
  JOIN timescaledb_information.chunks c
    ON c.hypertable_schema = ht.hypertable_schema
   AND c.hypertable_name   = ht.hypertable_name
  JOIN pg_class pc
    ON pc.oid = format('%%I.%%I', c.chunk_schema, c.chunk_name)::regclass
  GROUP BY 1,2
),

all_tables AS (
  SELECT * FROM plain_tables
  UNION ALL
  SELECT * FROM hypertables
),

dedup AS (
  -- если таблица hypertable, она также видна как обычная таблица: берём hypertable-версию
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
  total_size,
  heap_size,
  indexes_toast_size,
  approx_row_count
)
SELECT
  now() AT TIME ZONE 'utc' AS run_ts_utc,
  %s::timestamptz AS logical_date_utc,
  table_schema,
  table_name,
  is_hypertable,
  pg_size_pretty(total_bytes)         AS total_size,
  pg_size_pretty(heap_bytes)          AS heap_size,
  pg_size_pretty(indexes_toast_bytes) AS indexes_toast_size,
  approx_row_count
FROM dedup;
"""

# 3) Чистим старые срезы
SQL_DELETE_OLD = r"""
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
            # 1) удаляем срез на эту дату (перезапись)
            cur.execute(SQL_DELETE_THIS_SLICE, (logical_date_utc,))

            # 2) вставляем новый срез
            cur.execute(SQL_INSERT_SLICE, (logical_date_utc,))

            # 3) чистим историю
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
