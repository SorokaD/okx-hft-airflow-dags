from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from okx.common.etl_common import (
    db_sanity_checks,
    get_logical_run_date,
    log_diagnostics,
)

# ============================================================
# 0) Project-wide constants (единый стандарт для всех DAG)
# ============================================================

CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"

DAG_ID = "okx_health_table_inventory_daily"
SCHEDULE = None  # запускается master DAG (t-1)
TAGS = ["okx", "health", "timescaledb", "inventory", "sanity"]


# ============================================================
# 1) Config (всё настраиваемое — только тут)
# ============================================================

@dataclass(frozen=True)
class Config:
    target_table_fq: str = "okx_health.table_inventory_daily"
    retention_days: int = 30  # сколько дней хранить историю срезов


CFG = Config()

SQL_DDL = """
CREATE SCHEMA IF NOT EXISTS okx_health;

CREATE TABLE IF NOT EXISTS okx_health.table_inventory_daily (
  run_ts_utc       timestamptz NOT NULL DEFAULT now(),
  logical_date_utc timestamptz NOT NULL,

  table_schema     text        NOT NULL,
  table_name       text        NOT NULL,

  is_hypertable    boolean     NOT NULL,

  total_bytes      bigint      NOT NULL,
  heap_bytes       bigint      NOT NULL,
  indexes_toast_bytes bigint   NOT NULL,

  approx_row_count bigint      NULL,

  min_ts           timestamptz NULL,
  max_ts           timestamptz NULL,
  coverage_days    numeric(18,2) NULL,

  PRIMARY KEY (logical_date_utc, table_schema, table_name)
);

CREATE INDEX IF NOT EXISTS ix_table_inventory_daily_schema_date
ON okx_health.table_inventory_daily (table_schema, logical_date_utc);

CREATE INDEX IF NOT EXISTS ix_table_inventory_daily_date
ON okx_health.table_inventory_daily (logical_date_utc);
"""

# Вставка "среза" по всем таблицам во всех пользовательских схемах.
# Важно:
# - обычные таблицы: размеры считаются по самой таблице, rowcount берём из pg_stat (оценка)
# - hypertables: размеры считаются как сумма чанков, rowcount — оценка reltuples по чанкам
# - coverage_days только для hypertables (min/max из range_start/range_end)
SQL_UPSERT = """
WITH
plain_tables AS (
  SELECT
    n.nspname AS table_schema,
    c.relname AS table_name,

    pg_total_relation_size(c.oid) AS total_bytes,
    pg_relation_size(c.oid)       AS heap_bytes,
    (pg_total_relation_size(c.oid) - pg_relation_size(c.oid)) AS indexes_toast_bytes,

    -- оценка числа строк (для health достаточно, точный COUNT(*) не делаем)
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

    -- оценка строк (reltuples может быть -1, приводим к NULL)
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
  -- если таблица является hypertable, она также присутствует в plain_tables (как "корневая").
  -- берём hypertable-версию (is_hypertable=true) как более корректную по размерам/coverage.
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
  %(logical_date_utc)s::timestamptz AS logical_date_utc,
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
  run_ts_utc          = EXCLUDED.run_ts_utc,
  is_hypertable       = EXCLUDED.is_hypertable,
  total_bytes         = EXCLUDED.total_bytes,
  heap_bytes          = EXCLUDED.heap_bytes,
  indexes_toast_bytes = EXCLUDED.indexes_toast_bytes,
  approx_row_count    = EXCLUDED.approx_row_count,
  min_ts              = EXCLUDED.min_ts,
  max_ts              = EXCLUDED.max_ts,
  coverage_days       = EXCLUDED.coverage_days
;
"""

SQL_CLEANUP = """
DELETE FROM okx_health.table_inventory_daily
WHERE logical_date_utc < (%(logical_date_utc)s::timestamptz - make_interval(days => %(retention_days)s));
"""


# ============================================================
# 2) DAG
# ============================================================

def _sync(**context: Any) -> None:
    """
    Собирает ежедневный "срез" по всем таблицам:
    - размеры (total/heap/indexes_toast)
    - оценка строк (без COUNT(*))
    - покрытие по времени (только hypertable)
    Пишет в okx_health.table_inventory_daily и чистит старые срезы.
    """
    hook = PostgresHook(postgres_conn_id=CONN_ID)

    # logical_date: как у тебя принято для DAG'ов t-1
    logical_date_utc = get_logical_run_date(context)  # ожидаем datetime (UTC)

    # базовые sanity checks (коннект + база)
    db_sanity_checks(hook, db_name_expected=DB_NAME_EXPECTED)

    # диагностический лог (по желанию — у тебя это часто используется)
    log_diagnostics(hook, logical_date_utc=logical_date_utc)

    # 1) DDL
    hook.run(SQL_DDL)

    # 2) Upsert inventory
    hook.run(
        SQL_UPSERT,
        parameters={
            "logical_date_utc": logical_date_utc,
        },
    )

    # 3) Cleanup старых срезов
    hook.run(
        SQL_CLEANUP,
        parameters={
            "logical_date_utc": logical_date_utc,
            "retention_days": CFG.retention_days,
        },
    )


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule=SCHEDULE,
    catchup=False,
    tags=TAGS,
    default_args={
        "owner": "okx",
        "retries": 0,
    },
    max_active_runs=1,
) as dag:

    sync = PythonOperator(
        task_id="sync",
        python_callable=_sync,
        provide_context=True,
    )
