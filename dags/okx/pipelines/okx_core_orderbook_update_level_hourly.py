from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"

TOP_N = 20

SQL_SELECT_1 = "SELECT 1;"
SQL_CURRENT_DB = "SELECT current_database();"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_db(hook: PostgresHook) -> None:
    v = hook.get_first(SQL_SELECT_1)
    if not v or v[0] != 1:
        raise RuntimeError(f"DB ping failed: {v}")

    row = hook.get_first(SQL_CURRENT_DB)
    dbname = (row[0] if row else None)
    if DB_NAME_EXPECTED and dbname != DB_NAME_EXPECTED:
        raise RuntimeError(f"Unexpected database: {dbname} (expected {DB_NAME_EXPECTED})")


def load_one_hour(**context) -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    _ensure_db(hook)

    v_from = context["data_interval_start"]  # pendulum datetime
    v_to = context["data_interval_end"]

    sql = f"""
    WITH params AS (
      SELECT
        '{v_from.isoformat()}'::timestamptz AS v_from,
        '{v_to.isoformat()}'::timestamptz AS v_to
    ),
    batch AS (
      SELECT inst_id, ts_event, ts_ingest, bids_delta, asks_delta, checksum
      FROM okx_core.fact_orderbook_update, params
      WHERE ts_ingest >= params.v_from
        AND ts_ingest <  params.v_to
        AND (bids_delta IS NOT NULL OR asks_delta IS NOT NULL)
    ),
    lvl AS (
      SELECT
        b.inst_id, b.ts_event, b.ts_ingest,
        z.side,
        (z.elem->>'price')::float8 AS price_px,
        (z.elem->>'size')::float8  AS size_qty,
        b.checksum,
        row_number() OVER (
          PARTITION BY b.inst_id, b.ts_event, z.side
          ORDER BY
            CASE WHEN z.side='bid' THEN (z.elem->>'price')::float8 END DESC,
            CASE WHEN z.side='ask' THEN (z.elem->>'price')::float8 END ASC
        ) AS rn
      FROM batch b
      CROSS JOIN LATERAL (
        SELECT 'bid'::text AS side, e AS elem
        FROM jsonb_array_elements(COALESCE(b.bids_delta, '[]'::jsonb)) e
        UNION ALL
        SELECT 'ask'::text AS side, e AS elem
        FROM jsonb_array_elements(COALESCE(b.asks_delta, '[]'::jsonb)) e
      ) z
    )
    INSERT INTO okx_core.fact_orderbook_update_level
      (inst_id, ts_event, ts_ingest, side, price_px, size_qty, checksum)
    SELECT inst_id, ts_event, ts_ingest, side, price_px, size_qty, checksum
    FROM lvl
    WHERE rn <= {TOP_N}
    ON CONFLICT (inst_id, ts_event, side, price_px)
    DO UPDATE SET
      size_qty = EXCLUDED.size_qty,
      ts_ingest = EXCLUDED.ts_ingest,
      checksum  = EXCLUDED.checksum;
    """

    hook.run(sql)
    print(f"[load_one_hour] window=[{v_from} .. {v_to}) done")


default_args = {
    "owner": "okx-data",
    "retries": 0,
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="okx_core_orderbook_update_level_hourly",
    description="Backfill+increment: explode orderbook deltas to top-20 levels (hourly windows)",
    default_args=default_args,
    start_date=datetime(2026, 1, 2, tzinfo=timezone.utc),
    schedule="0 * * * *",
    catchup=True,          # ВАЖНО для backfill
    max_active_runs=1,     # чтобы не убить базу
    tags=["okx", "etl", "timescaledb", "orderbook"],
) as dag:
    PythonOperator(
        task_id="load_one_hour",
        python_callable=load_one_hour,
        provide_context=True,
    )
