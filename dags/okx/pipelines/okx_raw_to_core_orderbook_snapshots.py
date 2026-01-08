from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

DEFAULT_ARGS = {
    "owner": "hft",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

SQL_INCREMENT = r"""
DO $$
DECLARE
  v_last_ms bigint := 0;
  v_new_last_ms bigint;
  v_rows bigint;
  v_batch int := 300000;
  v_loops int := 0;
  v_max_loops int := 20;
BEGIN
  SELECT COALESCE(MAX(EXTRACT(EPOCH FROM ts_ingest) * 1000)::bigint, 0)
    INTO v_last_ms
  FROM okx_core.fact_orderbook_snapshot;

  LOOP
    v_loops := v_loops + 1;
    EXIT WHEN v_loops > v_max_loops;

    WITH batch AS (
      SELECT *
      FROM okx_raw.orderbook_snapshots
      WHERE ts_ingest_ms > v_last_ms
      ORDER BY ts_ingest_ms
      LIMIT v_batch
    ),
    ins AS (
      INSERT INTO okx_core.fact_orderbook_snapshot
        (snapshot_id, inst_id, ts_event, side, level_no, price_px, size_qty, ts_ingest)
      SELECT
        b.snapshot_id,
        b.instid::text,
        to_timestamp(b.ts_event_ms / 1000.0),
        CASE b.side WHEN 1 THEN 'bid' WHEN 2 THEN 'ask' ELSE NULL END,
        b.level::int2,
        b.price,
        b.size,
        to_timestamp(b.ts_ingest_ms / 1000.0)
      FROM batch b
      ON CONFLICT (snapshot_id, ts_event) DO NOTHING
      RETURNING 1
    )
    SELECT
      (SELECT COALESCE(MAX(ts_ingest_ms), v_last_ms) FROM batch),
      (SELECT COUNT(*) FROM ins)
    INTO v_new_last_ms, v_rows;

    v_last_ms := v_new_last_ms;
    EXIT WHEN v_rows = 0;
  END LOOP;
END $$;
"""

with DAG(
    dag_id="okx_raw_to_core_orderbook_snapshots",
    start_date=datetime(2026, 1, 1),
    schedule="*/1 * * * *",     # раз в минуту
    catchup=False,
    max_active_runs=1,          # критично: не параллелить
    default_args=DEFAULT_ARGS,
    tags=["okx", "raw_to_core"],
) as dag:

    sync_orderbook_snapshots = PostgresOperator(
        task_id="sync_orderbook_snapshots",
        postgres_conn_id="timescaledb_okx",  # имя твоего Airflow Connection
        sql=SQL_INCREMENT,
    )
