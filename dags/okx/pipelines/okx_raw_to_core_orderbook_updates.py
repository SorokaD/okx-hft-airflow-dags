from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"  # самопроверка

SQL_SELECT_1 = "SELECT 1;"
SQL_CURRENT_DB = "SELECT current_database();"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class LoadCfg:
    batch_rows: int = 300_000
    max_loops: int = 20
    statement_timeout_ms: int = 0  # 0 = без таймаута на уровне сессии


CFG = LoadCfg()


def _run_sql(hook: PostgresHook, sql: str) -> None:
    # run() удобен тем, что выполняет без возврата результата
    hook.run(sql)


def run_sync_orderbook_updates() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)

    # 0) ping
    v = hook.get_first(SQL_SELECT_1)
    if not v or v[0] != 1:
        raise RuntimeError(f"DB ping failed: {v}")

    # 1) sanity: db name
    dbname_row = hook.get_first(SQL_CURRENT_DB)
    dbname = (dbname_row[0] if dbname_row else None)
    if DB_NAME_EXPECTED and dbname != DB_NAME_EXPECTED:
        raise RuntimeError(f"Connected to unexpected database: {dbname} (expected {DB_NAME_EXPECTED})")

    # (опционально) снять statement_timeout на сессию, чтобы большие батчи не рубило
    if CFG.statement_timeout_ms >= 0:
        _run_sql(hook, f"SET statement_timeout = {int(CFG.statement_timeout_ms)};")

    now = _now_utc()

    # 2) DO-блок: инкремент по ts_ingest_ms (raw) относительно max(ts_ingest) в core
    #    v_last_ms = max(ts_ingest) из core -> epoch_ms
    #    тянем raw батчами по ts_ingest_ms
    do_sql = f"""
DO $$
DECLARE
  v_last_ms bigint := 0;
  v_new_last_ms bigint;
  v_rows bigint;
  v_batch int := {int(CFG.batch_rows)};
  v_loops int := 0;
  v_max_loops int := {int(CFG.max_loops)};
BEGIN
  SELECT COALESCE(MAX((EXTRACT(EPOCH FROM ts_ingest) * 1000)::bigint), 0)
    INTO v_last_ms
  FROM okx_core.fact_orderbook_update;

  LOOP
    v_loops := v_loops + 1;
    EXIT WHEN v_loops > v_max_loops;

    WITH batch AS (
      SELECT
        instid,
        ts_event_ms,
        ts_ingest_ms,
        bids_delta,
        asks_delta,
        checksum
      FROM okx_raw.orderbook_updates
      WHERE ts_ingest_ms > v_last_ms
      ORDER BY ts_ingest_ms
      LIMIT v_batch
    ),
    ins AS (
      INSERT INTO okx_core.fact_orderbook_update
        (inst_id, ts_event, ts_ingest, bids_delta, asks_delta, checksum)
      SELECT
        b.instid::text,
        to_timestamp(b.ts_event_ms / 1000.0),
        to_timestamp(b.ts_ingest_ms / 1000.0),
        b.bids_delta,
        b.asks_delta,
        b.checksum
      FROM batch b
      -- у тебя (inst_id, ts_event) уникальный (и ещё есть (inst_id, ts_event, checksum))
      -- поэтому конфликт ловим по (inst_id, ts_event) и просто "освежаем" payload
      ON CONFLICT (inst_id, ts_event)
      DO UPDATE SET
        ts_ingest  = EXCLUDED.ts_ingest,
        bids_delta = EXCLUDED.bids_delta,
        asks_delta = EXCLUDED.asks_delta,
        checksum   = EXCLUDED.checksum
      RETURNING 1
    )
    SELECT
      (SELECT COALESCE(MAX(ts_ingest_ms), v_last_ms) FROM batch),
      (SELECT COUNT(*) FROM ins)
    INTO v_new_last_ms, v_rows;

    v_last_ms := v_new_last_ms;

    -- если за проход не вставили/обновили ни одной строки — выходим
    EXIT WHEN v_rows = 0;
  END LOOP;
END $$;
"""
    _run_sql(hook, do_sql)

    print(f"[okx_raw_to_core_orderbook_updates] now_utc={now.isoformat()} db={dbname} OK")


default_args = {
    "owner": "okx-data",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
    "execution_timeout": timedelta(minutes=5),
}

with DAG(
    dag_id="okx_raw_to_core_orderbook_updates",
    description="Increment: okx_raw.orderbook_updates -> okx_core.fact_orderbook_update (by ts_ingest_ms)",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="*/1 * * * *",  # каждую минуту; можно */2 если тяжело
    catchup=False,
    max_active_runs=1,
    tags=["okx", "raw-to-core", "timescaledb", "orderbook"],
) as dag:
    PythonOperator(
        task_id="sync_orderbook_updates",
        python_callable=run_sync_orderbook_updates,
    )
