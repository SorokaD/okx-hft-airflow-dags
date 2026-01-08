from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"  # самопроверка

DAG_ID = "okx_raw_to_core_orderbook_snapshots"
SCHEDULE = "*/1 * * * *"  # раз в минуту


# ---------------------------
# Настройки ETL
# ---------------------------

@dataclass(frozen=True)
class EtlConfig:
    raw_table_fq: str = "okx_raw.orderbook_snapshots"
    core_table_fq: str = "okx_core.fact_orderbook_snapshot"

    # watermark в core (timestamptz)
    core_wm_col: str = "ts_ingest"

    # инкремент в raw (epoch ms bigint)
    raw_wm_ms_col: str = "ts_ingest_ms"

    # конфликт-ключ (timescale hypertable требует ts_event в unique)
    conflict_cols: tuple[str, str] = ("snapshot_id", "ts_event")

    # батчи и лимиты
    batch_size: int = 300_000
    max_loops: int = 20

    # safety
    execution_timeout_sec: int = 120


CFG = EtlConfig()

SQL_SELECT_1 = "SELECT 1;"
SQL_CURRENT_DB = "SELECT current_database();"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _ms_from_timestamptz(dt: datetime) -> int:
    # dt может прийти naive из драйвера — делаем UTC safe
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _get_core_watermark_ms(hook: PostgresHook) -> int:
    sql = f"SELECT max({CFG.core_wm_col}) FROM {CFG.core_table_fq};"
    row = hook.get_first(sql)
    max_ts = row[0] if row else None
    if max_ts is None:
        return 0
    return _ms_from_timestamptz(max_ts)


def run_sync_orderbook_snapshots() -> None:
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

    now = _now_utc()
    v_last_ms = _get_core_watermark_ms(hook)

    total_inserted = 0
    loops = 0

    # 2) loop-batched incremental
    while loops < CFG.max_loops:
        loops += 1

        # Берём батч raw и вставляем в core. Возвращаем:
        # - новый watermark (max ts_ingest_ms из батча)
        # - сколько реально вставили (с учётом ON CONFLICT DO NOTHING)
        sql = f"""
        WITH batch AS (
          SELECT *
          FROM {CFG.raw_table_fq}
          WHERE {CFG.raw_wm_ms_col} > {v_last_ms}
          ORDER BY {CFG.raw_wm_ms_col}
          LIMIT {CFG.batch_size}
        ),
        ins AS (
          INSERT INTO {CFG.core_table_fq}
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
          ON CONFLICT ({CFG.conflict_cols[0]}, {CFG.conflict_cols[1]}) DO NOTHING
          RETURNING 1
        )
        SELECT
          COALESCE((SELECT max({CFG.raw_wm_ms_col}) FROM batch), {v_last_ms}) AS new_last_ms,
          (SELECT count(*) FROM ins) AS inserted_rows;
        """

        row = hook.get_first(sql)
        if not row:
            raise RuntimeError("ETL query returned no result row")

        v_new_last_ms = int(row[0])
        inserted = int(row[1]) if row[1] is not None else 0

        total_inserted += inserted
        v_last_ms = v_new_last_ms

        # если вставок не было — дальше смысла нет
        if inserted == 0:
            break

    print(
        f"[{DAG_ID}] now_utc={now.isoformat()} db={dbname} "
        f"loops={loops} total_inserted={total_inserted} last_ms={v_last_ms}"
    )


default_args = {
    "owner": "okx-data",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(seconds=CFG.execution_timeout_sec),
}

with DAG(
    dag_id=DAG_ID,
    description="ETL: incremental raw->core for orderbook_snapshots (batched, idempotent)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,  # критично: не параллелить
    tags=["okx", "etl", "raw-to-core", "timescaledb"],
) as dag:
    PythonOperator(
        task_id="sync_orderbook_snapshots",
        python_callable=run_sync_orderbook_snapshots,
    )
