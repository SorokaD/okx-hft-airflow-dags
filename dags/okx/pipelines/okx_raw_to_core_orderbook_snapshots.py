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

    # дедуп внутри батча по "смысловому ключу" снапшота
    # (если в raw есть повторы на одном уровне, берём самый свежий по ts_ingest_ms)
    dedup_partition_cols_raw: tuple[str, ...] = ("instid", "ts_event_ms", "side", "level")
    dedup_order_col_raw: str = "ts_ingest_ms"

    # батчи и лимиты
    batch_size: int = 300_000
    max_loops: int = 20

    # safety
    execution_timeout_sec: int = 180


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


def _sql_increment_once(last_ms: int) -> str:
    # Формируем PARTITION BY список
    part_cols = ", ".join([f"b.{c}" for c in CFG.dedup_partition_cols_raw])

    return f"""
    WITH batch AS (
      SELECT *
      FROM {CFG.raw_table_fq}
      WHERE {CFG.raw_wm_ms_col} > {last_ms}
      ORDER BY {CFG.raw_wm_ms_col}
      LIMIT {CFG.batch_size}
    ),
    dedup AS (
      SELECT *
      FROM (
        SELECT
          b.*,
          row_number() OVER (
            PARTITION BY {part_cols}
            ORDER BY b.{CFG.dedup_order_col_raw} DESC
          ) AS rn
        FROM batch b
      ) x
      WHERE x.rn = 1
    ),
    ins AS (
      INSERT INTO {CFG.core_table_fq}
        (snapshot_id, inst_id, ts_event, side, level_no, price_px, size_qty, ts_ingest)
      SELECT
        d.snapshot_id,
        d.instid::text,
        to_timestamp(d.ts_event_ms / 1000.0),
        CASE d.side WHEN 1 THEN 'bid' WHEN 2 THEN 'ask' ELSE NULL END,
        d.level::int2,
        d.price,
        d.size,
        to_timestamp(d.ts_ingest_ms / 1000.0)
      FROM dedup d
      RETURNING 1
    )
    SELECT
      COALESCE((SELECT max({CFG.raw_wm_ms_col}) FROM batch), {last_ms}) AS new_last_ms,
      (SELECT count(*) FROM batch) AS batch_rows,
      (SELECT count(*) FROM dedup) AS dedup_rows,
      (SELECT count(*) FROM ins) AS inserted_rows;
    """


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
    last_ms = _get_core_watermark_ms(hook)

    total_batch = 0
    total_dedup = 0
    total_inserted = 0
    loops = 0

    # 2) loop-batched incremental
    while loops < CFG.max_loops:
        loops += 1

        sql = _sql_increment_once(last_ms)
        row = hook.get_first(sql)
        if not row:
            raise RuntimeError("ETL query returned no result row")

        new_last_ms = int(row[0])
        batch_rows = int(row[1]) if row[1] is not None else 0
        dedup_rows = int(row[2]) if row[2] is not None else 0
        inserted_rows = int(row[3]) if row[3] is not None else 0

        total_batch += batch_rows
        total_dedup += dedup_rows
        total_inserted += inserted_rows

        last_ms = new_last_ms

        # Если батч пустой — данных больше нет
        if batch_rows == 0:
            break

        # Если после дедупа/вставки ничего не вставилось — дальше крутиться бессмысленно
        # (обычно значит одни повторы)
        if inserted_rows == 0:
            break

    print(
        f"[{DAG_ID}] now_utc={now.isoformat()} db={dbname} loops={loops} "
        f"batch={total_batch} dedup={total_dedup} inserted={total_inserted} last_ms={last_ms}"
    )


default_args = {
    "owner": "okx-data",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(seconds=CFG.execution_timeout_sec),
}

with DAG(
    dag_id=DAG_ID,
    description="ETL: incremental raw->core for orderbook_snapshots (batch + dedup-in-batch)",
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
