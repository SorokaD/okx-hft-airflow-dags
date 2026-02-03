from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Sequence, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from okx.common.etl_common import (
    batch_iter,
    db_sanity_checks,
    day_start_utc,
    get_logical_run_date,
    log_diagnostics,
    ms,
)


# ============================================================
# 0) Project-wide constants (единый стандарт для всех DAG)
# ============================================================

CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"

DAG_ID = "okx_raw_to_core_ticker_tick"
SCHEDULE = None  # запускается мастер-DAG'ом раз в сутки (t-1)

TAGS = ["okx", "etl", "raw-to-core", "timescaledb", "tickers"]

SQL_SELECT_1 = "SELECT 1;"
SQL_CURRENT_DB = "SELECT current_database();"


# ============================================================
# 1) Config (всё настраиваемое — только тут)
# ============================================================

@dataclass(frozen=True)
class EtlConfig:
    raw_table_fq: str = "okx_raw.tickers"
    core_table_fq: str = "okx_core.fact_ticker_tick"

    # ingestion window
    overlap_minutes: int = 2

    # time-window chunking (ограничивает объём одного INSERT — окно по времени)
    step_minutes: int = 60  # размер одного окна
    max_windows_per_run: int = 24  # макс. окон за запуск (24h при step=60)

    # batching by instrument (опционально; при True — старый путь с SELECT DISTINCT)
    batch_by_instid: bool = False
    batch_size: int = 20
    max_instruments_per_run: int | None = None

    # statement timeout (ms)
    statement_timeout_ms: int = 30 * 60 * 1000

    # safety/ops
    execution_timeout_sec: int = 5 * 60 * 60  # 5 часов (много окон)
    retries: int = 1
    retry_delay_sec: int = 120


CFG = EtlConfig()


# ============================================================
# 2) Helpers
# ============================================================

def _get_core_watermark_dt(cursor) -> datetime | None:
    sql = f"SELECT max(ts_ingest) FROM {CFG.core_table_fq};"
    cursor.execute(sql)
    row = cursor.fetchone()
    return row[0] if row and row[0] is not None else None


def _get_raw_max_ts_ingest_ms(cursor) -> int | None:
    sql = f"SELECT max(ts_ingest_ms) FROM {CFG.raw_table_fq};"
    cursor.execute(sql)
    row = cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def _get_distinct_instids(cursor, where_sql: str, alias: str) -> list[str]:
    cursor.execute(
        f"SELECT DISTINCT {alias}.instid FROM {CFG.raw_table_fq} {alias} WHERE {where_sql};"
    )
    return [r[0] for r in cursor.fetchall() if r and r[0] is not None]


def _floor_to_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def _time_windows(
    from_dt: datetime, to_dt: datetime
) -> Iterator[Tuple[datetime, datetime]]:
    """Генерирует окна [w_from, w_to) с шагом step_minutes, не более max_windows_per_run."""
    step = timedelta(minutes=CFG.step_minutes)
    w_from = _floor_to_minute(from_dt)
    if w_from.tzinfo is None:
        w_from = w_from.replace(tzinfo=timezone.utc)
    to_dt = to_dt.astimezone(w_from.tzinfo) if to_dt.tzinfo else to_dt.replace(tzinfo=timezone.utc)
    n = 0
    while w_from < to_dt and n < CFG.max_windows_per_run:
        w_to = min(w_from + step, to_dt)
        yield w_from, w_to
        w_from = w_to
        n += 1


def _sql_insert_bulk(where_sql: str, instids: Sequence[str] | None) -> tuple[str, dict | None]:
    return f"""
    WITH ins AS (
      INSERT INTO {CFG.core_table_fq}
        (
          inst_id,
          ts_event,
          ts_ingest,
          last_px,
          bid_px,
          bid_sz,
          ask_px,
          ask_sz,
          open_24h,
          high_24h,
          low_24h,
          vol_24h,
          vol_ccy_24h
        )
      SELECT
          b.instid::text AS inst_id,
          (to_timestamp(b.ts_event_ms / 1000.0) AT TIME ZONE 'UTC')::timestamptz AS ts_event,
          (to_timestamp(b.ts_ingest_ms / 1000.0) AT TIME ZONE 'UTC')::timestamptz AS ts_ingest,
          b.last      AS last_px,
          b.bidpx     AS bid_px,
          b.bidsz     AS bid_sz,
          b.askpx     AS ask_px,
          b.asksz     AS ask_sz,
          b.open24h   AS open_24h,
          b.high24h   AS high_24h,
          b.low24h    AS low_24h,
          b.vol24h    AS vol_24h,
          b.volccy24h AS vol_ccy_24h
      FROM {CFG.raw_table_fq} b
      WHERE {where_sql}
        {f"AND b.instid = ANY(%(instids)s)" if instids is not None else ""}
      ON CONFLICT (inst_id, ts_event) DO NOTHING
      RETURNING 1
    )
    SELECT count(*)::bigint AS inserted_rows FROM ins;
    """, ({"instids": list(instids)} if instids is not None else None)


# ============================================================
# 3) Main callable
# ============================================================

def run_sync() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = True
    cursor = conn.cursor()

    db_sanity_checks(cursor, DB_NAME_EXPECTED)
    cursor.execute("SET statement_timeout = %s", (CFG.statement_timeout_ms,))
    log_diagnostics(cursor, [CFG.raw_table_fq, CFG.core_table_fq])

    run_dt = get_logical_run_date()
    to_dt = day_start_utc(run_dt)

    wm = _get_core_watermark_dt(cursor)
    from_dt = wm - timedelta(minutes=CFG.overlap_minutes) if wm else None

    to_ms = ms(to_dt)
    where_sql = f"b.ts_ingest_ms < {to_ms}"
    if from_dt is not None:
        where_sql = f"b.ts_ingest_ms >= {ms(from_dt)} AND " + where_sql

    raw_max_ms = _get_raw_max_ts_ingest_ms(cursor)
    if raw_max_ms is None or (from_dt is not None and raw_max_ms < ms(from_dt)):
        print(f"[{DAG_ID}] SKIP: raw empty or older than window raw_max_ms={raw_max_ms}")
        return

    inserted_total = 0

    if not CFG.batch_by_instid:
        # Путь по временным окнам: нет тяжёлого SELECT DISTINCT, каждый INSERT ограничен окном
        windows = list(_time_windows(from_dt or (to_dt - timedelta(days=1)), to_dt))
        if not windows:
            print(f"[{DAG_ID}] SKIP: no windows")
            return
        print(f"[{DAG_ID}] time-window mode windows={len(windows)} step_min={CFG.step_minutes}")
        for w_from, w_to in windows:
            w_where = f"b.ts_ingest_ms >= {ms(w_from)} AND b.ts_ingest_ms < {ms(w_to)}"
            sql, params = _sql_insert_bulk(w_where, None)
            cursor.execute(sql, params)
            row = cursor.fetchone()
            inserted = int(row[0]) if row and row[0] is not None else 0
            inserted_total += inserted
            if inserted:
                print(f"[{DAG_ID}] window [{w_from!s}..{w_to!s}) inserted={inserted}")
        print(f"[{DAG_ID}] DONE inserted_total={inserted_total}")
        return

    # Путь по инструментам (может быть тяжёлым на больших таблицах)
    instids = _get_distinct_instids(cursor, where_sql, "b")
    if CFG.max_instruments_per_run is not None:
        instids = instids[: CFG.max_instruments_per_run]

    if not instids or len(instids) <= CFG.batch_size:
        sql, params = _sql_insert_bulk(where_sql, None)
        cursor.execute(sql, params)
        row = cursor.fetchone()
        inserted_total = int(row[0]) if row and row[0] is not None else 0
        print(f"[{DAG_ID}] inserted_total={inserted_total}")
        return

    print(f"[{DAG_ID}] batching instid count={len(instids)} batch_size={CFG.batch_size}")
    for batch in batch_iter(instids, CFG.batch_size):
        sql, params = _sql_insert_bulk(where_sql, batch)
        cursor.execute(sql, params)
        row = cursor.fetchone()
        inserted = int(row[0]) if row and row[0] is not None else 0
        inserted_total += inserted
        print(f"[{DAG_ID}] batch inserted={inserted}")

    print(f"[{DAG_ID}] DONE inserted_total={inserted_total}")


# ============================================================
# 4) DAG definition
# ============================================================

default_args: dict[str, Any] = {
    "owner": "okx-data",
    "retries": CFG.retries,
    "retry_delay": timedelta(seconds=CFG.retry_delay_sec),
    "execution_timeout": timedelta(seconds=CFG.execution_timeout_sec),
}

with DAG(
    dag_id=DAG_ID,
    description="OKX ETL: windowed raw->core for tickers (rolling/backfill; PK-dedup via ON CONFLICT)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=TAGS,
) as dag:
    PythonOperator(
        task_id="sync",
        python_callable=run_sync,
    )
