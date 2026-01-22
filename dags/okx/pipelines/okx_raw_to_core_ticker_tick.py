from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ============================================================
# 0) Project-wide constants (единый стандарт для всех DAG)
# ============================================================

CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"  # самопроверка, чтобы не залить "не туда"

# DAG identity
DAG_ID = "okx_raw_to_core_ticker_tick"
SCHEDULE = "0 */6 * * *"  # каждые 6 часов

# Tags (единый набор)
TAGS = ["okx", "etl", "raw-to-core", "timescaledb", "tickers"]

# SQL basics
SQL_SELECT_1 = "SELECT 1;"
SQL_CURRENT_DB = "SELECT current_database();"


# ============================================================
# 1) Config (всё настраиваемое — только тут)
# ============================================================

@dataclass(frozen=True)
class EtlConfig:
    # tables
    raw_table_fq: str = "okx_raw.tickers"             # schema.table
    core_table_fq: str = "okx_core.fact_ticker_tick"  # schema.table

    # window settings
    window_hours: int = 6            # сколько часов грузим за запуск
    step_minutes: int = 10           # размер под-окна (100k/10мин у тебя — отлично)
    overlap_minutes: int = 2          # небольшой overlap для безопасности (задержки/перестановки)
                                   # дедуп делаем PK/ON CONFLICT

    # safety/ops
    execution_timeout_sec: int = 2 * 60 * 60  # 2 часа
    retries: int = 1
    retry_delay_sec: int = 120

CFG = EtlConfig()


# ============================================================
# 2) Helpers
# ============================================================

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _floor_to_minute(dt: datetime) -> datetime:
    # чтобы окна были “ровные” и повторяемые
    return dt.replace(second=0, microsecond=0)

def _ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def _db_sanity_checks(hook: PostgresHook) -> str:
    v = hook.get_first(SQL_SELECT_1)
    if not v or v[0] != 1:
        raise RuntimeError(f"DB ping failed: {v}")

    row = hook.get_first(SQL_CURRENT_DB)
    dbname = row[0] if row else None
    if DB_NAME_EXPECTED and dbname != DB_NAME_EXPECTED:
        raise RuntimeError(
            f"Connected to unexpected database: {dbname} (expected {DB_NAME_EXPECTED})"
        )
    return dbname or "UNKNOWN"

def _window_bounds(now: datetime) -> Tuple[datetime, datetime]:
    """
    Основное окно: [to - window_hours, to)
    + overlap назад, чтобы не терять поздние/задержанные записи.
    """
    to_dt = _floor_to_minute(now)
    from_dt = to_dt - timedelta(hours=CFG.window_hours)
    from_dt = from_dt - timedelta(minutes=CFG.overlap_minutes)
    return from_dt, to_dt

def _sql_insert_window(from_ms: int, to_ms: int) -> str:
    """
    Оконная загрузка без ORDER BY/LIMIT (без индекса это критично).
    Вставляем только нужные поля. Возвращаем число вставленных строк.
    """
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
      WHERE b.ts_ingest_ms >= {from_ms}
        AND b.ts_ingest_ms <  {to_ms}
      ON CONFLICT (inst_id, ts_event) DO NOTHING
      RETURNING 1
    )
    SELECT count(*)::bigint AS inserted_rows FROM ins;
    """

def _log_run(*, dag_id: str, dbname: str, now: datetime, from_dt: datetime, to_dt: datetime,
             step_minutes: int, windows: int, inserted_total: int) -> None:
    print(
        f"[{dag_id}] now_utc={now.isoformat()} db={dbname} "
        f"window=[{from_dt.isoformat()}..{to_dt.isoformat()}) "
        f"step_min={step_minutes} windows={windows} inserted_total={inserted_total}"
    )


# ============================================================
# 3) Main callable (single responsibility: sync)
# ============================================================

def run_sync() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    dbname = _db_sanity_checks(hook)

    now = _now_utc()
    from_dt, to_dt = _window_bounds(now)

    step = timedelta(minutes=CFG.step_minutes)
    t = from_dt
    inserted_total = 0
    windows = 0

    while t < to_dt:
        w_from = t
        w_to = min(t + step, to_dt)

        sql = _sql_insert_window(_ms(w_from), _ms(w_to))
        row = hook.get_first(sql)
        inserted_rows = int(row[0]) if row and row[0] is not None else 0

        inserted_total += inserted_rows
        windows += 1

        # лёгкий лог по каждому окну (можно убрать если шумно)
        print(
            f"[{DAG_ID}] window [{w_from.isoformat()}..{w_to.isoformat()}) "
            f"inserted={inserted_rows}"
        )

        t = w_to

    _log_run(
        dag_id=DAG_ID,
        dbname=dbname,
        now=now,
        from_dt=from_dt,
        to_dt=to_dt,
        step_minutes=CFG.step_minutes,
        windows=windows,
        inserted_total=inserted_total,
    )


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
    description="OKX ETL: windowed raw->core for tickers (no ORDER BY/LIMIT; PK-dedup via ON CONFLICT)",
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
