from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Sequence, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from okx.common.etl_common import (
    batch_iter,
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

DAG_ID = "okx_raw_to_core_trades_tick"
SCHEDULE = None  # запускается мастер-DAG'ом раз в сутки (t-1)

TAGS = ["okx", "etl", "raw-to-core", "timescaledb", "trades"]

SQL_SELECT_1 = "SELECT 1;"
SQL_CURRENT_DB = "SELECT current_database();"


# ============================================================
# 1) Config (всё настраиваемое — только тут)
# ============================================================
  

@dataclass(frozen=True)
class EtlConfig:
    # sources/targets
    raw_table_fq: str = "okx_raw.trades"
    core_table_fq: str = "okx_core.fact_trades_tick"

    # --- MODE SWITCH ---
    # "rolling"  -> грузим последние window_hours (поддержка)
    # "backfill" -> догоняем от watermark в core до now (но ограниченно)
    mode: str = "backfill"  # <<< переключатель

    # rolling window
    window_hours: int = 6

    # batching by time
    # ВАЖНО: step_minutes и max_windows_per_run согласованы:
    # step_minutes=5 => 288 окон = 24 часа данных за один запуск
    step_minutes: int = 5
    overlap_minutes: int = 1

    # backfill controls (ограничение "сколько догоняем" за запуск)
    max_windows_per_run: int = 288  # 288*5min = 24h

    # logging/ops
    log_every_n_windows: int = 20  # печатаем прогресс раз в N окон
    execution_timeout_sec: int = 6 * 60 * 60  # 6 часов
    retries: int = 1
    retry_delay_sec: int = 120

    # optional: safety cap, чтобы не пытаться backfill "в вечность"
    max_backfill_lookback_days: int = (
        365  # если watermark очень старый, начнем не раньше now-365d
    )

    # batching by instrument
    batch_size: int = 20
    max_instruments_per_run: int | None = None

    # statement timeout (ms)
    statement_timeout_ms: int = 6 * 60 * 60 * 1000


CFG = EtlConfig()


# ============================================================
# 2) Helpers
# ============================================================


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _day_start_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


def _floor_to_minute(dt: datetime) -> datetime:
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


def _get_core_watermark_dt(cursor) -> Optional[datetime]:
    """
    Быстрый watermark:
    - вместо SELECT max(ts_ingest) (который может быть тяжелым),
      используем ORDER BY ts_ingest DESC LIMIT 1.
    - Очень желательно иметь индекс на (ts_ingest DESC) или хотя бы (ts_ingest).
    """
    sql = f"""
    SELECT ts_ingest
    FROM {CFG.core_table_fq}
    ORDER BY ts_ingest DESC
    LIMIT 1;
    """
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


def _window_bounds_rolling(now: datetime) -> Tuple[datetime, datetime]:
    to_dt = _day_start_utc(now)
    from_dt = (
        to_dt
        - timedelta(hours=CFG.window_hours)
        - timedelta(minutes=CFG.overlap_minutes)
    )
    from_dt = _floor_to_minute(from_dt)
    return from_dt, to_dt


def _window_bounds_backfill(
    hook: PostgresHook, now: datetime
) -> Tuple[datetime, datetime]:
    to_dt = _day_start_utc(now)

    wm = _get_core_watermark_dt(hook)
    if wm is None:
        # если core пустой — грузим t-1 сутки
        from_dt = to_dt - timedelta(days=1)
    else:
        from_dt = wm - timedelta(minutes=CFG.overlap_minutes)

    # safety cap: не начинаем слишком далеко в прошлое
    min_from = to_dt - timedelta(days=CFG.max_backfill_lookback_days)
    if from_dt < min_from:
        from_dt = min_from

    from_dt = _floor_to_minute(from_dt)
    return from_dt, to_dt


def _sql_insert_window(where_sql: str, instids: Sequence[str] | None) -> tuple[str, dict | None]:
    """
    Вставка окна.
    Важно:
    - Фильтр по raw.ts_ingest_ms должен опираться на индекс на raw(ts_ingest_ms),
      иначе будет тяжело.
    - ON CONFLICT опирается на unique index/constraint на core (inst_id, ts_event, trade_id).
    """
    return f"""
    WITH ins AS (
      INSERT INTO {CFG.core_table_fq}
        (
          inst_id,
          ts_event,
          ts_ingest,
          trade_id,
          trade_px,
          trade_sz,
          side
        )
      SELECT
          t.instid::text AS inst_id,
          (to_timestamp(t.ts_event_ms / 1000.0) AT TIME ZONE 'UTC')::timestamptz AS ts_event,
          (to_timestamp(t.ts_ingest_ms / 1000.0) AT TIME ZONE 'UTC')::timestamptz AS ts_ingest,
          t.tradeid::text AS trade_id,
          t.px::float8 AS trade_px,
          t.sz::float8 AS trade_sz,
          t.side::text AS side
      FROM {CFG.raw_table_fq} t
      WHERE {where_sql}
        {f"AND t.instid = ANY(%(instids)s)" if instids is not None else ""}
      ON CONFLICT (inst_id, ts_event, trade_id) DO NOTHING
      RETURNING 1
    )
    SELECT count(*)::bigint AS inserted_rows FROM ins;
    """, ({"instids": list(instids)} if instids is not None else None)


def _sql_check_required_indexes() -> str:
    """
    Ненавязчивая проверка индексов (быстро, без блокировок).
    Мы не валим DAG, но печатаем предупреждения в лог.
    """
    return f"""
    WITH idx AS (
      SELECT
        schemaname,
        tablename,
        indexname,
        indexdef
      FROM pg_indexes
      WHERE (schemaname, tablename) IN (
        ('okx_raw', 'trades'),
        ('okx_core', 'fact_trades_tick')
      )
    )
    SELECT
      'raw_trades_has_ts_ingest_ms_idx' AS check_name,
      EXISTS (
        SELECT 1 FROM idx
        WHERE schemaname='okx_raw'
          AND tablename='trades'
          AND indexdef ILIKE '%(ts_ingest_ms%'
      ) AS ok
    UNION ALL
    SELECT
      'core_fact_has_ts_ingest_idx' AS check_name,
      EXISTS (
        SELECT 1 FROM idx
        WHERE schemaname='okx_core'
          AND tablename='fact_trades_tick'
          AND indexdef ILIKE '%(ts_ingest%'
      ) AS ok
    UNION ALL
    SELECT
      'core_fact_has_unique_inst_event_trade' AS check_name,
      EXISTS (
        SELECT 1 FROM idx
        WHERE schemaname='okx_core'
          AND tablename='fact_trades_tick'
          AND (indexdef ILIKE '%UNIQUE%'
               AND indexdef ILIKE '%(inst_id%'
               AND indexdef ILIKE '%ts_event%'
               AND indexdef ILIKE '%trade_id%')
      ) AS ok
    ;
    """


# ============================================================
# 3) Main callable
# ============================================================


def run_sync() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    _db_sanity_checks(hook)

    conn = hook.get_conn()
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("SET statement_timeout = %s", (CFG.statement_timeout_ms,))
    log_diagnostics(cursor, [CFG.raw_table_fq, CFG.core_table_fq])

    # быстрая диагностика индексов
    try:
        cursor.execute(_sql_check_required_indexes())
        rows = cursor.fetchall()
        for check_name, ok in rows:
            if not ok:
                print(f"[{DAG_ID}] WARNING: index check failed: {check_name}=false")
    except Exception as e:
        print(f"[{DAG_ID}] WARNING: index checks skipped due to error: {e!r}")

    run_dt = get_logical_run_date()
    to_dt = day_start_utc(run_dt)

    wm = _get_core_watermark_dt(cursor)
    from_dt = wm - timedelta(minutes=CFG.overlap_minutes) if wm else None

    to_ms = ms(to_dt)
    where_sql = f"t.ts_ingest_ms < {to_ms}"
    if from_dt is not None:
        where_sql = f"t.ts_ingest_ms >= {ms(from_dt)} AND " + where_sql

    raw_max_ms = _get_raw_max_ts_ingest_ms(cursor)
    if raw_max_ms is None or (from_dt is not None and raw_max_ms < ms(from_dt)):
        print(f"[{DAG_ID}] SKIP: raw empty or older than window raw_max_ms={raw_max_ms}")
        return

    instids = _get_distinct_instids(cursor, where_sql, "t")
    if CFG.max_instruments_per_run is not None:
        instids = instids[: CFG.max_instruments_per_run]

    inserted_total = 0
    if not instids or len(instids) <= CFG.batch_size:
        sql, params = _sql_insert_window(where_sql, None)
        cursor.execute(sql, params)
        row = cursor.fetchone()
        inserted_total = int(row[0]) if row and row[0] is not None else 0
        print(f"[{DAG_ID}] inserted_total={inserted_total}")
        return

    print(f"[{DAG_ID}] batching instid count={len(instids)} batch_size={CFG.batch_size}")
    for batch in batch_iter(instids, CFG.batch_size):
        sql, params = _sql_insert_window(where_sql, batch)
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
    description="OKX ETL: windowed raw->core for trades (rolling/backfill; fast watermark; throttled logs)",
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
        # оставляем один источник истины: timeout в default_args
    )
