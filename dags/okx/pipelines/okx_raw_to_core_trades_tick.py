from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ============================================================
# 0) Project-wide constants (единый стандарт для всех DAG)
# ============================================================

CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"

DAG_ID = "okx_raw_to_core_trades_tick"
SCHEDULE = "45 0,6,12,18 * * *"  # 00:45, 06:45, 12:45, 18:45 UTC

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


CFG = EtlConfig()


# ============================================================
# 2) Helpers
# ============================================================


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


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


def _get_core_watermark_dt(hook: PostgresHook) -> Optional[datetime]:
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
    row = hook.get_first(sql)
    return row[0] if row and row[0] is not None else None


def _window_bounds_rolling(now: datetime) -> Tuple[datetime, datetime]:
    to_dt = _floor_to_minute(now)
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
    to_dt = _floor_to_minute(now)

    wm = _get_core_watermark_dt(hook)
    if wm is None:
        # если core пустой — грузим как rolling окно
        from_dt = to_dt - timedelta(hours=CFG.window_hours)
    else:
        from_dt = wm - timedelta(minutes=CFG.overlap_minutes)

    # safety cap: не начинаем слишком далеко в прошлое
    min_from = to_dt - timedelta(days=CFG.max_backfill_lookback_days)
    if from_dt < min_from:
        from_dt = min_from

    from_dt = _floor_to_minute(from_dt)
    return from_dt, to_dt


def _sql_insert_window(from_ms: int, to_ms: int) -> str:
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
      WHERE t.ts_ingest_ms >= {from_ms}
        AND t.ts_ingest_ms <  {to_ms}
      ON CONFLICT (inst_id, ts_event, trade_id) DO NOTHING
      RETURNING 1
    )
    SELECT count(*)::bigint AS inserted_rows FROM ins;
    """


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
    dbname = _db_sanity_checks(hook)

    now = _now_utc()

    if CFG.mode not in ("rolling", "backfill"):
        raise ValueError(f"CFG.mode must be 'rolling' or 'backfill', got: {CFG.mode}")

    # быстрая диагностика индексов
    try:
        rows = hook.get_records(_sql_check_required_indexes())
        for check_name, ok in rows:
            if not ok:
                print(f"[{DAG_ID}] WARNING: index check failed: {check_name}=false")
    except Exception as e:
        # не критично — просто не мешаем загрузке
        print(f"[{DAG_ID}] WARNING: index checks skipped due to error: {e!r}")

    if CFG.mode == "rolling":
        from_dt, to_dt = _window_bounds_rolling(now)
        windows_budget = 10**9  # без ограничения
    else:
        from_dt, to_dt = _window_bounds_backfill(hook, now)
        windows_budget = CFG.max_windows_per_run

    step = timedelta(minutes=CFG.step_minutes)
    t = from_dt

    inserted_total = 0
    windows_done = 0
    started_at = _now_utc()

    print(
        f"[{DAG_ID}] START mode={CFG.mode} db={dbname} "
        f"window=[{from_dt.isoformat()}..{to_dt.isoformat()}) "
        f"step_min={CFG.step_minutes} overlap_min={CFG.overlap_minutes} "
        f"budget_windows={windows_budget}"
    )

    while t < to_dt and windows_done < windows_budget:
        w_from = t
        w_to = min(t + step, to_dt)

        sql = _sql_insert_window(_ms(w_from), _ms(w_to))
        row = hook.get_first(sql)
        inserted_rows = int(row[0]) if row and row[0] is not None else 0

        inserted_total += inserted_rows
        windows_done += 1

        # логирование не на каждое окно
        if (
            windows_done == 1
            or (windows_done % CFG.log_every_n_windows == 0)
            or w_to == to_dt
        ):
            elapsed = _now_utc() - started_at
            print(
                f"[{DAG_ID}] PROGRESS windows_done={windows_done} "
                f"last_window=[{w_from.isoformat()}..{w_to.isoformat()}) inserted={inserted_rows} "
                f"inserted_total={inserted_total} elapsed={elapsed}"
            )

        t = w_to

    remaining = to_dt - t
    elapsed = _now_utc() - started_at
    print(
        f"[{DAG_ID}] DONE mode={CFG.mode} windows_done={windows_done} inserted_total={inserted_total} "
        f"stopped_at={t.isoformat()} remaining={remaining} elapsed={elapsed}"
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
