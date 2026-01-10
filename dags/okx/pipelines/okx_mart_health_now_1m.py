from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ============================================================
# 0) Project-wide constants (единый стандарт для всех DAG)
# ============================================================

CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"  # самопроверка, чтобы не залить "не туда"

# DAG identity
DAG_ID = "okx_health_now_1m"
SCHEDULE = "*/1 * * * *"

# Tags (единый набор)
TAGS = ["okx", "health", "data-quality", "timescaledb"]

# SQL basics
SQL_SELECT_1 = "SELECT 1;"
SQL_CURRENT_DB = "SELECT current_database();"


# ============================================================
# 1) Config (всё настраиваемое — только тут)
# ============================================================


@dataclass(frozen=True)
class HealthCfg:
    # scheduling/ops
    execution_timeout_sec: int = 120
    retries: int = 1
    retry_delay_sec: int = 30

    # thresholds (tune later)
    warn_lag_sec: int = 60
    crit_lag_sec: int = 300

    warn_core_gap_sec: int = 120
    crit_core_gap_sec: int = 600

    warn_raw_core_lag_ms: int = 60_000     # 1 min
    crit_raw_core_lag_ms: int = 300_000    # 5 min

    # windows
    gap_lookback_hours: int = 2            # minutes-bucket gap calc over last N hours
    rows_window_minutes: int = 5           # rows_5m


CFG = HealthCfg()


@dataclass(frozen=True)
class EntitySpec:
    name: str
    raw_table_fq: str
    raw_ingest_ms_col: str
    core_table_fq: str
    core_ts_ingest_col: str


# ============================================================
# 1.5) ENTITIES (raw + core pairs)
# ============================================================

ENTITIES: list[EntitySpec] = [
    EntitySpec(
        name="trades",
        raw_table_fq="okx_raw.trades",
        raw_ingest_ms_col="ts_ingest_ms",
        core_table_fq="okx_core.fact_trades_tick",
        core_ts_ingest_col="ts_ingest",
    ),
    EntitySpec(
        name="tickers",
        raw_table_fq="okx_raw.tickers",
        raw_ingest_ms_col="ts_ingest_ms",
        core_table_fq="okx_core.fact_ticker_tick",
        core_ts_ingest_col="ts_ingest",
    ),
    EntitySpec(
        name="orderbook_snapshots",
        raw_table_fq="okx_raw.orderbook_snapshots",
        raw_ingest_ms_col="ts_ingest_ms",
        core_table_fq="okx_core.fact_orderbook_snapshot",
        core_ts_ingest_col="ts_ingest",
    ),
    EntitySpec(
        name="orderbook_updates",
        raw_table_fq="okx_raw.orderbook_updates",
        raw_ingest_ms_col="ts_ingest_ms",
        core_table_fq="okx_core.fact_orderbook_update",
        core_ts_ingest_col="ts_ingest",
    ),
    EntitySpec(
        name="funding_rates",
        raw_table_fq="okx_raw.funding_rates",
        raw_ingest_ms_col="ts_ingest_ms",
        core_table_fq="okx_core.fact_funding_rate_event",
        core_ts_ingest_col="ts_ingest",
    ),
]


# ============================================================
# 2) Helpers
# ============================================================


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


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


def _sql_health_one(spec: EntitySpec) -> str:
    # We compute gap on "minute buckets" to keep it cheap.
    # For raw(ms): bucket_ms = floor(ts_ingest_ms/60000)*60000
    # For core(ts): time_bucket('1 minute', ts_ingest)
    #
    # We also write:
    # - okx_mart.health_history_1m (append)
    # - okx_mart.health_now (upsert by entity)
    lookback_ms = CFG.gap_lookback_hours * 60 * 60 * 1000
    rows_window_ms = CFG.rows_window_minutes * 60 * 1000
    lookback_interval = f"{CFG.gap_lookback_hours} hours"
    rows_window_interval = f"{CFG.rows_window_minutes} minutes"

    return f"""
    WITH
    params AS (
      SELECT
        now()::timestamptz AS ts_check,
        (EXTRACT(EPOCH FROM now()) * 1000)::bigint AS now_ms
    ),

    -- =========================
    -- RAW metrics (epoch-ms)
    -- =========================
    raw_max AS (
      SELECT MAX(r.{spec.raw_ingest_ms_col}) AS raw_max_ingest_ms
      FROM {spec.raw_table_fq} r
    ),
    raw_rows_5m AS (
      SELECT COUNT(*)::bigint AS raw_rows_5m
      FROM {spec.raw_table_fq} r
      CROSS JOIN params p
      WHERE r.{spec.raw_ingest_ms_col} >= p.now_ms - {rows_window_ms}::bigint
    ),
    raw_buckets AS (
      SELECT ((r.{spec.raw_ingest_ms_col} / 60000) * 60000)::bigint AS b_ms
      FROM {spec.raw_table_fq} r
      CROSS JOIN params p
      WHERE r.{spec.raw_ingest_ms_col} >= p.now_ms - {lookback_ms}::bigint
      GROUP BY 1
    ),
    raw_gap_base AS (
    SELECT
        b_ms,
        (b_ms - LAG(b_ms) OVER (ORDER BY b_ms)) AS gap_ms
    FROM raw_buckets
    ),
    raw_gap AS (
    SELECT
        COALESCE(MAX(gap_ms / 1000), 0)::int AS raw_max_gap_1h_sec
    FROM raw_gap_base
    ),

    ),
    raw_final AS (
      SELECT
        rm.raw_max_ingest_ms,
        CASE
          WHEN rm.raw_max_ingest_ms IS NULL THEN NULL
          ELSE ((p.now_ms - rm.raw_max_ingest_ms) / 1000)::int
        END AS raw_ingest_lag_sec,
        rr.raw_rows_5m,
        rg.raw_max_gap_1h_sec
      FROM raw_max rm
      CROSS JOIN params p
      CROSS JOIN raw_rows_5m rr
      CROSS JOIN raw_gap rg
    ),

    -- =========================
    -- CORE metrics (timestamptz)
    -- =========================
    core_max AS (
      SELECT MAX(c.{spec.core_ts_ingest_col}) AS core_max_ts_ingest
      FROM {spec.core_table_fq} c
    ),
    core_rows_5m AS (
      SELECT COUNT(*)::bigint AS core_rows_5m
      FROM {spec.core_table_fq} c
      WHERE c.{spec.core_ts_ingest_col} >= now() - interval '{rows_window_interval}'
    ),
    core_buckets AS (
      SELECT time_bucket('1 minute', c.{spec.core_ts_ingest_col}) AS b
      FROM {spec.core_table_fq} c
      WHERE c.{spec.core_ts_ingest_col} >= now() - interval '{lookback_interval}'
      GROUP BY 1
    ),
    core_gap_base AS (
    SELECT
        b,
        (b - LAG(b) OVER (ORDER BY b)) AS gap
    FROM core_buckets
    ),
    core_gap AS (
    SELECT
        COALESCE(MAX(EXTRACT(EPOCH FROM gap)), 0)::int AS core_max_gap_1h_sec
    FROM core_gap_base
    ),
    core_final AS (
      SELECT
        cm.core_max_ts_ingest,
        CASE
          WHEN cm.core_max_ts_ingest IS NULL THEN NULL
          ELSE EXTRACT(EPOCH FROM (now() - cm.core_max_ts_ingest))::int
        END AS core_ingest_lag_sec,
        cr.core_rows_5m,
        cg.core_max_gap_1h_sec,
        CASE
          WHEN cm.core_max_ts_ingest IS NULL THEN NULL
          ELSE (EXTRACT(EPOCH FROM cm.core_max_ts_ingest) * 1000)::bigint
        END AS core_max_ingest_ms
      FROM core_max cm
      CROSS JOIN core_rows_5m cr
      CROSS JOIN core_gap cg
    ),

    merged AS (
      SELECT
        p.ts_check,
        '{spec.name}'::text AS entity,

        rf.raw_max_ingest_ms,
        rf.raw_ingest_lag_sec,
        rf.raw_rows_5m,
        rf.raw_max_gap_1h_sec,

        cf.core_max_ts_ingest,
        cf.core_ingest_lag_sec,
        cf.core_rows_5m,
        cf.core_max_gap_1h_sec,

        CASE
          WHEN rf.raw_max_ingest_ms IS NULL OR cf.core_max_ingest_ms IS NULL THEN NULL
          ELSE (rf.raw_max_ingest_ms - cf.core_max_ingest_ms)
        END AS raw_core_lag_ms
      FROM params p
      CROSS JOIN raw_final rf
      CROSS JOIN core_final cf
    ),

    scored AS (
      SELECT
        m.*,
        CASE
          WHEN m.raw_ingest_lag_sec IS NULL OR m.core_ingest_lag_sec IS NULL THEN 'CRIT'
          WHEN m.raw_ingest_lag_sec >= {CFG.crit_lag_sec} THEN 'CRIT'
          WHEN m.core_ingest_lag_sec >= {CFG.crit_lag_sec} THEN 'CRIT'
          WHEN m.core_max_gap_1h_sec >= {CFG.crit_core_gap_sec} THEN 'CRIT'
          WHEN m.raw_core_lag_ms IS NOT NULL AND m.raw_core_lag_ms >= {CFG.crit_raw_core_lag_ms} THEN 'CRIT'
          WHEN m.raw_ingest_lag_sec >= {CFG.warn_lag_sec} THEN 'WARN'
          WHEN m.core_ingest_lag_sec >= {CFG.warn_lag_sec} THEN 'WARN'
          WHEN m.core_max_gap_1h_sec >= {CFG.warn_core_gap_sec} THEN 'WARN'
          WHEN m.raw_core_lag_ms IS NOT NULL AND m.raw_core_lag_ms >= {CFG.warn_raw_core_lag_ms} THEN 'WARN'
          ELSE 'OK'
        END AS status,
        CASE
          WHEN m.raw_ingest_lag_sec IS NULL THEN 'raw empty'
          WHEN m.core_ingest_lag_sec IS NULL THEN 'core empty'
          WHEN m.raw_ingest_lag_sec >= {CFG.crit_lag_sec} THEN 'raw stalled'
          WHEN m.core_ingest_lag_sec >= {CFG.crit_lag_sec} THEN 'core stalled'
          WHEN m.raw_core_lag_ms IS NOT NULL AND m.raw_core_lag_ms >= {CFG.crit_raw_core_lag_ms} THEN 'core lagging raw'
          ELSE NULL
        END AS details
      FROM merged m
    ),

    ins_hist AS (
      INSERT INTO okx_mart.health_history_1m (
        ts_check, entity,
        raw_max_ingest_ms, raw_ingest_lag_sec, raw_rows_5m, raw_max_gap_1h_sec,
        core_max_ts_ingest, core_ingest_lag_sec, core_rows_5m, core_max_gap_1h_sec,
        raw_core_lag_ms,
        status, details
      )
      SELECT
        ts_check, entity,
        raw_max_ingest_ms, raw_ingest_lag_sec, raw_rows_5m, raw_max_gap_1h_sec,
        core_max_ts_ingest, core_ingest_lag_sec, core_rows_5m, core_max_gap_1h_sec,
        raw_core_lag_ms,
        status, details
      FROM scored
      RETURNING 1
    )

    INSERT INTO okx_mart.health_now (
      entity, ts_check,
      raw_max_ingest_ms, raw_ingest_lag_sec, raw_rows_5m, raw_max_gap_1h_sec,
      core_max_ts_ingest, core_ingest_lag_sec, core_rows_5m, core_max_gap_1h_sec,
      raw_core_lag_ms,
      status, details
    )
    SELECT
      entity, ts_check,
      raw_max_ingest_ms, raw_ingest_lag_sec, raw_rows_5m, raw_max_gap_1h_sec,
      core_max_ts_ingest, core_ingest_lag_sec, core_rows_5m, core_max_gap_1h_sec,
      raw_core_lag_ms,
      status, details
    FROM scored
    ON CONFLICT (entity) DO UPDATE SET
      ts_check = EXCLUDED.ts_check,
      raw_max_ingest_ms = EXCLUDED.raw_max_ingest_ms,
      raw_ingest_lag_sec = EXCLUDED.raw_ingest_lag_sec,
      raw_rows_5m = EXCLUDED.raw_rows_5m,
      raw_max_gap_1h_sec = EXCLUDED.raw_max_gap_1h_sec,
      core_max_ts_ingest = EXCLUDED.core_max_ts_ingest,
      core_ingest_lag_sec = EXCLUDED.core_ingest_lag_sec,
      core_rows_5m = EXCLUDED.core_rows_5m,
      core_max_gap_1h_sec = EXCLUDED.core_max_gap_1h_sec,
      raw_core_lag_ms = EXCLUDED.raw_core_lag_ms,
      status = EXCLUDED.status,
      details = EXCLUDED.details
    ;
    """


def run_health() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    dbname = _db_sanity_checks(hook)
    now = _now_utc()

    # Ensure mart tables exist (optional safety)
    hook.run(
        """
        SELECT 1;
        """
    )

    for spec in ENTITIES:
        hook.run(_sql_health_one(spec))
        print(f"[{DAG_ID}] now_utc={now.isoformat()} db={dbname} entity={spec.name} -> ok")


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
    description="OKX Health NOW: raw+core freshness/gaps/pipeline lag -> okx_mart.health_now + history_1m",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,  # never overlap same DAG runs
    tags=TAGS,
) as dag:
    PythonOperator(
        task_id="run_health",
        python_callable=run_health,
    )
