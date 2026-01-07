from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"  # для самопроверки


# ---------------------------
# Настройки health-порогов
# ---------------------------

@dataclass(frozen=True)
class FreshnessCheck:
    name: str
    table_fq: str               # schema.table
    ts_col: str                 # ts_ingest OR ts_event OR etc. (timestamptz)
    max_lag_seconds: int        # если лаг больше -> FAIL


@dataclass(frozen=True)
class FreshnessMsCheck:
    name: str
    table_fq: str               # schema.table
    ts_ms_col: str              # bigint ms epoch
    max_lag_seconds: int


FRESHNESS_CHECKS = [
    # Подстрой под твои “источники истины”:
    FreshnessCheck(
        name="raw_trades_ingest",
        table_fq="okx_raw.trades",
        ts_col="ts_ingest_ms",
        max_lag_seconds=120,    # 2 минуты
    ),
    FreshnessCheck(
        name="raw_funding_rates_ingest",
        table_fq="okx_raw.funding_rates",
        ts_col="ts_ingest_ms",
        max_lag_seconds=900,    # funding редко, но raw тик может приходить чаще; поставим 15 мин
    ),
]

# Если у тебя есть таблицы с ts_ingest_ms (bigint):
FRESHNESS_MS_CHECKS = [
    FreshnessMsCheck(
        name="raw_orderbook_updates_ingest_ms",
        table_fq="okx_raw.orderbook_updates",
        ts_ms_col="ts_ingest_ms",
        max_lag_seconds=30,     # стакан должен идти почти постоянно
    ),
]


# ---------------------------
# SQL утилиты
# ---------------------------

SQL_SELECT_1 = "SELECT 1;"
SQL_CURRENT_DB = "SELECT current_database();"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _lag_seconds_from_ts(dt: datetime, now: datetime) -> float:
    # dt может прийти naive из драйвера — делаем UTC safe
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (now - dt).total_seconds()


# ---------------------------
# Health logic
# ---------------------------

def run_data_health() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)

    # 0) ping
    v = hook.get_first(SQL_SELECT_1)
    if not v or v[0] != 1:
        raise RuntimeError(f"DB ping failed: {v}")

    # 1) sanity: db name
    dbname = hook.get_first(SQL_CURRENT_DB)
    dbname = (dbname[0] if dbname else None)
    if DB_NAME_EXPECTED and dbname != DB_NAME_EXPECTED:
        raise RuntimeError(f"Connected to unexpected database: {dbname} (expected {DB_NAME_EXPECTED})")

    now = _now_utc()

    results = []
    failures = []

    # 2) freshness (timestamptz)
    for chk in FRESHNESS_CHECKS:
        sql = f"SELECT max({chk.ts_col}) FROM {chk.table_fq};"
        row = hook.get_first(sql)
        max_ts = row[0] if row else None

        if max_ts is None:
            failures.append(f"{chk.name}: max({chk.ts_col}) is NULL in {chk.table_fq}")
            results.append((chk.name, "NULL", None, chk.max_lag_seconds, "FAIL"))
            continue

        lag = _lag_seconds_from_ts(max_ts, now)
        status = "OK" if lag <= chk.max_lag_seconds else "FAIL"
        results.append((chk.name, max_ts.isoformat(), int(lag), chk.max_lag_seconds, status))

        if status == "FAIL":
            failures.append(
                f"{chk.name}: lag={int(lag)}s > {chk.max_lag_seconds}s (table={chk.table_fq}, col={chk.ts_col})"
            )

    # 3) freshness (epoch ms)
    for chk in FRESHNESS_MS_CHECKS:
        sql = f"SELECT max({chk.ts_ms_col}) FROM {chk.table_fq};"
        row = hook.get_first(sql)
        max_ms = row[0] if row else None

        if max_ms is None:
            failures.append(f"{chk.name}: max({chk.ts_ms_col}) is NULL in {chk.table_fq}")
            results.append((chk.name, "NULL", None, chk.max_lag_seconds, "FAIL"))
            continue

        # max_ms -> datetime UTC
        max_ts = datetime.fromtimestamp(max_ms / 1000.0, tz=timezone.utc)
        lag = (now - max_ts).total_seconds()
        status = "OK" if lag <= chk.max_lag_seconds else "FAIL"
        results.append((chk.name, max_ts.isoformat(), int(lag), chk.max_lag_seconds, status))

        if status == "FAIL":
            failures.append(
                f"{chk.name}: lag={int(lag)}s > {chk.max_lag_seconds}s (table={chk.table_fq}, col={chk.ts_ms_col})"
            )

    # 4) gap check (orderbook_updates ts_event_ms) за последний час
    # Здесь “качество стрима”: если есть огромные провалы между соседними ts_event_ms — плохо.
    # Подстрой таблицу/колонку под реальность.
    gap_sql = """
    WITH x AS (
      SELECT ts_event_ms
      FROM okx_raw.orderbook_updates
      WHERE ts_event_ms >= (extract(epoch from now()) * 1000)::bigint - (60*60*1000)
      ORDER BY ts_event_ms
    ),
    d AS (
      SELECT ts_event_ms - lag(ts_event_ms) OVER (ORDER BY ts_event_ms) AS gap_ms
      FROM x
    )
    SELECT
      count(*) FILTER (WHERE gap_ms IS NOT NULL) AS gaps_cnt,
      max(gap_ms) AS max_gap_ms
    FROM d;
    """
    row = hook.get_first(gap_sql)
    gaps_cnt = int(row[0]) if row and row[0] is not None else 0
    max_gap_ms = int(row[1]) if row and row[1] is not None else None

    # порог на max gap (например 5 секунд)
    max_gap_threshold_ms = 5_000
    if max_gap_ms is None:
        # если данных нет — это подозрительно (но бывает на пустой базе)
        failures.append("orderbook_gap_1h: no data in last hour (max_gap_ms is NULL)")
        results.append(("orderbook_gap_1h", "NULL", None, max_gap_threshold_ms // 1000, "FAIL"))
    else:
        status = "OK" if max_gap_ms <= max_gap_threshold_ms else "FAIL"
        results.append(("orderbook_gap_1h", f"gaps_cnt={gaps_cnt}", max_gap_ms, max_gap_threshold_ms, status))
        if status == "FAIL":
            failures.append(f"orderbook_gap_1h: max_gap_ms={max_gap_ms} > {max_gap_threshold_ms}ms")

    # 5) печать отчёта
    print(f"[okx_data_health_5m] now_utc={now.isoformat()} db={dbname}")
    for r in results:
        print(f"[health] name={r[0]} value={r[1]} lag={r[2]} threshold={r[3]} status={r[4]}")

    # 6) итог
    if failures:
        msg = " ; ".join(failures)
        raise RuntimeError(f"DATA HEALTH FAIL: {msg}")

    print("[okx_data_health_5m] OK")


default_args = {
    "owner": "okx-data",
    "retries": 0,  # health обычно не надо ретраить, лучше сразу сигналить
    "execution_timeout": timedelta(seconds=30),
}

with DAG(
    dag_id="okx_data_health_5m",
    description="Health: freshness + gaps checks for okx raw/core streams",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["okx", "health", "timescaledb", "data-quality"],
) as dag:
    PythonOperator(
        task_id="run_checks",
        python_callable=run_data_health,
    )
