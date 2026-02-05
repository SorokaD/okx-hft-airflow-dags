from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from okx.common.etl_common import (
    db_sanity_checks,
    log_diagnostics,
)

CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"

DAG_ID = "okx_core_to_feat_hybrid_10ms"
SCHEDULE = None

TAGS = ["okx", "etl", "core-to-feat", "timescaledb", "feat", "10ms"]


@dataclass(frozen=True)
class EtlConfig:
    src_book_fq: str = "okx_core.fact_orderbook_l10_snapshot"
    src_trades_fq: str = "okx_core.fact_trades_tick"
    dst_feat_fq: str = "okx_feat.feat_hybrid_10ms"

    overlap_minutes: int = 2
    chunk_hours: int = 2
    statement_timeout_ms: int = 30 * 60 * 1000

    # не считаем самый хвост, пока core ещё может догружаться
    safety_lag_seconds: int = 5

    # (опционально) чтобы за 1 запуск не пытался прожевать бесконечный backlog
    # поставь None, если хочешь "всё сразу"
    max_catchup_hours_per_run: int | None = 24

    execution_timeout_sec: int = 2 * 60 * 60
    retries: int = 1
    retry_delay_sec: int = 120


CFG = EtlConfig()


def _get_dst_watermark_dt(cursor) -> datetime | None:
    cursor.execute(f"SELECT max(ts_bucket) FROM {CFG.dst_feat_fq};")
    row = cursor.fetchone()
    return row[0] if row and row[0] is not None else None


def _get_src_bounds(cursor) -> Tuple[datetime | None, datetime | None]:
    """
    Границы источников в core:
      src_min_dt = max(min_trades, min_book)
      src_max_dt = min(max_trades, max_book)
    """
    cursor.execute(
        f"SELECT min(ts_event), max(ts_event) FROM {CFG.src_trades_fq};")
    tr_min, tr_max = cursor.fetchone()

    cursor.execute(
        f"SELECT min(ts_event), max(ts_event) FROM {CFG.src_book_fq};")
    bk_min, bk_max = cursor.fetchone()

    if tr_min is None or tr_max is None or bk_min is None or bk_max is None:
        return None, None

    return max(tr_min, bk_min), min(tr_max, bk_max)


def _sql_upsert_feat_window(from_dt: datetime, to_dt: datetime) -> str:
    return f"""
    WITH params AS (
      SELECT
        '{from_dt.isoformat()}'::timestamptz AS v_from,
        '{to_dt.isoformat()}'::timestamptz AS v_to
    ),
    trades AS (
      SELECT
        t.inst_id,
        (to_timestamp(floor(extract(epoch from t.ts_event) * 100.0) / 100.0) AT TIME ZONE 'UTC')::timestamptz AS ts_bucket,

        count(*)::int AS trades_cnt,
        sum(t.trade_sz)::float8 AS qty_sum,
        sum(t.trade_sz * t.trade_px)::float8 AS notional_sum,
        (sum(t.trade_sz * t.trade_px) / NULLIF(sum(t.trade_sz), 0.0))::float8 AS vwap_px,

        sum(CASE WHEN lower(t.side) IN ('buy','b','1') THEN t.trade_sz ELSE 0 END)::float8 AS buy_qty_sum,
        sum(CASE WHEN lower(t.side) IN ('sell','s','2') THEN t.trade_sz ELSE 0 END)::float8 AS sell_qty_sum,

        (sum(CASE WHEN lower(t.side) IN ('buy','b','1') THEN t.trade_sz ELSE 0 END)
         - sum(CASE WHEN lower(t.side) IN ('sell','s','2') THEN t.trade_sz ELSE 0 END))::float8 AS net_qty_sum

      FROM {CFG.src_trades_fq} t, params
      WHERE t.ts_event >= params.v_from
        AND t.ts_event <  params.v_to
      GROUP BY t.inst_id, ts_bucket
    ),
    joined AS (
      SELECT
        tr.*,
        b.ts_event AS book_ts_event,
        (extract(epoch from (tr.ts_bucket - b.ts_event)) * 1000.0)::int AS book_age_ms,

        b.mid_px,
        b.spread_px,

        ((b.ask_px_01 * b.bid_sz_01 + b.bid_px_01 * b.ask_sz_01) / NULLIF((b.bid_sz_01 + b.ask_sz_01), 0.0))::float8 AS microprice,

        ((b.bid_sz_01 - b.ask_sz_01) / NULLIF((b.bid_sz_01 + b.ask_sz_01), 0.0))::float8 AS imb_01,

        ((
          (b.bid_sz_01+b.bid_sz_02+b.bid_sz_03+b.bid_sz_04+b.bid_sz_05)
          - (b.ask_sz_01+b.ask_sz_02+b.ask_sz_03+b.ask_sz_04+b.ask_sz_05)
        ) / NULLIF(
          (b.bid_sz_01+b.bid_sz_02+b.bid_sz_03+b.bid_sz_04+b.bid_sz_05)
          + (b.ask_sz_01+b.ask_sz_02+b.ask_sz_03+b.ask_sz_04+b.ask_sz_05),
          0.0
        ))::float8 AS imb_05,

        ((
          (b.bid_sz_01+b.bid_sz_02+b.bid_sz_03+b.bid_sz_04+b.bid_sz_05+b.bid_sz_06+b.bid_sz_07+b.bid_sz_08+b.bid_sz_09+b.bid_sz_10)
          - (b.ask_sz_01+b.ask_sz_02+b.ask_sz_03+b.ask_sz_04+b.ask_sz_05+b.ask_sz_06+b.ask_sz_07+b.ask_sz_08+b.ask_sz_09+b.ask_sz_10)
        ) / NULLIF(
          (b.bid_sz_01+b.bid_sz_02+b.bid_sz_03+b.bid_sz_04+b.bid_sz_05+b.bid_sz_06+b.bid_sz_07+b.bid_sz_08+b.bid_sz_09+b.bid_sz_10)
          + (b.ask_sz_01+b.ask_sz_02+b.ask_sz_03+b.ask_sz_04+b.ask_sz_05+b.ask_sz_06+b.ask_sz_07+b.ask_sz_08+b.ask_sz_09+b.ask_sz_10),
          0.0
        ))::float8 AS imb_10,

        (b.bid_sz_01+b.bid_sz_02+b.bid_sz_03+b.bid_sz_04+b.bid_sz_05+b.bid_sz_06+b.bid_sz_07+b.bid_sz_08+b.bid_sz_09+b.bid_sz_10)::float8 AS depth_bid_10,
        (b.ask_sz_01+b.ask_sz_02+b.ask_sz_03+b.ask_sz_04+b.ask_sz_05+b.ask_sz_06+b.ask_sz_07+b.ask_sz_08+b.ask_sz_09+b.ask_sz_10)::float8 AS depth_ask_10

      FROM trades tr
      LEFT JOIN LATERAL (
        SELECT *
        FROM {CFG.src_book_fq} b
        WHERE b.inst_id = tr.inst_id
          AND b.ts_event <= tr.ts_bucket
        ORDER BY b.ts_event DESC
        LIMIT 1
      ) b ON TRUE
    ),
    ins AS (
      INSERT INTO {CFG.dst_feat_fq} (
        inst_id, ts_bucket,
        trades_cnt, qty_sum, notional_sum, vwap_px, buy_qty_sum, sell_qty_sum, net_qty_sum,
        book_ts_event, book_age_ms, spread_px, mid_px, microprice,
        imb_01, imb_05, imb_10, depth_bid_10, depth_ask_10
      )
      SELECT
        inst_id, ts_bucket,
        trades_cnt, qty_sum, notional_sum, vwap_px, buy_qty_sum, sell_qty_sum, net_qty_sum,
        book_ts_event, book_age_ms, spread_px, mid_px, microprice,
        imb_01, imb_05, imb_10, depth_bid_10, depth_ask_10
      FROM joined
      ON CONFLICT (inst_id, ts_bucket)
      DO UPDATE SET
        trades_cnt   = EXCLUDED.trades_cnt,
        qty_sum      = EXCLUDED.qty_sum,
        notional_sum = EXCLUDED.notional_sum,
        vwap_px      = EXCLUDED.vwap_px,
        buy_qty_sum  = EXCLUDED.buy_qty_sum,
        sell_qty_sum = EXCLUDED.sell_qty_sum,
        net_qty_sum  = EXCLUDED.net_qty_sum,
        book_ts_event= EXCLUDED.book_ts_event,
        book_age_ms  = EXCLUDED.book_age_ms,
        spread_px    = EXCLUDED.spread_px,
        mid_px       = EXCLUDED.mid_px,
        microprice   = EXCLUDED.microprice,
        imb_01       = EXCLUDED.imb_01,
        imb_05       = EXCLUDED.imb_05,
        imb_10       = EXCLUDED.imb_10,
        depth_bid_10 = EXCLUDED.depth_bid_10,
        depth_ask_10 = EXCLUDED.depth_ask_10
      RETURNING 1
    )
    SELECT count(*)::bigint FROM ins;
    """


def run_sync() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = True
    cursor = conn.cursor()

    db_sanity_checks(cursor, DB_NAME_EXPECTED)
    cursor.execute("SET statement_timeout = %s", (CFG.statement_timeout_ms,))
    log_diagnostics(cursor, [CFG.src_trades_fq,
                    CFG.src_book_fq, CFG.dst_feat_fq])

    src_min_dt, src_max_dt = _get_src_bounds(cursor)
    if src_min_dt is None or src_max_dt is None:
        print(f"[{DAG_ID}] sources are empty -> nothing to do")
        return

    # to_dt = актуальный max по источникам (trades+book) минус safety lag
    to_dt = src_max_dt - timedelta(seconds=CFG.safety_lag_seconds)

    wm = _get_dst_watermark_dt(cursor)

    # from_dt = watermark в feat (или начало источников, если feat пустой)
    if wm is None:
        from_dt = src_min_dt
    else:
        from_dt = wm - timedelta(minutes=CFG.overlap_minutes)
        if from_dt < src_min_dt:
            from_dt = src_min_dt

    if from_dt >= to_dt:
        print(f"[{DAG_ID}] up-to-date: window=[{from_dt}..{to_dt}) -> nothing to do")
        return

    # (опционально) ограничим объём догонки за 1 запуск, чтобы не убить базу
    if CFG.max_catchup_hours_per_run is not None:
        limit_to = from_dt + \
            timedelta(hours=int(CFG.max_catchup_hours_per_run))
        if limit_to < to_dt:
            print(f"[{DAG_ID}] catchup cap: limiting to_dt {to_dt} -> {limit_to}")
            to_dt = limit_to

    print(f"[{DAG_ID}] window=[{from_dt}..{to_dt}) src=[{src_min_dt}..{src_max_dt}) wm={wm}")

    chunk = timedelta(hours=CFG.chunk_hours)
    t = from_dt
    total = 0

    while t < to_dt:
        w_from = t
        w_to = min(t + chunk, to_dt)
        sql = _sql_upsert_feat_window(w_from, w_to)
        cursor.execute(sql)
        row = cursor.fetchone()
        up = int(row[0]) if row and row[0] is not None else 0
        total += up
        print(
            f"[{DAG_ID}] chunk [{w_from.isoformat()}..{w_to.isoformat()}) upserted={up}")
        t = w_to

    print(f"[{DAG_ID}] DONE upserted_total={total}")


default_args: dict[str, Any] = {
    "owner": "okx-data",
    "retries": CFG.retries,
    "retry_delay": timedelta(seconds=CFG.retry_delay_sec),
    "execution_timeout": timedelta(seconds=CFG.execution_timeout_sec),
}

with DAG(
    dag_id=DAG_ID,
    description="OKX ETL: hybrid 10ms features from trades + hold-last L10 snapshot",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=TAGS,
) as dag:
    PythonOperator(task_id="sync", python_callable=run_sync)
