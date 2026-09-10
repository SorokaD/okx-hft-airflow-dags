from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from okx.common.etl_common import db_sanity_checks, log_diagnostics
from okx.common.utils import get_current_timestamp_ms
from okx.orderbook.models import QualityCode, ReconstructionResult
from okx.orderbook.reconstructor import reconstruct
from okx.orderbook.repository import (
    fetch_anchor,
    fetch_gap_diagnostics,
    fetch_snapshots,
    fetch_updates,
    get_instids,
    get_raw_event_bounds,
    get_watermarks,
    upsert_samples,
    upsert_validations,
)
from okx.orderbook.schema import FACT_TABLE, VALIDATION_TABLE, require_schema

# ============================================================
# 0) Project-wide constants
# ============================================================

CONN_ID = "timescaledb"
DB_NAME_EXPECTED = "okx_hft"

DAG_ID = "okx_core_build_orderbook_l10_100ms"
SCHEDULE = None  # запускается master DAG / вручную

TAGS = [
    "okx",
    "etl",
    "core",
    "timescaledb",
    "orderbook",
    "l10",
    "100ms",
]


# ============================================================
# 1) Config
# ============================================================


@dataclass(frozen=True)
class EtlConfig:
    raw_snapshots_fq: str = "okx_raw.orderbook_snapshots"
    raw_updates_fq: str = "okx_raw.orderbook_updates"
    core_table_fq: str = FACT_TABLE
    validation_table_fq: str = VALIDATION_TABLE

    sample_step_ms: int = 100
    overlap_minutes: int = 2
    # snapshots arrive ~every 30s; look back far enough for an anchor
    anchor_lookback_ms: int = 5 * 60 * 1000
    chunk_minutes: int = 15
    max_catchup_hours_per_run: int | None = 24
    update_gap_threshold_ms: int = 5_000
    insert_batch_size: int = 5_000

    statement_timeout_ms: int = 30 * 60 * 1000
    safety_lag_seconds: int = 5

    execution_timeout_sec: int = 2 * 60 * 60
    retries: int = 1
    retry_delay_sec: int = 120


CFG = EtlConfig()


# ============================================================
# 2) Helpers
# ============================================================


@dataclass
class _InstAcc:
    inst_id: str
    from_ms: int
    to_ms: int
    snapshot_count: int = 0
    update_count: int = 0
    n_rows: int = 0
    valid_rows: int = 0
    invalid_rows: int = 0
    crossed: int = 0
    max_age: int = 0
    skipped_no_anchor: bool = False
    validations: list[Any] = field(default_factory=list)

    def add(self, result: ReconstructionResult) -> None:
        self.snapshot_count += result.snapshot_count
        self.update_count += result.update_count
        self.skipped_no_anchor = self.skipped_no_anchor or result.skipped_no_anchor
        self.validations.extend(result.validations)
        for row in result.rows:
            self.n_rows += 1
            if row.is_valid:
                self.valid_rows += 1
            else:
                self.invalid_rows += 1
            if row.quality_code == int(QualityCode.CROSSED_BOOK):
                self.crossed += 1
            if row.last_update_age_ms > self.max_age:
                self.max_age = row.last_update_age_ms


def _quality_report(acc: _InstAcc) -> dict[str, Any]:
    n_val = len(acc.validations)
    vals = acc.validations
    if n_val:
        l1_px = (
            sum(1 for v in vals if v.l1_bid_price_match and v.l1_ask_price_match)
            / n_val
        )
        l1_sz = (
            sum(1 for v in vals if v.l1_bid_size_match and v.l1_ask_size_match) / n_val
        )
        l10_px = (
            sum(
                (v.l10_bid_price_match_ratio + v.l10_ask_price_match_ratio) / 2.0
                for v in vals
            )
            / n_val
        )
        l10_sz = (
            sum(
                (v.l10_bid_size_match_ratio + v.l10_ask_size_match_ratio) / 2.0
                for v in vals
            )
            / n_val
        )
        l1_exact = (
            sum(
                1
                for v in vals
                if v.l1_bid_price_match
                and v.l1_ask_price_match
                and v.l1_bid_size_match
                and v.l1_ask_size_match
            )
            / n_val
        )
    else:
        l1_px = l1_sz = l10_px = l10_sz = l1_exact = None

    return {
        "instrument": acc.inst_id,
        "from_ms": acc.from_ms,
        "to_ms": acc.to_ms,
        "raw_snapshots": acc.snapshot_count,
        "raw_updates": acc.update_count,
        "reconstructed_100ms_rows": acc.n_rows,
        "valid_rows": acc.valid_rows,
        "invalid_rows": acc.invalid_rows,
        "validation_checkpoints": n_val,
        "l1_exact_match_pct": None if l1_exact is None else round(l1_exact * 100, 4),
        "l1_price_match_pct": None if l1_px is None else round(l1_px * 100, 4),
        "l1_size_match_pct": None if l1_sz is None else round(l1_sz * 100, 4),
        "l10_price_match_pct": None if l10_px is None else round(l10_px * 100, 4),
        "l10_size_match_pct": None if l10_sz is None else round(l10_sz * 100, 4),
        "crossed_book_count": acc.crossed,
        "max_last_update_age_ms": acc.max_age,
        "skipped_no_anchor": acc.skipped_no_anchor,
    }


def _process_chunk(
    cursor,
    inst_id: str,
    from_ms: int,
    to_ms: int,
) -> ReconstructionResult:
    anchor = fetch_anchor(cursor, inst_id, from_ms, CFG.anchor_lookback_ms)
    if anchor is None:
        empty = ReconstructionResult(
            inst_id=inst_id,
            from_ms=from_ms,
            to_ms=to_ms,
            skipped_no_anchor=True,
        )
        return empty

    _anchor_id, anchor_ts = anchor
    snapshots = fetch_snapshots(cursor, inst_id, anchor_ts, to_ms)
    updates = fetch_updates(cursor, inst_id, anchor_ts, to_ms)
    return reconstruct(
        inst_id=inst_id,
        snapshots=snapshots,
        updates=updates,
        from_ms=from_ms,
        to_ms=to_ms,
        sample_step_ms=CFG.sample_step_ms,
        update_gap_threshold_ms=CFG.update_gap_threshold_ms,
    )


# ============================================================
# 3) Main callable
# ============================================================


def run_sync() -> None:
    t0 = time.perf_counter()
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = True
    cursor = conn.cursor()

    db_sanity_checks(cursor, DB_NAME_EXPECTED)
    cursor.execute("SET statement_timeout = %s", (CFG.statement_timeout_ms,))
    require_schema(cursor)
    log_diagnostics(
        cursor,
        [CFG.raw_snapshots_fq, CFG.raw_updates_fq, CFG.core_table_fq],
    )

    src_min_ms, src_max_ms = get_raw_event_bounds(cursor)
    if src_min_ms is None or src_max_ms is None:
        print(f"[{DAG_ID}] SKIP: raw snapshots/updates are empty")
        return

    now_ms = get_current_timestamp_ms()
    safety_ms = CFG.safety_lag_seconds * 1000
    to_ms = min(src_max_ms, now_ms - safety_ms)

    watermarks = get_watermarks(cursor)
    overlap_ms = CFG.overlap_minutes * 60 * 1000
    catchup_ms = (
        None
        if CFG.max_catchup_hours_per_run is None
        else int(CFG.max_catchup_hours_per_run) * 3600 * 1000
    )

    global_from = src_min_ms
    if watermarks:
        global_from = min(watermarks.values()) - overlap_ms
        if global_from < src_min_ms:
            global_from = src_min_ms
    if catchup_ms is not None and to_ms - global_from > catchup_ms:
        global_from = to_ms - catchup_ms

    if global_from >= to_ms:
        print(
            f"[{DAG_ID}] up-to-date: window=[{global_from}..{to_ms}) -> nothing to do"
        )
        return

    scan_from = max(src_min_ms, global_from - CFG.anchor_lookback_ms)
    instids = get_instids(cursor, scan_from, to_ms)
    if not instids:
        print(f"[{DAG_ID}] SKIP: no instruments in window [{scan_from}..{to_ms})")
        return

    print(
        f"[{DAG_ID}] window=[{global_from}..{to_ms}) "
        f"src=[{src_min_ms}..{src_max_ms}] instids={instids} "
        f"watermarks={watermarks}"
    )

    chunk_ms = CFG.chunk_minutes * 60 * 1000
    inserted_total = 0
    validation_total = 0
    reports: list[dict[str, Any]] = []

    for inst_id in instids:
        wm = watermarks.get(inst_id)
        if wm is None:
            from_ms = global_from
        else:
            from_ms = wm - overlap_ms
            if from_ms < src_min_ms:
                from_ms = src_min_ms
        if from_ms >= to_ms:
            print(f"[{DAG_ID}] {inst_id} up-to-date wm={wm}")
            continue

        inst_rows = 0
        inst_vals = 0
        acc = _InstAcc(inst_id=inst_id, from_ms=from_ms, to_ms=to_ms)
        t = from_ms
        while t < to_ms:
            chunk_to = min(t + chunk_ms, to_ms)
            result = _process_chunk(cursor, inst_id, t, chunk_to)
            acc.add(result)
            if result.skipped_no_anchor:
                print(
                    f"[{DAG_ID}] {inst_id} chunk [{t}..{chunk_to}) "
                    f"NO_ANCHOR_SNAPSHOT — skip"
                )
            else:
                n_ins = upsert_samples(
                    cursor, result.rows, page_size=CFG.insert_batch_size
                )
                n_val = upsert_validations(
                    cursor,
                    inst_id,
                    result.validations,
                    page_size=CFG.insert_batch_size,
                )
                inst_rows += n_ins
                inst_vals += n_val
                print(
                    f"[{DAG_ID}] {inst_id} chunk [{t}..{chunk_to}) "
                    f"snapshots={result.snapshot_count} updates={result.update_count} "
                    f"sampled={n_ins} validations={n_val}"
                )
            t = chunk_to

        inserted_total += inst_rows
        validation_total += inst_vals
        report = _quality_report(acc)
        reports.append(report)
        print(f"[{DAG_ID}] QUALITY {report}")

        diag = fetch_gap_diagnostics(cursor, inst_id, from_ms, to_ms)
        if diag:
            print(
                f"[{DAG_ID}] DIAG {inst_id} rows={diag[0]} "
                f"gap_ms p50={diag[1]} p90={diag[2]} p99={diag[3]} "
                f"max={diag[4]} non_100={diag[5]}"
            )

    elapsed = time.perf_counter() - t0
    print(
        f"[{DAG_ID}] DONE inserted_total={inserted_total} "
        f"validations_total={validation_total} "
        f"execution_time_sec={elapsed:.3f} reports={reports}"
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
    description=(
        "OKX ETL: reconstruct full-depth order book from raw snapshots+"
        "updates and sample L10 every 100ms"
    ),
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=TAGS,
    doc_md="""
    ### okx_core_build_orderbook_l10_100ms

    Sequential reconstruction of the OKX order book from
    `okx_raw.orderbook_snapshots` + `okx_raw.orderbook_updates`, then
    100ms sampling into `okx_core.fact_orderbook_l10_100ms`.

    Reconstruction is **not** SQL `time_bucket` resampling.
    See `docs/orderbook_reconstruction.md`.
    """,
) as dag:
    PythonOperator(
        task_id="sync",
        python_callable=run_sync,
    )
