"""TimescaleDB IO for L10 100ms reconstruction. Cursor-based, no Airflow."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

try:
    from psycopg2.extras import execute_values
except ImportError:  # pragma: no cover - Airflow worker has psycopg2
    execute_values = None

from okx.orderbook.models import (
    SampledRow,
    Snapshot,
    SnapshotLevel,
    Update,
    ValidationReport,
    parse_delta,
)
from okx.orderbook.schema import (
    FACT_COLUMNS,
    FACT_TABLE,
    FACT_UPDATE_COLUMNS,
    VALIDATION_TABLE,
)

INSERT_BATCH_SIZE = 5_000

SQL_FACT_UPSERT = f"""
INSERT INTO {FACT_TABLE} ({", ".join(FACT_COLUMNS)})
VALUES %s
ON CONFLICT (inst_id, ts_event) DO UPDATE SET
{chr(10).join(f"    {c} = EXCLUDED.{c}," for c in FACT_UPDATE_COLUMNS).rstrip(",")}
"""

VALIDATION_COLUMNS = (
    "inst_id",
    "validation_ts_event",
    "validation_ts_event_ms",
    "validation_snapshot_id",
    "source_snapshot_id",
    "source_snapshot_ts_event_ms",
    "l1_bid_price_match",
    "l1_ask_price_match",
    "l1_bid_size_match",
    "l1_ask_size_match",
    "l10_bid_price_match_ratio",
    "l10_ask_price_match_ratio",
    "l10_bid_size_match_ratio",
    "l10_ask_size_match_ratio",
    "reconstructed_best_bid",
    "actual_best_bid",
    "reconstructed_best_ask",
    "actual_best_ask",
    "max_bid_price_diff",
    "max_ask_price_diff",
    "number_updates_applied",
    "interval_duration_ms",
    "is_match",
)

VALIDATION_UPDATE_COLUMNS = tuple(
    c
    for c in VALIDATION_COLUMNS
    if c not in {"inst_id", "validation_snapshot_id", "validation_ts_event"}
)

SQL_VALIDATION_UPSERT = f"""
INSERT INTO {VALIDATION_TABLE} ({", ".join(VALIDATION_COLUMNS)})
VALUES %s
ON CONFLICT (inst_id, validation_snapshot_id, validation_ts_event)
DO UPDATE SET
{chr(10).join(f"    {c} = EXCLUDED.{c}," for c in VALIDATION_UPDATE_COLUMNS).rstrip(",")}
"""

SQL_WATERMARKS = f"""
SELECT inst_id, max(ts_event_ms)
FROM {FACT_TABLE}
GROUP BY inst_id;
"""

SQL_RAW_EVENT_BOUNDS = """
SELECT min(m), max(m)
FROM (
    SELECT min(ts_event_ms) AS m FROM okx_raw.orderbook_snapshots
    UNION ALL
    SELECT min(ts_event_ms) FROM okx_raw.orderbook_updates
    UNION ALL
    SELECT max(ts_event_ms) FROM okx_raw.orderbook_snapshots
    UNION ALL
    SELECT max(ts_event_ms) FROM okx_raw.orderbook_updates
) s
WHERE m IS NOT NULL;
"""

SQL_INSTIDS = """
SELECT DISTINCT instid
FROM okx_raw.orderbook_snapshots
WHERE ts_event_ms >= %s AND ts_event_ms < %s
ORDER BY 1;
"""

SQL_ANCHOR = """
SELECT snapshot_id, ts_event_ms
FROM okx_raw.orderbook_snapshots
WHERE instid = %s
  AND ts_event_ms <= %s
  AND ts_event_ms >= %s
ORDER BY ts_event_ms DESC, ts_ingest_ms DESC
LIMIT 1;
"""

SQL_SNAPSHOT_ROWS = """
SELECT
    snapshot_id, instid, ts_event_ms, ts_ingest_ms,
    side, price, size, level
FROM okx_raw.orderbook_snapshots
WHERE instid = %s
  AND ts_event_ms >= %s
  AND ts_event_ms < %s
ORDER BY ts_event_ms, ts_ingest_ms, snapshot_id, side, level;
"""

SQL_UPDATES = """
SELECT instid, ts_event_ms, ts_ingest_ms, bids_delta, asks_delta, checksum
FROM okx_raw.orderbook_updates
WHERE instid = %s
  AND ts_event_ms > %s
  AND ts_event_ms < %s
ORDER BY ts_event_ms, ts_ingest_ms;
"""

SQL_GAP_DIAGNOSTICS = f"""
WITH ordered AS (
    SELECT
        inst_id,
        ts_event_ms,
        ts_event_ms - lag(ts_event_ms) OVER (
            PARTITION BY inst_id ORDER BY ts_event_ms
        ) AS delta_ms
    FROM {FACT_TABLE}
    WHERE inst_id = %s
      AND ts_event_ms >= %s
      AND ts_event_ms < %s
),
gaps AS (
    SELECT delta_ms FROM ordered WHERE delta_ms IS NOT NULL
)
SELECT
    (SELECT count(*) FROM {FACT_TABLE}
     WHERE inst_id = %s AND ts_event_ms >= %s AND ts_event_ms < %s) AS row_cnt,
    (SELECT percentile_cont(0.50) WITHIN GROUP (ORDER BY delta_ms) FROM gaps) AS p50,
    (SELECT percentile_cont(0.90) WITHIN GROUP (ORDER BY delta_ms) FROM gaps) AS p90,
    (SELECT percentile_cont(0.99) WITHIN GROUP (ORDER BY delta_ms) FROM gaps) AS p99,
    (SELECT max(delta_ms) FROM gaps) AS max_delta,
    (SELECT count(*) FROM gaps WHERE delta_ms <> 100) AS non_100_cnt;
"""


def _ms_to_dt(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


def sampled_row_values(row: SampledRow) -> tuple[Any, ...]:
    vals: list[Any] = [
        row.inst_id,
        _ms_to_dt(row.ts_event_ms),
        row.ts_event_ms,
        row.source_snapshot_id,
        row.source_snapshot_ts_event_ms,
        row.last_update_ts_event_ms,
        row.last_update_age_ms,
    ]
    for i in range(10):
        vals.append(row.bid_px[i])
        vals.append(row.bid_sz[i])
    for i in range(10):
        vals.append(row.ask_px[i])
        vals.append(row.ask_sz[i])
    vals.extend(
        [
            row.mid_px,
            row.spread_px,
            row.bid_volume_l1,
            row.bid_volume_l5,
            row.bid_volume_l10,
            row.ask_volume_l1,
            row.ask_volume_l5,
            row.ask_volume_l10,
            row.total_volume_l1,
            row.total_volume_l5,
            row.total_volume_l10,
            row.imbalance_l1,
            row.imbalance_l5,
            row.imbalance_l10,
            row.imbalance_weighted_l10,
            row.microprice,
            row.microprice_delta,
            row.bid_px_size_l1,
            row.bid_px_size_l5,
            row.bid_px_size_l10,
            row.ask_px_size_l1,
            row.ask_px_size_l5,
            row.ask_px_size_l10,
            row.is_valid,
            row.quality_code,
        ]
    )
    return tuple(vals)


def validation_values(inst_id: str, report: ValidationReport) -> tuple[Any, ...]:
    return (
        inst_id,
        _ms_to_dt(report.validation_ts_event_ms),
        report.validation_ts_event_ms,
        report.validation_snapshot_id,
        report.source_snapshot_id,
        report.source_snapshot_ts_event_ms,
        report.l1_bid_price_match,
        report.l1_ask_price_match,
        report.l1_bid_size_match,
        report.l1_ask_size_match,
        report.l10_bid_price_match_ratio,
        report.l10_ask_price_match_ratio,
        report.l10_bid_size_match_ratio,
        report.l10_ask_size_match_ratio,
        report.reconstructed_best_bid,
        report.actual_best_bid,
        report.reconstructed_best_ask,
        report.actual_best_ask,
        report.max_bid_price_diff,
        report.max_ask_price_diff,
        report.number_updates_applied,
        report.interval_duration_ms,
        report.is_match,
    )


def rows_to_snapshots(rows: Iterable[tuple]) -> list[Snapshot]:
    by_id: dict[str, Snapshot] = {}
    order: list[str] = []
    for r in rows:
        sid = str(r[0])
        snap = by_id.get(sid)
        if snap is None:
            snap = Snapshot(
                snapshot_id=sid,
                inst_id=str(r[1]),
                ts_event_ms=int(r[2]),
                ts_ingest_ms=int(r[3]),
                levels=[],
            )
            by_id[sid] = snap
            order.append(sid)
        elif int(r[3]) > snap.ts_ingest_ms:
            snap.ts_ingest_ms = int(r[3])
        snap.levels.append(
            SnapshotLevel(
                side=int(r[4]),
                price=float(r[5]),
                size=float(r[6]),
                level=int(r[7]) if r[7] is not None else 0,
            )
        )
    return [by_id[s] for s in order]


def rows_to_updates(rows: Iterable[tuple]) -> list[Update]:
    out: list[Update] = []
    for r in rows:
        out.append(
            Update(
                inst_id=str(r[0]),
                ts_event_ms=int(r[1]),
                ts_ingest_ms=int(r[2]),
                bids=parse_delta(r[3]),
                asks=parse_delta(r[4]),
                checksum=int(r[5]) if r[5] is not None else None,
            )
        )
    return out


def get_watermarks(cursor) -> dict[str, int]:
    cursor.execute(SQL_WATERMARKS)
    return {str(r[0]): int(r[1]) for r in cursor.fetchall() if r and r[0] is not None}


def get_raw_event_bounds(cursor) -> tuple[int | None, int | None]:
    cursor.execute(SQL_RAW_EVENT_BOUNDS)
    row = cursor.fetchone()
    if not row or row[0] is None or row[1] is None:
        return None, None
    return int(row[0]), int(row[1])


def get_instids(cursor, from_ms: int, to_ms: int) -> list[str]:
    cursor.execute(SQL_INSTIDS, (from_ms, to_ms))
    return [str(r[0]) for r in cursor.fetchall() if r and r[0] is not None]


def fetch_anchor(
    cursor, inst_id: str, from_ms: int, lookback_ms: int
) -> tuple[str, int] | None:
    cursor.execute(SQL_ANCHOR, (inst_id, from_ms, from_ms - lookback_ms))
    row = cursor.fetchone()
    if not row or row[0] is None:
        return None
    return str(row[0]), int(row[1])


def fetch_snapshots(cursor, inst_id: str, from_ms: int, to_ms: int) -> list[Snapshot]:
    cursor.execute(SQL_SNAPSHOT_ROWS, (inst_id, from_ms, to_ms))
    return rows_to_snapshots(cursor.fetchall())


def fetch_updates(cursor, inst_id: str, from_ms: int, to_ms: int) -> list[Update]:
    cursor.execute(SQL_UPDATES, (inst_id, from_ms, to_ms))
    return rows_to_updates(cursor.fetchall())


def upsert_samples(
    cursor, rows: list[SampledRow], page_size: int = INSERT_BATCH_SIZE
) -> int:
    if not rows:
        return 0
    if execute_values is None:
        raise RuntimeError("psycopg2 is required for bulk upsert")
    payload = [sampled_row_values(r) for r in rows]
    execute_values(
        cursor,
        SQL_FACT_UPSERT,
        payload,
        page_size=page_size,
    )
    return len(payload)


def upsert_validations(
    cursor,
    inst_id: str,
    reports: list[ValidationReport],
    page_size: int = INSERT_BATCH_SIZE,
) -> int:
    if not reports:
        return 0
    if execute_values is None:
        raise RuntimeError("psycopg2 is required for bulk upsert")
    payload = [validation_values(inst_id, r) for r in reports]
    execute_values(
        cursor,
        SQL_VALIDATION_UPSERT,
        payload,
        page_size=page_size,
    )
    return len(payload)


def fetch_gap_diagnostics(cursor, inst_id: str, from_ms: int, to_ms: int) -> tuple:
    cursor.execute(
        SQL_GAP_DIAGNOSTICS,
        (inst_id, from_ms, to_ms, inst_id, from_ms, to_ms),
    )
    return cursor.fetchone()
