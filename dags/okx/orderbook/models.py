"""Domain models and constants for L10 order book reconstruction.

RAW encoding (confirmed by okx_raw_to_core_orderbook_l10_snapshot):
    side 1 = bid, side 2 = ask

RAW update JSON (confirmed by okx_core_orderbook_update_level):
    bids_delta / asks_delta = JSON array of objects
    [{"price": <float>, "size": <float>}, ...]

Checksum is stored on raw updates but is not validated here.
OKX checksum semantics are not implemented in this repository.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any

SIDE_BID = 1
SIDE_ASK = 2

SAMPLE_STEP_MS = 100
TOP_N = 10

# size == 0 means delete the price level (OKX books incremental protocol)
DELETE_SIZE = 0.0


class QualityCode(IntEnum):
    VALID = 0
    NO_ANCHOR_SNAPSHOT = 1
    UPDATE_GAP = 2
    CROSSED_BOOK = 3
    # Ex-post only. Written to orderbook_reconstruction_validation
    # (is_match=false), never to fact_orderbook_l10_100ms rows.
    SNAPSHOT_MISMATCH = 4


QUALITY_NAME = {
    QualityCode.VALID: "VALID",
    QualityCode.NO_ANCHOR_SNAPSHOT: "NO_ANCHOR_SNAPSHOT",
    QualityCode.UPDATE_GAP: "UPDATE_GAP",
    QualityCode.CROSSED_BOOK: "CROSSED_BOOK",
    QualityCode.SNAPSHOT_MISMATCH: "SNAPSHOT_MISMATCH",
}


@dataclass(slots=True)
class SnapshotLevel:
    side: int
    price: float
    size: float
    level: int = 0


@dataclass(slots=True)
class Snapshot:
    snapshot_id: str
    inst_id: str
    ts_event_ms: int
    ts_ingest_ms: int
    levels: list[SnapshotLevel] = field(default_factory=list)


@dataclass(slots=True)
class Update:
    inst_id: str
    ts_event_ms: int
    ts_ingest_ms: int
    bids: list[tuple[float, float]] = field(default_factory=list)
    asks: list[tuple[float, float]] = field(default_factory=list)
    checksum: int | None = None


@dataclass(slots=True)
class BookEvent:
    ts_event_ms: int
    ts_ingest_ms: int
    kind: str  # "snapshot" | "update"
    seq: int
    snapshot: Snapshot | None = None
    update: Update | None = None


@dataclass(slots=True)
class LevelMismatch:
    side: str
    level: int
    recon_px: float | None
    actual_px: float | None
    recon_sz: float | None
    actual_sz: float | None
    price_match: bool
    size_match: bool
    px_abs_err: float | None
    sz_abs_err: float | None


@dataclass(slots=True)
class ValidationReport:
    validation_snapshot_id: str
    validation_ts_event_ms: int
    source_snapshot_id: str
    source_snapshot_ts_event_ms: int
    l1_bid_price_match: bool
    l1_ask_price_match: bool
    l1_bid_size_match: bool
    l1_ask_size_match: bool
    l5_bid_price_match_ratio: float
    l5_ask_price_match_ratio: float
    l5_bid_size_match_ratio: float
    l5_ask_size_match_ratio: float
    l10_bid_price_match_ratio: float
    l10_ask_price_match_ratio: float
    l10_bid_size_match_ratio: float
    l10_ask_size_match_ratio: float
    reconstructed_best_bid: float | None
    actual_best_bid: float | None
    reconstructed_best_ask: float | None
    actual_best_ask: float | None
    max_bid_price_diff: float | None
    max_ask_price_diff: float | None
    reconstructed_imbalance_l1: float | None
    actual_imbalance_l1: float | None
    reconstructed_imbalance_l5: float | None
    actual_imbalance_l5: float | None
    reconstructed_imbalance_l10: float | None
    actual_imbalance_l10: float | None
    reconstructed_bid_volume_l5: float | None
    actual_bid_volume_l5: float | None
    reconstructed_ask_volume_l5: float | None
    actual_ask_volume_l5: float | None
    reconstructed_bid_volume_l10: float | None
    actual_bid_volume_l10: float | None
    reconstructed_ask_volume_l10: float | None
    actual_ask_volume_l10: float | None
    number_updates_applied: int
    interval_duration_ms: int
    is_match: bool
    mismatched_levels: list[LevelMismatch] = field(default_factory=list)


@dataclass(slots=True)
class SampledRow:
    inst_id: str
    ts_event_ms: int
    source_snapshot_id: str
    source_snapshot_ts_event_ms: int
    last_update_ts_event_ms: int
    last_update_age_ms: int
    bid_px: list[float | None]
    bid_sz: list[float | None]
    ask_px: list[float | None]
    ask_sz: list[float | None]
    mid_px: float | None
    spread_px: float | None
    bid_volume_l1: float | None
    bid_volume_l5: float | None
    bid_volume_l10: float | None
    ask_volume_l1: float | None
    ask_volume_l5: float | None
    ask_volume_l10: float | None
    total_volume_l1: float | None
    total_volume_l5: float | None
    total_volume_l10: float | None
    imbalance_l1: float | None
    imbalance_l5: float | None
    imbalance_l10: float | None
    imbalance_weighted_l10: float | None
    microprice: float | None
    microprice_delta: float | None
    bid_px_size_l1: float | None
    bid_px_size_l5: float | None
    bid_px_size_l10: float | None
    ask_px_size_l1: float | None
    ask_px_size_l5: float | None
    ask_px_size_l10: float | None
    is_valid: bool
    quality_code: int


@dataclass
class ReconstructionResult:
    inst_id: str
    from_ms: int
    to_ms: int
    rows: list[SampledRow] = field(default_factory=list)
    validations: list[ValidationReport] = field(default_factory=list)
    snapshot_count: int = 0
    update_count: int = 0
    skipped_no_anchor: bool = False


def parse_delta(raw: Any) -> list[tuple[float, float]]:
    """Parse bids_delta / asks_delta into (price, size) pairs.

    Confirmed on-disk format (okx_core_orderbook_update_level):
        JSON array of objects with keys ``price`` and ``size``.
    """
    if raw is None:
        return []
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            return []
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    if not isinstance(raw, list):
        return []

    out: list[tuple[float, float]] = []
    for elem in raw:
        if not isinstance(elem, dict):
            continue
        px = elem.get("price")
        sz = elem.get("size")
        if px is None or sz is None:
            continue
        try:
            out.append((float(px), float(sz)))
        except (TypeError, ValueError):
            continue
    return out


def pad_levels(
    levels: list[tuple[float, float]], n: int = TOP_N
) -> tuple[list[float | None], list[float | None]]:
    px: list[float | None] = [None] * n
    sz: list[float | None] = [None] * n
    for i, (price, size) in enumerate(levels[:n]):
        px[i] = price
        sz[i] = size
    return px, sz
