"""Snapshot-aligned validation of reconstructed L10 vs a real snapshot.

Prices are compared exactly (raw is float8, no extra rounding).
Sizes use a small absolute tolerance.

Mismatch vs snapshot B is ex-post: it is recorded here, not written
back onto already-emitted 100ms samples.
"""

from __future__ import annotations

from okx.orderbook.book import OrderBook
from okx.orderbook.metrics import depth_volume, signed_imbalance
from okx.orderbook.models import (
    SIDE_ASK,
    SIDE_BID,
    LevelMismatch,
    Snapshot,
    TOP_N,
    ValidationReport,
)

L5_N = 5


def snapshot_top_n(
    snap: Snapshot, n: int = TOP_N
) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
    bids: dict[float, float] = {}
    asks: dict[float, float] = {}
    for lvl in snap.levels:
        if lvl.size <= 0.0:
            continue
        if lvl.side == SIDE_BID:
            bids[lvl.price] = lvl.size
        elif lvl.side == SIDE_ASK:
            asks[lvl.price] = lvl.size
    bid_lvls = sorted(bids.items(), key=lambda x: x[0], reverse=True)[:n]
    ask_lvls = sorted(asks.items(), key=lambda x: x[0])[:n]
    return bid_lvls, ask_lvls


def _price_equal(a: float | None, b: float | None) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return a == b


def _size_equal(a: float | None, b: float | None, tol: float) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return abs(a - b) <= tol


def _level_at(
    levels: list[tuple[float, float]], i: int
) -> tuple[float | None, float | None]:
    if i >= len(levels):
        return None, None
    return levels[i][0], levels[i][1]


def _level_ratios(
    recon: list[tuple[float, float]],
    actual: list[tuple[float, float]],
    n: int,
    size_tol: float,
) -> tuple[float, float, float | None]:
    """Return (price_match_ratio, size_match_ratio, max_abs_price_diff)."""
    compared = 0
    px_ok = 0
    sz_ok = 0
    max_diff: float | None = None
    for i in range(n):
        rpx, rsz = _level_at(recon, i)
        apx, asz = _level_at(actual, i)
        if rpx is None and apx is None and rsz is None and asz is None:
            continue
        compared += 1
        if _price_equal(rpx, apx):
            px_ok += 1
        if _size_equal(rsz, asz, size_tol):
            sz_ok += 1
        if rpx is not None and apx is not None:
            diff = abs(rpx - apx)
            max_diff = diff if max_diff is None else max(max_diff, diff)
    if compared == 0:
        return 1.0, 1.0, None
    return px_ok / compared, sz_ok / compared, max_diff


def _mismatches(
    recon: list[tuple[float, float]],
    actual: list[tuple[float, float]],
    side: str,
    n: int,
    size_tol: float,
) -> list[LevelMismatch]:
    out: list[LevelMismatch] = []
    for i in range(n):
        rpx, rsz = _level_at(recon, i)
        apx, asz = _level_at(actual, i)
        if rpx is None and apx is None and rsz is None and asz is None:
            continue
        px_ok = _price_equal(rpx, apx)
        sz_ok = _size_equal(rsz, asz, size_tol)
        if px_ok and sz_ok:
            continue
        px_err = abs(rpx - apx) if rpx is not None and apx is not None else None
        sz_err = abs(rsz - asz) if rsz is not None and asz is not None else None
        out.append(
            LevelMismatch(
                side=side,
                level=i + 1,
                recon_px=rpx,
                actual_px=apx,
                recon_sz=rsz,
                actual_sz=asz,
                price_match=px_ok,
                size_match=sz_ok,
                px_abs_err=px_err,
                sz_abs_err=sz_err,
            )
        )
    return out


def validate_against_snapshot(
    book: OrderBook,
    snap: Snapshot,
    *,
    source_snapshot_id: str,
    source_snapshot_ts_event_ms: int,
    number_updates_applied: int,
    interval_duration_ms: int,
    size_tol: float = 1e-9,
) -> ValidationReport:
    recon_bids, recon_asks = book.top_n(TOP_N)
    actual_bids, actual_asks = snapshot_top_n(snap, TOP_N)

    bid_px_r5, bid_sz_r5, _ = _level_ratios(recon_bids, actual_bids, L5_N, size_tol)
    ask_px_r5, ask_sz_r5, _ = _level_ratios(recon_asks, actual_asks, L5_N, size_tol)
    bid_px_r, bid_sz_r, bid_diff = _level_ratios(
        recon_bids, actual_bids, TOP_N, size_tol
    )
    ask_px_r, ask_sz_r, ask_diff = _level_ratios(
        recon_asks, actual_asks, TOP_N, size_tol
    )

    rec_bb = recon_bids[0][0] if recon_bids else None
    rec_ba = recon_asks[0][0] if recon_asks else None
    rec_bb_sz = recon_bids[0][1] if recon_bids else None
    rec_ba_sz = recon_asks[0][1] if recon_asks else None
    act_bb = actual_bids[0][0] if actual_bids else None
    act_ba = actual_asks[0][0] if actual_asks else None
    act_bb_sz = actual_bids[0][1] if actual_bids else None
    act_ba_sz = actual_asks[0][1] if actual_asks else None

    l1_bid_px = _price_equal(rec_bb, act_bb)
    l1_ask_px = _price_equal(rec_ba, act_ba)
    l1_bid_sz = _size_equal(rec_bb_sz, act_bb_sz, size_tol)
    l1_ask_sz = _size_equal(rec_ba_sz, act_ba_sz, size_tol)

    is_match = (
        l1_bid_px
        and l1_ask_px
        and l1_bid_sz
        and l1_ask_sz
        and bid_px_r == 1.0
        and ask_px_r == 1.0
        and bid_sz_r == 1.0
        and ask_sz_r == 1.0
    )

    rec_bid_l5 = depth_volume(recon_bids, L5_N)
    rec_ask_l5 = depth_volume(recon_asks, L5_N)
    rec_bid_l10 = depth_volume(recon_bids, TOP_N)
    rec_ask_l10 = depth_volume(recon_asks, TOP_N)
    act_bid_l5 = depth_volume(actual_bids, L5_N)
    act_ask_l5 = depth_volume(actual_asks, L5_N)
    act_bid_l10 = depth_volume(actual_bids, TOP_N)
    act_ask_l10 = depth_volume(actual_asks, TOP_N)
    rec_bid_l1 = depth_volume(recon_bids, 1)
    rec_ask_l1 = depth_volume(recon_asks, 1)
    act_bid_l1 = depth_volume(actual_bids, 1)
    act_ask_l1 = depth_volume(actual_asks, 1)

    mismatched = _mismatches(
        recon_bids, actual_bids, "bid", TOP_N, size_tol
    ) + _mismatches(recon_asks, actual_asks, "ask", TOP_N, size_tol)

    return ValidationReport(
        validation_snapshot_id=snap.snapshot_id,
        validation_ts_event_ms=snap.ts_event_ms,
        source_snapshot_id=source_snapshot_id,
        source_snapshot_ts_event_ms=source_snapshot_ts_event_ms,
        l1_bid_price_match=l1_bid_px,
        l1_ask_price_match=l1_ask_px,
        l1_bid_size_match=l1_bid_sz,
        l1_ask_size_match=l1_ask_sz,
        l5_bid_price_match_ratio=bid_px_r5,
        l5_ask_price_match_ratio=ask_px_r5,
        l5_bid_size_match_ratio=bid_sz_r5,
        l5_ask_size_match_ratio=ask_sz_r5,
        l10_bid_price_match_ratio=bid_px_r,
        l10_ask_price_match_ratio=ask_px_r,
        l10_bid_size_match_ratio=bid_sz_r,
        l10_ask_size_match_ratio=ask_sz_r,
        reconstructed_best_bid=rec_bb,
        actual_best_bid=act_bb,
        reconstructed_best_ask=rec_ba,
        actual_best_ask=act_ba,
        max_bid_price_diff=bid_diff,
        max_ask_price_diff=ask_diff,
        reconstructed_imbalance_l1=signed_imbalance(rec_bid_l1, rec_ask_l1),
        actual_imbalance_l1=signed_imbalance(act_bid_l1, act_ask_l1),
        reconstructed_imbalance_l5=signed_imbalance(rec_bid_l5, rec_ask_l5),
        actual_imbalance_l5=signed_imbalance(act_bid_l5, act_ask_l5),
        reconstructed_imbalance_l10=signed_imbalance(rec_bid_l10, rec_ask_l10),
        actual_imbalance_l10=signed_imbalance(act_bid_l10, act_ask_l10),
        reconstructed_bid_volume_l5=rec_bid_l5,
        actual_bid_volume_l5=act_bid_l5,
        reconstructed_ask_volume_l5=rec_ask_l5,
        actual_ask_volume_l5=act_ask_l5,
        reconstructed_bid_volume_l10=rec_bid_l10,
        actual_bid_volume_l10=act_bid_l10,
        reconstructed_ask_volume_l10=rec_ask_l10,
        actual_ask_volume_l10=act_ask_l10,
        number_updates_applied=number_updates_applied,
        interval_duration_ms=interval_duration_ms,
        is_match=is_match,
        mismatched_levels=mismatched,
    )
