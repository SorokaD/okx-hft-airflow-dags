"""Point-in-time microstructure metrics from a single L10 state.

No tick-size / ctVal instrument metadata exists in this repository,
so spread_ticks and USD notional are not computed.

price * size is stored as bid_px_size_* / ask_px_size_* (not USD).
"""

from __future__ import annotations

from okx.orderbook.models import SampledRow, TOP_N, pad_levels

# Closer-to-touch levels get higher weight: L1=1.0 ... L10=0.1
WEIGHTED_IMBALANCE_WEIGHTS = tuple(1.0 - 0.1 * i for i in range(TOP_N))


def depth_volume(levels: list[tuple[float, float]], n: int) -> float | None:
    chunk = levels[:n]
    if not chunk:
        return None
    return float(sum(sz for _, sz in chunk))


def _sum_sz(levels: list[tuple[float, float]], n: int) -> float | None:
    return depth_volume(levels, n)


def _sum_px_sz(levels: list[tuple[float, float]], n: int) -> float | None:
    chunk = levels[:n]
    if not chunk:
        return None
    return float(sum(px * sz for px, sz in chunk))


def signed_imbalance(bid_vol: float | None, ask_vol: float | None) -> float | None:
    if bid_vol is None and ask_vol is None:
        return None
    b = 0.0 if bid_vol is None else bid_vol
    a = 0.0 if ask_vol is None else ask_vol
    denom = b + a
    if denom == 0.0:
        return None
    return (b - a) / denom


def _imbalance(bid_vol: float | None, ask_vol: float | None) -> float | None:
    return signed_imbalance(bid_vol, ask_vol)


def _weighted_imbalance(
    bids: list[tuple[float, float]],
    asks: list[tuple[float, float]],
) -> float | None:
    wb = sum(
        w * sz for w, (_, sz) in zip(WEIGHTED_IMBALANCE_WEIGHTS, bids, strict=False)
    )
    wa = sum(
        w * sz for w, (_, sz) in zip(WEIGHTED_IMBALANCE_WEIGHTS, asks, strict=False)
    )
    denom = wb + wa
    if denom == 0.0:
        return None
    return (wb - wa) / denom


def build_sampled_row(
    *,
    inst_id: str,
    ts_event_ms: int,
    source_snapshot_id: str,
    source_snapshot_ts_event_ms: int,
    last_update_ts_event_ms: int,
    bids: list[tuple[float, float]],
    asks: list[tuple[float, float]],
    is_valid: bool,
    quality_code: int,
) -> SampledRow:
    bid_px, bid_sz = pad_levels(bids, TOP_N)
    ask_px, ask_sz = pad_levels(asks, TOP_N)

    best_bid = bid_px[0]
    best_ask = ask_px[0]
    mid_px = None
    spread_px = None
    if best_bid is not None and best_ask is not None:
        mid_px = (best_bid + best_ask) / 2.0
        spread_px = best_ask - best_bid

    bid_l1 = _sum_sz(bids, 1)
    bid_l5 = _sum_sz(bids, 5)
    bid_l10 = _sum_sz(bids, 10)
    ask_l1 = _sum_sz(asks, 1)
    ask_l5 = _sum_sz(asks, 5)
    ask_l10 = _sum_sz(asks, 10)

    total_l1 = (
        None if bid_l1 is None and ask_l1 is None else (bid_l1 or 0.0) + (ask_l1 or 0.0)
    )
    total_l5 = (
        None if bid_l5 is None and ask_l5 is None else (bid_l5 or 0.0) + (ask_l5 or 0.0)
    )
    total_l10 = (
        None
        if bid_l10 is None and ask_l10 is None
        else (bid_l10 or 0.0) + (ask_l10 or 0.0)
    )

    microprice = None
    microprice_delta = None
    if (
        best_bid is not None
        and best_ask is not None
        and bid_sz[0] is not None
        and ask_sz[0] is not None
    ):
        denom = bid_sz[0] + ask_sz[0]
        if denom != 0.0:
            microprice = (best_ask * bid_sz[0] + best_bid * ask_sz[0]) / denom
            if mid_px is not None:
                microprice_delta = microprice - mid_px

    return SampledRow(
        inst_id=inst_id,
        ts_event_ms=ts_event_ms,
        source_snapshot_id=source_snapshot_id,
        source_snapshot_ts_event_ms=source_snapshot_ts_event_ms,
        last_update_ts_event_ms=last_update_ts_event_ms,
        last_update_age_ms=ts_event_ms - last_update_ts_event_ms,
        bid_px=bid_px,
        bid_sz=bid_sz,
        ask_px=ask_px,
        ask_sz=ask_sz,
        mid_px=mid_px,
        spread_px=spread_px,
        bid_volume_l1=bid_l1,
        bid_volume_l5=bid_l5,
        bid_volume_l10=bid_l10,
        ask_volume_l1=ask_l1,
        ask_volume_l5=ask_l5,
        ask_volume_l10=ask_l10,
        total_volume_l1=total_l1,
        total_volume_l5=total_l5,
        total_volume_l10=total_l10,
        imbalance_l1=_imbalance(bid_l1, ask_l1),
        imbalance_l5=_imbalance(bid_l5, ask_l5),
        imbalance_l10=_imbalance(bid_l10, ask_l10),
        imbalance_weighted_l10=_weighted_imbalance(bids, asks),
        microprice=microprice,
        microprice_delta=microprice_delta,
        bid_px_size_l1=_sum_px_sz(bids, 1),
        bid_px_size_l5=_sum_px_sz(bids, 5),
        bid_px_size_l10=_sum_px_sz(bids, 10),
        ask_px_size_l1=_sum_px_sz(asks, 1),
        ask_px_size_l5=_sum_px_sz(asks, 5),
        ask_px_size_l10=_sum_px_sz(asks, 10),
        is_valid=is_valid,
        quality_code=quality_code,
    )
