"""In-memory full-depth order book.

Internal state is not truncated to L10: raw updates may change levels
deeper than 10, and those levels can later enter the top of book.
L10 is derived only at sampling / validation time.
"""

from __future__ import annotations

from okx.orderbook.models import (
    DELETE_SIZE,
    SIDE_ASK,
    SIDE_BID,
    Snapshot,
    TOP_N,
    Update,
)


class OrderBook:
    """Mutable reconstructed book: price -> size on each side."""

    __slots__ = (
        "bids",
        "asks",
        "snapshot_id",
        "snapshot_ts_ms",
        "last_update_ts_ms",
        "updates_since_snapshot",
    )

    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.snapshot_id: str | None = None
        self.snapshot_ts_ms: int | None = None
        self.last_update_ts_ms: int | None = None
        self.updates_since_snapshot: int = 0

    def clear(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.snapshot_id = None
        self.snapshot_ts_ms = None
        self.last_update_ts_ms = None
        self.updates_since_snapshot = 0

    def apply_snapshot(self, snap: Snapshot) -> None:
        self.bids.clear()
        self.asks.clear()
        for lvl in snap.levels:
            self._set_level(lvl.side, lvl.price, lvl.size)
        self.snapshot_id = snap.snapshot_id
        self.snapshot_ts_ms = snap.ts_event_ms
        self.last_update_ts_ms = snap.ts_event_ms
        self.updates_since_snapshot = 0

    def apply_update(self, upd: Update) -> None:
        for price, size in upd.bids:
            self._set_level(SIDE_BID, price, size)
        for price, size in upd.asks:
            self._set_level(SIDE_ASK, price, size)
        self.last_update_ts_ms = upd.ts_event_ms
        self.updates_since_snapshot += 1

    def _set_level(self, side: int, price: float, size: float) -> None:
        if side == SIDE_BID:
            book = self.bids
        elif side == SIDE_ASK:
            book = self.asks
        else:
            return
        if size <= DELETE_SIZE:
            book.pop(price, None)
        else:
            book[price] = size

    def top_n(
        self, n: int = TOP_N
    ) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]
        asks = sorted(self.asks.items(), key=lambda x: x[0])[:n]
        return bids, asks

    def best_bid(self) -> float | None:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> float | None:
        return min(self.asks) if self.asks else None

    def best_bid_size(self) -> float | None:
        px = self.best_bid()
        return None if px is None else self.bids[px]

    def best_ask_size(self) -> float | None:
        px = self.best_ask()
        return None if px is None else self.asks[px]

    def is_crossed(self) -> bool:
        bid = self.best_bid()
        ask = self.best_ask()
        if bid is None or ask is None:
            return False
        return bid >= ask

    def has_l1(self) -> bool:
        return bool(self.bids) and bool(self.asks)
