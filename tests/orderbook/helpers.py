from __future__ import annotations

from okx.orderbook.models import (
    SIDE_ASK,
    SIDE_BID,
    Snapshot,
    SnapshotLevel,
    Update,
)

INST = "BTC-USDT-SWAP"
SID_A = "00000000-0000-0000-0000-00000000000a"
SID_B = "00000000-0000-0000-0000-00000000000b"


def snapshot(
    ts_ms: int,
    *,
    snapshot_id: str = SID_A,
    bids: list[tuple[float, float]] | None = None,
    asks: list[tuple[float, float]] | None = None,
    ingest_ms: int | None = None,
) -> Snapshot:
    if bids is None:
        bids = [(100.0 - i, 10.0 - i if i < 10 else 0.5) for i in range(11)]
    if asks is None:
        asks = [(101.0 + i, 10.0 - i if i < 10 else 0.5) for i in range(11)]
    levels: list[SnapshotLevel] = []
    for i, (px, sz) in enumerate(bids, start=1):
        levels.append(SnapshotLevel(SIDE_BID, px, sz, i))
    for i, (px, sz) in enumerate(asks, start=1):
        levels.append(SnapshotLevel(SIDE_ASK, px, sz, i))
    return Snapshot(
        snapshot_id=snapshot_id,
        inst_id=INST,
        ts_event_ms=ts_ms,
        ts_ingest_ms=ts_ms if ingest_ms is None else ingest_ms,
        levels=levels,
    )


def update(
    ts_ms: int,
    *,
    bids: list[tuple[float, float]] | None = None,
    asks: list[tuple[float, float]] | None = None,
    ingest_ms: int | None = None,
) -> Update:
    return Update(
        inst_id=INST,
        ts_event_ms=ts_ms,
        ts_ingest_ms=ts_ms if ingest_ms is None else ingest_ms,
        bids=list(bids or []),
        asks=list(asks or []),
    )
