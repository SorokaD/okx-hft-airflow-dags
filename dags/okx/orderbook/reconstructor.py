"""Stateful snapshot-aligned order book reconstruction + 100ms sampling.

Algorithm
---------
1. Apply the first (anchor) snapshot as the authoritative book.
2. Apply incremental updates in chronological order.
3. On each 100ms grid point, sample TOP-10 of the current book.
4. On the next real snapshot: validate reconstructed L10 against it,
   then reset the book to the snapshot (errors do not accumulate).

Look-ahead leakage is forbidden: snapshot B never mutates samples
with ts_event_ms < B. It only validates the previous interval and
becomes the new state starting at B.

Sample ``is_valid`` / ``quality_code`` are causal: known at ts_event
(no L1, crossed book, update gap). Snapshot-B mismatch is ex-post and
is stored only in ``orderbook_reconstruction_validation``.

Ordering (no exchange sequence id exists on raw updates):
    ORDER BY ts_event_ms, kind(update before snapshot), ts_ingest_ms, seq

Collector periodic snapshots are stamped with the last applied update
ts, so same-millisecond deltas must be applied before validating
against that snapshot. The snapshot then resets authoritative state.

Same-timestamp limitation is documented in docs/orderbook_reconstruction.md.
"""

from __future__ import annotations

from okx.orderbook.book import OrderBook
from okx.orderbook.metrics import build_sampled_row
from okx.orderbook.models import (
    BookEvent,
    QualityCode,
    ReconstructionResult,
    SampledRow,
    Snapshot,
    Update,
)
from okx.orderbook.sampler import first_sample_ts
from okx.orderbook.validator import validate_against_snapshot


def _event_sort_key(ev: BookEvent) -> tuple[int, int, int, int]:
    kind_rank = 0 if ev.kind == "update" else 1
    return (ev.ts_event_ms, kind_rank, ev.ts_ingest_ms, ev.seq)


def merge_events(
    snapshots: list[Snapshot],
    updates: list[Update],
) -> list[BookEvent]:
    events: list[BookEvent] = []
    seq = 0
    for snap in snapshots:
        events.append(
            BookEvent(
                ts_event_ms=snap.ts_event_ms,
                ts_ingest_ms=snap.ts_ingest_ms,
                kind="snapshot",
                seq=seq,
                snapshot=snap,
            )
        )
        seq += 1
    for upd in updates:
        events.append(
            BookEvent(
                ts_event_ms=upd.ts_event_ms,
                ts_ingest_ms=upd.ts_ingest_ms,
                kind="update",
                seq=seq,
                update=upd,
            )
        )
        seq += 1
    events.sort(key=_event_sort_key)
    return events


def _quality_for_state(*, crossed: bool, in_gap: bool) -> tuple[bool, int]:
    if crossed:
        return False, int(QualityCode.CROSSED_BOOK)
    if in_gap:
        return False, int(QualityCode.UPDATE_GAP)
    return True, int(QualityCode.VALID)


def reconstruct(
    *,
    inst_id: str,
    snapshots: list[Snapshot],
    updates: list[Update],
    from_ms: int,
    to_ms: int,
    sample_step_ms: int = 100,
    update_gap_threshold_ms: int = 5_000,
    size_tol: float = 1e-9,
) -> ReconstructionResult:
    """Reconstruct book state and sample it on a regular grid.

    Parameters
    ----------
    from_ms, to_ms:
        Half-open sampling window [from_ms, to_ms). Events before
        from_ms are applied as warmup so the book is valid at from_ms.
    """
    result = ReconstructionResult(
        inst_id=inst_id,
        from_ms=from_ms,
        to_ms=to_ms,
        snapshot_count=len(snapshots),
        update_count=len(updates),
    )
    if from_ms >= to_ms:
        return result
    if not snapshots:
        result.skipped_no_anchor = True
        return result

    events = merge_events(snapshots, updates)
    book = OrderBook()
    initialized = False
    in_gap = False
    last_event_ts: int | None = None
    interval_rows: list[SampledRow] = []
    sample_ts = first_sample_ts(from_ms, sample_step_ms)

    def emit_until(limit_ts: int) -> None:
        nonlocal sample_ts
        if not initialized or book.snapshot_id is None:
            # Advance cursor so we do not backfill with a later snapshot.
            while sample_ts < limit_ts and sample_ts < to_ms:
                sample_ts += sample_step_ms
            return
        while sample_ts < limit_ts and sample_ts < to_ms:
            if sample_ts >= from_ms:
                crossed = book.is_crossed()
                has_l1 = book.has_l1()
                is_ok, qcode = _quality_for_state(crossed=crossed, in_gap=in_gap)
                if not has_l1:
                    is_ok = False
                last_ts = book.last_update_ts_ms
                if last_ts is None:
                    last_ts = sample_ts
                bids, asks = book.top_n()
                row = build_sampled_row(
                    inst_id=inst_id,
                    ts_event_ms=sample_ts,
                    source_snapshot_id=book.snapshot_id,
                    source_snapshot_ts_event_ms=book.snapshot_ts_ms or sample_ts,
                    last_update_ts_event_ms=last_ts,
                    bids=bids,
                    asks=asks,
                    is_valid=is_ok,
                    quality_code=qcode,
                )
                interval_rows.append(row)
            sample_ts += sample_step_ms

    def flush_interval() -> None:
        # Do not rewrite quality from snapshot B: that would leak the
        # future checkpoint into samples that were already emitted.
        result.rows.extend(interval_rows)
        interval_rows.clear()

    for ev in events:
        emit_until(ev.ts_event_ms)

        if last_event_ts is not None and initialized:
            if ev.ts_event_ms - last_event_ts > update_gap_threshold_ms:
                in_gap = True

        if ev.kind == "snapshot":
            snap = ev.snapshot
            assert snap is not None
            if initialized:
                src_id = book.snapshot_id or ""
                src_ts = book.snapshot_ts_ms or snap.ts_event_ms
                interval_ms = snap.ts_event_ms - src_ts
                report = validate_against_snapshot(
                    book,
                    snap,
                    source_snapshot_id=src_id,
                    source_snapshot_ts_event_ms=src_ts,
                    number_updates_applied=book.updates_since_snapshot,
                    interval_duration_ms=max(interval_ms, 0),
                    size_tol=size_tol,
                )
                result.validations.append(report)
                flush_interval()
            book.apply_snapshot(snap)
            initialized = True
            in_gap = False
        else:
            upd = ev.update
            assert upd is not None
            if not initialized:
                last_event_ts = ev.ts_event_ms
                continue
            book.apply_update(upd)

        last_event_ts = ev.ts_event_ms

    emit_until(to_ms)
    flush_interval()
    return result
