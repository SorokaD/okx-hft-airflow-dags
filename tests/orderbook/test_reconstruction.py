"""Unit tests for OKX L10 order book reconstruction.

Covers the required cases (initial snapshot, deltas, TOP10, 100ms
sampling, snapshot reset/validation, crossed book, idempotency,
missing anchor, empty update, same-ts ordering, sort order).
"""

from __future__ import annotations

from okx.orderbook.book import OrderBook
from okx.orderbook.models import QualityCode, parse_delta
from okx.orderbook.reconstructor import reconstruct
from okx.orderbook.sampler import first_sample_ts, iter_sample_ts
from okx.orderbook.validator import snapshot_top_n, validate_against_snapshot
from tests.orderbook.helpers import INST, SID_A, SID_B, snapshot, update


def _by_ts(result):
    return {r.ts_event_ms: r for r in result.rows}


def test_01_initial_snapshot_builds_book() -> None:
    book = OrderBook()
    book.apply_snapshot(snapshot(0))
    bids, asks = book.top_n(10)
    assert bids[0] == (100.0, 10.0)
    assert asks[0] == (101.0, 10.0)
    assert len(bids) == 10
    assert len(asks) == 10
    assert 90.0 in book.bids  # L11 kept in full-depth state
    assert 111.0 in book.asks


def test_02_update_existing_price_level() -> None:
    book = OrderBook()
    book.apply_snapshot(snapshot(0))
    book.apply_update(update(37, bids=[(100.0, 12.0)]))
    assert book.bids[100.0] == 12.0
    assert book.best_bid() == 100.0
    assert book.best_bid_size() == 12.0


def test_03_insert_new_price_level() -> None:
    book = OrderBook()
    book.apply_snapshot(snapshot(0))
    book.apply_update(update(81, bids=[(100.5, 5.0)]))
    assert 100.5 in book.bids
    assert book.best_bid() == 100.5


def test_04_size_zero_removes_level() -> None:
    book = OrderBook()
    book.apply_snapshot(snapshot(0))
    book.apply_update(update(143, asks=[(101.0, 0.0)]))
    assert 101.0 not in book.asks
    assert book.best_ask() == 102.0


def test_bids_delta_does_not_touch_asks() -> None:
    book = OrderBook()
    book.apply_snapshot(snapshot(0))
    asks_before = dict(book.asks)
    book.apply_update(update(10, bids=[(100.0, 1.0)]))
    assert book.asks == asks_before
    assert book.bids[100.0] == 1.0


def test_asks_delta_does_not_touch_bids() -> None:
    book = OrderBook()
    book.apply_snapshot(snapshot(0))
    bids_before = dict(book.bids)
    book.apply_update(update(10, asks=[(101.0, 1.0)]))
    assert book.bids == bids_before
    assert book.asks[101.0] == 1.0


def test_05_new_bid_enters_top10() -> None:
    book = OrderBook()
    book.apply_snapshot(snapshot(0))
    book.apply_update(update(81, bids=[(100.5, 5.0)]))
    bids, _ = book.top_n(10)
    assert bids[0][0] == 100.5
    assert 100.5 in {px for px, _ in bids}


def test_06_new_ask_enters_top10() -> None:
    book = OrderBook()
    book.apply_snapshot(snapshot(0))
    book.apply_update(update(50, asks=[(100.5, 3.0)]))
    _, asks = book.top_n(10)
    assert asks[0][0] == 100.5


def test_07_old_l10_level_leaves_top10() -> None:
    book = OrderBook()
    book.apply_snapshot(snapshot(0))
    bids_before, _ = book.top_n(10)
    assert bids_before[-1][0] == 91.0
    book.apply_update(update(81, bids=[(100.5, 5.0)]))
    bids_after, _ = book.top_n(10)
    prices = [px for px, _ in bids_after]
    assert 100.5 in prices
    assert 91.0 not in prices
    assert 91.0 in book.bids  # still in full-depth book


def test_08_multiple_updates_between_two_100ms_samples() -> None:
    result = reconstruct(
        inst_id=INST,
        snapshots=[snapshot(0)],
        updates=[
            update(37, bids=[(100.0, 12.0)]),
            update(81, bids=[(100.5, 5.0)]),
        ],
        from_ms=0,
        to_ms=200,
    )
    rows = _by_ts(result)
    # 100ms sample must include BOTH 37 and 81, not a bucket aggregate
    assert rows[100].bid_px[0] == 100.5
    assert rows[100].bid_sz[0] == 5.0
    assert rows[100].bid_px[1] == 100.0
    assert rows[100].bid_sz[1] == 12.0


def test_09_100ms_sampling_uses_last_known_book_state() -> None:
    result = reconstruct(
        inst_id=INST,
        snapshots=[snapshot(0)],
        updates=[
            update(37, bids=[(100.0, 12.0)]),
            update(81, bids=[(100.5, 5.0)]),
            update(143, asks=[(101.0, 0.0)]),
            update(267, bids=[(90.0, 0.0)]),
        ],
        from_ms=100,
        to_ms=400,
    )
    ts = [r.ts_event_ms for r in result.rows]
    assert ts == [100, 200, 300]
    rows = _by_ts(result)
    # 100: after 81 (not 37 alone, not 143)
    assert rows[100].bid_px[0] == 100.5
    assert rows[100].ask_px[0] == 101.0
    # 200: after 143
    assert rows[200].ask_px[0] == 102.0
    assert 101.0 not in [p for p in rows[200].ask_px if p is not None]
    # 300: after 267; L11 gone, L10 bids unchanged vs 200
    assert rows[300].bid_px[0] == 100.5
    assert rows[300].ask_px[0] == 102.0
    assert rows[200].last_update_ts_event_ms == 143
    assert rows[200].last_update_age_ms == 57
    assert rows[300].last_update_ts_event_ms == 267
    assert rows[300].last_update_age_ms == 33


def test_10_next_real_snapshot_resets_reconstructed_state() -> None:
    snap_b = snapshot(
        200,
        snapshot_id=SID_B,
        bids=[(99.0, 1.0)] + [(90.0 - i, 1.0) for i in range(9)],
        asks=[(100.0, 1.0)] + [(101.0 + i, 1.0) for i in range(9)],
    )
    result = reconstruct(
        inst_id=INST,
        snapshots=[snapshot(0), snap_b],
        updates=[update(81, bids=[(100.5, 5.0)])],
        from_ms=0,
        to_ms=300,
    )
    rows = _by_ts(result)
    assert rows[100].bid_px[0] == 100.5
    assert rows[100].source_snapshot_id == SID_A
    # sample at 200 is AFTER reset to snapshot B (no look-ahead into 100)
    assert rows[200].bid_px[0] == 99.0
    assert rows[200].ask_px[0] == 100.0
    assert rows[200].source_snapshot_id == SID_B


def test_11_snapshot_validation_match() -> None:
    # snapshot B equals reconstructed L10 after no-op interval
    snap_b = snapshot(200, snapshot_id=SID_B)
    result = reconstruct(
        inst_id=INST,
        snapshots=[snapshot(0), snap_b],
        updates=[],
        from_ms=0,
        to_ms=300,
    )
    assert len(result.validations) == 1
    assert result.validations[0].is_match is True
    assert result.validations[0].l1_bid_price_match is True
    assert result.validations[0].l10_bid_price_match_ratio == 1.0
    assert all(r.quality_code == int(QualityCode.VALID) for r in result.rows)


def test_12_snapshot_validation_mismatch() -> None:
    snap_b = snapshot(
        200,
        snapshot_id=SID_B,
        bids=[(50.0, 9.0)] + [(40.0 - i, 1.0) for i in range(9)],
        asks=[(60.0, 9.0)] + [(61.0 + i, 1.0) for i in range(9)],
    )
    result = reconstruct(
        inst_id=INST,
        snapshots=[snapshot(0), snap_b],
        updates=[update(81, bids=[(100.5, 5.0)])],
        from_ms=0,
        to_ms=300,
    )
    assert len(result.validations) == 1
    report = result.validations[0]
    assert report.is_match is False
    assert report.l1_bid_price_match is False
    assert report.reconstructed_best_bid == 100.5
    assert report.actual_best_bid == 50.0
    pre = [r for r in result.rows if r.ts_event_ms < 200]
    assert pre
    # Ex-post mismatch must not leak into causal sample flags.
    assert all(r.quality_code == int(QualityCode.VALID) for r in pre)
    assert all(r.is_valid is True for r in pre)
    assert all(
        r.quality_code != int(QualityCode.SNAPSHOT_MISMATCH) for r in result.rows
    )
    # prices of pre-B samples are NOT rewritten with snapshot B
    assert _by_ts(result)[100].bid_px[0] == 100.5


def test_13_crossed_book_marks_invalid() -> None:
    result = reconstruct(
        inst_id=INST,
        snapshots=[snapshot(0)],
        updates=[update(50, bids=[(102.0, 1.0)])],  # 102 >= 101 ask
        from_ms=0,
        to_ms=200,
    )
    rows = _by_ts(result)
    assert rows[100].quality_code == int(QualityCode.CROSSED_BOOK)
    assert rows[100].is_valid is False
    assert rows[100].bid_px[0] == 102.0
    assert rows[100].ask_px[0] == 101.0


def test_14_idempotent_output() -> None:
    kwargs = dict(
        inst_id=INST,
        snapshots=[snapshot(0)],
        updates=[
            update(37, bids=[(100.0, 12.0)]),
            update(81, bids=[(100.5, 5.0)]),
            update(143, asks=[(101.0, 0.0)]),
        ],
        from_ms=0,
        to_ms=300,
    )
    a = reconstruct(**kwargs)
    b = reconstruct(**kwargs)
    assert [r.ts_event_ms for r in a.rows] == [r.ts_event_ms for r in b.rows]
    for ra, rb in zip(a.rows, b.rows, strict=True):
        assert ra.bid_px == rb.bid_px
        assert ra.ask_px == rb.ask_px
        assert ra.bid_sz == rb.bid_sz
        assert ra.ask_sz == rb.ask_sz
        assert ra.mid_px == rb.mid_px
        assert ra.microprice == rb.microprice
        assert ra.quality_code == rb.quality_code


def test_15_missing_anchor_snapshot_handled_safely() -> None:
    result = reconstruct(
        inst_id=INST,
        snapshots=[],
        updates=[update(37, bids=[(100.0, 1.0)])],
        from_ms=0,
        to_ms=200,
    )
    assert result.skipped_no_anchor is True
    assert result.rows == []
    assert result.validations == []


def test_16_empty_update() -> None:
    result = reconstruct(
        inst_id=INST,
        snapshots=[snapshot(0)],
        updates=[update(50, bids=[], asks=[])],
        from_ms=0,
        to_ms=200,
    )
    rows = _by_ts(result)
    assert rows[100].bid_px[0] == 100.0
    assert rows[100].ask_px[0] == 101.0
    assert rows[100].bid_sz[0] == 10.0


def test_17_same_ts_event_ms_deterministic() -> None:
    result = reconstruct(
        inst_id=INST,
        snapshots=[snapshot(0)],
        updates=[
            update(50, bids=[(100.0, 1.0)], ingest_ms=10),
            update(50, bids=[(100.0, 2.0)], ingest_ms=20),
        ],
        from_ms=0,
        to_ms=200,
    )
    # later ts_ingest_ms wins at the same ts_event_ms
    assert _by_ts(result)[100].bid_sz[0] == 2.0
    again = reconstruct(
        inst_id=INST,
        snapshots=[snapshot(0)],
        updates=[
            update(50, bids=[(100.0, 2.0)], ingest_ms=20),
            update(50, bids=[(100.0, 1.0)], ingest_ms=10),
        ],
        from_ms=0,
        to_ms=200,
    )
    assert _by_ts(again)[100].bid_sz[0] == 2.0


def test_18_bid_ask_sorting_correct() -> None:
    book = OrderBook()
    book.apply_snapshot(
        snapshot(
            0,
            bids=[(95.0, 1.0), (100.0, 2.0), (99.0, 3.0)],
            asks=[(105.0, 1.0), (101.0, 2.0), (103.0, 3.0)],
        )
    )
    bids, asks = book.top_n(10)
    assert [px for px, _ in bids] == [100.0, 99.0, 95.0]
    assert [px for px, _ in asks] == [101.0, 103.0, 105.0]


def test_parse_delta_confirmed_object_format() -> None:
    raw = [{"price": "100.5", "size": "3"}, {"price": 99.0, "size": 0}]
    assert parse_delta(raw) == [(100.5, 3.0), (99.0, 0.0)]
    assert parse_delta('[{"price": 1, "size": 2}]') == [(1.0, 2.0)]
    assert parse_delta(None) == []
    assert parse_delta([]) == []
    assert parse_delta("[]") == []
    # list-of-pairs is NOT the on-disk format used by this project
    assert parse_delta([["100", "1"]]) == []


def test_sampler_grid() -> None:
    assert first_sample_ts(0) == 0
    assert first_sample_ts(1) == 100
    assert iter_sample_ts(0, 400) == [0, 100, 200, 300]
    assert iter_sample_ts(100, 400) == [100, 200, 300]


def test_metrics_mid_spread_imbalance_microprice() -> None:
    result = reconstruct(
        inst_id=INST,
        snapshots=[snapshot(0)],
        updates=[],
        from_ms=0,
        to_ms=100,
    )
    row = result.rows[0]
    assert row.mid_px == 100.5
    assert row.spread_px == 1.0
    assert row.bid_volume_l1 == 10.0
    assert row.ask_volume_l1 == 10.0
    assert row.imbalance_l1 == 0.0
    # microprice = (101*10 + 100*10) / 20 = 100.5
    assert row.microprice == 100.5
    assert row.microprice_delta == 0.0
    assert row.bid_px_size_l1 == 100.0 * 10.0
    assert row.is_valid is True


def test_validate_against_snapshot_direct() -> None:
    book = OrderBook()
    snap = snapshot(0)
    book.apply_snapshot(snap)
    report = validate_against_snapshot(
        book,
        snap,
        source_snapshot_id=SID_A,
        source_snapshot_ts_event_ms=0,
        number_updates_applied=0,
        interval_duration_ms=0,
    )
    assert report.is_match is True
    bids, asks = snapshot_top_n(snap)
    assert bids[0][0] == 100.0
    assert asks[0][0] == 101.0
    assert report.l5_bid_price_match_ratio == 1.0
    assert report.l5_ask_size_match_ratio == 1.0
    assert report.mismatched_levels == []
    assert report.reconstructed_imbalance_l1 == report.actual_imbalance_l1


def test_updates_before_first_snapshot_are_ignored() -> None:
    result = reconstruct(
        inst_id=INST,
        snapshots=[snapshot(80)],
        updates=[update(10, bids=[(999.0, 1.0)])],
        from_ms=0,
        to_ms=200,
    )
    rows = _by_ts(result)
    assert 0 not in rows  # no look-ahead / no book yet
    assert rows[100].bid_px[0] == 100.0
    assert 999.0 not in [p for p in rows[100].bid_px if p is not None]
