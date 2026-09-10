"""100ms sampling grid helpers.

Reconstruction is sequential. Sampling only records the last known
book state on each aligned 100ms boundary. It does not bucket or
aggregate updates.
"""

from __future__ import annotations

from okx.orderbook.models import SAMPLE_STEP_MS


def align_down(ts_ms: int, step_ms: int = SAMPLE_STEP_MS) -> int:
    return (ts_ms // step_ms) * step_ms


def align_up(ts_ms: int, step_ms: int = SAMPLE_STEP_MS) -> int:
    q, r = divmod(ts_ms, step_ms)
    return ts_ms if r == 0 else (q + 1) * step_ms


def first_sample_ts(from_ms: int, step_ms: int = SAMPLE_STEP_MS) -> int:
    """First grid timestamp in the half-open window starting at from_ms."""
    return align_up(from_ms, step_ms)


def iter_sample_ts(
    from_ms: int,
    to_ms: int,
    step_ms: int = SAMPLE_STEP_MS,
) -> list[int]:
    """Grid timestamps T where from_ms <= T < to_ms and T % step_ms == 0."""
    ts = first_sample_ts(from_ms, step_ms)
    out: list[int] = []
    while ts < to_ms:
        out.append(ts)
        ts += step_ms
    return out
