# OKX L10 order book reconstruction (100ms)

## What this pipeline is

`okx_core.fact_orderbook_l10_100ms` is a **reconstructed** L10 book on a
regular 100ms grid. It is **not** a resample / `time_bucket` of raw
updates, and it is **not** a copy of exchange snapshots.

`okx_core.fact_orderbook_l10_snapshot` stays as-is: it is the compact
form of **real** exchange snapshots and is the authoritative checkpoint.

```
okx_raw.orderbook_snapshots     (full-depth rows per snapshot_id)
okx_raw.orderbook_updates       (incremental JSON deltas)
        │
        │  sequential Python reconstruction (stateful book)
        ▼
in-memory full-depth book
        │
        │  sample TOP 10 on 100ms grid
        ▼
okx_core.fact_orderbook_l10_100ms
        │
        │  checkpoint vs next real snapshot
        ▼
okx_core.orderbook_reconstruction_validation
```

## Reconstruction ≠ resampling

Raw updates are applied **one by one, in time order**. Sampling only
records the last known book on each `...00.000`, `...00.100`, … boundary.

Example:

| event | ts |
|---|---|
| snapshot | 12:00:00.000 |
| update | 12:00:00.037 |
| update | 12:00:00.081 |
| update | 12:00:00.143 |

| sample | book after |
|---|---|
| 12:00:00.100 | update 00.081 |
| 12:00:00.200 | update 00.143 |

The 100ms grid is **not** an aggregation bucket. An update at 00.037 is
not mixed with 00.081; both are applied, then the grid point stores the
result.

Internal book state is **full depth** (not truncated to 10). L10 is
taken only when sampling / validating. Deeper levels can later enter
the top of book.

## Raw formats (as used in this repo)

Confirmed by existing DAGs, not assumed:

**Snapshots** `okx_raw.orderbook_snapshots`

- `side`: `1 = bid`, `2 = ask` (see `okx_raw_to_core_orderbook_l10_snapshot.py`)
- one snapshot = many rows `(snapshot_id, side, level, price, size)`
- real snapshots arrive roughly every 30s

**Updates** `okx_raw.orderbook_updates`

`bids_delta` / `asks_delta` are JSON arrays of objects
(see `okx_core_orderbook_update_level.py`):

```json
[{"price": 95000.1, "size": 12.4}, {"price": 95000.0, "size": 0}]
```

- `bids_delta` is applied **only** to bids; `asks_delta` **only** to asks.
  Updates have no `side` field.
- `size > 0` — upsert that price level on that side
- `size == 0` — delete that price level (OKX: "If the quantity is zero,
  remove the price level from the order book.")

There is **no exchange sequence id** on raw updates. Ordering is:

```text
ORDER BY ts_event_ms, kind(update before snapshot), ts_ingest_ms, seq
```

Collector snapshots are stamped with the last applied update time, so
same-millisecond deltas are applied before the checkpoint comparison.
The snapshot then becomes the new authoritative book.

Same `ts_event_ms` without a sequence number is a documented limitation:
the sort above is deterministic, but it may not match OKX matching-engine
order. Checksums are stored on raw rows and **not** validated here
(no checksum implementation exists in this repository).

## Snapshot-aligned validation / reset

```
snapshot A  →  apply updates  →  reconstructed state
                                      │
                                      ▼
                               compare L10 vs snapshot B
                                      │
                                      ▼
                               reset book to snapshot B
                                      │
                                      ▼
                               apply further updates
```

Snapshot B is used only to:

1. validate the interval that started at A
2. become the new authoritative state **starting at B**

Samples with `ts_event < B` are **never** rewritten using B (no
look-ahead leakage). Checkpoint mismatch is stored only in
`okx_core.orderbook_reconstruction_validation`. Fact-table
`is_valid` / `quality_code` stay causal: they describe the book at
`ts_event`, not a later snapshot B.

## Incremental processing

DAG: `okx_core_build_orderbook_l10_100ms` (`schedule=None`, triggered by
`okx_master_raw_to_core_daily` after other raw→core jobs, or manually).

- Watermark: `max(ts_event_ms)` per `inst_id` in the fact table
- Overlap: 2 minutes (idempotent `ON CONFLICT DO UPDATE`)
- Safety lag: 5 seconds (do not reconstruct the live ingest tail)
- Each chunk loads the last real snapshot **≤ from_ts** (anchor),
  replays updates up to `from_ts` without writing, then samples
  `[from_ts, to_ts)`
- Catch-up cap: 24 hours per run
- Chunk size: 15 minutes of event time, one instrument at a time
- Bulk upsert via `psycopg2.extras.execute_values`

## Target table

TimescaleDB hypertable `okx_core.fact_orderbook_l10_100ms`:

| | |
|---|---|
| time column | `ts_event` |
| chunk | 1 day (~864k rows/instrument/day) |
| PRIMARY KEY | `(inst_id, ts_event)` — ASC; time column is required by Timescale |
| extra DESC index | none (PK btree scans both directions) |
| compression | classic `timescaledb.compress`, `segmentby=inst_id`, `orderby=ts_event DESC`, policy 1 day |
| retention | 30 days native policy |

DDL ownership: copy `docs/sql/okx_core_fact_orderbook_l10_100ms.sql` into
**okx-hft-timescaledb** and apply it there. The Airflow DAG only checks that
the tables and PRIMARY KEY exist; it does not CREATE/ALTER schema at runtime. |

Hot ≈ last 1 day uncompressed, warm 1–30 days compressed, older dropped.

Point-in-time metrics stored on each row: mid, spread, depth, imbalance,
weighted imbalance (L1=1.0 … L10=0.1), microprice, `price*size` sums.

**Not stored here** (no instrument metadata in this repo):

- `spread_ticks` / `microprice_delta_ticks` (no tick size table)
- USD notional (SWAP `size` is contracts; `ctVal` is not available).
  `bid_px_size_*` / `ask_px_size_*` are `sum(price * size)` only.

**Not stored here** on purpose (feature/research layer):

realized vol, momentum, rolling stats, future returns, MFE/MAE.

## Quality flags

Fact table `okx_core.fact_orderbook_l10_100ms` — **causal only**
(known at `ts_event`, no future snapshot B):

| `quality_code` | name | `is_valid` |
|---|---|---|
| 0 | VALID | true (L1 exists and book is not crossed) |
| 1 | NO_ANCHOR_SNAPSHOT | no rows written for that chunk |
| 2 | UPDATE_GAP | false (gap > 5s between events) |
| 3 | CROSSED_BOOK | false (`best_bid >= best_ask`) |

`SNAPSHOT_MISMATCH` is **not** written to 100ms rows. Research /
backtest filter `WHERE is_valid = true` does not drop an interval just
because a later checkpoint disagreed.

Ex-post checkpoint status lives in
`okx_core.orderbook_reconstruction_validation` (`is_match`, L1/L10
ratios). Retention 90 days.

## How to run

DDL (once, in okx-hft-timescaledb — not from the DAG):

```sql
-- docs/sql/okx_core_fact_orderbook_l10_100ms.sql
```

Airflow:

1. Unpause `okx_core_build_orderbook_l10_100ms`
2. Trigger it from `okx_master_raw_to_core_daily`, or Trigger DAG in the UI
3. Task `sync` logs a QUALITY line per instrument and DIAG gap percentiles

Local tests:

```bash
pytest tests/orderbook -v
```

## Example query

```sql
SELECT
    ts_event,
    bid_px_01,
    ask_px_01,
    mid_px,
    spread_px,
    bid_volume_l10,
    ask_volume_l10,
    imbalance_l10,
    microprice,
    last_update_age_ms,
    is_valid
FROM okx_core.fact_orderbook_l10_100ms
WHERE inst_id = 'BTC-USDT-SWAP'
ORDER BY ts_event DESC
LIMIT 10;
```

Diagnostics: `docs/sql/okx_core_orderbook_l10_100ms_diagnostics.sql`.
