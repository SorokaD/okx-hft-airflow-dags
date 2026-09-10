-- Production DDL for okx-hft-timescaledb
-- Copy this migration into the okx-hft-timescaledb repository.
-- Do NOT apply from Airflow DAG runtime.
--
-- Database: okx_hft
-- Safe to re-run (IF NOT EXISTS / if_not_exists).
--
-- Hypertable: ts_event, chunk 1 day
-- PRIMARY KEY (inst_id, ts_event) — ASC, includes the partition column
--   (TimescaleDB requires the time column in unique constraints).
-- No extra DESC index: the PK already supports
--   WHERE inst_id = ? AND ts_event >= ? AND ts_event < ? ORDER BY ts_event
--   (PostgreSQL can scan a btree index in both directions).
-- Compression: classic timescaledb.compress, segmentby inst_id,
--              orderby ts_event DESC, compress chunks older than 1 day
-- Retention: 30 days (fact), 90 days (validation)

CREATE SCHEMA IF NOT EXISTS okx_core;

CREATE TABLE IF NOT EXISTS okx_core.fact_orderbook_l10_100ms (
    inst_id varchar(50) NOT NULL,
    ts_event timestamptz NOT NULL,
    ts_event_ms int8 NOT NULL,
    source_snapshot_id uuid NOT NULL,
    source_snapshot_ts_event_ms int8 NOT NULL,
    last_update_ts_event_ms int8 NOT NULL,
    last_update_age_ms int4 NOT NULL,
    bid_px_01 float8,
    bid_sz_01 float8,
    bid_px_02 float8,
    bid_sz_02 float8,
    bid_px_03 float8,
    bid_sz_03 float8,
    bid_px_04 float8,
    bid_sz_04 float8,
    bid_px_05 float8,
    bid_sz_05 float8,
    bid_px_06 float8,
    bid_sz_06 float8,
    bid_px_07 float8,
    bid_sz_07 float8,
    bid_px_08 float8,
    bid_sz_08 float8,
    bid_px_09 float8,
    bid_sz_09 float8,
    bid_px_10 float8,
    bid_sz_10 float8,
    ask_px_01 float8,
    ask_sz_01 float8,
    ask_px_02 float8,
    ask_sz_02 float8,
    ask_px_03 float8,
    ask_sz_03 float8,
    ask_px_04 float8,
    ask_sz_04 float8,
    ask_px_05 float8,
    ask_sz_05 float8,
    ask_px_06 float8,
    ask_sz_06 float8,
    ask_px_07 float8,
    ask_sz_07 float8,
    ask_px_08 float8,
    ask_sz_08 float8,
    ask_px_09 float8,
    ask_sz_09 float8,
    ask_px_10 float8,
    ask_sz_10 float8,
    mid_px float8,
    spread_px float8,
    bid_volume_l1 float8,
    bid_volume_l5 float8,
    bid_volume_l10 float8,
    ask_volume_l1 float8,
    ask_volume_l5 float8,
    ask_volume_l10 float8,
    total_volume_l1 float8,
    total_volume_l5 float8,
    total_volume_l10 float8,
    imbalance_l1 float8,
    imbalance_l5 float8,
    imbalance_l10 float8,
    imbalance_weighted_l10 float8,
    microprice float8,
    microprice_delta float8,
    bid_px_size_l1 float8,
    bid_px_size_l5 float8,
    bid_px_size_l10 float8,
    ask_px_size_l1 float8,
    ask_px_size_l5 float8,
    ask_px_size_l10 float8,
    is_valid boolean NOT NULL DEFAULT true,
    quality_code int2 NOT NULL DEFAULT 0,
    PRIMARY KEY (inst_id, ts_event)
);

CREATE TABLE IF NOT EXISTS okx_core.orderbook_reconstruction_validation (
    inst_id varchar(50) NOT NULL,
    validation_ts_event timestamptz NOT NULL,
    validation_ts_event_ms int8 NOT NULL,
    validation_snapshot_id uuid NOT NULL,
    source_snapshot_id uuid NOT NULL,
    source_snapshot_ts_event_ms int8 NOT NULL,
    l1_bid_price_match boolean NOT NULL,
    l1_ask_price_match boolean NOT NULL,
    l1_bid_size_match boolean NOT NULL,
    l1_ask_size_match boolean NOT NULL,
    l10_bid_price_match_ratio float8 NOT NULL,
    l10_ask_price_match_ratio float8 NOT NULL,
    l10_bid_size_match_ratio float8 NOT NULL,
    l10_ask_size_match_ratio float8 NOT NULL,
    reconstructed_best_bid float8,
    actual_best_bid float8,
    reconstructed_best_ask float8,
    actual_best_ask float8,
    max_bid_price_diff float8,
    max_ask_price_diff float8,
    number_updates_applied int4 NOT NULL,
    interval_duration_ms int8 NOT NULL,
    is_match boolean NOT NULL,
    PRIMARY KEY (inst_id, validation_snapshot_id, validation_ts_event)
);

SELECT create_hypertable(
    'okx_core.fact_orderbook_l10_100ms',
    'ts_event',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

SELECT create_hypertable(
    'okx_core.orderbook_reconstruction_validation',
    'validation_ts_event',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM timescaledb_information.hypertables
        WHERE hypertable_schema = 'okx_core'
          AND hypertable_name = 'fact_orderbook_l10_100ms'
          AND compression_enabled
    ) THEN
        ALTER TABLE okx_core.fact_orderbook_l10_100ms SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'inst_id',
            timescaledb.compress_orderby = 'ts_event DESC'
        );
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM timescaledb_information.hypertables
        WHERE hypertable_schema = 'okx_core'
          AND hypertable_name = 'orderbook_reconstruction_validation'
          AND compression_enabled
    ) THEN
        ALTER TABLE okx_core.orderbook_reconstruction_validation SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'inst_id',
            timescaledb.compress_orderby = 'validation_ts_event DESC'
        );
    END IF;
END $$;

SELECT add_compression_policy(
    'okx_core.fact_orderbook_l10_100ms',
    INTERVAL '1 day',
    if_not_exists => TRUE
);

SELECT add_compression_policy(
    'okx_core.orderbook_reconstruction_validation',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

SELECT add_retention_policy(
    'okx_core.fact_orderbook_l10_100ms',
    INTERVAL '30 days',
    if_not_exists => TRUE
);

SELECT add_retention_policy(
    'okx_core.orderbook_reconstruction_validation',
    INTERVAL '90 days',
    if_not_exists => TRUE
);

COMMENT ON TABLE okx_core.fact_orderbook_l10_100ms IS
    'Reconstructed OKX L10 order book sampled every 100ms. Sequential book replay then sampling, not exchange snapshots. Retention 30 days.';

COMMENT ON COLUMN okx_core.fact_orderbook_l10_100ms.ts_event IS
    'Aligned 100ms UTC timestamp of the sampled book state. Includes all events with ts_event_ms <= this instant. No look-ahead.';

COMMENT ON COLUMN okx_core.fact_orderbook_l10_100ms.ts_event_ms IS
    'ts_event as epoch milliseconds. Always a multiple of 100.';

COMMENT ON COLUMN okx_core.fact_orderbook_l10_100ms.source_snapshot_id IS
    'Last authoritative raw snapshot applied before this sample. Book is reset to each real snapshot after validation.';

COMMENT ON COLUMN okx_core.fact_orderbook_l10_100ms.last_update_age_ms IS
    'ts_event_ms - last_update_ts_event_ms. Large values mean a stale book.';

COMMENT ON COLUMN okx_core.fact_orderbook_l10_100ms.imbalance_l1 IS
    '(bid_volume_l1 - ask_volume_l1) / (bid_volume_l1 + ask_volume_l1). Range [-1, 1].';

COMMENT ON COLUMN okx_core.fact_orderbook_l10_100ms.imbalance_l5 IS
    'Signed size imbalance using top 5 levels. Range [-1, 1].';

COMMENT ON COLUMN okx_core.fact_orderbook_l10_100ms.imbalance_l10 IS
    'Signed size imbalance using top 10 levels. Range [-1, 1].';

COMMENT ON COLUMN okx_core.fact_orderbook_l10_100ms.microprice IS
    'L1 microprice: (ask_px_01*bid_sz_01 + bid_px_01*ask_sz_01) / (bid_sz_01 + ask_sz_01).';

COMMENT ON COLUMN okx_core.fact_orderbook_l10_100ms.is_valid IS
    'TRUE when the sample is causally valid at ts_event: L1 exists, book is not crossed, and no update-gap is open. Snapshot-B mismatch is NOT folded in (that would be look-ahead).';

COMMENT ON COLUMN okx_core.fact_orderbook_l10_100ms.quality_code IS
    'Causal only: 0=VALID 1=NO_ANCHOR_SNAPSHOT 2=UPDATE_GAP 3=CROSSED_BOOK. Code 4 SNAPSHOT_MISMATCH is reserved and stored only in orderbook_reconstruction_validation.is_match=false.';

COMMENT ON COLUMN okx_core.fact_orderbook_l10_100ms.bid_px_size_l10 IS
    'Sum(price*size) over bid L1..L10. Not USD notional: SWAP size is contracts and ctVal is not in this pipeline.';

COMMENT ON TABLE okx_core.orderbook_reconstruction_validation IS
    'Checkpoint comparison of reconstructed L10 vs the next real snapshot. Retention 90 days.';

COMMENT ON COLUMN okx_core.orderbook_reconstruction_validation.validation_snapshot_id IS
    'Real snapshot used as the checkpoint (snapshot B).';

COMMENT ON COLUMN okx_core.orderbook_reconstruction_validation.is_match IS
    'TRUE if reconstructed L1 and L10 prices and sizes match snapshot B.';
