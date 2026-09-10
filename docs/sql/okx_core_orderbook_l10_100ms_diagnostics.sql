-- Diagnostic checks for okx_core.fact_orderbook_l10_100ms
-- Expected for a fully covered UTC day of BTC-USDT-SWAP:
--   row count ~ 864000
--   gap p50 = p90 = p99 = 100 ms (except known invalid/gap intervals)

-- 1) Inter-sample gaps
WITH ordered AS (
    SELECT
        inst_id,
        ts_event_ms,
        ts_event_ms - lag(ts_event_ms) OVER (
            PARTITION BY inst_id ORDER BY ts_event_ms
        ) AS delta_ms
    FROM okx_core.fact_orderbook_l10_100ms
    WHERE inst_id = 'BTC-USDT-SWAP'
      AND ts_event >= now() - interval '1 day'
),
gaps AS (
    SELECT delta_ms FROM ordered WHERE delta_ms IS NOT NULL
)
SELECT
    count(*) AS row_cnt,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY delta_ms) AS p50_gap_ms,
    percentile_cont(0.90) WITHIN GROUP (ORDER BY delta_ms) AS p90_gap_ms,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY delta_ms) AS p99_gap_ms,
    max(delta_ms) AS max_gap_ms,
    count(*) FILTER (WHERE delta_ms <> 100) AS non_100_ms_cnt
FROM gaps;

-- 2) Rows per UTC day (is_valid is causal; snapshot mismatch is in validation table)
SELECT
    time_bucket('1 day', ts_event) AS day_utc,
    count(*) AS rows_per_day,
    count(*) FILTER (WHERE is_valid) AS valid_rows,
    count(*) FILTER (WHERE NOT is_valid) AS invalid_rows
FROM okx_core.fact_orderbook_l10_100ms
WHERE inst_id = 'BTC-USDT-SWAP'
GROUP BY 1
ORDER BY 1 DESC;

-- 3) Latest reconstructed states
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

-- 4) Recent snapshot-checkpoint quality
SELECT
    validation_ts_event,
    l1_bid_price_match,
    l1_ask_price_match,
    l1_bid_size_match,
    l1_ask_size_match,
    l10_bid_price_match_ratio,
    l10_ask_price_match_ratio,
    l10_bid_size_match_ratio,
    l10_ask_size_match_ratio,
    is_match,
    number_updates_applied,
    interval_duration_ms
FROM okx_core.orderbook_reconstruction_validation
WHERE inst_id = 'BTC-USDT-SWAP'
ORDER BY validation_ts_event DESC
LIMIT 20;
