# Architecture orderbook_updates 

RAW
 └── orderbook_updates (FULL JSON)   ← истина

CORE
 ├── fact_orderbook_update           ← 1 row / update (json)
 ├── fact_orderbook_update_level     ← top-K or size!=0 ONLY
 └── fact_orderbook_snapshot         ← snapshot levels (top-K)

MART / FEATURES
 ├── orderbook_features_100ms
 ├── orderbook_features_1s
 ├── microprice / imbalance / pressure
 └── ML-ready tables


