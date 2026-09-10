"""Runtime table names/columns and schema presence checks.

Production CREATE/ALTER lives in docs/sql (to be copied into
okx-hft-timescaledb). Airflow must not mutate schema on each run.
"""

from __future__ import annotations

from okx.orderbook.models import TOP_N

FACT_TABLE = "okx_core.fact_orderbook_l10_100ms"
VALIDATION_TABLE = "okx_core.orderbook_reconstruction_validation"

_LEVEL_COLS: list[str] = []
for _i in range(1, TOP_N + 1):
    _LEVEL_COLS.append(f"bid_px_{_i:02d}")
    _LEVEL_COLS.append(f"bid_sz_{_i:02d}")
for _i in range(1, TOP_N + 1):
    _LEVEL_COLS.append(f"ask_px_{_i:02d}")
    _LEVEL_COLS.append(f"ask_sz_{_i:02d}")

FACT_COLUMNS: tuple[str, ...] = (
    "inst_id",
    "ts_event",
    "ts_event_ms",
    "source_snapshot_id",
    "source_snapshot_ts_event_ms",
    "last_update_ts_event_ms",
    "last_update_age_ms",
    *_LEVEL_COLS,
    "mid_px",
    "spread_px",
    "bid_volume_l1",
    "bid_volume_l5",
    "bid_volume_l10",
    "ask_volume_l1",
    "ask_volume_l5",
    "ask_volume_l10",
    "total_volume_l1",
    "total_volume_l5",
    "total_volume_l10",
    "imbalance_l1",
    "imbalance_l5",
    "imbalance_l10",
    "imbalance_weighted_l10",
    "microprice",
    "microprice_delta",
    "bid_px_size_l1",
    "bid_px_size_l5",
    "bid_px_size_l10",
    "ask_px_size_l1",
    "ask_px_size_l5",
    "ask_px_size_l10",
    "is_valid",
    "quality_code",
)

_CONFLICT_COLS = {"inst_id", "ts_event"}
FACT_UPDATE_COLUMNS: tuple[str, ...] = tuple(
    c for c in FACT_COLUMNS if c not in _CONFLICT_COLS
)

_REQUIRED = (
    ("okx_core", "fact_orderbook_l10_100ms"),
    ("okx_core", "orderbook_reconstruction_validation"),
)

SQL_TABLE_EXISTS = """
SELECT 1
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = %s AND c.relname = %s AND c.relkind IN ('r', 'p');
"""

SQL_PK_COLS = """
SELECT a.attname
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_attribute a
  ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
JOIN pg_catalog.pg_class c ON c.oid = i.indrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = %s
  AND c.relname = %s
  AND i.indisprimary
ORDER BY array_position(i.indkey, a.attnum);
"""


def require_schema(cursor) -> None:
    """Fail if production tables are missing. Does not CREATE/ALTER."""
    missing: list[str] = []
    for schema, table in _REQUIRED:
        cursor.execute(SQL_TABLE_EXISTS, (schema, table))
        if cursor.fetchone() is None:
            missing.append(f"{schema}.{table}")
    if missing:
        raise RuntimeError(
            "Missing Timescale objects: "
            + ", ".join(missing)
            + ". Apply docs/sql/okx_core_fact_orderbook_l10_100ms.sql "
            "from the okx-hft-timescaledb repository. "
            "Airflow does not create production schema."
        )

    cursor.execute(SQL_PK_COLS, ("okx_core", "fact_orderbook_l10_100ms"))
    pk = tuple(r[0] for r in cursor.fetchall())
    if pk != ("inst_id", "ts_event"):
        raise RuntimeError(
            "okx_core.fact_orderbook_l10_100ms PRIMARY KEY must be "
            f"(inst_id, ts_event), got {pk}"
        )
