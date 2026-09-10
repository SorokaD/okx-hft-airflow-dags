from __future__ import annotations

from pathlib import Path


def test_fact_primary_key_is_asc_not_desc() -> None:
    sql = Path("docs/sql/okx_core_fact_orderbook_l10_100ms.sql").read_text(
        encoding="utf-8"
    )
    assert "PRIMARY KEY (inst_id, ts_event)" in sql
    assert "PRIMARY KEY (inst_id, ts_event DESC)" not in sql
    assert "ON okx_core.fact_orderbook_l10_100ms (inst_id, ts_event DESC)" not in sql


def test_validation_primary_key_includes_time() -> None:
    sql = Path("docs/sql/okx_core_fact_orderbook_l10_100ms.sql").read_text(
        encoding="utf-8"
    )
    assert "PRIMARY KEY (inst_id, validation_snapshot_id, validation_ts_event)" in sql
