"""One-hour BTC-USDT-SWAP reconstruction review (not a production DAG).

Does not backfill history. Writes only the chosen hour into core tables.
ASCII-only stdout so Windows cp1251 cannot crash the process.
"""

from __future__ import annotations

import os
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))

INST = "BTC-USDT-SWAP"
HOUR_MS = 3_600_000
LOOKBACK_MS = 5 * 60 * 1000
REVIEW_FROM = datetime(2026, 9, 10, 9, 0, tzinfo=timezone.utc)


def _configure_stdio() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconf = getattr(stream, "reconfigure", None)
        if callable(reconf):
            reconf(encoding="utf-8", errors="replace")


def _load_dotenv(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        out[key.strip()] = val.strip().strip('"').strip("'")
    return out


def _connect():
    import psycopg2

    print("connecting...", flush=True)

    dsn = os.environ.get("TIMESCALEDB_DSN") or os.environ.get("DATABASE_URL")
    if dsn:
        return psycopg2.connect(dsn)

    env: dict[str, str] = {}
    for rel in (
        Path("d:/tumar/okx-hft-timescaledb/.env"),
        Path("d:/tumar/okx-hft-collector/docker/.env"),
        Path("d:/tumar/okx-hft-ops/.env"),
        ROOT.parent / "okx-hft-timescaledb" / ".env",
    ):
        env.update(_load_dotenv(rel))

    user = env.get("POSTGRES_USER") or os.environ.get("POSTGRES_USER")
    password = env.get("POSTGRES_PASSWORD") or os.environ.get("POSTGRES_PASSWORD")
    dbname = env.get("POSTGRES_DB") or os.environ.get("POSTGRES_DB") or "okx_hft"
    host = (
        env.get("POSTGRES_HOST")
        or env.get("POSTGRES_LINK")
        or os.environ.get("POSTGRES_HOST")
        or "127.0.0.1"
    )
    if host in {"timescaledb", "pgbouncer", "postgres"}:
        host = "127.0.0.1"
    port = int(env.get("POSTGRES_PORT") or os.environ.get("POSTGRES_PORT") or "6432")
    if not user or not password:
        try:
            from airflow.providers.postgres.hooks.postgres import PostgresHook

            return PostgresHook(postgres_conn_id="timescaledb").get_conn()
        except Exception as exc:  # noqa: BLE001
            raise SystemExit(
                "No DB connection. Set TIMESCALEDB_DSN. " f"({type(exc).__name__})"
            ) from exc

    last_err = None
    attempts: list[tuple[str, int]] = [
        (host, port),
        ("127.0.0.1", 6432),
        ("127.0.0.1", 5432),
    ]
    seen: set[tuple[str, int]] = set()
    for h, p in attempts:
        if (h, p) in seen:
            continue
        seen.add((h, p))
        try:
            conn = psycopg2.connect(
                host=h,
                port=p,
                user=user,
                password=password,
                dbname=dbname,
                connect_timeout=3,
            )
            print(f"Connected host={h} port={p} db={dbname} user={user}", flush=True)
            return conn
        except Exception as exc:  # noqa: BLE001
            last_err = exc
    raise SystemExit(f"DB connect failed: {type(last_err).__name__}: {last_err}")


def _pctile(vals: list[float], p: float) -> float | None:
    if not vals:
        return None
    ordered = sorted(vals)
    idx = min(len(ordered) - 1, max(0, int(round((p / 100) * (len(ordered) - 1)))))
    return float(ordered[idx])


def _abs_err(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    return abs(a - b)


def _rel_err(recon: float | None, actual: float | None) -> float | None:
    if recon is None or actual is None:
        return None
    if actual == 0.0:
        return 0.0 if recon == 0.0 else None
    return abs(recon - actual) / abs(actual)


def _err_stats(
    errs: list[float],
) -> tuple[float | None, float | None, float | None, float | None]:
    if not errs:
        return None, None, None, None
    mae = sum(errs) / len(errs)
    med = float(statistics.median(errs))
    return mae, med, _pctile(errs, 90), max(errs)


def _print_err_block(title: str, recon_name: str, actual_name: str, vals) -> None:
    errs: list[float] = []
    for v in vals:
        err = _abs_err(getattr(v, recon_name), getattr(v, actual_name))
        if err is not None:
            errs.append(err)
    mae, med, p90, mx = _err_stats(errs)
    print(
        f"{title} n={len(errs)} MAE={mae} median={med} p90={p90} max={mx}", flush=True
    )


def _print_rel_block(title: str, recon_name: str, actual_name: str, vals) -> None:
    errs: list[float] = []
    skipped = 0
    for v in vals:
        err = _rel_err(getattr(v, recon_name), getattr(v, actual_name))
        if err is None:
            skipped += 1
            continue
        errs.append(err)
    mae, med, p90, mx = _err_stats(errs)
    print(
        f"{title} n={len(errs)} skipped={skipped} "
        f"mean_rel={mae} median_rel={med} p90_rel={p90} max_rel={mx}",
        flush=True,
    )


def _iso_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


def main() -> int:
    _configure_stdio()
    from okx.orderbook.models import QualityCode
    from okx.orderbook.reconstructor import reconstruct
    from okx.orderbook.repository import (
        fetch_anchor,
        fetch_snapshots,
        fetch_updates,
        upsert_samples,
        upsert_validations,
    )
    from okx.orderbook.schema import require_schema

    conn = _connect()
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("SELECT current_database();")
    dbname = cursor.fetchone()[0]
    print(f"Connected database={dbname} (password not logged)", flush=True)
    require_schema(cursor)

    from_ms = int(REVIEW_FROM.timestamp() * 1000)
    to_ms = from_ms + HOUR_MS
    from_iso = _iso_ms(from_ms)
    to_iso = _iso_ms(to_ms)
    print(f"\n=== processing interval [{from_iso} .. {to_iso}) ===", flush=True)

    cursor.execute(
        """
        SELECT count(DISTINCT snapshot_id), count(*)
        FROM okx_raw.orderbook_snapshots
        WHERE instid=%s AND ts_event_ms>=%s AND ts_event_ms<%s
        """,
        (INST, from_ms, to_ms),
    )
    snap_ids, snap_rows = cursor.fetchone()
    cursor.execute(
        """
        SELECT count(*) FROM okx_raw.orderbook_updates
        WHERE instid=%s AND ts_event_ms>=%s AND ts_event_ms<%s
        """,
        (INST, from_ms, to_ms),
    )
    upd_cnt = int(cursor.fetchone()[0])
    print(f"raw snapshots (distinct ids)={snap_ids} rows={snap_rows}", flush=True)
    print(f"raw updates={upd_cnt}", flush=True)
    if int(snap_ids or 0) < 1 or upd_cnt < 1:
        raise SystemExit("Hour has no snapshots or updates")

    anchor = fetch_anchor(cursor, INST, from_ms, LOOKBACK_MS)
    if anchor is None:
        raise SystemExit("NO_ANCHOR_SNAPSHOT for chosen hour")
    _, anchor_ts = anchor
    snapshots = fetch_snapshots(cursor, INST, anchor_ts, to_ms)
    updates = fetch_updates(cursor, INST, anchor_ts, to_ms)
    result = reconstruct(
        inst_id=INST,
        snapshots=snapshots,
        updates=updates,
        from_ms=from_ms,
        to_ms=to_ms,
    )
    upsert_samples(cursor, result.rows)
    upsert_validations(cursor, INST, result.validations)

    expected = (to_ms - from_ms) // 100
    actual = len(result.rows)
    valid = sum(1 for r in result.rows if r.is_valid)
    invalid = actual - valid
    mismatch_on_rows = sum(
        1 for r in result.rows if r.quality_code == int(QualityCode.SNAPSHOT_MISMATCH)
    )
    crossed = sum(
        1 for r in result.rows if r.quality_code == int(QualityCode.CROSSED_BOOK)
    )
    mismatch_ck = sum(1 for v in result.validations if not v.is_match)
    ages = [float(r.last_update_age_ms) for r in result.rows]
    gaps = [
        float(result.rows[i].ts_event_ms - result.rows[i - 1].ts_event_ms)
        for i in range(1, len(result.rows))
    ]

    vals = result.validations
    n_val = len(vals)

    def ratio(pred) -> float | None:
        if not n_val:
            return None
        return sum(1 for v in vals if pred(v)) / n_val * 100.0

    def mean_attr(name: str) -> float | None:
        if not n_val:
            return None
        return sum(getattr(v, name) for v in vals) / n_val

    print("\n=== reconstruction metrics ===", flush=True)
    print(f"generated 100ms rows={actual}", flush=True)
    print(f"expected rows={expected}", flush=True)
    print(f"actual rows={actual}", flush=True)
    print(f"p50 gap_ms={_pctile(gaps, 50)}", flush=True)
    print(f"p90 gap_ms={_pctile(gaps, 90)}", flush=True)
    print(f"p99 gap_ms={_pctile(gaps, 99)}", flush=True)
    print(f"valid rows %={0 if actual==0 else 100.0*valid/actual:.4f}", flush=True)
    print(f"invalid rows %={0 if actual==0 else 100.0*invalid/actual:.4f}", flush=True)
    print(f"fact SNAPSHOT_MISMATCH rows={mismatch_on_rows}", flush=True)
    print(f"number of validation checkpoints={n_val}", flush=True)
    print(f"L1 bid price match %={ratio(lambda v: v.l1_bid_price_match)}", flush=True)
    print(f"L1 ask price match %={ratio(lambda v: v.l1_ask_price_match)}", flush=True)
    print(f"L1 bid size match %={ratio(lambda v: v.l1_bid_size_match)}", flush=True)
    print(f"L1 ask size match %={ratio(lambda v: v.l1_ask_size_match)}", flush=True)
    print(
        f"L5 bid price match ratio={mean_attr('l5_bid_price_match_ratio')}", flush=True
    )
    print(
        f"L5 ask price match ratio={mean_attr('l5_ask_price_match_ratio')}", flush=True
    )
    print(f"L5 bid size match ratio={mean_attr('l5_bid_size_match_ratio')}", flush=True)
    print(f"L5 ask size match ratio={mean_attr('l5_ask_size_match_ratio')}", flush=True)
    print(
        f"L10 bid price match ratio={mean_attr('l10_bid_price_match_ratio')}",
        flush=True,
    )
    print(
        f"L10 ask price match ratio={mean_attr('l10_ask_price_match_ratio')}",
        flush=True,
    )
    print(
        f"L10 bid size match ratio={mean_attr('l10_bid_size_match_ratio')}", flush=True
    )
    print(
        f"L10 ask size match ratio={mean_attr('l10_ask_size_match_ratio')}", flush=True
    )
    print(f"crossed book count={crossed}", flush=True)
    print(f"snapshot mismatch count={mismatch_ck}", flush=True)
    print(f"max last_update_age_ms={max(ages) if ages else None}", flush=True)
    print(f"p50 last_update_age_ms={_pctile(ages, 50)}", flush=True)
    print(f"p90 last_update_age_ms={_pctile(ages, 90)}", flush=True)
    print(f"p99 last_update_age_ms={_pctile(ages, 99)}", flush=True)
    print(f"gap stdev={statistics.pstdev(gaps) if len(gaps)>1 else None}", flush=True)

    print("\n=== derived metric errors vs snapshot (all checkpoints) ===", flush=True)
    _print_err_block(
        "imbalance_l1", "reconstructed_imbalance_l1", "actual_imbalance_l1", vals
    )
    _print_err_block(
        "imbalance_l5", "reconstructed_imbalance_l5", "actual_imbalance_l5", vals
    )
    _print_err_block(
        "imbalance_l10", "reconstructed_imbalance_l10", "actual_imbalance_l10", vals
    )
    _print_rel_block(
        "bid_volume_l5 relative",
        "reconstructed_bid_volume_l5",
        "actual_bid_volume_l5",
        vals,
    )
    _print_rel_block(
        "ask_volume_l5 relative",
        "reconstructed_ask_volume_l5",
        "actual_ask_volume_l5",
        vals,
    )
    _print_rel_block(
        "bid_volume_l10 relative",
        "reconstructed_bid_volume_l10",
        "actual_bid_volume_l10",
        vals,
    )
    _print_rel_block(
        "ask_volume_l10 relative",
        "reconstructed_ask_volume_l10",
        "actual_ask_volume_l10",
        vals,
    )

    print("\n=== mismatch checkpoints (ex-post, not on fact is_valid) ===", flush=True)
    mismatches = [v for v in vals if not v.is_match]
    print(f"count={len(mismatches)}", flush=True)
    for v in mismatches:
        l5_levels = [m for m in v.mismatched_levels if m.level <= 5]
        l10_imb_err = _abs_err(v.reconstructed_imbalance_l10, v.actual_imbalance_l10)
        l5_imb_err = _abs_err(v.reconstructed_imbalance_l5, v.actual_imbalance_l5)
        print(
            f"\nB {_iso_ms(v.validation_ts_event_ms)} id={v.validation_snapshot_id}",
            flush=True,
        )
        print(
            f"  A_ts={v.source_snapshot_ts_event_ms} updates={v.number_updates_applied} "
            f"interval_ms={v.interval_duration_ms}",
            flush=True,
        )
        print(
            f"  L1 px bid={v.l1_bid_price_match} ask={v.l1_ask_price_match} "
            f"sz bid={v.l1_bid_size_match} ask={v.l1_ask_size_match}",
            flush=True,
        )
        print(
            f"  L5 ratios px_bid={v.l5_bid_price_match_ratio} "
            f"px_ask={v.l5_ask_price_match_ratio} "
            f"sz_bid={v.l5_bid_size_match_ratio} "
            f"sz_ask={v.l5_ask_size_match_ratio}",
            flush=True,
        )
        print(
            f"  L10 ratios px_bid={v.l10_bid_price_match_ratio} "
            f"px_ask={v.l10_ask_price_match_ratio} "
            f"sz_bid={v.l10_bid_size_match_ratio} "
            f"sz_ask={v.l10_ask_size_match_ratio}",
            flush=True,
        )
        print(
            f"  affects_L5={bool(l5_levels)} "
            f"imbalance_l5_abs_err={l5_imb_err} imbalance_l10_abs_err={l10_imb_err}",
            flush=True,
        )
        print(
            f"  imbalance_l5 recon={v.reconstructed_imbalance_l5} "
            f"actual={v.actual_imbalance_l5}",
            flush=True,
        )
        print(
            f"  imbalance_l10 recon={v.reconstructed_imbalance_l10} "
            f"actual={v.actual_imbalance_l10}",
            flush=True,
        )
        if not v.mismatched_levels:
            print("  mismatched_levels=(none listed)", flush=True)
            continue
        for m in v.mismatched_levels:
            kind = []
            if not m.price_match:
                kind.append("price")
            if not m.size_match:
                kind.append("size")
            print(
                f"  {m.side} L{m.level} {'+'.join(kind)} "
                f"recon_px={m.recon_px} actual_px={m.actual_px} px_err={m.px_abs_err} "
                f"recon_sz={m.recon_sz} actual_sz={m.actual_sz} sz_err={m.sz_abs_err}",
                flush=True,
            )

    cursor.execute(
        """
        SELECT ts_event, ts_event_ms, bid_px_01, bid_sz_01, ask_px_01, ask_sz_01,
               imbalance_l1, imbalance_l5, imbalance_l10, is_valid, quality_code
        FROM okx_core.fact_orderbook_l10_100ms
        WHERE inst_id = %s AND ts_event >= %s AND ts_event < %s
        ORDER BY ts_event
        LIMIT 20;
        """,
        (
            INST,
            datetime.fromtimestamp(from_ms / 1000.0, tz=timezone.utc),
            datetime.fromtimestamp(to_ms / 1000.0, tz=timezone.utc),
        ),
    )
    print("\n=== 20 consecutive target rows (causal flags) ===", flush=True)
    print(
        "ts_event | ts_event_ms | bid_px_01 | bid_sz_01 | ask_px_01 | ask_sz_01 | "
        "imbalance_l1 | imbalance_l5 | imbalance_l10 | is_valid | quality_code",
        flush=True,
    )
    for row in cursor.fetchall():
        print(" | ".join("" if v is None else str(v) for v in row), flush=True)

    if len(snapshots) >= 2:
        a, b = snapshots[0], snapshots[1]
        n_upd = sum(
            1 for u in updates if a.ts_event_ms < u.ts_event_ms <= b.ts_event_ms
        )
        report = next(
            (
                v
                for v in result.validations
                if v.validation_snapshot_id == b.snapshot_id
            ),
            None,
        )
        print("\n=== snapshot A -> updates -> snapshot B ===", flush=True)
        print(
            f"snapshot A time={_iso_ms(a.ts_event_ms)} id={a.snapshot_id}", flush=True
        )
        print(
            f"snapshot B time={_iso_ms(b.ts_event_ms)} id={b.snapshot_id}", flush=True
        )
        print(f"number of updates between A and B={n_upd}", flush=True)
        if report:
            print(
                f"reconstructed L1 bid={report.reconstructed_best_bid} "
                f"ask={report.reconstructed_best_ask}",
                flush=True,
            )
            print(
                f"actual snapshot B L1 bid={report.actual_best_bid} "
                f"ask={report.actual_best_ask}",
                flush=True,
            )
            print(
                "L10 ratios "
                f"bid_px={report.l10_bid_price_match_ratio} "
                f"ask_px={report.l10_ask_price_match_ratio} "
                f"bid_sz={report.l10_bid_size_match_ratio} "
                f"ask_sz={report.l10_ask_size_match_ratio}",
                flush=True,
            )
            print(f"is_match={report.is_match}", flush=True)

    cursor.close()
    conn.close()
    print("\nSTATUS=SUCCESS", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        _configure_stdio()
        print(f"STATUS=FAILED {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        raise
