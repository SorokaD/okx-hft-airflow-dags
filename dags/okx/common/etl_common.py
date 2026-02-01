"""
Shared ingestion-based ETL helpers for OKX DAGs.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable
import logging

from airflow.operators.python import get_current_context


logger = logging.getLogger(__name__)


def parse_run_date(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            try:
                dt = datetime.strptime(value, "%Y-%m-%d")
            except ValueError:
                return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def get_logical_run_date() -> datetime:
    try:
        ctx = get_current_context()
    except Exception:
        return datetime.now(timezone.utc)

    dr = ctx.get("dag_run")
    if dr and dr.conf:
        dt = parse_run_date(dr.conf.get("logical_date"))
        if dt:
            return dt

    logical_date = ctx.get("logical_date")
    if isinstance(logical_date, datetime):
        return logical_date.astimezone(timezone.utc)

    data_interval_end = ctx.get("data_interval_end")
    if isinstance(data_interval_end, datetime):
        return data_interval_end.astimezone(timezone.utc)

    return datetime.now(timezone.utc)


def day_start_utc(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def db_sanity_checks(cursor, expected_db: str) -> None:
    cursor.execute("SELECT 1;")
    row = cursor.fetchone()
    if not row or row[0] != 1:
        raise RuntimeError(f"DB ping failed: {row}")

    cursor.execute("SELECT current_database();")
    row = cursor.fetchone()
    dbname = row[0] if row else None
    if expected_db and dbname != expected_db:
        raise RuntimeError(
            f"Connected to unexpected database: {dbname} (expected {expected_db})"
        )


def log_diagnostics(cursor, tables: Iterable[str]) -> None:
    table_names = [t.split(".")[-1] for t in tables]
    tables_sql = ", ".join([f"'{t}'" for t in table_names])

    cursor.execute(
        f"""
        SELECT hypertable_schema, hypertable_name, compression_enabled
        FROM timescaledb_information.hypertables
        WHERE hypertable_schema IN ('okx_raw', 'okx_core')
          AND hypertable_name IN ({tables_sql});
        """
    )
    logger.info("Hypertables: %s", cursor.fetchall())

    cursor.execute(
        """
        SELECT application_name, proc_name, config
        FROM timescaledb_information.jobs
        WHERE proc_name IN ('policy_retention', 'policy_compression');
        """
    )
    logger.info("Policies: %s", cursor.fetchall())

    cursor.execute(
        f"""
        SELECT tablename, count(*) AS idx_cnt
        FROM pg_indexes
        WHERE schemaname IN ('okx_raw', 'okx_core')
          AND tablename IN ({tables_sql})
        GROUP BY tablename
        ORDER BY tablename;
        """
    )
    logger.info("Index counts: %s", cursor.fetchall())

    cursor.execute(
        f"""
        SELECT tablename, indexdef, count(*) AS dup_cnt
        FROM pg_indexes
        WHERE schemaname IN ('okx_raw', 'okx_core')
          AND tablename IN ({tables_sql})
        GROUP BY tablename, indexdef
        HAVING count(*) > 1;
        """
    )
    logger.info("Duplicate indexes: %s", cursor.fetchall())


def batch_iter(values: list[str], batch_size: int) -> Iterable[list[str]]:
    for i in range(0, len(values), batch_size):
        yield values[i : i + batch_size]
"""
Common ingestion-based ETL helpers for OKX raw->core and core->core DAGs.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Sequence
import logging

from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EtlSpec:
    dag_id: str
    source_table: str
    target_table: str
    source_ingest_col: str  # ts_ingest_ms or ts_ingest
    source_ingest_type: str  # "ms" | "ts"
    target_ingest_col: str  # usually ts_ingest in core
    conflict_cols: Sequence[str]
    select_exprs: Sequence[tuple[str, str]]  # (target_col, sql_expr)
    from_sql_template: str | None = None  # if set, must include {where_clause}
    batch_key_col: str | None = None  # e.g. instid or inst_id
    batch_size: int = 20
    overlap_minutes: int = 2
    statement_timeout_ms: int = 30 * 60 * 1000  # 30 minutes
    diagnostics_enabled: bool = True


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_run_date(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            try:
                dt = datetime.strptime(value, "%Y-%m-%d")
            except ValueError:
                return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _get_logical_run_date() -> datetime:
    try:
        ctx = get_current_context()
    except Exception:
        return _now_utc()

    dr = ctx.get("dag_run")
    if dr and dr.conf:
        dt = _parse_run_date(dr.conf.get("logical_date"))
        if dt:
            return dt

    logical_date = ctx.get("logical_date")
    if isinstance(logical_date, datetime):
        return logical_date.astimezone(timezone.utc)

    data_interval_end = ctx.get("data_interval_end")
    if isinstance(data_interval_end, datetime):
        return data_interval_end.astimezone(timezone.utc)

    return _now_utc()


def _day_start_utc(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def _ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _db_sanity_checks(cursor) -> None:
    cursor.execute("SELECT 1;")
    row = cursor.fetchone()
    if not row or row[0] != 1:
        raise RuntimeError(f"DB ping failed: {row}")

    cursor.execute("SELECT current_database();")
    row = cursor.fetchone()
    dbname = row[0] if row else None
    if dbname != "okx_hft":
        raise RuntimeError(
            f"Connected to unexpected database: {dbname} (expected okx_hft)"
        )


def _get_target_watermark(cursor, target_table: str, target_ingest_col: str) -> datetime | None:
    cursor.execute(f"SELECT max({target_ingest_col}) FROM {target_table};")
    row = cursor.fetchone()
    return row[0] if row and row[0] is not None else None


def _get_source_max_ms(cursor, source_table: str, source_ingest_col: str) -> int | None:
    cursor.execute(f"SELECT max({source_ingest_col}) FROM {source_table};")
    row = cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def _get_source_max_ts(cursor, source_table: str, source_ingest_col: str) -> datetime | None:
    cursor.execute(f"SELECT max({source_ingest_col}) FROM {source_table};")
    row = cursor.fetchone()
    return row[0] if row and row[0] is not None else None


def _build_where_clause(
    source_ingest_col: str,
    source_ingest_type: str,
    from_dt: datetime | None,
    to_dt: datetime,
) -> str:
    if source_ingest_type == "ms":
        to_ms = _ms(to_dt)
        if from_dt is None:
            return f"{source_ingest_col} < {to_ms}"
        from_ms = _ms(from_dt)
        return f"{source_ingest_col} >= {from_ms} AND {source_ingest_col} < {to_ms}"

    if source_ingest_type == "ts":
        to_ts = to_dt.isoformat()
        if from_dt is None:
            return f"{source_ingest_col} < '{to_ts}'::timestamptz"
        from_ts = from_dt.isoformat()
        return (
            f"{source_ingest_col} >= '{from_ts}'::timestamptz "
            f"AND {source_ingest_col} < '{to_ts}'::timestamptz"
        )

    raise ValueError(f"Unsupported source_ingest_type: {source_ingest_type}")


def _log_diagnostics(cursor, tables: Sequence[str]) -> None:
    table_names = [t.split(".")[-1] for t in tables]
    tables_sql = ", ".join([f"'{t}'" for t in table_names])

    cursor.execute(
        f"""
        SELECT hypertable_schema, hypertable_name, compression_enabled
        FROM timescaledb_information.hypertables
        WHERE hypertable_schema IN ('okx_raw', 'okx_core')
          AND hypertable_name IN ({tables_sql});
        """
    )
    logger.info("Hypertables: %s", cursor.fetchall())

    cursor.execute(
        """
        SELECT application_name, proc_name, config
        FROM timescaledb_information.jobs
        WHERE proc_name IN ('policy_retention', 'policy_compression');
        """
    )
    logger.info("Policies: %s", cursor.fetchall())

    cursor.execute(
        f"""
        SELECT tablename, count(*) AS idx_cnt
        FROM pg_indexes
        WHERE schemaname IN ('okx_raw', 'okx_core')
          AND tablename IN ({tables_sql})
        GROUP BY tablename
        ORDER BY tablename;
        """
    )
    logger.info("Index counts: %s", cursor.fetchall())

    cursor.execute(
        f"""
        SELECT tablename, indexdef, count(*) AS dup_cnt
        FROM pg_indexes
        WHERE schemaname IN ('okx_raw', 'okx_core')
          AND tablename IN ({tables_sql})
        GROUP BY tablename, indexdef
        HAVING count(*) > 1;
        """
    )
    logger.info("Duplicate indexes: %s", cursor.fetchall())


def _insert_batch(
    cursor,
    spec: EtlSpec,
    from_sql: str,
    where_clause: str,
    batch_values: Sequence[str] | None,
) -> int:
    cols = ", ".join([c for c, _ in spec.select_exprs])
    exprs = ", ".join([e for _, e in spec.select_exprs])
    conflict_cols = ", ".join(spec.conflict_cols)

    batch_filter = ""
    params = None
    if batch_values is not None:
        batch_filter = f" AND {spec.batch_key_col} = ANY(%(batch_keys)s)"
        params = {"batch_keys": list(batch_values)}

    sql = f"""
    WITH ins AS (
      INSERT INTO {spec.target_table} ({cols})
      SELECT {exprs}
      FROM {from_sql}
      WHERE {where_clause}{batch_filter}
      ON CONFLICT ({conflict_cols}) DO NOTHING
      RETURNING 1
    )
    SELECT count(*)::bigint AS inserted_rows FROM ins;
    """
    cursor.execute(sql, params)
    row = cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def run_etl(spec: EtlSpec, conn_id: str = "timescaledb") -> None:
    hook = PostgresHook(postgres_conn_id=conn_id)
    run_dt = _get_logical_run_date()
    to_dt = _day_start_utc(run_dt)

    conn = hook.get_conn()
    conn.autocommit = True
    cursor = conn.cursor()

    _db_sanity_checks(cursor)
    cursor.execute("SET statement_timeout = %s", (spec.statement_timeout_ms,))

    if spec.diagnostics_enabled:
        _log_diagnostics(
            cursor, tables=[spec.source_table, spec.target_table]
        )

    watermark = _get_target_watermark(cursor, spec.target_table, spec.target_ingest_col)
    from_dt = (
        watermark - timedelta(minutes=spec.overlap_minutes)
        if watermark is not None
        else None
    )

    where_clause = _build_where_clause(
        spec.source_ingest_col, spec.source_ingest_type, from_dt, to_dt
    )

    if spec.from_sql_template:
        from_sql = spec.from_sql_template.format(where_clause=where_clause)
        insert_where_clause = "TRUE"
    else:
        from_sql = spec.source_table
        insert_where_clause = where_clause

    if spec.source_ingest_type == "ms":
        src_max = _get_source_max_ms(cursor, spec.source_table, spec.source_ingest_col)
        if src_max is None:
            logger.info("[%s] SKIP: raw empty", spec.dag_id)
            return
        if from_dt is not None and src_max < _ms(from_dt):
            logger.info(
                "[%s] SKIP: raw older than window raw_max_ms=%s",
                spec.dag_id,
                src_max,
            )
            return
    else:
        src_max = _get_source_max_ts(cursor, spec.source_table, spec.source_ingest_col)
        if src_max is None or (from_dt is not None and src_max < from_dt):
            logger.info(
                "[%s] SKIP: src empty or older than window src_max=%s",
                spec.dag_id,
                src_max,
            )
            return

    inserted_total = 0

    if spec.batch_key_col:
        cursor.execute(
            f"SELECT DISTINCT {spec.batch_key_col} FROM {spec.source_table} WHERE {where_clause};"
        )
        keys = [r[0] for r in cursor.fetchall() if r and r[0] is not None]
    else:
        keys = []

    if not keys or len(keys) <= spec.batch_size:
        inserted_total += _insert_batch(
            cursor,
            spec,
            from_sql,
            insert_where_clause,
            None,
        )
        logger.info("[%s] inserted_total=%s", spec.dag_id, inserted_total)
        return

    logger.info(
        "[%s] batching by %s: total_keys=%s batch_size=%s",
        spec.dag_id,
        spec.batch_key_col,
        len(keys),
        spec.batch_size,
    )

    for i in range(0, len(keys), spec.batch_size):
        batch = keys[i : i + spec.batch_size]
        inserted = _insert_batch(
            cursor,
            spec,
            from_sql,
            insert_where_clause,
            batch,
        )
        inserted_total += inserted
        logger.info(
            "[%s] batch %s-%s inserted=%s",
            spec.dag_id,
            i,
            i + len(batch) - 1,
            inserted,
        )

    logger.info("[%s] DONE inserted_total=%s", spec.dag_id, inserted_total)
