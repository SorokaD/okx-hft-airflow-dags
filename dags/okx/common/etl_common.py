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
    """
    Дата run'а для t-1: при запуске через мастер — из dag_run.conf
    (logical_date или data_interval_end); при ручном — из context.
    Окно загрузки: ts_ingest < day_start_utc(run_dt) = полночь data_interval_end.
    """
    try:
        ctx = get_current_context()
    except Exception:
        logger.warning("get_logical_run_date: no context, using now(utc)")
        return datetime.now(timezone.utc)

    dr = ctx.get("dag_run")
    if dr and isinstance(getattr(dr, "conf", None), dict):
        conf = dr.conf
        # Сначала logical_date (мастер передаёт data_interval_end как logical_date)
        dt = parse_run_date(conf.get("logical_date"))
        if dt is not None:
            return dt
        # Запасной вариант: data_interval_end из conf (тот же контракт)
        dt = parse_run_date(conf.get("data_interval_end"))
        if dt is not None:
            return dt
        # conf передан, но даты не распарсились — не молча грузить по now(utc)
        raise ValueError(
            "get_logical_run_date: conf передан, но logical_date и data_interval_end "
            "не удалось распарсить. Проверьте, что мастер передаёт ISO-строки в conf."
        )

    logical_date = ctx.get("logical_date")
    if isinstance(logical_date, datetime):
        return logical_date.astimezone(timezone.utc)

    data_interval_end = ctx.get("data_interval_end")
    if isinstance(data_interval_end, datetime):
        return data_interval_end.astimezone(timezone.utc)

    logger.warning(
        "get_logical_run_date: no conf and no context date, using now(utc)"
    )
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
            f"Connected to unexpected database: {dbname} "
            f"(expected {expected_db})"
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
        yield values[i:i + batch_size]
