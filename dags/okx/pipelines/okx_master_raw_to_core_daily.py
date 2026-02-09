from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
from airflow.models.dag import DagModel
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.session import create_session


MASTER_DAG_ID = "okx_master_raw_to_core_daily"
# t-1: 00:10 UTC, data_interval = [вчера 00:00, сегодня 00:00)
SCHEDULE = "10 0 * * *"

TAGS = ["okx", "etl", "master", "raw-to-core", "t-1"]

# Порядок: сначала raw->core, затем core->feat / core->core
CHILD_DAGS_IN_ORDER = [
    "okx_raw_to_core_ticker_tick",
    "okx_raw_to_core_trades_tick",
    # Orderbook snapshots:
    # (оставляем, если этот DAG уже существует и делает raw->core по snapshot-таблице)
    # "okx_raw_to_core_orderbook_snapshot",
    # Новый компактный слой L10 из raw snapshots:
    "okx_raw_to_core_orderbook_l10_snapshot",
    "okx_raw_to_core_funding_rate_tick",
    "okx_raw_to_core_mark_price_tick",
    "okx_raw_to_core_open_interest_tick",
    "okx_raw_to_core_index_tick",
    "okx_core_tick_to_core_funding_rate_event",
    # Новый feature слой:
    "okx_core_to_feat_hybrid_10ms",
    # Health tables:
    "okx_health_table_ranges_daily",
]

default_args = {
    "owner": "okx-data",
    "retries": 0,
}


with DAG(
    dag_id=MASTER_DAG_ID,
    description="OKX master: t-1 raw->core and core->feat/core->core (trigger only, no wait)",
    default_args=default_args,
    start_date=datetime(2026, 2, 1, tzinfo=timezone.utc),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=TAGS,
) as dag:

    def _check_children_not_paused() -> None:
        """Падаем, если хотя бы один дочерний DAG на паузе — иначе зелёный мастер без данных."""
        with create_session() as session:
            for dag_id in CHILD_DAGS_IN_ORDER:
                row = session.query(DagModel).filter(
                    DagModel.dag_id == dag_id).first()
                if row is None:
                    raise RuntimeError(
                        f"[{MASTER_DAG_ID}] Дочерний DAG не найден: {dag_id}")
                if row.is_paused:
                    raise RuntimeError(
                        f"[{MASTER_DAG_ID}] Дочерний DAG на паузе: {dag_id}. "
                        "Снимите с паузы перед запуском мастера."
                    )

    check = PythonOperator(
        task_id="check_children_not_paused",
        python_callable=_check_children_not_paused,
    )

    prev = None
    for child_dag_id in CHILD_DAGS_IN_ORDER:
        trigger = TriggerDagRunOperator(
            task_id=f"run_{child_dag_id}",
            trigger_dag_id=child_dag_id,
            logical_date="{{ data_interval_end }}",
            conf={
                "logical_date": "{{ data_interval_end.isoformat() }}",
                "data_interval_start": "{{ data_interval_start.isoformat() }}",
                "data_interval_end": "{{ data_interval_end.isoformat() }}",
            },
            wait_for_completion=True,
            reset_dag_run=True,
            allowed_states=["success"],
            failed_states=["failed", "upstream_failed"],
        )
        if prev is None:
            check >> trigger
        else:
            prev >> trigger
        prev = trigger
 