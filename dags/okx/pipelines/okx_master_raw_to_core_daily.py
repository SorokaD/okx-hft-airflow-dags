from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


MASTER_DAG_ID = "okx_master_raw_to_core_daily"
SCHEDULE = "10 0 * * *"  # t-1: запускаем после закрытия суток (UTC)

TAGS = ["okx", "etl", "master", "raw-to-core", "t-1"]

CHILD_DAGS_IN_ORDER = [
    # raw -> core
    "okx_raw_to_core_ticker_tick",
    "okx_raw_to_core_trades_tick",
    "okx_raw_to_core_orderbook_updates",
    "okx_raw_to_core_orderbook_snapshot",
    "okx_raw_to_core_funding_rate_tick",
    "okx_raw_to_core_mark_price_tick",
    "okx_raw_to_core_open_interest_tick",
    "okx_raw_to_core_index_tick",
    # core -> core (зависимые)
    "okx_core_orderbook_update_level",
    "okx_core_tick_to_core_funding_rate_event",
]


# Максимум ожидания дочернего DAG (не висеть бесконечно при «залипании»)
TRIGGER_TASK_TIMEOUT_SEC = 6 * 60 * 60

default_args = {
    "owner": "okx-data",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(seconds=TRIGGER_TASK_TIMEOUT_SEC),
}


with DAG(
    dag_id=MASTER_DAG_ID,
    description="OKX master: t-1 raw->core and core->core loads (sequential)",
    default_args=default_args,
    start_date=datetime(2026, 2, 1, tzinfo=timezone.utc),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=TAGS,
) as dag:
    prev = None
    for dag_id in CHILD_DAGS_IN_ORDER:
        task = TriggerDagRunOperator(
            task_id=f"run_{dag_id}",
            trigger_dag_id=dag_id,
            conf={"logical_date": "{{ data_interval_end }}"},
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=60,
            allowed_states=["success"],
            failed_states=["failed"],
        )
        if prev is not None:
            prev >> task
        prev = task
