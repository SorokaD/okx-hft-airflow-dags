from __future__ import annotations

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


MASTER_DAG_ID = "okx_master_raw_to_core_daily"
SCHEDULE = "10 0 * * *"  # t-1: запускаем после закрытия суток (UTC)

TAGS = ["okx", "etl", "master", "raw-to-core", "t-1"]

CHILD_DAGS_IN_ORDER = [
    "okx_raw_to_core_ticker_tick",
    "okx_raw_to_core_trades_tick",
    "okx_raw_to_core_orderbook_updates",
    "okx_raw_to_core_orderbook_snapshot",
    "okx_raw_to_core_funding_rate_tick",
    "okx_raw_to_core_mark_price_tick",
    "okx_raw_to_core_open_interest_tick",
    "okx_raw_to_core_index_tick",
    "okx_core_orderbook_update_level",
    "okx_core_tick_to_core_funding_rate_event",
]

# Без wait_for_completion и без сенсоров: мастер только запускает дочерние DAG по очереди
# и сразу идёт дальше. Дочерние отрабатывают сами (по одному или параллельно — как решит executor).
# Так мастер не зависает; порядок запуска сохраняется (ticker → trades → ...).

default_args = {
    "owner": "okx-data",
    "retries": 0,
}

with DAG(
    dag_id=MASTER_DAG_ID,
    description="OKX master: t-1 raw->core and core->core (trigger only, no wait)",
    default_args=default_args,
    start_date=datetime(2026, 2, 1, tzinfo=timezone.utc),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=TAGS,
) as dag:
    prev = None
    for child_dag_id in CHILD_DAGS_IN_ORDER:
        trigger = TriggerDagRunOperator(
            task_id=f"run_{child_dag_id}",
            trigger_dag_id=child_dag_id,
            conf={"logical_date": "{{ data_interval_end }}"},
            wait_for_completion=False,
            reset_dag_run=True,
        )
        if prev is not None:
            prev >> trigger
        prev = trigger
