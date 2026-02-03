from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


MASTER_DAG_ID = "okx_master_raw_to_core_daily"
SCHEDULE = "10 0 * * *"  # t-1: запускаем после закрытия суток (UTC)

TAGS = ["okx", "etl", "master", "raw-to-core", "t-1"]

# У дочерних DAG одна задача — sync
CHILD_TASK_ID = "sync"

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


# Ожидание через ExternalTaskSensor надёжнее, чем wait_for_completion=True в триггере
# (известный баг: TriggerDagRunOperator иногда не видит success дочернего DAG).
#
# Важно: триггер передаёт conf={"logical_date": "{{ data_interval_end }}"}, и дочерний
# run создаётся с execution_date = data_interval_end. Сенсор по умолчанию ищет задачу
# с execution_date текущего run (00:10:00) — даты не совпадают, сенсор "залипает".
# execution_date_fn возвращает data_interval_end, чтобы искать тот же run, что создал триггер.


def _external_execution_date_fn(execution_date, context=None, **kwargs):
    """Возвращаем data_interval_end — с ним создаётся дочерний run (conf logical_date)."""
    ctx = context if isinstance(context, dict) else kwargs
    end = ctx.get("data_interval_end") if isinstance(ctx, dict) else None
    if end is not None:
        return end
    # Fallback: schedule "10 0 * * *" => data_interval_end = следующий день 00:00 UTC
    if execution_date.tzinfo is None:
        execution_date = execution_date.replace(tzinfo=timezone.utc)
    day_start = execution_date.replace(hour=0, minute=0, second=0, microsecond=0)
    return day_start + timedelta(days=1)


default_args = {
    "owner": "okx-data",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
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
    for child_dag_id in CHILD_DAGS_IN_ORDER:
        trigger = TriggerDagRunOperator(
            task_id=f"run_{child_dag_id}",
            trigger_dag_id=child_dag_id,
            conf={"logical_date": "{{ data_interval_end }}"},
            wait_for_completion=False,
            reset_dag_run=True,
        )
        wait = ExternalTaskSensor(
            task_id=f"wait_{child_dag_id}",
            external_dag_id=child_dag_id,
            external_task_id=CHILD_TASK_ID,
            allowed_states=["success"],
            failed_states=["failed"],
            poke_interval=60,
            mode="poke",
            execution_date_fn=_external_execution_date_fn,
        )
        trigger >> wait
        if prev is not None:
            prev >> trigger
        prev = wait
