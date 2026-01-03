"""
Alerting utilities for OKX DAGs.

Provides functions for sending alerts on DAG failures.
Actual notification channels (Telegram, Slack, etc.) are configured
via Airflow Connections.
"""

import logging

logger = logging.getLogger(__name__)


def on_failure_callback(context: dict) -> None:
    """
    Callback for DAG/task failure.
    
    Use this in default_args:
        default_args = {
            **DEFAULT_ARGS,
            "on_failure_callback": on_failure_callback,
        }
    """
    dag_id = context.get("dag", {}).dag_id if context.get("dag") else "unknown"
    task_id = context.get("task_instance", {}).task_id if context.get("task_instance") else "unknown"
    execution_date = context.get("execution_date", "unknown")
    
    message = f"❌ DAG Failed: {dag_id} | Task: {task_id} | Date: {execution_date}"
    logger.error(message)
    
    # TODO: Integrate with Telegram/Slack via Airflow Connection
    # Example:
    # from airflow.hooks.base import BaseHook
    # conn = BaseHook.get_connection("telegram_alerts")
    # send_telegram_message(conn.password, conn.host, message)


def on_success_callback(context: dict) -> None:
    """
    Callback for DAG/task success.
    """
    dag_id = context.get("dag", {}).dag_id if context.get("dag") else "unknown"
    logger.info(f"✅ DAG Succeeded: {dag_id}")


