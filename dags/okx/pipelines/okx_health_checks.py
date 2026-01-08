"""
OKX Health Checks DAG.

Simple health check pipeline that runs every 5 minutes
to verify Airflow is operational.
"""

from datetime import datetime

from airflow.decorators import dag, task

from okx.common.settings import DEFAULT_ARGS, TAGS_HEALTH


@dag(
    dag_id="okx_health_checks",
    default_args=DEFAULT_ARGS,
    description="Health check pipeline for OKX platform",
    schedule="*/5 * * * *",  # Every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=TAGS_HEALTH,
)
def okx_health_checks():
    """
    Simple health check DAG.

    Verifies that:
    - Airflow scheduler is running
    - DAGs are being picked up
    - Tasks can execute
    """

    @task
    def health_check() -> str:
        """Log that the pipeline is alive."""
        import logging
        from datetime import datetime, timezone

        logger = logging.getLogger(__name__)

        current_time = datetime.now(timezone.utc).isoformat()
        message = f"OKX pipeline alive at {current_time}"

        logger.info(message)

        return message

    health_check()


# Instantiate the DAG
okx_health_checks()
