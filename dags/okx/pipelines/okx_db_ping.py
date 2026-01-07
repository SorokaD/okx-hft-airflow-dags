from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


CONN_ID = "timescaledb"  # <-- твой Conn Id из UI


def ping_db() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)

    # SELECT 1
    val = hook.get_first("SELECT 1;")
    if not val or val[0] != 1:
        raise RuntimeError(f"Unexpected SELECT 1 result: {val}")

    # Бонус: узнаем имя базы (полезно для валидации, что подключились куда надо)
    dbname = hook.get_first("SELECT current_database();")
    print(f"OK: SELECT 1 returned {val[0]}; current_database()={dbname[0] if dbname else None}")


default_args = {
    "owner": "okx-data",
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
    # чтобы не зависало бесконечно, если сеть легла
    "execution_timeout": timedelta(seconds=20),
}

with DAG(
    dag_id="okx_db_ping_1m",
    description="Health: every minute ping TimescaleDB via SELECT 1",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="*/1 * * * *",  # каждую минуту
    catchup=False,
    max_active_runs=1,
    tags=["okx", "health", "timescaledb"],
) as dag:
    PythonOperator(
        task_id="select_1",
        python_callable=ping_db,
    )
