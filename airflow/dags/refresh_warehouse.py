from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from warehouse.refresh import Refresher, refresh_lock


def run_etl(**kwargs):
    with refresh_lock():
        job = Refresher()
        result = job.refresh()
        return result


def cleanup_snapshots():
    with refresh_lock():
        return Refresher().cleanup(2)


with DAG(
    dag_id="refresh_warehouse",
    description="Upsert changed entities, rebuild affected dates and publish reports",
    schedule="* * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(minutes=30),
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1),
                  "execution_timeout": timedelta(minutes=20)},
    tags=["ecommerce", "etl", "clickhouse", "silver", "gold"],
) as dag:
    refresh = PythonOperator(task_id="refresh_silver_gold", python_callable=run_etl)
    cleanup = PythonOperator(task_id="cleanup_old_snapshots", python_callable=cleanup_snapshots)
    refresh >> cleanup
