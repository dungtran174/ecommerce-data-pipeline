"""Materialize and publish the Silver/Gold warehouse every five minutes."""
from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from helpers.warehouse_refresh import Refresher, refresh_lock


def run_etl(**kwargs):
    with refresh_lock():
        job = Refresher()
        result = job.refresh()
        # Cleanup is a separate task: retention failure cannot mark a published
        # generation as an unsuccessful ETL and trigger a misleading rollback.
        return result


def cleanup_snapshots():
    with refresh_lock():
        return Refresher().cleanup(int(os.getenv("WAREHOUSE_RETENTION_HOURS", "2")))


with DAG(
    dag_id="etl_fact_sales_product",
    description="Build, validate and publish physical Silver/Gold snapshots",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1),
                  "execution_timeout": timedelta(minutes=20)},
    tags=["ecommerce", "etl", "clickhouse", "silver", "gold"],
) as dag:
    refresh = PythonOperator(task_id="run_etl_fact_sales_product", python_callable=run_etl)
    cleanup = PythonOperator(task_id="cleanup_old_snapshots", python_callable=cleanup_snapshots)
    refresh >> cleanup
