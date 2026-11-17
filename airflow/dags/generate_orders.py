from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from generator.orders import generate_bulk_orders


with DAG(
    dag_id="generate_orders",
    description="Simulate varied orders and their fulfillment lifecycle",
    schedule="* * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["ecommerce", "simulation"],
) as dag:
    PythonOperator(
        task_id="generate_bulk_orders",
        python_callable=generate_bulk_orders,
        op_kwargs={"count": 100},
    )
