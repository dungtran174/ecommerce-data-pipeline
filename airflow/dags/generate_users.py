from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from generator.users import register_users, assign_roles, create_addresses


with DAG(
    dag_id="generate_users",
    description="Simulate customer registrations, roles and addresses",
    schedule="*/2 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["ecommerce", "simulation"],
) as dag:
    users = PythonOperator(task_id="register_users", python_callable=register_users)
    roles = PythonOperator(task_id="assign_roles", python_callable=assign_roles)
    addresses = PythonOperator(task_id="create_addresses", python_callable=create_addresses)
    users >> roles >> addresses
