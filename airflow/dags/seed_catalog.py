from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from generator import catalog


with DAG(
    dag_id="seed_catalog",
    description="Initialize reference data, products and marketing campaigns",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["ecommerce", "catalog"],
) as dag:
    regions = PythonOperator(task_id="generate_regions", python_callable=catalog.generate_regions)
    provinces = PythonOperator(task_id="generate_provinces", python_callable=catalog.generate_provinces)
    brands = PythonOperator(task_id="generate_brands", python_callable=catalog.generate_brands)
    categories = PythonOperator(task_id="generate_parent_categories", python_callable=catalog.generate_parent_categories)
    subcategories = PythonOperator(task_id="generate_sub_categories", python_callable=catalog.generate_sub_categories)
    tags = PythonOperator(task_id="generate_tags", python_callable=catalog.generate_tags)
    campaigns = PythonOperator(task_id="generate_campaigns", python_callable=catalog.generate_campaigns)
    discounts = PythonOperator(task_id="generate_discounts", python_callable=catalog.generate_discounts)
    products = PythonOperator(task_id="generate_products", python_callable=catalog.generate_products)
    product_tags = PythonOperator(task_id="assign_product_tags", python_callable=catalog.assign_product_tags)

    regions >> provinces
    categories >> subcategories
    [brands, subcategories] >> products
    [products, tags] >> product_tags
    campaigns >> discounts
