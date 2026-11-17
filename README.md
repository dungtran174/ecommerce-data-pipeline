# E-Commerce Data Pipeline

An e-commerce analytics pipeline using simulated transactions that captures changes from PostgreSQL and builds a ClickHouse warehouse for sales, orders, customers, and marketing reports.

![Pipeline architecture](images/architecture.png)

## Problem and solution

Analytical queries on the transactional database compete with order processing, especially during flash sales. This project moves reporting to a separate warehouse and updates reports as source data changes.

- **Debezium and Kafka** capture and buffer changes from 22 PostgreSQL tables.
- **ClickHouse Medallion architecture:** Bronze retains CDC records, Silver stores cleaned current-state entities, and Gold contains precomputed facts and dimensions for reporting.
- **Incremental processing** updates changed records and rebuilds affected report dates, including historical corrections and cancellations.
- **Airflow** refreshes the warehouse every minute. Validated reports are published atomically; failed jobs preserve the previous reports, and retries avoid duplicate results.
- **Metabase** reads five reporting views covering sales, orders, customers, marketing, and delivered-order average order value.

Sales reports include delivered orders, allocate discounts to order lines, and use the unit cost recorded at purchase time.

## Run locally

Requires Docker Compose v2, Python 3, `curl`, and `jq`.

```bash
git clone https://github.com/dungtran174/ecommerce-realtime-data-pipeline.git
cd ecommerce-realtime-data-pipeline
cp .env.example .env
```

The example includes working local demo defaults. Adjust `.env` if needed, then run:

```bash
bash scripts/setup.sh
```

Setup starts the services, registers CDC, and initializes the warehouse.

- **Airflow:** http://localhost:8080 — use the account configured in `.env`. Run `seed_catalog` and wait for success, then run `generate_users` and wait for success. Enable `generate_users`, `generate_orders`, and `refresh_warehouse`.
- **Metabase:** http://localhost:3000 — connect to ClickHouse at `clickhouse:8123`, database `report`, using credentials from `.env`.
- **Kafka UI:** http://localhost:8085.

Stop services while keeping data: `bash scripts/clean.sh`.
