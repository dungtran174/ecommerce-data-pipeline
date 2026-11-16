-- Physical warehouse objects are initialized by scripts/refresh_warehouse.sh.
-- See airflow/dags/sql/warehouse.
CREATE DATABASE IF NOT EXISTS silver ENGINE = Atomic;
