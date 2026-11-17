#!/bin/bash
# Bootstrap the e-commerce pipeline.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."
cd "${ROOT_DIR}"

echo "================================================================="
echo "   E-Commerce Data Pipeline — Environment Bootstrap"
echo "================================================================="

# 1. Check prerequisites
command -v docker >/dev/null 2>&1 || { echo "Error: docker is not installed. Aborting."; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "Error: curl is required. Aborting."; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "Error: jq is required. Aborting."; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "Error: Python 3 is required. Aborting."; exit 1; }
docker compose version >/dev/null 2>&1 || {
    docker-compose version >/dev/null 2>&1 || { echo "Error: docker compose is not installed. Aborting."; exit 1; }
}

DOCKER_COMPOSE_CMD="docker compose"
if ! docker compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE_CMD="docker-compose"
fi

# 2. Ensure .env exists
if [ ! -f .env ]; then
    echo "[1/4] Creating .env from .env.example..."
    cp .env.example .env
else
    echo "[1/4] .env already exists."
fi

python3 - <<'ENV_CHECK'
from pathlib import Path
unfilled = []
for line in Path('.env').read_text().splitlines():
    key, sep, value = line.partition('=')
    if sep and not key.lstrip().startswith('#') and value.strip().strip("\"'").startswith('your_'):
        unfilled.append(key.strip())
if unfilled:
    raise SystemExit('Replace placeholders in .env: ' + ', '.join(unfilled))
ENV_CHECK

# Apply migrations before enabling source generation.
echo "[2/4] Starting core Docker services in background..."
${DOCKER_COMPOSE_CMD} up -d postgres-main kafka clickhouse debezium

# 4. Wait for core healthchecks
echo "[3/4] Waiting for services to become healthy..."
echo "  - Checking PostgreSQL (Main)..."
for attempt in $(seq 1 90); do
    if ${DOCKER_COMPOSE_CMD} exec -T postgres-main sh -lc 'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"' >/dev/null 2>&1; then
        break
    fi
    if [ "$attempt" -eq 90 ]; then
        echo "PostgreSQL did not become ready within 180 seconds" >&2
        exit 1
    fi
    sleep 2
done
echo "    ✓ PostgreSQL (Main) is healthy."

echo "  - Checking ClickHouse..."
for attempt in $(seq 1 90); do
    if ${DOCKER_COMPOSE_CMD} exec -T clickhouse sh -lc 'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "SELECT count() FROM bronze.orderdetails"' >/dev/null 2>&1; then
        break
    fi
    if [ "$attempt" -eq 90 ]; then
        echo "ClickHouse schema did not become ready within 180 seconds" >&2
        ${DOCKER_COMPOSE_CMD} logs --tail 30 clickhouse >&2
        exit 1
    fi
    sleep 2
done
echo "    ✓ ClickHouse is healthy."

echo "  - Applying idempotent unit-cost snapshot migrations..."
${DOCKER_COMPOSE_CMD} exec -T clickhouse sh -lc \
    'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
    < clickhouse/migrations/001_snapshot_unit_cost.sql
echo "  - Enabling CDC quarantine and parser-error recovery..."
${DOCKER_COMPOSE_CMD} exec -T clickhouse sh -lc \
    'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
    < clickhouse/init/05_quarantine_events.sql
python3 scripts/migrations/kafka_consumers.py
${DOCKER_COMPOSE_CMD} exec -T postgres-main sh -lc \
    'psql -X -v ON_ERROR_STOP=1 -1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
    < postgres/init/03_snapshot_unit_cost.sql

echo "  - Checking Debezium..."
bash "${SCRIPT_DIR}/register_connector.sh"

echo "  - Initializing incremental Silver/Gold processing..."
# Derived-layer migration only: never reset source data or Kafka offsets.
for attempt in $(seq 1 3); do
    if ${DOCKER_COMPOSE_CMD} --profile warehouse run --build --rm --no-deps warehouse-refresh --migrate; then
        break
    fi
    if [ "$attempt" -eq 3 ]; then
        echo "Warehouse bootstrap failed. Inspect refresh validation errors; source data was not reset." >&2
        exit 1
    fi
    echo "  - Warehouse not ready; retrying in 10 seconds (CDC may still be catching up)..."
    sleep 10
done

echo "  - Starting Airflow, Metabase, and service UIs..."
${DOCKER_COMPOSE_CMD} up -d

echo "  - Checking Airflow init and DAG imports..."
for attempt in $(seq 1 120); do
    init_container="$(${DOCKER_COMPOSE_CMD} ps -a -q airflow-init 2>/dev/null || true)"
    init_state="$(docker inspect -f '{{.State.Status}}:{{.State.ExitCode}}' "$init_container" 2>/dev/null || true)"
    if [ "$init_state" = "exited:0" ]; then
        break
    fi
    if [ "${init_state%%:*}" = "exited" ] || [ "$attempt" -eq 120 ]; then
        echo "Airflow init failed or timed out: ${init_state}" >&2
        ${DOCKER_COMPOSE_CMD} logs --tail 40 airflow-init >&2
        exit 1
    fi
    sleep 2
done
for attempt in $(seq 1 60); do
    if ${DOCKER_COMPOSE_CMD} exec -T airflow-scheduler airflow dags list-import-errors --output json 2>/dev/null \
        | jq -e 'length == 0' >/dev/null 2>&1; then
        break
    fi
    if [ "$attempt" -eq 60 ]; then
        echo "Airflow scheduler did not load DAGs without errors" >&2
        ${DOCKER_COMPOSE_CMD} logs --tail 40 airflow-scheduler >&2
        exit 1
    fi
    sleep 2
done

echo "  - Checking Metabase..."
env_port() {
    local value
    value="$(sed -n "s/^${1}=//p" .env | head -n 1)"
    printf '%s' "${value:-$2}"
}
METABASE_PORT="$(env_port METABASE_PORT 3000)"
METABASE_HOST="${METABASE_HOST:-localhost}"
for attempt in $(seq 1 120); do
    if curl -fsS --max-time 3 "http://${METABASE_HOST}:${METABASE_PORT}/api/health" 2>/dev/null \
        | jq -e '.status == "ok"' >/dev/null 2>&1; then
        break
    fi
    if [ "$attempt" -eq 120 ]; then
        echo "Metabase did not become healthy" >&2
        ${DOCKER_COMPOSE_CMD} logs --tail 40 metabase >&2
        exit 1
    fi
    sleep 2
done

echo "[4/4] Pipeline services are ready. DAGs are paused until enabled in Airflow."
echo "================================================================="
echo "                      Service Endpoints                          "
echo "================================================================="
echo "  • Airflow Web UI:      http://localhost:8080"
echo "  • Kafka UI:            http://localhost:$(env_port KAFKA_UI_PORT 8085)"
echo "  • Debezium UI:         http://localhost:$(env_port DEBEZIUM_UI_PORT 8084)"
echo "  • Metabase BI:         http://localhost:${METABASE_PORT}"
echo "  • ClickHouse HTTP:     http://localhost:$(env_port CLICKHOUSE_HTTP_PORT 8123)"
echo "  • PostgreSQL OLTP:     localhost:$(env_port POSTGRES_PORT 5432)"
echo "================================================================="
echo "  Next steps:"
echo "    - Run seed_catalog, then generate_users; enable generate_users, generate_orders and refresh_warehouse."
echo "================================================================="
