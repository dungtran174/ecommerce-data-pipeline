#!/usr/bin/env bash
# Read-only comparison; run after source workload is paused and ETL caught up.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
docker compose --profile warehouse run --build --rm --no-deps \
  --entrypoint python warehouse-refresh -m tools.benchmark "$@"
