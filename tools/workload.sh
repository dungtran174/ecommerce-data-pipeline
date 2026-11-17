#!/usr/bin/env bash
# Finite synthetic source workload; use --help for rates and history options.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
docker compose --profile warehouse run --build --rm --no-deps \
  --entrypoint python warehouse-refresh -m tools.workload "$@"
