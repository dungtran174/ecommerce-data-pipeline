#!/usr/bin/env bash
# Incremental refresh by default; --full rebuilds. Errors return a nonzero status.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
docker compose --profile warehouse run --build --rm --no-deps warehouse-refresh "$@"
