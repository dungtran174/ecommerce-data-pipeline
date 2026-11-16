#!/usr/bin/env bash
# Full snapshot refresh; errors propagate as a nonzero exit status.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
docker compose --profile warehouse run --build --rm --no-deps warehouse-refresh "$@"
