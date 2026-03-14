#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

exec node \
  "${ROOT_DIR}/services/orchestrator/src/cli/benchmark-station-review.js" \
  --root "${ROOT_DIR}" \
  "$@"
