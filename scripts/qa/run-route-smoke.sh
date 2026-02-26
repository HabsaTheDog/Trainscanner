#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

API_URL="${API_URL:-http://localhost:3000}"
REPORT_DIR="${ROOT_DIR}/reports/qa"

node "${ROOT_DIR}/services/orchestrator/src/cli/run-route-regression.js" \
  --api-url "$API_URL" \
  --cases "${ROOT_DIR}/services/orchestrator/test/routes/smoke_cases.json" \
  --baselines-dir "${ROOT_DIR}/services/orchestrator/test/routes/baselines" \
  --report-dir "$REPORT_DIR" \
  "$@"
