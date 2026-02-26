#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

API_URL="${API_URL:-http://localhost:3000}"
CASES_FILE="${ROOT_DIR}/services/orchestrator/test/routes/regression_cases.json"
BASELINES_DIR="${ROOT_DIR}/services/orchestrator/test/routes/baselines"
REPORT_DIR="${ROOT_DIR}/reports/qa"
FAIL_ON_DIFF="true"

usage() {
  cat <<USAGE
Usage: scripts/qa/run-route-regression.sh [options]

Run route regression suite against /api/routes and compare with baselines.

Options:
  --api-url URL           API base URL (default: http://localhost:3000)
  --cases PATH            Regression case file (default: services/orchestrator/test/routes/regression_cases.json)
  --baselines-dir PATH    Baselines directory (default: services/orchestrator/test/routes/baselines)
  --report-dir PATH       Report output directory (default: reports/qa)
  --no-fail-on-diff       Exit 0 even when mismatches are found
  -h, --help              Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --api-url)
      API_URL="${2:-}"
      shift 2
      ;;
    --cases)
      CASES_FILE="${2:-}"
      shift 2
      ;;
    --baselines-dir)
      BASELINES_DIR="${2:-}"
      shift 2
      ;;
    --report-dir)
      REPORT_DIR="${2:-}"
      shift 2
      ;;
    --no-fail-on-diff)
      FAIL_ON_DIFF="false"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

ARGS=(
  "${ROOT_DIR}/services/orchestrator/src/cli/run-route-regression.js"
  --api-url "$API_URL"
  --cases "$CASES_FILE"
  --baselines-dir "$BASELINES_DIR"
  --report-dir "$REPORT_DIR"
)

if [[ "$FAIL_ON_DIFF" != "true" ]]; then
  ARGS+=(--no-fail-on-diff)
fi

node "${ARGS[@]}"
