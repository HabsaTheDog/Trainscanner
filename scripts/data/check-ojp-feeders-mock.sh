#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

MOCK_CONFIG_FILE="${ROOT_DIR}/config/ojp-endpoints.mock.json"
MOCK_HOST="127.0.0.1"
MOCK_PORT="18080"
PROVIDER_ID="de_ojp_mock_local"
WAIT_SEC="10"
TMP_REPORT=""
TMP_SERVER_LOG=""
MOCK_PID=""

usage() {
  cat <<USAGE
Usage: scripts/data/check-ojp-feeders-mock.sh [options]

Run deterministic local OJP feeder happy-path check against mock endpoint.

Options:
  --config PATH       Mock config path (default: config/ojp-endpoints.mock.json)
  --host HOST         Mock host (default: 127.0.0.1)
  --port PORT         Mock port (default: 18080)
  --provider-id ID    Provider id in mock config (default: de_ojp_mock_local)
  --wait-sec N        Max seconds to wait for mock health (default: 10)
  -h, --help          Show this help
USAGE
}

log() {
  printf '[check-ojp-mock] %s\n' "$*"
}

fail() {
  printf '[check-ojp-mock] ERROR: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "Missing required command: $1"
}

cleanup() {
  if [[ -n "$MOCK_PID" ]] && kill -0 "$MOCK_PID" >/dev/null 2>&1; then
    kill "$MOCK_PID" >/dev/null 2>&1 || true
    wait "$MOCK_PID" >/dev/null 2>&1 || true
  fi
  [[ -n "$TMP_REPORT" ]] && rm -f "$TMP_REPORT" >/dev/null 2>&1 || true
  [[ -n "$TMP_SERVER_LOG" ]] && rm -f "$TMP_SERVER_LOG" >/dev/null 2>&1 || true
}
trap cleanup EXIT

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --config)
        [[ $# -ge 2 ]] || fail "Missing value for --config"
        MOCK_CONFIG_FILE="$2"
        shift 2
        ;;
      --host)
        [[ $# -ge 2 ]] || fail "Missing value for --host"
        MOCK_HOST="$2"
        shift 2
        ;;
      --port)
        [[ $# -ge 2 ]] || fail "Missing value for --port"
        MOCK_PORT="$2"
        shift 2
        ;;
      --provider-id)
        [[ $# -ge 2 ]] || fail "Missing value for --provider-id"
        PROVIDER_ID="$2"
        shift 2
        ;;
      --wait-sec)
        [[ $# -ge 2 ]] || fail "Missing value for --wait-sec"
        WAIT_SEC="$2"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        fail "Unknown argument: $1"
        ;;
    esac
  done

  [[ "$MOCK_PORT" =~ ^[0-9]+$ ]] || fail "--port must be an integer"
  [[ "$WAIT_SEC" =~ ^[0-9]+$ ]] || fail "--wait-sec must be an integer"
  [[ -f "$MOCK_CONFIG_FILE" ]] || fail "Mock config not found: $MOCK_CONFIG_FILE"
}

wait_for_health() {
  local started now elapsed
  started="$(date +%s)"

  while true; do
    if curl -fsS "http://${MOCK_HOST}:${MOCK_PORT}/health" >/dev/null 2>&1; then
      return 0
    fi

    now="$(date +%s)"
    elapsed="$((now - started))"
    if [[ "$elapsed" -ge "$WAIT_SEC" ]]; then
      return 1
    fi
    sleep 1
  done
}

main() {
  parse_args "$@"

  require_cmd node
  require_cmd curl
  require_cmd jq

  TMP_SERVER_LOG="$(mktemp)"
  TMP_REPORT="$(mktemp --suffix=.json)"

  log "Starting mock OJP server on ${MOCK_HOST}:${MOCK_PORT}"
  node "${SCRIPT_DIR}/mock/mock-ojp-server.js" --host "$MOCK_HOST" --port "$MOCK_PORT" >"$TMP_SERVER_LOG" 2>&1 &
  MOCK_PID="$!"

  wait_for_health || {
    printf '[check-ojp-mock] mock server log:\n' >&2
    cat "$TMP_SERVER_LOG" >&2 || true
    fail "Mock OJP server did not become healthy within ${WAIT_SEC}s"
  }

  log "Running feeder probe against mock provider '${PROVIDER_ID}'"
  OJP_ENDPOINTS_CONFIG="$MOCK_CONFIG_FILE" \
    "${SCRIPT_DIR}/test-ojp-feeders.sh" \
      --provider-id "$PROVIDER_ID" \
      --output "$TMP_REPORT" >/dev/null

  jq -e '.ok == true and .response.httpStatus == 200 and .response.tripResultCount >= 1 and .response.tripSectionCount >= 1' "$TMP_REPORT" >/dev/null \
    || fail "Mock probe completed but response assertions failed"

  log "Mock feeder check passed"
  jq '{providerId, country, response}' "$TMP_REPORT"
}

main "$@"
