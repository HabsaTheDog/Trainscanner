#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

API_URL="${API_URL:-http://localhost:3000}"
POLL_INTERVAL="${POLL_INTERVAL:-2}"
TIMEOUT_SEC="${TIMEOUT_SEC:-300}"
SMOKE_GATE="${SMOKE_GATE:-false}"
SMOKE_STRICT="${SMOKE_STRICT:-true}"
SMOKE_MAX_ATTEMPTS="${SMOKE_MAX_ATTEMPTS:-120}"
SMOKE_TARGET_DATE="${SMOKE_TARGET_DATE:-}"
PROFILE=""
REIMPORT="false"

usage() {
  cat <<USAGE
Usage:
  scripts/switch-gtfs.sh --profile <name> [--reimport] [--api-url <url>] [--timeout-sec <seconds>] [--smoke-gate]

Examples:
  scripts/switch-gtfs.sh --profile sample_de
  scripts/switch-gtfs.sh --profile sample_de --reimport
  scripts/switch-gtfs.sh --profile sample_dach --api-url http://localhost:3000
  scripts/switch-gtfs.sh --profile sample_de --smoke-gate --smoke-max-attempts 180
USAGE
  return 0
}

json_field() {
  local json="$1"
  local key="$2"

  if command -v jq >/dev/null 2>&1; then
    printf '%s' "$json" | jq -r ".${key} // empty"
    return 0
  else
    printf '%s' "$json" | sed -n "s/.*\"${key}\":\"\([^\"]*\)\".*/\1/p"
    return 0
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="${2:-}"
      shift 2
      ;;
    --api-url)
      API_URL="${2:-}"
      shift 2
      ;;
    --timeout-sec)
      TIMEOUT_SEC="${2:-}"
      shift 2
      ;;
    --reimport)
      REIMPORT="true"
      shift
      ;;
    --smoke-gate)
      SMOKE_GATE="true"
      shift
      ;;
    --smoke-strict)
      SMOKE_STRICT="true"
      shift
      ;;
    --smoke-nonstrict)
      SMOKE_STRICT="false"
      shift
      ;;
    --smoke-max-attempts)
      SMOKE_MAX_ATTEMPTS="${2:-}"
      shift 2
      ;;
    --smoke-target-date)
      SMOKE_TARGET_DATE="${2:-}"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$PROFILE" ]]; then
  echo "Missing required --profile" >&2
  usage
  exit 1
fi

if ! [[ "$SMOKE_MAX_ATTEMPTS" =~ ^[0-9]+$ ]]; then
  echo "Invalid --smoke-max-attempts value: $SMOKE_MAX_ATTEMPTS" >&2
  exit 1
fi

if [[ "$REIMPORT" == "true" ]]; then
  echo "Running full MOTIS re-import before activation for profile '${PROFILE}'..."
  "${ROOT_DIR}/scripts/init-motis.sh" --profile "$PROFILE"
fi

echo "Starting GTFS switch for profile '${PROFILE}' via ${API_URL}"
ACTIVATE_PAYLOAD="$(printf '{"profile":"%s"}' "$PROFILE")"
HTTP_CODE="$(curl -sS -o /tmp/switch_activate_resp.json -w '%{http_code}' -X POST "${API_URL}/api/gtfs/activate" -H 'Content-Type: application/json' -d "$ACTIVATE_PAYLOAD")"

if [[ "$HTTP_CODE" -ge 400 ]]; then
  echo "Activation request failed (HTTP ${HTTP_CODE}):" >&2
  cat /tmp/switch_activate_resp.json >&2
  echo >&2
  exit 1
fi

START_TS="$(date +%s)"
LAST_STATE=""

while true; do
  STATUS_JSON="$(curl -sS "${API_URL}/api/gtfs/status")"
  STATE="$(json_field "$STATUS_JSON" state)"
  MESSAGE="$(json_field "$STATUS_JSON" message)"

  if [[ "$STATE" != "$LAST_STATE" ]]; then
    echo "state=${STATE} message=${MESSAGE}"
    LAST_STATE="$STATE"
  fi

  if [[ "$STATE" == "ready" ]]; then
    echo "Profile '${PROFILE}' activated successfully."
    if [[ "$SMOKE_GATE" == "true" ]]; then
      SMOKE_ARGS=(--api-url "$API_URL" --max-attempts "$SMOKE_MAX_ATTEMPTS")
      if [[ -n "$SMOKE_TARGET_DATE" ]]; then
        SMOKE_ARGS+=(--target-date "$SMOKE_TARGET_DATE")
      fi

      echo "Running post-activation route smoke gate..."
      if "${ROOT_DIR}/scripts/find-working-route.sh" "${SMOKE_ARGS[@]}"; then
        echo "Route smoke gate passed."
      else
        if [[ "$SMOKE_STRICT" == "true" ]]; then
          echo "Route smoke gate failed in strict mode." >&2
          exit 1
        fi
        echo "Route smoke gate failed (non-strict mode): continuing." >&2
      fi
    fi
    exit 0
  fi

  if [[ "$STATE" == "failed" ]]; then
    echo "Profile switch failed:" >&2
    printf '%s\n' "$STATUS_JSON" >&2
    exit 1
  fi

  NOW_TS="$(date +%s)"
  ELAPSED="$((NOW_TS - START_TS))"
  if [[ "$ELAPSED" -ge "$TIMEOUT_SEC" ]]; then
    echo "Timeout waiting for switch completion after ${TIMEOUT_SEC}s" >&2
    printf '%s\n' "$STATUS_JSON" >&2
    exit 1
  fi

  sleep "$POLL_INTERVAL"
done
