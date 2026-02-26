#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PROFILE=""
OSM_URL=""
OSM_FILE=""
NO_BUILD="false"
FORCE_OSM_DOWNLOAD="false"
OPEN_LOGS="false"
WAIT_SEC="60"

usage() {
  cat <<USAGE
Usage:
  scripts/run-test-env.sh [--profile <name>] [options]

This is the main day-to-day command for running the test environment.

Options:
  --profile <name>         GTFS profile to initialize (default: active profile, else first configured profile)
  --osm-url <url>          OSM extract URL for setup.sh download
  --osm-file <path>        Use local OSM extract file
  --force-osm-download     Re-download OSM even if data/motis/osm.pbf exists
  --no-build               Skip image rebuild for compose up
  --logs                   Follow motis + orchestrator logs after startup
  --wait-sec <n>           Seconds to wait for orchestrator health (default: 60)
  --help                   Show this help
USAGE
  return 0
}

fail() {
  printf '[run-test-env] ERROR: %s\n' "$*" >&2
  return 1
}

log() {
  printf '[run-test-env] %s\n' "$*"
  return 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="${2:-}"
      shift 2
      ;;
    --osm-url)
      OSM_URL="${2:-}"
      shift 2
      ;;
    --osm-file)
      OSM_FILE="${2:-}"
      shift 2
      ;;
    --force-osm-download)
      FORCE_OSM_DOWNLOAD="true"
      shift
      ;;
    --no-build)
      NO_BUILD="true"
      shift
      ;;
    --logs)
      OPEN_LOGS="true"
      shift
      ;;
    --wait-sec)
      WAIT_SEC="${2:-}"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      fail "Unknown argument: $1"
      ;;
  esac
done

"${ROOT_DIR}/scripts/validate-config.sh" --only profiles >/dev/null

if [[ -z "$PROFILE" ]]; then
  PROFILE="$(node "${ROOT_DIR}/services/orchestrator/src/cli/profile-runtime.js" resolve-default-profile --root "$ROOT_DIR" 2>/dev/null || true)"
fi

[[ -n "$PROFILE" ]] || fail "No profile detected. Set one in config/gtfs-profiles.json or pass --profile."

SETUP_ARGS=(--profile "$PROFILE" --detach)
if [[ "$NO_BUILD" == "true" ]]; then
  SETUP_ARGS+=(--no-build)
fi
if [[ -n "$OSM_URL" ]]; then
  SETUP_ARGS+=(--osm-url "$OSM_URL")
fi
if [[ -n "$OSM_FILE" ]]; then
  SETUP_ARGS+=(--osm-file "$OSM_FILE")
fi
if [[ "$FORCE_OSM_DOWNLOAD" == "true" ]]; then
  SETUP_ARGS+=(--force-osm-download)
fi

log "Starting test environment with profile '$PROFILE'"
"$ROOT_DIR/scripts/setup.sh" "${SETUP_ARGS[@]}"

log "Waiting for orchestrator health (timeout ${WAIT_SEC}s)..."
start_ts="$(date +%s)"
while true; do
  if curl -fsS http://localhost:3000/health >/dev/null 2>&1; then
    break
  fi

  now_ts="$(date +%s)"
  elapsed="$((now_ts - start_ts))"
  if [[ "$elapsed" -ge "$WAIT_SEC" ]]; then
    fail "Timed out waiting for http://localhost:3000/health"
  fi

  sleep 1
done

log "Environment ready."
printf '\n'
printf 'Frontend:   http://localhost:3000\n'
printf 'MOTIS:      http://localhost:8080\n'
printf 'Profile UI: use the dropdown in the frontend\n'
printf '\n'
printf 'Quick checks:\n'
printf '  curl -s http://localhost:3000/health\n'
printf '  curl -s http://localhost:3000/api/gtfs/status\n'
printf '\n'
printf 'Stop later with:\n'
printf '  scripts/stop-test-env.sh\n'

if [[ "$OPEN_LOGS" == "true" ]]; then
  log "Following logs (Ctrl+C to stop log tail only)..."
  (cd "$ROOT_DIR" && docker compose logs -f motis orchestrator)
fi
