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
}

fail() {
  printf '[run-test-env] ERROR: %s\n' "$*" >&2
  exit 1
}

log() {
  printf '[run-test-env] %s\n' "$*"
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

if [[ -z "$PROFILE" ]]; then
  PROFILE="$(node - <<'NODE' "$ROOT_DIR/state/active-gtfs.json" "$ROOT_DIR/config/active-gtfs.json" "$ROOT_DIR/config/gtfs-profiles.json"
const fs = require('node:fs');
const activePath = process.argv[2];
const legacyActivePath = process.argv[3];
const profilesPath = process.argv[4];
let active = '';
try {
  const raw = JSON.parse(fs.readFileSync(activePath, 'utf8'));
  active = typeof raw.activeProfile === 'string' ? raw.activeProfile : '';
} catch {}
if (!active) {
  try {
    const raw = JSON.parse(fs.readFileSync(legacyActivePath, 'utf8'));
    active = typeof raw.activeProfile === 'string' ? raw.activeProfile : '';
  } catch {}
}

let names = [];
try {
  const raw = JSON.parse(fs.readFileSync(profilesPath, 'utf8'));
  const source = raw && typeof raw === 'object' ? (raw.profiles || raw) : {};
  names = Object.keys(source);
} catch {}

if (active && names.includes(active)) {
  process.stdout.write(active);
} else if (names.length > 0) {
  process.stdout.write(names[0]);
}
NODE
)"
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
