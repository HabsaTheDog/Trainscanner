#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PROFILE=""
OSM_DEST="${ROOT_DIR}/data/motis/osm.pbf"
OSM_FILE=""
OSM_URL="${OSM_URL:-https://download.geofabrik.de/europe/germany-latest.osm.pbf}"
FORCE_OSM_DOWNLOAD="false"
NO_START="false"
NO_BUILD="false"
DETACH="false"
SKIP_IMPORT="false"

usage() {
  cat <<USAGE
Usage:
  scripts/setup.sh --profile <name> [options]

Options:
  --profile <name>         GTFS profile name from config/gtfs-profiles.json (required)
  --osm-url <url>          OSM extract URL to download if osm.pbf is missing
  --osm-file <path>        Use local OSM extract file instead of downloading
  --force-osm-download     Re-download OSM file even if data/motis/osm.pbf exists
  --skip-import            Skip MOTIS import step during bootstrap
  --no-start               Do not run docker compose up after bootstrap
  --no-build               Run docker compose up without --build
  --detach                 Start docker compose in background (-d)
  --help                   Show this help

Examples:
  scripts/setup.sh --profile sample_de
  scripts/setup.sh --profile sample_de --detach
  scripts/setup.sh --profile sample_de --osm-url https://download.geofabrik.de/europe/dach-latest.osm.pbf
  scripts/setup.sh --profile sample_de --osm-file /tmp/my.osm.pbf
USAGE
  return 0
}

log() {
  printf '[setup] %s\n' "$*"
  return 0
}

fail() {
  printf '[setup] ERROR: %s\n' "$*" >&2
  exit 1
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
    --skip-import)
      SKIP_IMPORT="true"
      shift
      ;;
    --no-start)
      NO_START="true"
      shift
      ;;
    --no-build)
      NO_BUILD="true"
      shift
      ;;
    --detach)
      DETACH="true"
      shift
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

[[ -n "$PROFILE" ]] || fail "Missing required --profile"
command -v docker >/dev/null 2>&1 || fail "docker command not found in PATH"

mkdir -p "${ROOT_DIR}/data/motis"

if [[ -n "$OSM_FILE" ]]; then
  [[ -f "$OSM_FILE" ]] || fail "--osm-file not found: $OSM_FILE"
  log "Copying local OSM extract: $OSM_FILE -> $OSM_DEST"
  cp "$OSM_FILE" "$OSM_DEST"
else
  if [[ "$FORCE_OSM_DOWNLOAD" == "true" || ! -f "$OSM_DEST" ]]; then
    log "Downloading OSM extract from: $OSM_URL"
    if command -v curl >/dev/null 2>&1; then
      curl -fL --retry 3 --retry-delay 2 -o "$OSM_DEST" "$OSM_URL"
    elif command -v wget >/dev/null 2>&1; then
      wget -O "$OSM_DEST" "$OSM_URL"
    else
      fail "Neither curl nor wget found. Install one or pass --osm-file <path>."
    fi
  else
    log "OSM extract already exists at $OSM_DEST (use --force-osm-download to replace)"
  fi
fi

INIT_ARGS=(--profile "$PROFILE")
if [[ "$SKIP_IMPORT" == "true" ]]; then
  INIT_ARGS+=(--skip-import)
fi

log "Bootstrapping MOTIS data for profile '$PROFILE'"
"${ROOT_DIR}/scripts/init-motis.sh" "${INIT_ARGS[@]}"

if [[ "$NO_START" == "true" ]]; then
  log "Bootstrap complete. Skipping docker compose start due to --no-start."
  exit 0
fi

COMPOSE_ARGS=(up)
if [[ "$NO_BUILD" != "true" ]]; then
  COMPOSE_ARGS+=(--build)
fi
if [[ "$DETACH" == "true" ]]; then
  COMPOSE_ARGS+=(-d)
fi

log "Starting docker compose: docker compose ${COMPOSE_ARGS[*]}"
(cd "$ROOT_DIR" && docker compose "${COMPOSE_ARGS[@]}")
