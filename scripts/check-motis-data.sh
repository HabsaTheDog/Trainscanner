#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

MISSING=0

check_file() {
  local path="$1"
  local hint="$2"
  if [[ ! -f "$path" ]]; then
    echo "[missing] $path" >&2
    echo "          $hint" >&2
    MISSING=1
  fi
}

check_file "$ROOT_DIR/data/motis/osm.pbf" "Place an OSM extract there before startup."
check_file "$ROOT_DIR/data/motis/active-gtfs.zip" "Run scripts/init-motis.sh --profile <name>."
check_file "$ROOT_DIR/data/motis/config.yml" "Run scripts/init-motis.sh --profile <name>."

if [[ "$MISSING" -ne 0 ]]; then
  cat >&2 <<MSG

MOTIS data preflight failed.
Either initialize now:
  scripts/init-motis.sh --profile <name>
Or start via wrapper:
  scripts/up.sh --profile <name>
MSG
  exit 1
fi

echo "MOTIS data preflight OK."
