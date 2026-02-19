#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PROFILE=""
OSM_FILE="${ROOT_DIR}/data/motis/osm.pbf"
MOTIS_IMAGE="${MOTIS_IMAGE:-ghcr.io/motis-project/motis:latest}"
MOTIS_CLI_BIN="${MOTIS_CLI_BIN:-}"
MOTIS_DISABLE_STREET_FEATURES="${MOTIS_DISABLE_STREET_FEATURES:-true}"
SKIP_IMPORT="false"

usage() {
  cat <<USAGE
Usage:
  scripts/init-motis.sh --profile <name> [--osm-file <path>] [--motis-image <image>] [--skip-import]

What it does:
  1) Resolves profile artifact from config/gtfs-profiles.json (static zip or runtime descriptor)
  2) Copies selected GTFS zip artifact to data/motis/active-gtfs.zip
  3) Generates data/motis/config.yml using MOTIS config command
  4) Applies MVP-safe config defaults (disable street/geocoding/tiles unless MOTIS_DISABLE_STREET_FEATURES=false)
  5) Runs MOTIS import (unless --skip-import)
  6) Updates state/active-gtfs.json
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="${2:-}"
      shift 2
      ;;
    --osm-file)
      OSM_FILE="${2:-}"
      shift 2
      ;;
    --motis-image)
      MOTIS_IMAGE="${2:-}"
      shift 2
      ;;
    --skip-import)
      SKIP_IMPORT="true"
      shift
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
  echo "Missing required argument --profile" >&2
  usage
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker command not found in PATH" >&2
  exit 1
fi

"${ROOT_DIR}/scripts/validate-config.sh" --only profiles >/dev/null

PROFILE_ARTIFACT_INFO="$(node "${ROOT_DIR}/orchestrator/src/cli/profile-runtime.js" resolve-artifact --root "$ROOT_DIR" --profile "$PROFILE")" || {
  echo "Profile '$PROFILE' not found/invalid or runtime artifact unresolved in config/gtfs-profiles.json" >&2
  exit 1
}

PROFILE_ZIP_RELATIVE="$(node -e 'const obj = JSON.parse(process.argv[1]); process.stdout.write(obj.zipPath || \"\");' "$PROFILE_ARTIFACT_INFO")"
PROFILE_ZIP_ABSOLUTE="$(node -e 'const obj = JSON.parse(process.argv[1]); process.stdout.write(obj.absolutePath || \"\");' "$PROFILE_ARTIFACT_INFO")"
PROFILE_SOURCE_TYPE="$(node -e 'const obj = JSON.parse(process.argv[1]); process.stdout.write(obj.sourceType || \"static\");' "$PROFILE_ARTIFACT_INFO")"

if [[ ! -f "$PROFILE_ZIP_ABSOLUTE" ]]; then
  echo "GTFS zip for profile '$PROFILE' not found: $PROFILE_ZIP_ABSOLUTE" >&2
  exit 1
fi

if [[ ! -f "$OSM_FILE" ]]; then
  cat >&2 <<MSG
Missing OSM file: $OSM_FILE
Provide one first, e.g. place an extract at data/motis/osm.pbf.
MSG
  exit 1
fi

mkdir -p "$ROOT_DIR/data/motis"
cp "$PROFILE_ZIP_ABSOLUTE" "$ROOT_DIR/data/motis/active-gtfs.zip"
echo "Using profile artifact source='${PROFILE_SOURCE_TYPE}' zip='${PROFILE_ZIP_RELATIVE}'"

motis_cmd0_from_image() {
  docker image inspect "$MOTIS_IMAGE" --format '{{json .Config.Cmd}}' 2>/dev/null | node -e '
    const fs = require("node:fs");
    const raw = fs.readFileSync(0, "utf8").trim();
    if (!raw || raw === "null") process.exit(0);
    try {
      const cmd = JSON.parse(raw);
      if (Array.isArray(cmd) && cmd.length > 0 && typeof cmd[0] === "string") {
        process.stdout.write(cmd[0]);
      }
    } catch {}
  ' || true
}

MOTIS_DETECTED_STYLE=""

run_motis_cli() {
  local action="$1"
  shift

  local image_cmd0=""
  image_cmd0="$(motis_cmd0_from_image)"

  local -a candidates=()
  if [[ -n "$MOTIS_DETECTED_STYLE" ]]; then
    candidates+=("$MOTIS_DETECTED_STYLE")
  else
    candidates+=("")
  fi
  if [[ -n "$image_cmd0" ]]; then
    candidates+=("$image_cmd0")
  fi
  if [[ -n "$MOTIS_CLI_BIN" ]]; then
    candidates+=("$MOTIS_CLI_BIN")
  fi
  candidates+=("motis" "/usr/bin/motis" "/bin/motis")

  local candidate
  local -a cmd
  local -a tried=()
  for candidate in "${candidates[@]}"; do
    local duplicate="false"
    local prev
    for prev in "${tried[@]}"; do
      if [[ "$prev" == "$candidate" ]]; then
        duplicate="true"
        break
      fi
    done
    if [[ "$duplicate" == "true" ]]; then
      continue
    fi
    tried+=("$candidate")

    cmd=(docker run --rm --user "$(id -u):$(id -g)" -w /data -v "$ROOT_DIR/data/motis:/data" "$MOTIS_IMAGE")
    if [[ -z "$candidate" ]]; then
      cmd+=("$action" "$@")
      echo "Trying MOTIS command style: <entrypoint> $action"
    else
      cmd+=("$candidate" "$action" "$@")
      echo "Trying MOTIS command style: $candidate $action"
    fi

    if "${cmd[@]}"; then
      MOTIS_DETECTED_STYLE="$candidate"
      return 0
    fi
  done

  cat >&2 <<MSG
Failed to execute MOTIS action '$action' with all known command styles.
Set an explicit binary and retry, e.g.:
  MOTIS_CLI_BIN=motis scripts/init-motis.sh --profile $PROFILE
or inspect image help:
  docker run --rm $MOTIS_IMAGE --help
MSG
  return 1
}

patch_config_for_mvp() {
  local config_path="$ROOT_DIR/data/motis/config.yml"
  if [[ ! -f "$config_path" ]]; then
    echo "Warning: MOTIS config not found at $config_path (skip MVP patch)."
    return 0
  fi

  if [[ "$MOTIS_DISABLE_STREET_FEATURES" != "true" ]]; then
    echo "Keeping street/geocoding features enabled (MOTIS_DISABLE_STREET_FEATURES=false)."
    return 0
  fi

  echo "Applying MVP-safe MOTIS config patch (disable street/geocoding/tiles)..."
  awk '
    BEGIN { skip_tiles = 0 }
    {
      if ($0 ~ /^tiles:[[:space:]]*$/) {
        skip_tiles = 1
        next
      }
      if (skip_tiles == 1) {
        if ($0 ~ /^[^[:space:]].*:[[:space:]]*.*$/) {
          skip_tiles = 0
        } else {
          next
        }
      }
      if ($0 ~ /^street_routing:[[:space:]]*true([[:space:]]*)$/) {
        sub(/true/, "false")
      }
      if ($0 ~ /^geocoding:[[:space:]]*true([[:space:]]*)$/) {
        sub(/true/, "false")
      }
      if ($0 ~ /^reverse_geocoding:[[:space:]]*true([[:space:]]*)$/) {
        sub(/true/, "false")
      }
      print
    }
  ' "$config_path" > "$config_path.tmp"
  mv "$config_path.tmp" "$config_path"
}

echo "Generating MOTIS config.yml from OSM + GTFS..."
run_motis_cli config /data/osm.pbf /data/active-gtfs.zip
patch_config_for_mvp

if [[ "$SKIP_IMPORT" != "true" ]]; then
  echo "Running MOTIS import..."
  run_motis_cli import
fi

node - <<'NODE' "$ROOT_DIR/state/active-gtfs.json" "$PROFILE" "$PROFILE_ZIP_RELATIVE"
const fs = require('node:fs');
const file = process.argv[2];
const profile = process.argv[3];
const zipPath = process.argv[4];
const payload = {
  activeProfile: profile,
  zipPath,
  activatedAt: new Date().toISOString()
};
try {
  fs.mkdirSync(require('node:path').dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify(payload, null, 2) + '\n', 'utf8');
} catch (err) {
  if (err && (err.code === 'EACCES' || err.code === 'EPERM')) {
    console.error(
      `Warning: could not update ${file} due to permissions (${err.code}). ` +
      'Continuing because profile activation via API will refresh runtime state.'
    );
    process.exit(0);
  }
  throw err;
}
NODE

echo "MOTIS data initialized for profile '$PROFILE'."
