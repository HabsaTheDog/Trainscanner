#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PROFILE=""
NO_BUILD="false"
PASS_THROUGH=()

usage() {
  cat <<USAGE
Usage:
  scripts/up.sh [--profile <name>] [--no-build] [--detach] [-- <extra docker compose up args>]

Behavior:
  - Runs MOTIS data preflight checks.
  - If checks fail and --profile is set, runs scripts/setup.sh --profile <name> --no-start automatically.
  - Starts docker compose stack.
USAGE
  return 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="${2:-}"
      shift 2
      ;;
    --no-build)
      NO_BUILD="true"
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --)
      shift
      PASS_THROUGH+=("$@")
      break
      ;;
    *)
      PASS_THROUGH+=("$1")
      shift
      ;;
  esac
done

if ! "$ROOT_DIR/scripts/check-motis-data.sh" >/dev/null 2>&1; then
  if [[ -z "$PROFILE" ]]; then
    echo "MOTIS preflight failed and no --profile was provided for auto-init." >&2
    "$ROOT_DIR/scripts/check-motis-data.sh"
    exit 1
  fi

  echo "Preflight failed. Auto-running one-time setup for profile '$PROFILE'..."
  "$ROOT_DIR/scripts/setup.sh" --profile "$PROFILE" --no-start
fi

echo "Starting docker compose services..."
if [[ "$NO_BUILD" == "true" ]]; then
  (cd "$ROOT_DIR" && docker compose up "${PASS_THROUGH[@]}")
else
  (cd "$ROOT_DIR" && docker compose up --build "${PASS_THROUGH[@]}")
fi
