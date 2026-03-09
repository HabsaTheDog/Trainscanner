#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
FRONTEND_DIR="${ROOT_DIR}/frontend"
DIST_INDEX="${FRONTEND_DIR}/dist/index.html"

log() {
  printf '[frontend-build] %s\n' "$*"
  return 0
}

frontend_build_required() {
  if [[ ! -f "$DIST_INDEX" ]]; then
    return 0
  fi

  local dependency
  local -a dependencies=(
    "${FRONTEND_DIR}/index.html"
    "${FRONTEND_DIR}/curation.html"
    "${FRONTEND_DIR}/package.json"
    "${FRONTEND_DIR}/vite.config.js"
  )

  for dependency in "${dependencies[@]}"; do
    if [[ "$dependency" -nt "$DIST_INDEX" ]]; then
      return 0
    fi
  done

  if [[ -d "${FRONTEND_DIR}/src" ]] && find "${FRONTEND_DIR}/src" -type f -newer "$DIST_INDEX" | grep -q .; then
    return 0
  fi

  if [[ -d "${FRONTEND_DIR}/public" ]] && find "${FRONTEND_DIR}/public" -type f -newer "$DIST_INDEX" | grep -q .; then
    return 0
  fi

  return 1
}

if frontend_build_required; then
  log "Building frontend assets..."
  (cd "$ROOT_DIR" && npm run -w frontend build)
else
  log "Reusing existing frontend build."
fi
