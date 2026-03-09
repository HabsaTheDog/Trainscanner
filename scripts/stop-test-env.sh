#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
STOP_TIMEOUT_SEC="${STOP_TIMEOUT_SEC:-2}"

(cd "$ROOT_DIR" && docker compose down --timeout "$STOP_TIMEOUT_SEC")
