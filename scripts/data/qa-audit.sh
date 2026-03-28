#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

exec node "${ROOT_DIR}/services/orchestrator/src/cli/qa-audit.js" --root "${ROOT_DIR}" "$@"
