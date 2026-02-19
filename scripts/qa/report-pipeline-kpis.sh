#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

exec node "${ROOT_DIR}/orchestrator/src/cli/report-pipeline-kpis.js" --root "${ROOT_DIR}" "$@"
