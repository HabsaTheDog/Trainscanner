#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

rm -rf coverage/sonar

npx --no-install c8 \
  --all \
  --reporter=lcovonly \
  --reporter=text-summary \
  --report-dir coverage/sonar \
  --include "services/orchestrator/src/**/*.js" \
  --include "frontend/src/**/*.js" \
  --include "frontend/src/**/*.jsx" \
  --include "services/control-plane/src/**/*.ts" \
  bash -lc '
    node --test services/orchestrator/test/unit services/orchestrator/test/integration services/orchestrator/test/e2e
    node --test frontend/test/*.test.js
    node --import tsx --test services/control-plane/test/*.test.ts
  '
