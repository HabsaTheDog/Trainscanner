#!/usr/bin/env bash
set -euo pipefail

BASE_REF="${1:-}"
HEAD_REF="${2:-HEAD}"

if [[ -z "$BASE_REF" ]]; then
  echo "Usage: scripts/ci/check-docs-updated.sh <base-ref> [head-ref]" >&2
  exit 2
fi

CHANGED_FILES="$(git diff --name-only "$BASE_REF" "$HEAD_REF")"
if [[ -z "$CHANGED_FILES" ]]; then
  echo "[docs-check] no changes detected"
  exit 0
fi

REQUIRES_DOCS=0
while IFS= read -r file; do
  [[ -z "$file" ]] && continue
  if [[ "$file" =~ ^(orchestrator/|frontend/|scripts/|config/|db/|docker-compose\.yml|README\.md|AGENTS\.md)$ ]]; then
    REQUIRES_DOCS=1
    break
  fi
done <<<"$CHANGED_FILES"

if [[ "$REQUIRES_DOCS" -eq 0 ]]; then
  echo "[docs-check] no scoped runtime/script/config changes detected"
  exit 0
fi

required_docs=(
  "README.md"
  "AGENTS.md"
  "orchestrator/AGENTS.md"
  "frontend/AGENTS.md"
)

missing=()
for doc in "${required_docs[@]}"; do
  if ! grep -qx "$doc" <<<"$CHANGED_FILES"; then
    missing+=("$doc")
  fi
done

if (( ${#missing[@]} > 0 )); then
  echo "[docs-check] ERROR: runtime/script/config changes require doc updates in:" >&2
  for doc in "${missing[@]}"; do
    echo "  - $doc" >&2
  done
  exit 1
fi

echo "[docs-check] required docs updated"
