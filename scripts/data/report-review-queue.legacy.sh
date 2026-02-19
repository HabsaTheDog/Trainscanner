#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib-db.sh"

COUNTRY_FILTER=""
AS_OF=""
LIMIT_ROWS="20"
ALL_SCOPES="false"

usage() {
  cat <<USAGE
Usage: scripts/data/report-review-queue.sh [options]

Report canonical review-queue coverage and open/resolved issues.

Options:
  --country DE|AT|CH   Restrict report to one country
  --as-of YYYY-MM-DD   Report queue entries generated for this as-of scope tag
  --all-scopes         Report all scope tags (instead of latest/as-of tag)
  --limit N            Number of detailed rows to show (default: 20)
  -h, --help           Show this help
USAGE
}

log() {
  printf '[report-review-queue] %s\n' "$*"
}

fail() {
  printf '[report-review-queue] ERROR: %s\n' "$*" >&2
  exit 1
}

is_iso_date() {
  local d="$1"
  [[ "$d" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || return 1
  date -u -d "$d" +%F >/dev/null 2>&1
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --country)
        [[ $# -ge 2 ]] || fail "Missing value for --country"
        COUNTRY_FILTER="$2"
        shift 2
        ;;
      --as-of)
        [[ $# -ge 2 ]] || fail "Missing value for --as-of"
        AS_OF="$2"
        shift 2
        ;;
      --all-scopes)
        ALL_SCOPES="true"
        shift
        ;;
      --limit)
        [[ $# -ge 2 ]] || fail "Missing value for --limit"
        LIMIT_ROWS="$2"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        fail "Unknown argument: $1"
        ;;
    esac
  done

  if [[ -n "$COUNTRY_FILTER" && "$COUNTRY_FILTER" != "DE" && "$COUNTRY_FILTER" != "AT" && "$COUNTRY_FILTER" != "CH" ]]; then
    fail "Invalid --country '$COUNTRY_FILTER' (expected DE, AT, or CH)"
  fi

  if [[ -n "$AS_OF" ]] && ! is_iso_date "$AS_OF"; then
    fail "Invalid --as-of value '$AS_OF' (expected YYYY-MM-DD)"
  fi

  [[ "$LIMIT_ROWS" =~ ^[0-9]+$ ]] || fail "--limit must be an integer"
}

main() {
  local scope_tag metrics total open confirmed dismissed resolved auto_resolved reviewed coverage
  local country_filter_esc scope_tag_esc all_scopes_esc limit_rows_esc

  parse_args "$@"

  db_load_env
  db_resolve_connection
  db_ensure_ready

  "${SCRIPT_DIR}/db-migrate.sh" --quiet

  scope_tag="${AS_OF:-latest}"
  country_filter_esc="$(db_sql_escape "$COUNTRY_FILTER")"
  scope_tag_esc="$(db_sql_escape "$scope_tag")"
  all_scopes_esc="$(db_sql_escape "$ALL_SCOPES")"
  limit_rows_esc="$(db_sql_escape "$LIMIT_ROWS")"

  log "Scope: country=${COUNTRY_FILTER:-ALL} scope_tag=${scope_tag} all_scopes=${ALL_SCOPES}"

  metrics="$(db_psql -At -c "
SELECT
  COUNT(*) AS total_items,
  COUNT(*) FILTER (WHERE status = 'open') AS open_items,
  COUNT(*) FILTER (WHERE status = 'confirmed') AS confirmed_items,
  COUNT(*) FILTER (WHERE status = 'dismissed') AS dismissed_items,
  COUNT(*) FILTER (WHERE status = 'resolved') AS resolved_items,
  COUNT(*) FILTER (WHERE status = 'auto_resolved') AS auto_resolved_items
FROM canonical_review_queue q
WHERE (NULLIF('${country_filter_esc}', '') IS NULL OR q.country = '${country_filter_esc}')
  AND (
    '${all_scopes_esc}' = 'true'
    OR q.provenance_run_tag = '${scope_tag_esc}'
  );")"

  IFS='|' read -r total open confirmed dismissed resolved auto_resolved <<<"$metrics"

  reviewed=$((dismissed + resolved + auto_resolved))
  coverage="0.00"
  if [[ "$total" -gt 0 ]]; then
    coverage="$(awk "BEGIN { printf \"%.2f\", (${reviewed} / ${total}) * 100 }")"
  fi

  printf 'total_items=%s\n' "$total"
  printf 'open_items=%s\n' "$open"
  printf 'confirmed_items=%s\n' "$confirmed"
  printf 'dismissed_items=%s\n' "$dismissed"
  printf 'resolved_items=%s\n' "$resolved"
  printf 'auto_resolved_items=%s\n' "$auto_resolved"
  printf 'review_coverage_percent=%s\n' "$coverage"

  printf '\ncounts_by_issue_type:\n'
  db_psql -c "
SELECT
  issue_type,
  status,
  COUNT(*) AS items
FROM canonical_review_queue q
WHERE (NULLIF('${country_filter_esc}', '') IS NULL OR q.country = '${country_filter_esc}')
  AND (
    '${all_scopes_esc}' = 'true'
    OR q.provenance_run_tag = '${scope_tag_esc}'
  )
GROUP BY issue_type, status
ORDER BY issue_type, status;
"

  printf '\nopen_or_confirmed_items:\n'
  db_psql -c "
SELECT
  review_item_id,
  issue_type,
  severity,
  country,
  COALESCE(canonical_station_id, '-') AS canonical_station_id,
  provenance_run_tag,
  to_char(last_detected_at, 'YYYY-MM-DD HH24:MI:SSOF') AS last_detected_at,
  left(details::text, 180) AS details
FROM canonical_review_queue q
WHERE (NULLIF('${country_filter_esc}', '') IS NULL OR q.country = '${country_filter_esc}')
  AND (
    '${all_scopes_esc}' = 'true'
    OR q.provenance_run_tag = '${scope_tag_esc}'
  )
  AND q.status IN ('open', 'confirmed')
ORDER BY
  CASE q.severity WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
  q.last_detected_at DESC
LIMIT NULLIF('${limit_rows_esc}', '')::integer;
"

  printf '\nrecently_resolved_items:\n'
  db_psql -c "
SELECT
  review_item_id,
  issue_type,
  status,
  country,
  COALESCE(canonical_station_id, '-') AS canonical_station_id,
  provenance_run_tag,
  COALESCE(to_char(resolved_at, 'YYYY-MM-DD HH24:MI:SSOF'), '-') AS resolved_at,
  COALESCE(resolved_by, '-') AS resolved_by,
  COALESCE(left(resolution_note, 140), '-') AS resolution_note
FROM canonical_review_queue q
WHERE (NULLIF('${country_filter_esc}', '') IS NULL OR q.country = '${country_filter_esc}')
  AND (
    '${all_scopes_esc}' = 'true'
    OR q.provenance_run_tag = '${scope_tag_esc}'
  )
  AND q.status IN ('dismissed', 'resolved', 'auto_resolved')
ORDER BY q.resolved_at DESC NULLS LAST, q.updated_at DESC
LIMIT NULLIF('${limit_rows_esc}', '')::integer;
"

  if [[ "$total" -eq 0 ]]; then
    fail "No review queue items found in selected scope"
  fi

  log "Review queue report complete"
}

main "$@"
