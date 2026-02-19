#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib-db.sh"

OJP_JSON="${SCRIPT_DIR}/samples/ojp-feeder-sample.json"
MOTIS_JSON="${SCRIPT_DIR}/samples/motis-backbone-sample.json"
OUTPUT_JSON="${ROOT_DIR}/state/stitch-prototype-report.json"
TOP_N="5"
COUNTRY_FILTER=""
AS_OF=""
TMP_FILES=()

usage() {
  cat <<USAGE
Usage: scripts/data/run-stitch-prototype.sh [options]

Run offline stitching prototype from feeder + backbone samples.

Options:
  --ojp-json PATH         OJP feeder segments JSON (default: scripts/data/samples/ojp-feeder-sample.json)
  --motis-json PATH       MOTIS backbone JSON (default: scripts/data/samples/motis-backbone-sample.json)
  --output PATH           Output report path (default: state/stitch-prototype-report.json)
  --top-n N               Number of ranked itineraries (default: 5)
  --country DE|AT|CH      Restrict to one country
  --as-of YYYY-MM-DD      Evaluate transfer rules as-of date (default: current date)
  -h, --help              Show this help
USAGE
}

log() {
  printf '[run-stitch-prototype] %s\n' "$*"
}

fail() {
  printf '[run-stitch-prototype] ERROR: %s\n' "$*" >&2
  exit 1
}

cleanup() {
  local f
  for f in "${TMP_FILES[@]}"; do
    [[ -n "$f" ]] && rm -f "$f" 2>/dev/null || true
  done
}
trap cleanup EXIT

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "Missing required command: $1"
}

is_iso_date() {
  local d="$1"
  [[ "$d" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || return 1
  date -u -d "$d" +%F >/dev/null 2>&1
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --ojp-json)
        [[ $# -ge 2 ]] || fail "Missing value for --ojp-json"
        OJP_JSON="$2"
        shift 2
        ;;
      --motis-json)
        [[ $# -ge 2 ]] || fail "Missing value for --motis-json"
        MOTIS_JSON="$2"
        shift 2
        ;;
      --output)
        [[ $# -ge 2 ]] || fail "Missing value for --output"
        OUTPUT_JSON="$2"
        shift 2
        ;;
      --top-n)
        [[ $# -ge 2 ]] || fail "Missing value for --top-n"
        TOP_N="$2"
        shift 2
        ;;
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
      -h|--help)
        usage
        exit 0
        ;;
      *)
        fail "Unknown argument: $1"
        ;;
    esac
  done

  [[ "$TOP_N" =~ ^[0-9]+$ ]] || fail "--top-n must be an integer"
  if [[ -n "$COUNTRY_FILTER" && "$COUNTRY_FILTER" != "DE" && "$COUNTRY_FILTER" != "AT" && "$COUNTRY_FILTER" != "CH" ]]; then
    fail "Invalid --country '$COUNTRY_FILTER' (expected DE, AT, or CH)"
  fi
  if [[ -n "$AS_OF" ]] && ! is_iso_date "$AS_OF"; then
    fail "Invalid --as-of value '$AS_OF' (expected YYYY-MM-DD)"
  fi

  [[ -f "$OJP_JSON" ]] || fail "OJP feeder JSON not found: $OJP_JSON"
  [[ -f "$MOTIS_JSON" ]] || fail "MOTIS backbone JSON not found: $MOTIS_JSON"
}

main() {
  local rules_file rules_json report_json effective_date
  local -a node_args
  local country_filter_esc effective_date_esc

  parse_args "$@"

  require_cmd node
  require_cmd jq

  db_load_env
  db_resolve_connection
  db_ensure_ready

  "${SCRIPT_DIR}/db-migrate.sh" --quiet

  effective_date="${AS_OF:-$(date -u +%F)}"
  country_filter_esc="$(db_sql_escape "$COUNTRY_FILTER")"
  effective_date_esc="$(db_sql_escape "$effective_date")"

  rules_json="$(db_psql -At -c "
SELECT COALESCE(json_agg(row_to_json(t)), '[]'::json)::text
FROM (
  SELECT
    rule_id,
    rule_scope,
    country,
    canonical_station_id,
    hub_name,
    min_transfer_minutes,
    long_wait_minutes,
    priority,
    source_reference
  FROM station_transfer_rules
  WHERE is_active = true
    AND (NULLIF('${country_filter_esc}', '') IS NULL OR country = '${country_filter_esc}')
    AND (effective_from IS NULL OR effective_from <= '${effective_date_esc}'::date)
    AND (effective_to IS NULL OR effective_to >= '${effective_date_esc}'::date)
  ORDER BY priority ASC, rule_id ASC
) t;
")"

  rules_file="$(mktemp)"
  TMP_FILES+=("$rules_file")
  printf '%s\n' "$rules_json" > "$rules_file"

  log "Running stitch prototype (country=${COUNTRY_FILTER:-ALL} top_n=${TOP_N} as_of=${effective_date})"
  node_args=(
    "${SCRIPT_DIR}/stitch-prototype.js"
    --ojp "$OJP_JSON"
    --motis "$MOTIS_JSON"
    --rules "$rules_file"
    --top-n "$TOP_N"
    --output "$OUTPUT_JSON"
  )
  if [[ -n "$COUNTRY_FILTER" ]]; then
    node_args+=(--country "$COUNTRY_FILTER")
  fi

  report_json="$(node "${node_args[@]}")"

  log "Wrote report: $OUTPUT_JSON"

  if [[ -f "$OUTPUT_JSON" ]]; then
    local escaped_report
    escaped_report="$(jq -c . "$OUTPUT_JSON" | sed -e "s/'/''/g")"
    db_psql -c "
      INSERT INTO system_state (key, value)
      VALUES ('stitch_prototype_report', '${escaped_report}'::jsonb)
      ON CONFLICT (key) DO UPDATE
      SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP;
    " >/dev/null 2>&1 || log "Warning: Failed to persist report to system_state DB"
  fi

  printf '%s\n' "$report_json" | jq '{generatedAt, inputSummary, rankedCount: (.rankedItineraries | length), topFlags: [.rankedItineraries[]?.flags]}'
}

main "$@"
