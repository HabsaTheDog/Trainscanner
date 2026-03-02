#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib-db.sh"

COUNTRY_FILTER=""

usage() {
  cat <<USAGE
Usage: scripts/data/report-canonical.sh [options]

Show canonical-station pipeline counts and recent run metadata.

Options:
  --country DE|AT|CH   Restrict report to one country
  -h, --help           Show this help
USAGE
  return 0
}

fail() {
  printf '[report-canonical] ERROR: %s\n' "$*" >&2
  return 1
}

log() {
  printf '[report-canonical] %s\n' "$*"
  return 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --country)
      [[ $# -ge 2 ]] || fail "Missing value for --country"
      COUNTRY_FILTER="$2"
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
  fail "Invalid --country '$COUNTRY_FILTER'"
fi

db_load_env
db_resolve_connection
db_ensure_ready

"${SCRIPT_DIR}/db-bootstrap.sh" --quiet

log "Scope: ${COUNTRY_FILTER:-ALL}"
country_filter_esc="$(db_sql_escape "$COUNTRY_FILTER")"

summary="$(db_psql -At -c "
SELECT
  (SELECT COUNT(*) FROM raw_snapshots rs WHERE NULLIF('${country_filter_esc}', '') IS NULL OR rs.country = '${country_filter_esc}') AS raw_snapshots,
  (SELECT COUNT(*) FROM netex_stops_staging s WHERE NULLIF('${country_filter_esc}', '') IS NULL OR s.country = '${country_filter_esc}') AS staging_rows,
  (SELECT COUNT(*) FROM canonical_stations cs WHERE NULLIF('${country_filter_esc}', '') IS NULL OR cs.country = '${country_filter_esc}') AS canonical_rows,
  (SELECT COUNT(*) FROM canonical_station_sources css WHERE NULLIF('${country_filter_esc}', '') IS NULL OR css.country = '${country_filter_esc}') AS mapping_rows;
")"

IFS='|' read -r raw_snapshots staging_rows canonical_rows mapping_rows <<<"$summary"

printf 'raw_snapshots=%s\n' "$raw_snapshots"
printf 'staging_rows=%s\n' "$staging_rows"
printf 'canonical_rows=%s\n' "$canonical_rows"
printf 'mapping_rows=%s\n' "$mapping_rows"

printf '\nrecent_import_runs:\n'
db_psql -c "
SELECT
  run_id,
  pipeline,
  status,
  COALESCE(source_id, '-') AS source_id,
  COALESCE(country, '-') AS country,
  COALESCE(to_char(snapshot_date, 'YYYY-MM-DD'), '-') AS snapshot_date,
  to_char(started_at, 'YYYY-MM-DD\"T\"HH24:MI:SSOF') AS started_at,
  COALESCE(to_char(ended_at, 'YYYY-MM-DD\"T\"HH24:MI:SSOF'), '-') AS ended_at
FROM import_runs
WHERE NULLIF('${country_filter_esc}', '') IS NULL OR country = '${country_filter_esc}'
ORDER BY started_at DESC
LIMIT 10;
"

if [[ "$canonical_rows" == "0" ]]; then
  fail "canonical_rows is 0 (run ingest/build first)"
fi

log "Report completed"
