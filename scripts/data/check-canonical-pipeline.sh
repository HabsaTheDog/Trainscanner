#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib-db.sh"

COUNTRY_FILTER=""
MIN_CANONICAL_ROWS="1"

usage() {
  cat <<USAGE
Usage: scripts/data/check-canonical-pipeline.sh [options]

Minimal verification checks for migrations + ingest + canonical build outputs.

Options:
  --country DE|AT|CH   Restrict checks to one country
  --min-canonical N    Minimum canonical rows expected (default: 1)
  -h, --help           Show this help
USAGE
  return 0
}

fail() {
  printf '[check-canonical] ERROR: %s\n' "$*" >&2
  return 1
}

log() {
  printf '[check-canonical] %s\n' "$*"
  return 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --country)
      [[ $# -ge 2 ]] || fail "Missing value for --country"
      COUNTRY_FILTER="$2"
      shift 2
      ;;
    --min-canonical)
      [[ $# -ge 2 ]] || fail "Missing value for --min-canonical"
      MIN_CANONICAL_ROWS="$2"
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

[[ "$MIN_CANONICAL_ROWS" =~ ^[0-9]+$ ]] || fail "--min-canonical must be an integer"

if [[ -n "$COUNTRY_FILTER" && "$COUNTRY_FILTER" != "DE" && "$COUNTRY_FILTER" != "AT" && "$COUNTRY_FILTER" != "CH" ]]; then
  fail "Invalid --country '$COUNTRY_FILTER'"
fi

db_load_env
db_resolve_connection
db_ensure_ready

"${SCRIPT_DIR}/db-migrate.sh" --quiet
country_filter_esc="$(db_sql_escape "$COUNTRY_FILTER")"

metrics="$(db_psql -At -c "
SELECT
  (SELECT COUNT(*) FROM pg_extension WHERE extname = 'postgis') AS has_postgis,
  (SELECT COUNT(*) FROM raw_snapshots rs WHERE NULLIF('${country_filter_esc}', '') IS NULL OR rs.country = '${country_filter_esc}') AS raw_snapshots,
  (SELECT COUNT(*) FROM netex_stops_staging s WHERE NULLIF('${country_filter_esc}', '') IS NULL OR s.country = '${country_filter_esc}') AS staging_rows,
  (SELECT COUNT(*) FROM canonical_stations cs WHERE NULLIF('${country_filter_esc}', '') IS NULL OR cs.country = '${country_filter_esc}') AS canonical_rows,
  (SELECT COUNT(*) FROM canonical_station_sources css WHERE NULLIF('${country_filter_esc}', '') IS NULL OR css.country = '${country_filter_esc}') AS mapping_rows;
")"

IFS='|' read -r has_postgis raw_snapshots staging_rows canonical_rows mapping_rows <<<"$metrics"

[[ "$has_postgis" == "1" ]] || fail "PostGIS extension missing"
[[ "$raw_snapshots" =~ ^[0-9]+$ ]] || fail "Could not read raw snapshot count"
[[ "$staging_rows" =~ ^[0-9]+$ ]] || fail "Could not read staging row count"
[[ "$canonical_rows" =~ ^[0-9]+$ ]] || fail "Could not read canonical row count"
[[ "$mapping_rows" =~ ^[0-9]+$ ]] || fail "Could not read mapping row count"

(( raw_snapshots > 0 )) || fail "No raw snapshots registered for scope ${COUNTRY_FILTER:-ALL}"
(( staging_rows > 0 )) || fail "No staging rows for scope ${COUNTRY_FILTER:-ALL}"
(( canonical_rows >= MIN_CANONICAL_ROWS )) || fail "Canonical rows (${canonical_rows}) below required minimum (${MIN_CANONICAL_ROWS})"
(( mapping_rows > 0 )) || fail "No canonical station mappings for scope ${COUNTRY_FILTER:-ALL}"

log "Checks passed"
log "raw_snapshots=${raw_snapshots} staging_rows=${staging_rows} canonical_rows=${canonical_rows} mapping_rows=${mapping_rows}"
