#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib-db.sh"

COUNTRY_FILTER=""
AS_OF=""
CSV_FILE=""
LIMIT_ROWS=""
APPLIED_COUNT=0

usage() {
  cat <<USAGE
Usage: scripts/data/apply-station-overrides.sh [options]

Apply approved canonical-station overrides (merge/split/rename).

Options:
  --country DE|AT|CH   Restrict applied overrides to one country
  --as-of YYYY-MM-DD   Apply only overrides approved/requested on or before date
  --csv PATH           Import overrides from CSV before applying
  --limit N            Apply at most N approved overrides
  -h, --help           Show this help

CSV headers (required order):
  operation,country,source_canonical_station_id,target_canonical_station_id,source_id,source_stop_id,new_canonical_name,reason,requested_by,approved_by,external_ref
USAGE
}

log() {
  printf '[apply-overrides] %s\n' "$*"
}

fail() {
  printf '[apply-overrides] ERROR: %s\n' "$*" >&2
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
      --csv)
        [[ $# -ge 2 ]] || fail "Missing value for --csv"
        CSV_FILE="$2"
        shift 2
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

  if [[ -n "$LIMIT_ROWS" ]] && ! [[ "$LIMIT_ROWS" =~ ^[0-9]+$ ]]; then
    fail "--limit must be an integer"
  fi

  if [[ -n "$CSV_FILE" && ! -f "$CSV_FILE" ]]; then
    fail "CSV file not found: $CSV_FILE"
  fi
}

import_overrides_csv() {
  local import_started_at import_started_esc csv_path_esc

  import_started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  import_started_esc="$(db_sql_escape "$import_started_at")"
  csv_path_esc="$(db_sql_escape "$(realpath "$CSV_FILE")")"

  log "Importing overrides from CSV: $CSV_FILE"
  db_copy_csv_from_file "$CSV_FILE" "canonical_station_overrides (
    operation,
    country,
    source_canonical_station_id,
    target_canonical_station_id,
    source_id,
    source_stop_id,
    new_canonical_name,
    reason,
    requested_by,
    approved_by,
    external_ref
  )"

  db_psql -c "
    UPDATE canonical_station_overrides
    SET
      status = CASE WHEN status = 'draft' THEN 'approved' ELSE status END,
      approved_at = CASE
        WHEN (CASE WHEN status = 'draft' THEN 'approved' ELSE status END) IN ('approved', 'applied')
             AND approved_at IS NULL THEN now()
        ELSE approved_at
      END,
      created_via = 'csv_import',
      created_from_file = COALESCE(created_from_file, '${csv_path_esc}'),
      updated_at = now()
    WHERE created_at >= '${import_started_esc}'::timestamptz;
  " >/dev/null
}

mark_override_failed() {
  local override_id="$1"
  local message="$2"
  local override_id_esc message_esc

  override_id_esc="$(db_sql_escape "$override_id")"
  message_esc="$(db_sql_escape "$message")"

  db_psql -c "
    UPDATE canonical_station_overrides
    SET
      status = 'failed',
      applied_at = now(),
      applied_by = current_user,
      applied_summary = jsonb_build_object('error', '${message_esc}'),
      updated_at = now()
    WHERE override_id = '${override_id_esc}'::bigint;
  " >/dev/null || true
}

mark_override_applied() {
  local override_id="$1"
  local summary_json="$2"
  local override_id_esc summary_json_esc

  override_id_esc="$(db_sql_escape "$override_id")"
  summary_json_esc="$(db_sql_escape "$summary_json")"

  db_psql -c "
    UPDATE canonical_station_overrides
    SET
      status = 'applied',
      applied_at = now(),
      applied_by = current_user,
      applied_summary = '${summary_json_esc}'::jsonb,
      updated_at = now()
    WHERE override_id = '${override_id_esc}'::bigint;
  " >/dev/null
}

extract_json_line() {
  printf '%s\n' "$1" | tr -d '\r' | grep -E '^\{.*\}$' | tail -n 1
}

apply_rename_override() {
  local override_id="$1"
  local country="$2"
  local target_station_id="$3"
  local new_name="$4"

  db_psql -At \
    -v override_id="$override_id" \
    -v country="$country" \
    -v target_station_id="$target_station_id" \
    -v new_name="$new_name" <<'SQL'
BEGIN;

SELECT assert_true(
  EXISTS (
    SELECT 1
    FROM canonical_stations
    WHERE canonical_station_id = :'target_station_id'
  ),
  format('Rename override %s: target canonical station %s not found', :'override_id', :'target_station_id')
);

SELECT assert_true(
  (
    SELECT country
    FROM canonical_stations
    WHERE canonical_station_id = :'target_station_id'
  ) = NULLIF(:'country', '')::char(2),
  format('Rename override %s: target station country does not match override country %s', :'override_id', :'country')
);

WITH prev AS (
  SELECT canonical_name
  FROM canonical_stations
  WHERE canonical_station_id = :'target_station_id'
)
SELECT canonical_name
INTO TEMP TABLE _rename_prev
FROM prev;

WITH upd AS (
  UPDATE canonical_stations
  SET
    canonical_name = :'new_name',
    normalized_name = normalize_station_name(:'new_name'),
    updated_at = now()
  WHERE canonical_station_id = :'target_station_id'
  RETURNING canonical_station_id
)
UPDATE canonical_review_queue q
SET
  status = 'resolved',
  resolved_at = now(),
  resolved_by = current_user,
  resolution_note = COALESCE(q.resolution_note, format('Resolved by override #%s', :'override_id')),
  updated_at = now()
FROM upd
WHERE q.status IN ('open', 'confirmed')
  AND q.canonical_station_id = upd.canonical_station_id;

SELECT json_build_object(
  'overrideId', :'override_id'::bigint,
  'operation', 'rename',
  'targetCanonicalStationId', :'target_station_id',
  'previousName', (SELECT canonical_name FROM _rename_prev LIMIT 1),
  'newName', :'new_name'
)::text;

COMMIT;
SQL
}

apply_merge_override() {
  local override_id="$1"
  local country="$2"
  local source_station_id="$3"
  local target_station_id="$4"

  db_psql -At \
    -v override_id="$override_id" \
    -v country="$country" \
    -v source_station_id="$source_station_id" \
    -v target_station_id="$target_station_id" <<'SQL'
BEGIN;

SELECT assert_true(
  EXISTS (
    SELECT 1
    FROM canonical_stations
    WHERE canonical_station_id = :'source_station_id'
  ),
  format('Merge override %s: source canonical station %s not found', :'override_id', :'source_station_id')
);

SELECT assert_true(
  EXISTS (
    SELECT 1
    FROM canonical_stations
    WHERE canonical_station_id = :'target_station_id'
  ),
  format('Merge override %s: target canonical station %s not found', :'override_id', :'target_station_id')
);

SELECT assert_true(
  (
    SELECT country
    FROM canonical_stations
    WHERE canonical_station_id = :'source_station_id'
  ) = NULLIF(:'country', '')::char(2)
  AND
  (
    SELECT country
    FROM canonical_stations
    WHERE canonical_station_id = :'target_station_id'
  ) = NULLIF(:'country', '')::char(2),
  format('Merge override %s: source/target country mismatch for override country %s', :'override_id', :'country')
);

CREATE TEMP TABLE _merge_moved_count (moved_count integer NOT NULL);

WITH moved AS (
  UPDATE canonical_station_sources
  SET
    canonical_station_id = :'target_station_id',
    updated_at = now()
  WHERE canonical_station_id = :'source_station_id'
  RETURNING 1
)
INSERT INTO _merge_moved_count (moved_count)
SELECT COUNT(*)::integer FROM moved;

SELECT refresh_canonical_station(:'target_station_id');
SELECT refresh_canonical_station(:'source_station_id');

UPDATE canonical_review_queue q
SET
  status = 'resolved',
  resolved_at = now(),
  resolved_by = current_user,
  resolution_note = COALESCE(q.resolution_note, format('Resolved by override #%s', :'override_id')),
  updated_at = now()
WHERE q.status IN ('open', 'confirmed')
  AND (
    q.canonical_station_id = :'source_station_id'
    OR q.canonical_station_id = :'target_station_id'
  );

SELECT json_build_object(
  'overrideId', :'override_id'::bigint,
  'operation', 'merge',
  'sourceCanonicalStationId', :'source_station_id',
  'targetCanonicalStationId', :'target_station_id',
  'movedMappings', (SELECT moved_count FROM _merge_moved_count),
  'targetMemberCount', (
    SELECT member_count
    FROM canonical_stations
    WHERE canonical_station_id = :'target_station_id'
  )
)::text;

COMMIT;
SQL
}

apply_split_override() {
  local override_id="$1"
  local country="$2"
  local source_station_id="$3"
  local target_station_id="$4"
  local source_id="$5"
  local source_stop_id="$6"
  local new_name="$7"

  db_psql -At \
    -v override_id="$override_id" \
    -v country="$country" \
    -v source_station_id="$source_station_id" \
    -v target_station_id="$target_station_id" \
    -v source_id="$source_id" \
    -v source_stop_id="$source_stop_id" \
    -v new_name="$new_name" <<'SQL'
BEGIN;

CREATE TEMP TABLE _split_ctx AS
SELECT
  :'source_station_id'::text AS source_station_id,
  COALESCE(NULLIF(:'target_station_id', ''), 'cstn_' || substr(md5('split|' || :'source_id' || '|' || :'source_stop_id'), 1, 20)) AS target_station_id;

SELECT assert_true(
  EXISTS (
    SELECT 1
    FROM canonical_stations
    WHERE canonical_station_id = :'source_station_id'
  ),
  format('Split override %s: source canonical station %s not found', :'override_id', :'source_station_id')
);

SELECT assert_true(
  (
    SELECT country
    FROM canonical_stations
    WHERE canonical_station_id = :'source_station_id'
  ) = NULLIF(:'country', '')::char(2),
  format('Split override %s: source station country does not match override country %s', :'override_id', :'country')
);

SELECT assert_true(
  COALESCE((
    SELECT country
    FROM canonical_stations
    WHERE canonical_station_id = (SELECT target_station_id FROM _split_ctx)
  ), NULLIF(:'country', '')::char(2)) = NULLIF(:'country', '')::char(2),
  format('Split override %s: existing target station country mismatch', :'override_id')
);

SELECT assert_true(
  EXISTS (
    SELECT 1
    FROM canonical_station_sources css
    WHERE css.canonical_station_id = :'source_station_id'
      AND css.source_id = :'source_id'
      AND css.source_stop_id = :'source_stop_id'
  ),
  format('Split override %s: mapping (%s, %s) not found under source station %s', :'override_id', :'source_id', :'source_stop_id', :'source_station_id')
);

INSERT INTO canonical_stations (
  canonical_station_id,
  canonical_name,
  normalized_name,
  country,
  latitude,
  longitude,
  geom,
  match_method,
  member_count,
  first_seen_snapshot_date,
  last_seen_snapshot_date,
  updated_at
)
SELECT
  ctx.target_station_id,
  COALESCE(
    NULLIF(:'new_name', ''),
    s.stop_name,
    format('Split %s', ctx.target_station_id)
  ) AS canonical_name,
  normalize_station_name(
    COALESCE(
      NULLIF(:'new_name', ''),
      s.stop_name,
      format('Split %s', ctx.target_station_id)
    )
  ) AS normalized_name,
  src.country,
  s.latitude,
  s.longitude,
  s.geom,
  CASE WHEN css.hard_id IS NOT NULL AND btrim(css.hard_id) <> '' THEN 'hard_id' WHEN s.geom IS NOT NULL THEN 'name_geo' ELSE 'name_only' END AS match_method,
  0,
  css.snapshot_date,
  css.snapshot_date,
  now()
FROM _split_ctx ctx
JOIN canonical_stations src
  ON src.canonical_station_id = ctx.source_station_id
JOIN canonical_station_sources css
  ON css.canonical_station_id = ctx.source_station_id
 AND css.source_id = :'source_id'
 AND css.source_stop_id = :'source_stop_id'
LEFT JOIN netex_stops_staging s
  ON s.source_id = css.source_id
 AND s.source_stop_id = css.source_stop_id
 AND s.snapshot_date = css.snapshot_date
WHERE NOT EXISTS (
  SELECT 1
  FROM canonical_stations cs
  WHERE cs.canonical_station_id = ctx.target_station_id
);

CREATE TEMP TABLE _split_moved_count (moved_count integer NOT NULL);

WITH moved AS (
  UPDATE canonical_station_sources
  SET
    canonical_station_id = (SELECT target_station_id FROM _split_ctx),
    updated_at = now()
  WHERE canonical_station_id = :'source_station_id'
    AND source_id = :'source_id'
    AND source_stop_id = :'source_stop_id'
  RETURNING 1
)
INSERT INTO _split_moved_count (moved_count)
SELECT COUNT(*)::integer FROM moved;

SELECT assert_true(
  (SELECT moved_count FROM _split_moved_count) > 0,
  format('Split override %s: no rows moved', :'override_id')
);

SELECT refresh_canonical_station(:'source_station_id');
SELECT refresh_canonical_station((SELECT target_station_id FROM _split_ctx));

UPDATE canonical_stations
SET
  canonical_name = NULLIF(:'new_name', ''),
  normalized_name = normalize_station_name(NULLIF(:'new_name', '')),
  updated_at = now()
WHERE canonical_station_id = (SELECT target_station_id FROM _split_ctx)
  AND NULLIF(:'new_name', '') IS NOT NULL;

UPDATE canonical_review_queue q
SET
  status = 'resolved',
  resolved_at = now(),
  resolved_by = current_user,
  resolution_note = COALESCE(q.resolution_note, format('Resolved by override #%s', :'override_id')),
  updated_at = now()
WHERE q.status IN ('open', 'confirmed')
  AND (
    q.canonical_station_id = :'source_station_id'
    OR q.canonical_station_id = (SELECT target_station_id FROM _split_ctx)
  );

SELECT json_build_object(
  'overrideId', :'override_id'::bigint,
  'operation', 'split',
  'sourceCanonicalStationId', :'source_station_id',
  'targetCanonicalStationId', (SELECT target_station_id FROM _split_ctx),
  'movedMappings', (SELECT moved_count FROM _split_moved_count),
  'sourceId', :'source_id',
  'sourceStopId', :'source_stop_id'
)::text;

COMMIT;
SQL
}

main() {
  local summary all_rows query_suffix csv_imported
  local country_filter_esc as_of_esc

  parse_args "$@"

  db_load_env
  db_resolve_connection
  db_ensure_ready

  "${SCRIPT_DIR}/db-migrate.sh" --quiet

  csv_imported="false"
  if [[ -n "$CSV_FILE" ]]; then
    import_overrides_csv
    csv_imported="true"
  fi

  query_suffix=""
  if [[ -n "$LIMIT_ROWS" ]]; then
    query_suffix=" LIMIT ${LIMIT_ROWS}"
  fi
  country_filter_esc="$(db_sql_escape "$COUNTRY_FILTER")"
  as_of_esc="$(db_sql_escape "$AS_OF")"

  mapfile -t all_rows < <(db_psql -At -F $'\x1f' -c "
SELECT
  override_id,
  operation,
  country,
  COALESCE(source_canonical_station_id, ''),
  COALESCE(target_canonical_station_id, ''),
  COALESCE(source_id, ''),
  COALESCE(source_stop_id, ''),
  COALESCE(new_canonical_name, '')
FROM canonical_station_overrides
WHERE status = 'approved'
  AND (NULLIF('${country_filter_esc}', '') IS NULL OR country = '${country_filter_esc}')
  AND (
    NULLIF('${as_of_esc}', '') IS NULL
    OR COALESCE(approved_at, requested_at)::date <= NULLIF('${as_of_esc}', '')::date
  )
ORDER BY override_id ASC${query_suffix};")

  if [[ ${#all_rows[@]} -eq 0 ]]; then
    log "No approved overrides to apply (country=${COUNTRY_FILTER:-ALL} as_of=${AS_OF:-latest} csv_imported=${csv_imported})"
    exit 0
  fi

  log "Applying ${#all_rows[@]} approved override(s)"

  for row in "${all_rows[@]}"; do
    local override_id operation country source_station_id target_station_id source_id source_stop_id new_name
    local output_json output_status summary_json

    IFS=$'\x1f' read -r override_id operation country source_station_id target_station_id source_id source_stop_id new_name <<<"$row"

    log "Applying override #${override_id} (${operation})"

    set +e
    case "$operation" in
      rename)
        output_json="$(apply_rename_override "$override_id" "$country" "$target_station_id" "$new_name" 2>&1)"
        output_status=$?
        ;;
      merge)
        output_json="$(apply_merge_override "$override_id" "$country" "$source_station_id" "$target_station_id" 2>&1)"
        output_status=$?
        ;;
      split)
        output_json="$(apply_split_override "$override_id" "$country" "$source_station_id" "$target_station_id" "$source_id" "$source_stop_id" "$new_name" 2>&1)"
        output_status=$?
        ;;
      *)
        output_json="Unsupported override operation: ${operation}"
        output_status=1
        ;;
    esac
    set -e

    if [[ $output_status -ne 0 ]]; then
      mark_override_failed "$override_id" "$output_json"
      printf '%s\n' "$output_json" >&2
      fail "Override #${override_id} failed; marked as failed and stopping"
    fi

    summary_json="$(extract_json_line "$output_json")"
    if [[ -z "$summary_json" ]]; then
      mark_override_failed "$override_id" "Override applied but summary JSON missing"
      fail "Override #${override_id} applied without summary JSON"
    fi

    mark_override_applied "$override_id" "$summary_json"
    APPLIED_COUNT=$((APPLIED_COUNT + 1))
    log "Applied override #${override_id}"
  done

  summary="$(db_psql -At -c "
SELECT json_build_object(
  'appliedInRun', ${APPLIED_COUNT},
  'pendingApproved', (
    SELECT COUNT(*)
    FROM canonical_station_overrides
    WHERE status = 'approved'
      AND (NULLIF('${country_filter_esc}', '') IS NULL OR country = '${country_filter_esc}')
  ),
  'failedTotal', (
    SELECT COUNT(*)
    FROM canonical_station_overrides
    WHERE status = 'failed'
      AND (NULLIF('${country_filter_esc}', '') IS NULL OR country = '${country_filter_esc}')
  )
)::text;
")"
  summary="$(extract_json_line "$summary")"

  log "Override apply complete"
  printf '%s\n' "$summary" | jq .
}

main "$@"
