#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib-db.sh"

COUNTRY_FILTER=""
AS_OF=""
SOURCE_ID_SCOPE=""

usage() {
  cat <<USAGE
Usage: scripts/data/build-canonical-stations.sh [options]

Build canonical stations from NeTEx staging rows.

Options:
  --country DE|AT|CH   Restrict build scope to one country
  --as-of YYYY-MM-DD   Use latest snapshots <= date
  --source-id ID       Restrict build scope to one source id
  -h, --help           Show this help
USAGE
}

log() {
  printf '[build-canonical] %s\n' "$*"
}

fail() {
  printf '[build-canonical] ERROR: %s\n' "$*" >&2
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
      --source-id)
        [[ $# -ge 2 ]] || fail "Missing value for --source-id"
        SOURCE_ID_SCOPE="$2"
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
}

create_run() {
  local run_id
  local run_id_esc country_esc snapshot_date_esc source_id_esc
  run_id="$(python3 - <<'PY'
import uuid
print(uuid.uuid4())
PY
)"

  run_id_esc="$(db_sql_escape "$run_id")"
  country_esc="$(db_sql_escape "$COUNTRY_FILTER")"
  snapshot_date_esc="$(db_sql_escape "$AS_OF")"
  source_id_esc="$(db_sql_escape "$SOURCE_ID_SCOPE")"

  db_psql -c "
    INSERT INTO import_runs (run_id, pipeline, status, source_id, country, snapshot_date)
    VALUES (
      '${run_id_esc}'::uuid,
      'canonical_build',
      'running',
      NULLIF('${source_id_esc}', ''),
      NULLIF('${country_esc}', ''),
      NULLIF('${snapshot_date_esc}', '')::date
    );
  " >/dev/null

  printf '%s\n' "$run_id"
}

mark_run_failed() {
  local run_id="$1"
  local message="$2"
  local run_id_esc message_esc

  run_id_esc="$(db_sql_escape "$run_id")"
  message_esc="$(db_sql_escape "$message")"

  db_psql -c "
    UPDATE import_runs
    SET status = 'failed', ended_at = now(), error_message = '${message_esc}'
    WHERE run_id = '${run_id_esc}'::uuid;
  " >/dev/null || true
}

main() {
  local run_id summary_json sql_status source_rows canonical_rows inserted_rows updated_rows merged_rows conflicts
  local run_id_esc summary_json_esc

  parse_args "$@"

  db_load_env
  db_resolve_connection
  db_ensure_ready

  "${SCRIPT_DIR}/db-migrate.sh" --quiet

  run_id="$(create_run)"
  log "Building canonical stations (run ${run_id})"

  set +e
  summary_json="$(db_psql -At \
    -v run_id="$run_id" \
    -v country_filter="$COUNTRY_FILTER" \
    -v as_of="$AS_OF" \
    -v source_id_scope="$SOURCE_ID_SCOPE" <<'SQL'
BEGIN;

CREATE TEMP TABLE _selected_snapshots AS
SELECT rs.source_id, rs.country, MAX(rs.snapshot_date) AS snapshot_date
FROM raw_snapshots rs
WHERE rs.format = 'netex'
  AND (NULLIF(:'country_filter', '') IS NULL OR rs.country = :'country_filter')
  AND (NULLIF(:'source_id_scope', '') IS NULL OR rs.source_id = :'source_id_scope')
  AND (NULLIF(:'as_of', '') IS NULL OR rs.snapshot_date <= NULLIF(:'as_of', '')::date)
GROUP BY rs.source_id, rs.country;

DO $$
BEGIN
  IF (SELECT COUNT(*) FROM _selected_snapshots) = 0 THEN
    RAISE EXCEPTION 'No raw snapshots matched selected filters';
  END IF;
END $$;

CREATE TEMP TABLE _candidate AS
SELECT
  s.source_id,
  s.country,
  s.snapshot_date,
  s.source_stop_id,
  s.stop_name,
  s.normalized_name,
  s.latitude,
  s.longitude,
  s.geom,
  COALESCE(NULLIF(s.hard_id, ''), NULLIF(s.public_code, ''), NULLIF(s.private_code, '')) AS hard_key
FROM netex_stops_staging s
JOIN _selected_snapshots ss
  ON ss.source_id = s.source_id AND ss.snapshot_date = s.snapshot_date
WHERE s.stop_name IS NOT NULL
  AND btrim(s.stop_name) <> '';

DO $$
BEGIN
  IF (SELECT COUNT(*) FROM _candidate) = 0 THEN
    RAISE EXCEPTION 'No staging rows found for selected snapshots';
  END IF;
END $$;

CREATE TEMP TABLE _hard_groups AS
SELECT
  'cstn_' || substr(md5('hard|' || country || '|' || hard_key), 1, 20) AS canonical_station_id,
  country,
  hard_key,
  MIN(stop_name) AS canonical_name,
  MIN(normalized_name) AS normalized_name,
  AVG(latitude) FILTER (WHERE latitude IS NOT NULL) AS latitude,
  AVG(longitude) FILTER (WHERE longitude IS NOT NULL) AS longitude,
  CASE
    WHEN COUNT(*) FILTER (WHERE geom IS NOT NULL) > 0 THEN ST_SetSRID(ST_Centroid(ST_Collect(geom)), 4326)
    ELSE NULL
  END AS geom,
  'hard_id'::text AS match_method,
  COUNT(*)::integer AS member_count,
  MIN(snapshot_date) AS first_seen_snapshot_date,
  MAX(snapshot_date) AS last_seen_snapshot_date
FROM _candidate
WHERE hard_key IS NOT NULL
GROUP BY country, hard_key;

CREATE TEMP TABLE _soft_geo_clustered AS
SELECT
  c.*,
  ST_ClusterDBSCAN(ST_Transform(c.geom, 3857), eps := 250, minpoints := 1)
    OVER (PARTITION BY c.country, c.normalized_name ORDER BY c.source_id, c.source_stop_id) AS cluster_id
FROM _candidate c
WHERE c.hard_key IS NULL
  AND c.geom IS NOT NULL;

CREATE TEMP TABLE _soft_geo_groups AS
SELECT
  'cstn_' || substr(md5('geo|' || country || '|' || normalized_name || '|' || cluster_id::text), 1, 20) AS canonical_station_id,
  country,
  normalized_name,
  cluster_id,
  MIN(stop_name) AS canonical_name,
  AVG(latitude) AS latitude,
  AVG(longitude) AS longitude,
  ST_SetSRID(ST_Centroid(ST_Collect(geom)), 4326) AS geom,
  'name_geo'::text AS match_method,
  COUNT(*)::integer AS member_count,
  MIN(snapshot_date) AS first_seen_snapshot_date,
  MAX(snapshot_date) AS last_seen_snapshot_date
FROM _soft_geo_clustered
GROUP BY country, normalized_name, cluster_id;

CREATE TEMP TABLE _soft_name_only_groups AS
SELECT
  'cstn_' || substr(md5('name|' || country || '|' || normalized_name), 1, 20) AS canonical_station_id,
  country,
  normalized_name,
  MIN(stop_name) AS canonical_name,
  NULL::double precision AS latitude,
  NULL::double precision AS longitude,
  NULL::geometry(Point, 4326) AS geom,
  'name_only'::text AS match_method,
  COUNT(*)::integer AS member_count,
  MIN(snapshot_date) AS first_seen_snapshot_date,
  MAX(snapshot_date) AS last_seen_snapshot_date
FROM _candidate
WHERE hard_key IS NULL
  AND geom IS NULL
GROUP BY country, normalized_name;

CREATE TEMP TABLE _new_canonical AS
SELECT
  canonical_station_id,
  country,
  canonical_name,
  normalized_name,
  latitude,
  longitude,
  geom,
  match_method,
  member_count,
  first_seen_snapshot_date,
  last_seen_snapshot_date
FROM _hard_groups
UNION ALL
SELECT
  canonical_station_id,
  country,
  canonical_name,
  normalized_name,
  latitude,
  longitude,
  geom,
  match_method,
  member_count,
  first_seen_snapshot_date,
  last_seen_snapshot_date
FROM _soft_geo_groups
UNION ALL
SELECT
  canonical_station_id,
  country,
  canonical_name,
  normalized_name,
  latitude,
  longitude,
  geom,
  match_method,
  member_count,
  first_seen_snapshot_date,
  last_seen_snapshot_date
FROM _soft_name_only_groups;

CREATE TEMP TABLE _assign_hard AS
SELECT
  c.source_id,
  c.source_stop_id,
  c.country,
  c.snapshot_date,
  h.canonical_station_id,
  'hard_id'::text AS match_method,
  c.hard_key
FROM _candidate c
JOIN _hard_groups h
  ON h.country = c.country
 AND h.hard_key = c.hard_key
WHERE c.hard_key IS NOT NULL;

CREATE TEMP TABLE _assign_geo AS
SELECT
  c.source_id,
  c.source_stop_id,
  c.country,
  c.snapshot_date,
  g.canonical_station_id,
  'name_geo'::text AS match_method,
  c.hard_key
FROM _soft_geo_clustered c
JOIN _soft_geo_groups g
  ON g.country = c.country
 AND g.normalized_name = c.normalized_name
 AND g.cluster_id = c.cluster_id;

CREATE TEMP TABLE _assign_name AS
SELECT
  c.source_id,
  c.source_stop_id,
  c.country,
  c.snapshot_date,
  n.canonical_station_id,
  'name_only'::text AS match_method,
  c.hard_key
FROM _candidate c
JOIN _soft_name_only_groups n
  ON n.country = c.country
 AND n.normalized_name = c.normalized_name
WHERE c.hard_key IS NULL
  AND c.geom IS NULL;

CREATE TEMP TABLE _assignments AS
SELECT * FROM _assign_hard
UNION ALL
SELECT * FROM _assign_geo
UNION ALL
SELECT * FROM _assign_name;

CREATE TEMP TABLE _summary AS
SELECT
  (SELECT COUNT(*) FROM _candidate) AS source_rows,
  (SELECT COUNT(*) FROM _new_canonical) AS canonical_rows,
  (SELECT COUNT(*) FROM _soft_name_only_groups WHERE member_count > 1) AS conflicts,
  (SELECT COALESCE(SUM(member_count - 1), 0) FROM _new_canonical) AS merged,
  (
    SELECT COUNT(*)
    FROM _new_canonical nc
    LEFT JOIN canonical_stations cs ON cs.canonical_station_id = nc.canonical_station_id
    WHERE cs.canonical_station_id IS NULL
  ) AS inserted,
  (
    SELECT COUNT(*)
    FROM _new_canonical nc
    JOIN canonical_stations cs ON cs.canonical_station_id = nc.canonical_station_id
    WHERE cs.canonical_name IS DISTINCT FROM nc.canonical_name
       OR cs.latitude IS DISTINCT FROM nc.latitude
       OR cs.longitude IS DISTINCT FROM nc.longitude
       OR cs.match_method IS DISTINCT FROM nc.match_method
       OR cs.member_count IS DISTINCT FROM nc.member_count
       OR cs.last_seen_snapshot_date IS DISTINCT FROM nc.last_seen_snapshot_date
  ) AS updated;

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
  last_built_run_id,
  updated_at
)
SELECT
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
  :'run_id'::uuid,
  now()
FROM _new_canonical
ON CONFLICT (canonical_station_id)
DO UPDATE SET
  canonical_name = EXCLUDED.canonical_name,
  normalized_name = EXCLUDED.normalized_name,
  country = EXCLUDED.country,
  latitude = EXCLUDED.latitude,
  longitude = EXCLUDED.longitude,
  geom = EXCLUDED.geom,
  match_method = EXCLUDED.match_method,
  member_count = EXCLUDED.member_count,
  first_seen_snapshot_date = EXCLUDED.first_seen_snapshot_date,
  last_seen_snapshot_date = EXCLUDED.last_seen_snapshot_date,
  last_built_run_id = EXCLUDED.last_built_run_id,
  updated_at = now();

DELETE FROM canonical_station_sources css
USING _selected_snapshots ss
WHERE css.source_id = ss.source_id;

INSERT INTO canonical_station_sources (
  canonical_station_id,
  source_id,
  source_stop_id,
  country,
  snapshot_date,
  match_method,
  hard_id,
  import_run_id,
  updated_at
)
SELECT
  a.canonical_station_id,
  a.source_id,
  a.source_stop_id,
  a.country,
  a.snapshot_date,
  a.match_method,
  a.hard_key,
  :'run_id'::uuid,
  now()
FROM _assignments a
ON CONFLICT (source_id, source_stop_id)
DO UPDATE SET
  canonical_station_id = EXCLUDED.canonical_station_id,
  country = EXCLUDED.country,
  snapshot_date = EXCLUDED.snapshot_date,
  match_method = EXCLUDED.match_method,
  hard_id = EXCLUDED.hard_id,
  import_run_id = EXCLUDED.import_run_id,
  updated_at = now();

DELETE FROM canonical_stations cs
WHERE cs.country IN (SELECT DISTINCT country FROM _selected_snapshots)
  AND NOT EXISTS (
    SELECT 1
    FROM canonical_station_sources css
    WHERE css.canonical_station_id = cs.canonical_station_id
  );

SELECT json_build_object(
  'sourceRows', (SELECT source_rows FROM _summary),
  'canonicalRows', (SELECT canonical_rows FROM _summary),
  'inserted', (SELECT inserted FROM _summary),
  'updated', (SELECT updated FROM _summary),
  'merged', (SELECT merged FROM _summary),
  'conflicts', (SELECT conflicts FROM _summary),
  'countryFilter', NULLIF(:'country_filter', ''),
  'asOf', NULLIF(:'as_of', ''),
  'sourceScope', NULLIF(:'source_id_scope', '')
)::text;

COMMIT;
SQL
  )"
  summary_json="$(printf '%s\n' "$summary_json" | tr -d '\r' | grep -E '^\{.*\}$' | tail -n 1)"
  sql_status=$?
  set -e

  if [[ $sql_status -ne 0 || -z "$summary_json" ]]; then
    mark_run_failed "$run_id" "Canonical build SQL failed"
    fail "Canonical build failed"
  fi

  canonical_rows="$(jq -r '.canonicalRows // 0' <<<"$summary_json")"
  source_rows="$(jq -r '.sourceRows // 0' <<<"$summary_json")"
  inserted_rows="$(jq -r '.inserted // 0' <<<"$summary_json")"
  updated_rows="$(jq -r '.updated // 0' <<<"$summary_json")"
  merged_rows="$(jq -r '.merged // 0' <<<"$summary_json")"
  conflicts="$(jq -r '.conflicts // 0' <<<"$summary_json")"

  if [[ "$canonical_rows" == "0" ]]; then
    mark_run_failed "$run_id" "Canonical build produced 0 rows"
    fail "Canonical build produced 0 rows"
  fi

  run_id_esc="$(db_sql_escape "$run_id")"
  summary_json_esc="$(db_sql_escape "$summary_json")"
  db_psql -c "
    UPDATE import_runs
    SET status = 'succeeded', ended_at = now(), stats = '${summary_json_esc}'::jsonb
    WHERE run_id = '${run_id_esc}'::uuid;
  " >/dev/null

  log "Canonical build complete"
  log "Source rows: ${source_rows}"
  log "Canonical rows: ${canonical_rows}"
  log "Inserted: ${inserted_rows}, Updated: ${updated_rows}, Merged: ${merged_rows}, Conflicts: ${conflicts}"
}

main "$@"
