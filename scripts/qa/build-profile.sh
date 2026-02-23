#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/data/lib-db.sh"

PROFILE=""
AS_OF=""
COUNTRY_FILTER=""
OUTPUT_PATH=""
FORCE_REBUILD="false"

usage() {
  cat <<USAGE
Usage: scripts/qa/build-profile.sh --profile <name> --as-of YYYY-MM-DD [options]

Build deterministic GTFS runtime artifact from canonical PostGIS data.

Options:
  --profile <name>        Profile name in config/gtfs-profiles.json (required)
  --as-of YYYY-MM-DD      Snapshot cutoff date (required)
  --country DE|AT|CH      Optional country scope override
  --output <path>         Optional output zip path (default: data/gtfs/runtime/<profile>/<as-of>/active-gtfs.zip)
  --force                 Rebuild even if matching manifest/artifact already exists
  -h, --help              Show this help
USAGE
}

log() {
  printf '[build-profile] %s\n' "$*"
}

fail() {
  printf '[build-profile] ERROR: %s\n' "$*" >&2
  exit 1
}

is_iso_date() {
  local d="$1"
  [[ "$d" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || return 1
  date -u -d "$d" +%F >/dev/null 2>&1
}

sha256_file() {
  local file="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file" | awk '{print $1}'
    return
  fi

  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$file" | awk '{print $1}'
    return
  fi

  fail "Neither sha256sum nor shasum is available"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="${2:-}"
      shift 2
      ;;
    --as-of)
      AS_OF="${2:-}"
      shift 2
      ;;
    --country)
      COUNTRY_FILTER="${2:-}"
      shift 2
      ;;
    --output)
      OUTPUT_PATH="${2:-}"
      shift 2
      ;;
    --force)
      FORCE_REBUILD="true"
      shift
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

[[ -n "$PROFILE" ]] || fail "Missing required --profile"
[[ -n "$AS_OF" ]] || fail "Missing required --as-of"
is_iso_date "$AS_OF" || fail "Invalid --as-of value '$AS_OF' (expected YYYY-MM-DD)"

if [[ -n "$COUNTRY_FILTER" && "$COUNTRY_FILTER" != "DE" && "$COUNTRY_FILTER" != "AT" && "$COUNTRY_FILTER" != "CH" ]]; then
  fail "Invalid --country '$COUNTRY_FILTER' (expected DE, AT, or CH)"
fi

command -v node >/dev/null 2>&1 || fail "node is required"
command -v python3 >/dev/null 2>&1 || fail "python3 is required"
command -v jq >/dev/null 2>&1 || fail "jq is required"

"${ROOT_DIR}/scripts/validate-config.sh" --only profiles >/dev/null

PROFILE_META_JSON="$(node - <<'NODE' "$ROOT_DIR" "$PROFILE"
const fs = require('node:fs');
const path = require('node:path');

const rootDir = process.argv[2];
const profileName = process.argv[3];
const profilesPath = path.join(rootDir, 'config', 'gtfs-profiles.json');
const { normalizeProfiles } = require(path.join(rootDir, 'orchestrator', 'src', 'profile-resolver.js'));

let raw;
try {
  raw = JSON.parse(fs.readFileSync(profilesPath, 'utf8'));
} catch (err) {
  console.error(`Failed to read ${profilesPath}: ${err.message}`);
  process.exit(2);
}

const profiles = normalizeProfiles(raw);
const selected = profiles[profileName];
if (!selected) {
  console.error(`Profile '${profileName}' not found in ${profilesPath}`);
  process.exit(3);
}

const payload = {
  name: profileName,
  sourceType: selected.sourceType || 'static',
  runtimeMode: selected.runtime ? selected.runtime.mode || '' : '',
  runtimeCountry: selected.runtime ? selected.runtime.country || '' : '',
  runtimeProfile: selected.runtime ? selected.runtime.profile || '' : '',
  description: selected.description || ''
};

process.stdout.write(JSON.stringify(payload));
NODE
)"

RUNTIME_COUNTRY="$(jq -r '.runtimeCountry // empty' <<<"$PROFILE_META_JSON")"
RUNTIME_MODE="$(jq -r '.runtimeMode // empty' <<<"$PROFILE_META_JSON")"

if [[ -z "$COUNTRY_FILTER" && -n "$RUNTIME_COUNTRY" ]]; then
  COUNTRY_FILTER="$RUNTIME_COUNTRY"
fi

if [[ -n "$COUNTRY_FILTER" && "$COUNTRY_FILTER" != "DE" && "$COUNTRY_FILTER" != "AT" && "$COUNTRY_FILTER" != "CH" ]]; then
  fail "Profile/runtime country resolved to invalid scope '$COUNTRY_FILTER'"
fi

if [[ -n "$RUNTIME_MODE" && "$RUNTIME_MODE" != "canonical-export" ]]; then
  fail "Profile '$PROFILE' runtime mode '$RUNTIME_MODE' is unsupported (expected canonical-export)"
fi

if [[ -n "$OUTPUT_PATH" ]]; then
  if [[ -d "$OUTPUT_PATH" || "$OUTPUT_PATH" == */ ]]; then
    OUTPUT_ZIP_PATH="${OUTPUT_PATH%/}/active-gtfs.zip"
  else
    OUTPUT_ZIP_PATH="$OUTPUT_PATH"
  fi
else
  OUTPUT_ZIP_PATH="data/gtfs/runtime/${PROFILE}/${AS_OF}/active-gtfs.zip"
fi

if [[ "$OUTPUT_ZIP_PATH" != /* ]]; then
  OUTPUT_ZIP_PATH="${ROOT_DIR}/${OUTPUT_ZIP_PATH}"
fi

OUTPUT_DIR="$(dirname "$OUTPUT_ZIP_PATH")"
MANIFEST_PATH="${OUTPUT_DIR}/manifest.json"
mkdir -p "$OUTPUT_DIR"

if [[ "$FORCE_REBUILD" != "true" && -f "$OUTPUT_ZIP_PATH" && -f "$MANIFEST_PATH" ]]; then
  EXISTING_PROFILE="$(jq -r '.profile // empty' "$MANIFEST_PATH")"
  EXISTING_AS_OF="$(jq -r '.asOf // empty' "$MANIFEST_PATH")"
  EXISTING_COUNTRY="$(jq -r '.countryScope // empty' "$MANIFEST_PATH")"
  EXISTING_SHA="$(jq -r '.sha256 // empty' "$MANIFEST_PATH")"
  CURRENT_SHA="$(sha256_file "$OUTPUT_ZIP_PATH")"

  if [[ "$EXISTING_PROFILE" == "$PROFILE" && "$EXISTING_AS_OF" == "$AS_OF" && "$EXISTING_COUNTRY" == "$COUNTRY_FILTER" && "$EXISTING_SHA" == "$CURRENT_SHA" ]]; then
    log "Idempotent export hit: existing artifact+manifest already match requested scope"
    log "artifact=${OUTPUT_ZIP_PATH}"
    log "manifest=${MANIFEST_PATH}"
    log "sha256=${CURRENT_SHA}"
    exit 0
  fi
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

STOPS_CSV="${TMP_DIR}/canonical-stops.csv"
SUMMARY_JSON="${TMP_DIR}/export-summary.json"

log "Resolving canonical export scope for profile='${PROFILE}' as-of='${AS_OF}' country='${COUNTRY_FILTER:-ALL}'"

db_load_env
db_resolve_connection
db_ensure_ready

"${ROOT_DIR}/scripts/data/db-migrate.sh" --quiet

AS_OF_ESC="$(db_sql_escape "$AS_OF")"
COUNTRY_FILTER_ESC="$(db_sql_escape "$COUNTRY_FILTER")"

SNAPSHOT_META_JSON="$(db_psql -At -v as_of="$AS_OF" -v country_filter="$COUNTRY_FILTER" <<'SQL'
WITH selected_snapshots AS (
  SELECT rs.source_id, rs.country, MAX(rs.snapshot_date) AS snapshot_date
  FROM raw_snapshots rs
  WHERE rs.format = 'netex'
    AND rs.snapshot_date <= :'as_of'::date
    AND (NULLIF(:'country_filter', '') IS NULL OR rs.country = NULLIF(:'country_filter', '')::char(2))
  GROUP BY rs.source_id, rs.country
)
SELECT json_build_object(
  'sourceCount', COUNT(*),
  'snapshotMin', MIN(snapshot_date),
  'snapshotMax', MAX(snapshot_date),
  'countries', COALESCE(json_agg(DISTINCT country ORDER BY country), '[]'::json)
)::text
FROM selected_snapshots;
SQL
)"

if [[ -z "$SNAPSHOT_META_JSON" ]]; then
  fail "Failed to resolve selected snapshot metadata"
fi

SOURCE_COUNT="$(jq -r '.sourceCount // 0' <<<"$SNAPSHOT_META_JSON")"
if ! [[ "$SOURCE_COUNT" =~ ^[0-9]+$ ]]; then
  fail "Could not parse selected source count"
fi
if (( SOURCE_COUNT == 0 )); then
  fail "No canonical snapshots found for as-of=${AS_OF} scope=${COUNTRY_FILTER:-ALL}. Run fetch/ingest/build first."
fi

log "Exporting canonical stop scope rows from PostGIS"
db_psql -c "\copy (
WITH selected_snapshots AS (
  SELECT rs.source_id, rs.country, MAX(rs.snapshot_date) AS snapshot_date
  FROM raw_snapshots rs
  WHERE rs.format = 'netex'
    AND rs.snapshot_date <= '${AS_OF_ESC}'::date
    AND (NULLIF('${COUNTRY_FILTER_ESC}', '') IS NULL OR rs.country = NULLIF('${COUNTRY_FILTER_ESC}', '')::char(2))
  GROUP BY rs.source_id, rs.country
),
selected_mappings AS (
  SELECT
    css.canonical_station_id,
    css.country,
    css.snapshot_date,
    css.source_id,
    css.source_stop_id
  FROM canonical_station_sources css
  JOIN selected_snapshots ss
    ON ss.source_id = css.source_id
   AND ss.snapshot_date = css.snapshot_date
  WHERE css.country IN ('DE', 'AT', 'CH')
),
aggregated_stops AS (
  SELECT
    sm.canonical_station_id AS stop_id,
    sm.country,
    COALESCE(NULLIF(cs.canonical_name, ''), MIN(NULLIF(s.stop_name, '')), sm.canonical_station_id) AS stop_name,
    ROUND(AVG(s.latitude)::numeric, 6) AS stop_lat,
    ROUND(AVG(s.longitude)::numeric, 6) AS stop_lon,
    MIN(sm.snapshot_date) AS first_snapshot_date,
    MAX(sm.snapshot_date) AS last_snapshot_date,
    COUNT(*)::integer AS source_rows
  FROM selected_mappings sm
  LEFT JOIN canonical_stations cs
    ON cs.canonical_station_id = sm.canonical_station_id
  LEFT JOIN netex_stops_staging s
    ON s.source_id = sm.source_id
   AND s.source_stop_id = sm.source_stop_id
   AND s.snapshot_date = sm.snapshot_date
  GROUP BY sm.canonical_station_id, sm.country, cs.canonical_name
),
active_groups AS (
  SELECT
    g.group_id,
    g.cluster_id,
    g.country,
    g.display_name
  FROM qa_station_groups_v2 g
  WHERE g.is_active = true
    AND (NULLIF('${COUNTRY_FILTER_ESC}', '') IS NULL OR g.country = NULLIF('${COUNTRY_FILTER_ESC}', '')::char(2))
),
group_sections AS (
  SELECT
    s.section_id,
    s.group_id,
    s.section_name,
    s.section_type
  FROM qa_station_group_sections_v2 s
  JOIN active_groups g
    ON g.group_id = s.group_id
),
section_members AS (
  SELECT
    gs.section_id,
    gs.group_id,
    gm.canonical_station_id
  FROM group_sections gs
  JOIN qa_station_group_section_members_v2 gm
    ON gm.section_id = gs.section_id
),
group_member_ids AS (
  SELECT DISTINCT canonical_station_id
  FROM section_members
),
group_parent_rows AS (
  SELECT
    g.group_id AS stop_id,
    g.display_name AS stop_name,
    g.country,
    ROUND(AVG(a.stop_lat)::numeric, 6) AS stop_lat,
    ROUND(AVG(a.stop_lon)::numeric, 6) AS stop_lon,
    ''::text AS location_type,
    ''::text AS parent_station,
    true AS is_user_facing,
    '[]'::json AS walk_links_json,
    ''::text AS section_type,
    MIN(a.first_snapshot_date) AS first_snapshot_date,
    MAX(a.last_snapshot_date) AS last_snapshot_date,
    COUNT(*)::integer AS source_rows
  FROM active_groups g
  JOIN section_members sm
    ON sm.group_id = g.group_id
  JOIN aggregated_stops a
    ON a.stop_id = sm.canonical_station_id
  GROUP BY g.group_id, g.display_name, g.country
),
group_section_rows AS (
  SELECT
    gs.section_id AS stop_id,
    gs.section_name AS stop_name,
    g.country,
    ROUND(AVG(a.stop_lat)::numeric, 6) AS stop_lat,
    ROUND(AVG(a.stop_lon)::numeric, 6) AS stop_lon,
    '0'::text AS location_type,
    gs.group_id AS parent_station,
    false AS is_user_facing,
    COALESCE((
      SELECT json_agg(json_build_object(
        'to_stop_id', l.to_section_id,
        'min_walk_minutes', l.min_walk_minutes
      ) ORDER BY l.to_section_id)
      FROM qa_station_group_section_links_v2 l
      WHERE l.from_section_id = gs.section_id
    ), '[]'::json) AS walk_links_json,
    gs.section_type,
    MIN(a.first_snapshot_date) AS first_snapshot_date,
    MAX(a.last_snapshot_date) AS last_snapshot_date,
    COUNT(*)::integer AS source_rows
  FROM group_sections gs
  JOIN active_groups g
    ON g.group_id = gs.group_id
  JOIN section_members sm
    ON sm.section_id = gs.section_id
  JOIN aggregated_stops a
    ON a.stop_id = sm.canonical_station_id
  GROUP BY gs.section_id, gs.group_id, gs.section_name, gs.section_type, g.country
),
ungrouped_rows AS (
  SELECT
    a.stop_id,
    a.stop_name,
    a.country,
    a.stop_lat,
    a.stop_lon,
    ''::text AS location_type,
    ''::text AS parent_station,
    true AS is_user_facing,
    '[]'::json AS walk_links_json,
    ''::text AS section_type,
    a.first_snapshot_date,
    a.last_snapshot_date,
    a.source_rows
  FROM aggregated_stops a
  WHERE NOT EXISTS (
    SELECT 1
    FROM group_member_ids gm
    WHERE gm.canonical_station_id = a.stop_id
  )
),
export_rows AS (
  SELECT * FROM group_parent_rows
  UNION ALL
  SELECT * FROM group_section_rows
  UNION ALL
  SELECT * FROM ungrouped_rows
)
SELECT
  stop_id,
  stop_name,
  country,
  COALESCE(stop_lat::text, '') AS stop_lat,
  COALESCE(stop_lon::text, '') AS stop_lon,
  location_type,
  parent_station,
  CASE WHEN is_user_facing THEN 'true' ELSE 'false' END AS is_user_facing,
  walk_links_json::text AS walk_links_json,
  section_type,
  first_snapshot_date,
  last_snapshot_date,
  source_rows
FROM export_rows
ORDER BY country, is_user_facing DESC, stop_id
) TO STDOUT WITH CSV HEADER" > "$STOPS_CSV"

STOP_ROWS="$(($(wc -l < "$STOPS_CSV") - 1))"
if (( STOP_ROWS <= 0 )); then
  fail "Canonical export produced 0 stop rows for requested scope"
fi

log "Generating deterministic GTFS runtime artifact"
python3 "${SCRIPT_DIR}/export-canonical-gtfs.py" \
  --stops-csv "$STOPS_CSV" \
  --profile "$PROFILE" \
  --as-of "$AS_OF" \
  --output-zip "$OUTPUT_ZIP_PATH" \
  --summary-json "$SUMMARY_JSON"

"${SCRIPT_DIR}/validate-export.sh" --zip "$OUTPUT_ZIP_PATH"

ARTIFACT_SHA256="$(sha256_file "$OUTPUT_ZIP_PATH")"
ROW_COUNTS_JSON="$(jq -c '.counts' "$SUMMARY_JSON")"
BRIDGE_MODE="$(jq -r '.bridgeMode // "unknown"' "$SUMMARY_JSON")"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
MANIFEST_PATH_REL="$(realpath --relative-to "$ROOT_DIR" "$MANIFEST_PATH" 2>/dev/null || printf '%s' "$MANIFEST_PATH")"

jq -n \
  --arg profile "$PROFILE" \
  --arg asOf "$AS_OF" \
  --arg countryScope "${COUNTRY_FILTER}" \
  --arg artifactPath "$(realpath --relative-to "$ROOT_DIR" "$OUTPUT_ZIP_PATH" 2>/dev/null || printf '%s' "$OUTPUT_ZIP_PATH")" \
  --arg manifestPath "$MANIFEST_PATH_REL" \
  --arg bridgeMode "$BRIDGE_MODE" \
  --arg generationTimestamp "$GENERATED_AT" \
  --arg sha256 "$ARTIFACT_SHA256" \
  --argjson dbSnapshotBounds "$SNAPSHOT_META_JSON" \
  --argjson rowCounts "$ROW_COUNTS_JSON" \
  '{
    profile: $profile,
    asOf: $asOf,
    countryScope: (if $countryScope == "" then null else $countryScope end),
    bridgeMode: $bridgeMode,
    artifactPath: $artifactPath,
    manifestPath: $manifestPath,
    dbSnapshotBounds: $dbSnapshotBounds,
    rowCounts: $rowCounts,
    sha256: $sha256,
    generationTimestamp: $generationTimestamp
  }' > "$MANIFEST_PATH"

log "Export complete"
log "artifact=${OUTPUT_ZIP_PATH}"
log "manifest=${MANIFEST_PATH}"
log "sha256=${ARTIFACT_SHA256}"
