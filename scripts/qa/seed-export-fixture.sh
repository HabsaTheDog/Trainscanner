#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/data/lib-db.sh"

AS_OF="2026-01-15"

usage() {
  cat <<USAGE
Usage: scripts/qa/seed-export-fixture.sh [--as-of YYYY-MM-DD]

Seed minimal canonical fixture data used by CI export checks.
USAGE
  return 0
}

fail() {
  printf '[seed-export-fixture] ERROR: %s\n' "$*" >&2
  return 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --as-of)
      AS_OF="${2:-}"
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

[[ "$AS_OF" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || fail "Invalid --as-of date: $AS_OF"

SOURCE_ID="fixture_de_netex"
COUNTRY="DE"
PROVIDER="fixture_provider"
SNAPSHOT_DIR="${ROOT_DIR}/data/raw/${COUNTRY}/${PROVIDER}/netex/${AS_OF}"
MANIFEST_PATH="${SNAPSHOT_DIR}/manifest.json"

mkdir -p "$SNAPSHOT_DIR"
cat > "$MANIFEST_PATH" <<JSON
{
  "sourceId": "$SOURCE_ID",
  "country": "$COUNTRY",
  "format": "netex",
  "retrievalTimestamp": "${AS_OF}T00:00:00Z",
  "sha256": "fixture",
  "fileName": "fixture.zip"
}
JSON

printf 'PK\003\004' > "${SNAPSHOT_DIR}/fixture.zip"

db_load_env
db_resolve_connection
db_ensure_ready

"${ROOT_DIR}/scripts/data/db-bootstrap.sh" --quiet

db_psql -v as_of="$AS_OF" <<'SQL'
INSERT INTO import_runs (run_id, pipeline, status, source_id, country, snapshot_date, started_at, ended_at)
VALUES
  ('11111111-1111-1111-1111-111111111111'::uuid, 'netex_ingest', 'succeeded', 'fixture_de_netex', 'DE', :'as_of'::date, now(), now()),
  ('22222222-2222-2222-2222-222222222222'::uuid, 'canonical_build', 'succeeded', NULL, 'DE', :'as_of'::date, now(), now())
ON CONFLICT (run_id) DO UPDATE
SET status = EXCLUDED.status,
    source_id = EXCLUDED.source_id,
    country = EXCLUDED.country,
    snapshot_date = EXCLUDED.snapshot_date,
    ended_at = now();

INSERT INTO raw_snapshots (
  source_id,
  country,
  provider_slug,
  format,
  snapshot_date,
  manifest_path,
  manifest_sha256,
  manifest,
  file_name,
  file_size_bytes,
  retrieval_timestamp,
  requested_as_of,
  resolved_download_url,
  detected_version_or_date
)
VALUES (
  'fixture_de_netex',
  'DE',
  'fixture_provider',
  'netex',
  :'as_of'::date,
  'data/raw/DE/fixture_provider/netex/' || :'as_of' || '/manifest.json',
  'fixture',
  '{}'::jsonb,
  'fixture.zip',
  4,
  now(),
  :'as_of'::date,
  'https://example.invalid/fixture.zip',
  :'as_of'
)
ON CONFLICT (source_id, snapshot_date) DO UPDATE
SET
  country = EXCLUDED.country,
  provider_slug = EXCLUDED.provider_slug,
  manifest_path = EXCLUDED.manifest_path,
  manifest_sha256 = EXCLUDED.manifest_sha256,
  file_name = EXCLUDED.file_name,
  file_size_bytes = EXCLUDED.file_size_bytes,
  retrieval_timestamp = EXCLUDED.retrieval_timestamp,
  requested_as_of = EXCLUDED.requested_as_of,
  resolved_download_url = EXCLUDED.resolved_download_url,
  detected_version_or_date = EXCLUDED.detected_version_or_date,
  updated_at = now();

INSERT INTO netex_stops_staging (
  import_run_id,
  source_id,
  country,
  provider_slug,
  snapshot_date,
  manifest_sha256,
  source_stop_id,
  stop_name,
  latitude,
  longitude,
  grid_id,
  source_file,
  raw_payload
)
VALUES
  ('11111111-1111-1111-1111-111111111111'::uuid, 'fixture_de_netex', 'DE', 'fixture_provider', :'as_of'::date, 'fixture', 'fixture_stop_a', 'Fixture Hbf', 52.520008, 13.404954, compute_geo_grid_id('DE', 52.520008, 13.404954, NULL::geometry), 'fixture.xml', '{}'::jsonb),
  ('11111111-1111-1111-1111-111111111111'::uuid, 'fixture_de_netex', 'DE', 'fixture_provider', :'as_of'::date, 'fixture', 'fixture_stop_b', 'Fixture Ost', 52.515000, 13.454000, compute_geo_grid_id('DE', 52.515000, 13.454000, NULL::geometry), 'fixture.xml', '{}'::jsonb)
ON CONFLICT (grid_id, source_id, snapshot_date, source_stop_id) DO UPDATE
SET
  stop_name = EXCLUDED.stop_name,
  latitude = EXCLUDED.latitude,
  longitude = EXCLUDED.longitude,
  updated_at = now();

INSERT INTO canonical_stations (
  canonical_station_id,
  canonical_name,
  normalized_name,
  country,
  latitude,
  longitude,
  geom,
  grid_id,
  match_method,
  member_count,
  first_seen_snapshot_date,
  last_seen_snapshot_date,
  last_built_run_id,
  updated_at
)
VALUES
  (
    'cstn_fixture_de_a',
    'Fixture Hbf',
    normalize_station_name('Fixture Hbf'),
    'DE',
    52.520008,
    13.404954,
    ST_SetSRID(ST_MakePoint(13.404954, 52.520008), 4326),
    compute_geo_grid_id('DE', 52.520008, 13.404954, ST_SetSRID(ST_MakePoint(13.404954, 52.520008), 4326)),
    'hard_id',
    1,
    :'as_of'::date,
    :'as_of'::date,
    '22222222-2222-2222-2222-222222222222'::uuid,
    now()
  ),
  (
    'cstn_fixture_de_b',
    'Fixture Ost',
    normalize_station_name('Fixture Ost'),
    'DE',
    52.515000,
    13.454000,
    ST_SetSRID(ST_MakePoint(13.454000, 52.515000), 4326),
    compute_geo_grid_id('DE', 52.515000, 13.454000, ST_SetSRID(ST_MakePoint(13.454000, 52.515000), 4326)),
    'hard_id',
    1,
    :'as_of'::date,
    :'as_of'::date,
    '22222222-2222-2222-2222-222222222222'::uuid,
    now()
  )
ON CONFLICT (grid_id, canonical_station_id) DO UPDATE
SET
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
VALUES
  ('cstn_fixture_de_a', 'fixture_de_netex', 'fixture_stop_a', 'DE', :'as_of'::date, 'hard_id', 'fixture_a', '11111111-1111-1111-1111-111111111111'::uuid, now()),
  ('cstn_fixture_de_b', 'fixture_de_netex', 'fixture_stop_b', 'DE', :'as_of'::date, 'hard_id', 'fixture_b', '11111111-1111-1111-1111-111111111111'::uuid, now())
ON CONFLICT (source_id, source_stop_id) DO UPDATE
SET
  canonical_station_id = EXCLUDED.canonical_station_id,
  country = EXCLUDED.country,
  snapshot_date = EXCLUDED.snapshot_date,
  match_method = EXCLUDED.match_method,
  hard_id = EXCLUDED.hard_id,
  import_run_id = EXCLUDED.import_run_id,
  updated_at = now();
SQL

printf '[seed-export-fixture] Seeded fixture canonical rows for as-of %s\n' "$AS_OF"
