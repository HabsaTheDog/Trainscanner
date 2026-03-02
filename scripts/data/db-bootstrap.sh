#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SCHEMA_FILE="${ROOT_DIR}/db/schema.sql"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib-db.sh"

QUIET="false"

usage() {
  cat <<USAGE
Usage: scripts/data/db-bootstrap.sh [options]

Bootstrap the current PostGIS schema for the DACH canonical pipeline.

Options:
  --quiet      Reduce log output
  -h, --help   Show this help
USAGE
  return 0
}

log() {
  if [[ "$QUIET" != "true" ]]; then
    printf '[db-bootstrap] %s\n' "$*"
  fi
  return 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --quiet)
      QUIET="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf '[db-bootstrap] ERROR: Unknown argument: %s\n' "$1" >&2
      exit 1
      ;;
  esac
done

[[ -f "$SCHEMA_FILE" ]] || {
  printf '[db-bootstrap] ERROR: schema file not found: %s\n' "$SCHEMA_FILE" >&2
  exit 1
}

db_load_env
db_resolve_connection
db_ensure_ready

log "Applying baseline schema"
if [[ "$DB_MODE_EFFECTIVE" == "docker-compose" ]]; then
  db_psql < "$SCHEMA_FILE"
else
  db_psql -f "$SCHEMA_FILE"
fi

log "Validating required PostGIS objects"
validation="$(db_psql -At -c "
SELECT
  (SELECT COUNT(*) FROM pg_extension WHERE extname = 'postgis') AS has_postgis,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'import_runs') AS has_import_runs,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'raw_snapshots') AS has_raw_snapshots,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'netex_stops_staging') AS has_staging,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'canonical_stations') AS has_canonical,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'canonical_station_sources') AS has_mappings,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'canonical_review_queue') AS has_review_queue,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'canonical_station_overrides') AS has_overrides,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'station_transfer_rules') AS has_transfer_rules,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'ojp_stop_refs') AS has_ojp_refs,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'pipeline_jobs') AS has_pipeline_jobs,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_station_clusters') AS has_clusters,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_station_cluster_candidates') AS has_cluster_candidates,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_station_cluster_decisions') AS has_cluster_decisions,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_station_segments') AS has_segments,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'canonical_line_identities') AS has_lines,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_station_groups') AS has_groups,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_curated_stations') AS has_curated_stations,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_curated_station_members') AS has_curated_members,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_curated_station_lineage') AS has_curated_lineage,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_curated_station_field_provenance') AS has_curated_field_provenance;
")"

IFS='|' read -r has_postgis has_import_runs has_raw_snapshots has_staging has_canonical has_mappings has_review_queue has_overrides has_transfer_rules has_ojp_refs has_pipeline_jobs has_clusters has_cluster_candidates has_cluster_decisions has_segments has_lines has_groups has_curated_stations has_curated_members has_curated_lineage has_curated_field_provenance <<<"$validation"

if [[ "$has_postgis" != "1" || "$has_import_runs" != "1" || "$has_raw_snapshots" != "1" || "$has_staging" != "1" || "$has_canonical" != "1" || "$has_mappings" != "1" || "$has_review_queue" != "1" || "$has_overrides" != "1" || "$has_transfer_rules" != "1" || "$has_ojp_refs" != "1" || "$has_pipeline_jobs" != "1" || "$has_clusters" != "1" || "$has_cluster_candidates" != "1" || "$has_cluster_decisions" != "1" || "$has_segments" != "1" || "$has_lines" != "1" || "$has_groups" != "1" || "$has_curated_stations" != "1" || "$has_curated_members" != "1" || "$has_curated_lineage" != "1" || "$has_curated_field_provenance" != "1" ]]; then
  printf '[db-bootstrap] ERROR: validation failed (postgis=%s import_runs=%s raw_snapshots=%s staging=%s canonical=%s mappings=%s review_queue=%s overrides=%s transfer_rules=%s ojp_refs=%s pipeline_jobs=%s clusters=%s cluster_candidates=%s cluster_decisions=%s segments=%s lines=%s groups=%s curated_stations=%s curated_members=%s curated_lineage=%s curated_field_provenance=%s)\n' \
    "$has_postgis" "$has_import_runs" "$has_raw_snapshots" "$has_staging" "$has_canonical" "$has_mappings" "$has_review_queue" "$has_overrides" "$has_transfer_rules" "$has_ojp_refs" "$has_pipeline_jobs" "$has_clusters" "$has_cluster_candidates" "$has_cluster_decisions" "$has_segments" "$has_lines" "$has_groups" "$has_curated_stations" "$has_curated_members" "$has_curated_lineage" "$has_curated_field_provenance" >&2
  exit 1
fi

log "Schema bootstrap complete"
