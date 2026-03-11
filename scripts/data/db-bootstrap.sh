#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SCHEMA_FILE="${ROOT_DIR}/db/schema.sql"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib-db.sh"

QUIET="false"
IF_READY="false"

usage() {
  cat <<USAGE
Usage: scripts/data/db-bootstrap.sh [options]

Bootstrap the pan-European PostGIS routing schema.

Options:
  --quiet        Reduce log output
  --if-ready     Skip schema apply when required objects exist and schema hash matches
  -h, --help     Show this help
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
    --if-ready)
      IF_READY="true"
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

if command -v sha256sum >/dev/null 2>&1; then
  SCHEMA_SHA="$(sha256sum "$SCHEMA_FILE" | awk '{print $1}')"
elif command -v shasum >/dev/null 2>&1; then
  SCHEMA_SHA="$(shasum -a 256 "$SCHEMA_FILE" | awk '{print $1}')"
else
  printf '[db-bootstrap] ERROR: sha256sum or shasum is required\n' >&2
  exit 1
fi

if [[ "$IF_READY" == "true" ]]; then
  has_system_state="$(db_psql -At -c "SELECT CASE WHEN to_regclass('public.system_state') IS NULL THEN 0 ELSE 1 END;" | tr -d '[:space:]' | head -n 1)"
  if [[ "$has_system_state" == "1" ]]; then
    existing_schema_sha="$(db_psql -At -c "SELECT COALESCE(value ->> 'schemaSha256', '') FROM system_state WHERE key = 'schema_bootstrap' LIMIT 1;" | tr -d '[:space:]' | head -n 1)"
    if [[ "$existing_schema_sha" == "$SCHEMA_SHA" ]]; then
      table_check="$(db_psql -At -c "
SELECT
  (SELECT COUNT(*) FROM pg_extension WHERE extname = 'postgis') AS has_postgis,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'pipeline_jobs') AS has_pipeline_jobs,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'system_state') AS has_system_state,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'provider_datasets') AS has_provider_datasets,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'raw_provider_stop_places') AS has_stop_places,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'raw_provider_stop_points') AS has_stop_points,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'global_stations') AS has_global_stations,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'global_stop_points') AS has_global_stop_points,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'provider_global_station_mappings') AS has_station_mappings,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'provider_global_stop_point_mappings') AS has_stop_point_mappings,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'timetable_trips') AS has_timetable_trips,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'timetable_trip_stop_times') AS has_timetable_stop_times,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'transfer_edges') AS has_transfer_edges,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_merge_clusters') AS has_merge_clusters,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_merge_cluster_candidates') AS has_merge_candidates,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_merge_cluster_evidence') AS has_merge_evidence,
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'raw_provider_stop_places' AND column_name = 'topographic_place_ref') AS has_stop_places_topographic,
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'raw_provider_stop_points' AND column_name = 'topographic_place_ref') AS has_stop_points_topographic,
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'qa_merge_cluster_evidence' AND column_name = 'status') AS has_merge_evidence_status,
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'qa_merge_cluster_evidence' AND column_name = 'raw_value') AS has_merge_evidence_raw_value,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_merge_decisions') AS has_merge_decisions,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_merge_decision_members') AS has_merge_decision_members,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_merge_cluster_workspaces') AS has_merge_workspaces,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_merge_cluster_workspace_versions') AS has_merge_workspace_versions;
")"
      if [[ "$table_check" == "1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1" ]]; then
        log "Schema already ready (matching hash); skipping apply"
      else
        log "Schema hash matches but required objects missing; forcing apply"
      fi
    fi
  fi
fi

if [[ "${table_check:-}" != "1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1" ]]; then
  log "Applying baseline schema"
  if [[ "$DB_MODE_EFFECTIVE" == "docker-compose" ]]; then
    db_psql < "$SCHEMA_FILE"
  else
    db_psql -f "$SCHEMA_FILE"
  fi
fi

log "Validating required PostGIS objects"
validation="$(db_psql -At -c "
SELECT
  (SELECT COUNT(*) FROM pg_extension WHERE extname = 'postgis') AS has_postgis,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'pipeline_jobs') AS has_pipeline_jobs,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'system_state') AS has_system_state,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'provider_datasets') AS has_provider_datasets,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'raw_provider_stop_places') AS has_stop_places,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'raw_provider_stop_points') AS has_stop_points,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'global_stations') AS has_global_stations,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'global_stop_points') AS has_global_stop_points,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'provider_global_station_mappings') AS has_station_mappings,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'provider_global_stop_point_mappings') AS has_stop_point_mappings,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'timetable_trips') AS has_timetable_trips,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'timetable_trip_stop_times') AS has_timetable_stop_times,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'transfer_edges') AS has_transfer_edges,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_merge_clusters') AS has_merge_clusters,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_merge_cluster_candidates') AS has_merge_candidates,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_merge_cluster_evidence') AS has_merge_evidence,
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'raw_provider_stop_places' AND column_name = 'topographic_place_ref') AS has_stop_places_topographic,
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'raw_provider_stop_points' AND column_name = 'topographic_place_ref') AS has_stop_points_topographic,
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'qa_merge_cluster_evidence' AND column_name = 'status') AS has_merge_evidence_status,
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'qa_merge_cluster_evidence' AND column_name = 'raw_value') AS has_merge_evidence_raw_value,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_merge_decisions') AS has_merge_decisions,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_merge_decision_members') AS has_merge_decision_members,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_merge_cluster_workspaces') AS has_merge_workspaces,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'qa_merge_cluster_workspace_versions') AS has_merge_workspace_versions;
")"

IFS='|' read -r has_postgis has_pipeline_jobs has_system_state has_provider_datasets has_stop_places has_stop_points has_global_stations has_global_stop_points has_station_mappings has_stop_point_mappings has_timetable_trips has_timetable_stop_times has_transfer_edges has_merge_clusters has_merge_candidates has_merge_evidence has_stop_places_topographic has_stop_points_topographic has_merge_evidence_status has_merge_evidence_raw_value has_merge_decisions has_merge_decision_members has_merge_workspaces has_merge_workspace_versions <<<"$validation"

if [[ "$has_postgis" != "1" || "$has_pipeline_jobs" != "1" || "$has_system_state" != "1" || "$has_provider_datasets" != "1" || "$has_stop_places" != "1" || "$has_stop_points" != "1" || "$has_global_stations" != "1" || "$has_global_stop_points" != "1" || "$has_station_mappings" != "1" || "$has_stop_point_mappings" != "1" || "$has_timetable_trips" != "1" || "$has_timetable_stop_times" != "1" || "$has_transfer_edges" != "1" || "$has_merge_clusters" != "1" || "$has_merge_candidates" != "1" || "$has_merge_evidence" != "1" || "$has_stop_places_topographic" != "1" || "$has_stop_points_topographic" != "1" || "$has_merge_evidence_status" != "1" || "$has_merge_evidence_raw_value" != "1" || "$has_merge_decisions" != "1" || "$has_merge_decision_members" != "1" || "$has_merge_workspaces" != "1" || "$has_merge_workspace_versions" != "1" ]]; then
  printf '[db-bootstrap] ERROR: validation failed (postgis=%s pipeline_jobs=%s system_state=%s provider_datasets=%s stop_places=%s stop_points=%s global_stations=%s global_stop_points=%s station_mappings=%s stop_point_mappings=%s timetable_trips=%s timetable_stop_times=%s transfer_edges=%s merge_clusters=%s merge_candidates=%s merge_evidence=%s stop_places_topographic=%s stop_points_topographic=%s merge_evidence_status=%s merge_evidence_raw_value=%s merge_decisions=%s merge_decision_members=%s merge_workspaces=%s merge_workspace_versions=%s)\n' \
    "$has_postgis" "$has_pipeline_jobs" "$has_system_state" "$has_provider_datasets" "$has_stop_places" "$has_stop_points" "$has_global_stations" "$has_global_stop_points" "$has_station_mappings" "$has_stop_point_mappings" "$has_timetable_trips" "$has_timetable_stop_times" "$has_transfer_edges" "$has_merge_clusters" "$has_merge_candidates" "$has_merge_evidence" "$has_stop_places_topographic" "$has_stop_points_topographic" "$has_merge_evidence_status" "$has_merge_evidence_raw_value" "$has_merge_decisions" "$has_merge_decision_members" "$has_merge_workspaces" "$has_merge_workspace_versions" >&2
  exit 1
fi

db_psql -c "
INSERT INTO system_state (key, value, updated_at)
VALUES (
  'schema_bootstrap',
  jsonb_build_object('schemaSha256', '${SCHEMA_SHA}', 'appliedAt', now()::text),
  now()
)
ON CONFLICT (key) DO UPDATE
SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at;
" >/dev/null

log "Schema bootstrap complete"
