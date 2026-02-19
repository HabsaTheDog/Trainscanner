#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
MIGRATIONS_DIR="${ROOT_DIR}/db/migrations"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib-db.sh"

QUIET="false"

usage() {
  cat <<USAGE
Usage: scripts/data/db-migrate.sh [options]

Apply SQL migrations for the DACH canonical PostGIS pipeline.

Options:
  --quiet      Reduce log output
  -h, --help   Show this help
USAGE
}

log() {
  if [[ "$QUIET" != "true" ]]; then
    printf '[db-migrate] %s\n' "$*"
  fi
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
      printf '[db-migrate] ERROR: Unknown argument: %s\n' "$1" >&2
      exit 1
      ;;
  esac
done

[[ -d "$MIGRATIONS_DIR" ]] || {
  printf '[db-migrate] ERROR: migrations directory not found: %s\n' "$MIGRATIONS_DIR" >&2
  exit 1
}

db_load_env
db_resolve_connection
db_ensure_ready

log "Ensuring schema_migrations table exists"
db_psql -c "
CREATE TABLE IF NOT EXISTS schema_migrations (
  version text PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
);
"

mapfile -t migration_files < <(find "$MIGRATIONS_DIR" -maxdepth 1 -type f -name '*.sql' | sort)

if [[ ${#migration_files[@]} -eq 0 ]]; then
  printf '[db-migrate] ERROR: no migration files found under %s\n' "$MIGRATIONS_DIR" >&2
  exit 1
fi

for file in "${migration_files[@]}"; do
  version="$(basename "$file")"
  version_esc="$(db_sql_escape "$version")"
  already_applied="$(db_psql -At -c "SELECT 1 FROM schema_migrations WHERE version = '${version_esc}' LIMIT 1;")"
  if [[ "$already_applied" == "1" ]]; then
    log "Skipping already applied migration: $version"
    continue
  fi

  log "Applying migration: $version"
  if [[ "$DB_MODE_EFFECTIVE" == "docker-compose" ]]; then
    db_psql < "$file"
  else
    db_psql -f "$file"
  fi
  db_psql -c "INSERT INTO schema_migrations (version) VALUES ('${version_esc}');"
done

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
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'pipeline_jobs') AS has_pipeline_jobs;
")"

IFS='|' read -r has_postgis has_import_runs has_raw_snapshots has_staging has_canonical has_mappings has_review_queue has_overrides has_transfer_rules has_ojp_refs has_pipeline_jobs <<<"$validation"

if [[ "$has_postgis" != "1" || "$has_import_runs" != "1" || "$has_raw_snapshots" != "1" || "$has_staging" != "1" || "$has_canonical" != "1" || "$has_mappings" != "1" || "$has_review_queue" != "1" || "$has_overrides" != "1" || "$has_transfer_rules" != "1" || "$has_ojp_refs" != "1" || "$has_pipeline_jobs" != "1" ]]; then
  printf '[db-migrate] ERROR: validation failed (postgis=%s import_runs=%s raw_snapshots=%s staging=%s canonical=%s mappings=%s review_queue=%s overrides=%s transfer_rules=%s ojp_refs=%s pipeline_jobs=%s)\n' \
    "$has_postgis" "$has_import_runs" "$has_raw_snapshots" "$has_staging" "$has_canonical" "$has_mappings" "$has_review_queue" "$has_overrides" "$has_transfer_rules" "$has_ojp_refs" "$has_pipeline_jobs" >&2
  exit 1
fi

log "Migrations complete"
