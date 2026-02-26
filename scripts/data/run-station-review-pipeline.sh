#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

SKIP_MIGRATE="false"
DRY_RUN="false"
RUN_ID=""
STEP_SUMMARY=()
CURRENT_STEP=""
CURRENT_CMD=""
PIPELINE_STARTED_AT="$(date +%s)"
REFRESH_ARGS=()

timestamp_utc() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
  return 0
}

log() {
  printf '[station-review-pipeline] %s %s\n' "$(timestamp_utc)" "$*"
  return 0
}

fail() {
  log "ERROR: $*"
  exit 1
}

usage() {
  cat <<USAGE
Usage: scripts/data/run-station-review-pipeline.sh [options]

Run a full station-review data pipeline in one command:
  1) apply DB migrations
  2) fetch latest sources
  3) ingest NeTEx snapshots
  4) build canonical stations
  5) build review queue clusters

Options:
  --country DE|AT|CH          Restrict refresh scope to one country (default: all DACH)
  --as-of YYYY-MM-DD          Snapshot date override for refresh stages
  --source-id <id>            Restrict fetch/ingest/canonical to one source id
  --only <list>               Comma-separated steps: fetch,ingest,canonical,review-queue
  --from-step <step>          Start from step: fetch|ingest|canonical|review-queue
  --to-step <step>            Stop after step: fetch|ingest|canonical|review-queue
  --skip-fetch                Skip fetch step
  --skip-ingest               Skip ingest step
  --skip-canonical            Skip canonical build step
  --skip-review-queue         Skip review queue build step
  --skip-migrate              Skip DB migrations
  --run-id <id>               Optional run id prefix for refresh stage logs
  --dry-run                   Show execution plan without mutating data
  -h, --help                  Show this help

Examples:
  scripts/data/run-station-review-pipeline.sh
  scripts/data/run-station-review-pipeline.sh --country CH
  scripts/data/run-station-review-pipeline.sh --country DE --as-of 2026-02-20
  scripts/data/run-station-review-pipeline.sh --skip-migrate --from-step canonical
USAGE
  return 0
}

record_step() {
  local name="$1"
  local status="$2"
  local elapsed_sec="$3"
  STEP_SUMMARY+=("${name}|${status}|${elapsed_sec}")
  return 0
}

print_summary() {
  local total_elapsed
  total_elapsed="$(( $(date +%s) - PIPELINE_STARTED_AT ))"

  log "Execution summary:"
  for row in "${STEP_SUMMARY[@]}"; do
    local name status elapsed
    IFS='|' read -r name status elapsed <<<"$row"
    printf '  - %-20s status=%-8s elapsed=%ss\n' "$name" "$status" "$elapsed"
  done
  log "Total elapsed: ${total_elapsed}s"
  return 0
}

on_error() {
  local exit_code="$?"
  local line_no="${1:-unknown}"

  if [[ -n "$CURRENT_STEP" ]]; then
    record_step "$CURRENT_STEP" "failed" "?"
  fi

  log "FAILED at line ${line_no} (exit=${exit_code})"
  if [[ -n "$CURRENT_STEP" ]]; then
    log "Failed step: ${CURRENT_STEP}"
  fi
  if [[ -n "$CURRENT_CMD" ]]; then
    log "Failed command: ${CURRENT_CMD}"
  fi

  if [[ "$CURRENT_STEP" == "db-migrate" ]]; then
    log "Hint: verify PostGIS is reachable and CANONICAL_DB_* env values are correct."
  fi

  if [[ "$CURRENT_STEP" == "refresh-station-review" ]]; then
    log "Hint: fetch failures on DE usually mean missing DELFI auth env vars in .env.local (or .env)."
    log "Hint: set DE_DELFI_SOLLFAHRPLANDATEN_NETEX_USERNAME and ..._PASSWORD (or cookie/header alternatives)."
  fi

  print_summary
  exit "$exit_code"
}

trap 'on_error $LINENO' ERR

run_step() {
  local step_name="$1"
  shift

  local step_started step_elapsed
  step_started="$(date +%s)"
  CURRENT_STEP="$step_name"
  CURRENT_CMD="$*"

  log "START step=${step_name}"
  log "CMD   ${CURRENT_CMD}"
  "$@"
  step_elapsed="$(( $(date +%s) - step_started ))"

  record_step "$step_name" "ok" "$step_elapsed"
  log "DONE  step=${step_name} elapsed=${step_elapsed}s"
  CURRENT_STEP=""
  CURRENT_CMD=""
  return 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --country|--as-of|--source-id|--only|--from-step|--to-step|--run-id)
      [[ $# -ge 2 ]] || fail "Missing value for $1"
      if [[ "$1" == "--run-id" ]]; then
        RUN_ID="$2"
      else
        REFRESH_ARGS+=("$1" "$2")
      fi
      shift 2
      ;;
    --skip-fetch|--skip-ingest|--skip-canonical|--skip-review-queue)
      REFRESH_ARGS+=("$1")
      shift
      ;;
    --skip-migrate)
      SKIP_MIGRATE="true"
      shift
      ;;
    --dry-run)
      DRY_RUN="true"
      REFRESH_ARGS+=("--dry-run")
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

if [[ -z "$RUN_ID" ]]; then
  RUN_ID="station-review-$(date -u +%Y%m%dT%H%M%SZ)"
fi

REFRESH_ARGS+=("--run-id" "$RUN_ID" "--root" "$ROOT_DIR")

log "Root directory: ${ROOT_DIR}"
log "Run ID: ${RUN_ID}"
log "Migrate step: ${SKIP_MIGRATE}"
log "Dry run: ${DRY_RUN}"
log "Refresh args: ${REFRESH_ARGS[*]}"

if [[ "$DRY_RUN" == "true" ]]; then
  if [[ "$SKIP_MIGRATE" == "false" ]]; then
    record_step "db-migrate" "skipped" "0"
    log "DRY-RUN step=db-migrate command=${ROOT_DIR}/scripts/data/db-migrate.sh"
  fi
  run_step "refresh-station-review" "${ROOT_DIR}/scripts/data/refresh-station-review.sh" "${REFRESH_ARGS[@]}"
  print_summary
  exit 0
fi

if [[ "$SKIP_MIGRATE" == "false" ]]; then
  run_step "db-migrate" "${ROOT_DIR}/scripts/data/db-migrate.sh"
else
  record_step "db-migrate" "skipped" "0"
  log "SKIP step=db-migrate (--skip-migrate)"
fi

run_step "refresh-station-review" "${ROOT_DIR}/scripts/data/refresh-station-review.sh" "${REFRESH_ARGS[@]}"

print_summary
log "Station review data pipeline completed successfully."
log "Next: open http://localhost:3000/curation.html or query /api/qa/v2/clusters."
