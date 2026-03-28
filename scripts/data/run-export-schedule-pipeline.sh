#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

SKIP_DB_BOOTSTRAP="false"
DRY_RUN="false"
SKIP_FETCH="false"
SKIP_INGEST="false"
COMMON_ARGS=()
STEP_SUMMARY=()
CURRENT_STEP=""
CURRENT_CMD=""
PIPELINE_STARTED_AT="$(date +%s)"

timestamp_utc() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
  return 0
}

log() {
  printf '[export-schedule-pipeline] %s %s\n' "$(timestamp_utc)" "$*"
  return 0
}

fail() {
  log "ERROR: $*"
  return 1
}

usage() {
  cat <<USAGE
Usage: scripts/data/run-export-schedule-pipeline.sh [options]

Run the export/routing schedule pipeline:
  1) bootstrap the current DB schema
  2) fetch latest sources
  3) ingest timetable/export schedule data only

Options:
  --country <ISO2>       Restrict fetch/ingest scope to one country
  --as-of YYYY-MM-DD     Snapshot date override for fetch/ingest
  --source-id <id>       Restrict fetch/ingest scope to one source id
  --skip-fetch           Skip source fetch
  --skip-ingest          Skip export-schedule ingest
  --skip-db-bootstrap    Skip DB schema bootstrap
  --dry-run              Show execution plan without mutating data
  -h, --help             Show this help
USAGE
  return 0
}

record_step() {
  STEP_SUMMARY+=("$1|$2|$3")
  return 0
}

print_summary() {
  local total_elapsed
  total_elapsed="$(( $(date +%s) - PIPELINE_STARTED_AT ))"
  log "Execution summary:"
  local row
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

  print_summary
  return "$exit_code"
}

trap 'on_error "$LINENO" || exit $?' ERR

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

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --country|--as-of|--source-id)
        [[ $# -ge 2 ]] || fail "Missing value for $1"
        COMMON_ARGS+=("$1" "$2")
        shift 2
        ;;
      --skip-fetch)
        SKIP_FETCH="true"
        shift
        ;;
      --skip-ingest)
        SKIP_INGEST="true"
        shift
        ;;
      --skip-db-bootstrap)
        SKIP_DB_BOOTSTRAP="true"
        shift
        ;;
      --dry-run)
        DRY_RUN="true"
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
  return 0
}

main() {
  parse_args "$@"

  log "Root directory: ${ROOT_DIR}"
  log "Dry run: ${DRY_RUN}"
  log "Shared args: ${COMMON_ARGS[*]:-(none)}"

  if [[ "$DRY_RUN" == "true" ]]; then
    if [[ "$SKIP_DB_BOOTSTRAP" == "false" ]]; then
      record_step "db-bootstrap" "skipped" "0"
    fi
    if [[ "$SKIP_FETCH" == "false" ]]; then
      record_step "fetch" "skipped" "0"
    fi
    if [[ "$SKIP_INGEST" == "false" ]]; then
      record_step "export-schedule" "skipped" "0"
    fi
    print_summary
    exit 0
  fi

  if [[ "$SKIP_DB_BOOTSTRAP" == "false" ]]; then
    run_step "db-bootstrap" "${ROOT_DIR}/scripts/data/db-bootstrap.sh"
  else
    record_step "db-bootstrap" "skipped" "0"
  fi

  if [[ "$SKIP_FETCH" == "false" ]]; then
    run_step "fetch" "${ROOT_DIR}/scripts/data/fetch-sources.sh" "${COMMON_ARGS[@]}"
  else
    record_step "fetch" "skipped" "0"
  fi

  if [[ "$SKIP_INGEST" == "false" ]]; then
    run_step "export-schedule" "${ROOT_DIR}/scripts/data/ingest-netex.sh" --mode export-schedule "${COMMON_ARGS[@]}"
  else
    record_step "export-schedule" "skipped" "0"
  fi

  print_summary
  log "Export schedule pipeline completed successfully."
}

main "$@"
