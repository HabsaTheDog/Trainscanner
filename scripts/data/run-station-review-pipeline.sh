#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

SKIP_DB_BOOTSTRAP="false"
DRY_RUN="false"
RUN_QA_AUDIT="true"
RUN_ID=""
PROFILE=""
STEP_SUMMARY=()
CURRENT_STEP=""
CURRENT_CMD=""
PIPELINE_STARTED_AT="$(date +%s)"
STEP_DB_BOOTSTRAP="db-bootstrap"

STEP_IDS=(
  "fetch"
  "stop-topology"
  "qa-network-context"
  "global-stations"
  "reference-data"
  "qa-network-projection"
  "merge-queue"
)

SELECTED_STEPS=()
SKIPPED_STEPS=()
COMMON_ARGS=()

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
  return 1
}

usage() {
  cat <<USAGE
Usage: scripts/data/run-station-review-pipeline.sh [options]

Run the QA-only station-review pipeline:
  1) bootstrap the current DB schema
  2) fetch latest sources
  3) ingest stop topology only
  4) extract compact QA network context
  5) build pan-European global stations
  6) import external reviewer reference data
  7) project QA context onto global stations
  8) build global merge queue clusters

Options:
  --country <ISO2>                 Restrict fetch/ingest scope to one country
  --as-of YYYY-MM-DD               Snapshot date override for refresh stages
  --source-id <id>                 Restrict fetch/ingest scope to one source id
  --profile <name>                 One of: country-fast, europe-fast, merge-only, references-refresh
  --only <list>                    Comma-separated steps:
                                   fetch,stop-topology,qa-network-context,global-stations,reference-data,qa-network-projection,merge-queue
  --from-step <step>               Start from a step
  --to-step <step>                 Stop after a step
  --skip-fetch                     Skip fetch step
  --skip-stop-topology             Skip stop-topology ingest
  --skip-qa-network-context        Skip compact QA context extract
  --skip-global-stations           Skip global station build step
  --skip-reference-data            Skip external reference import/match step
  --skip-qa-network-projection     Skip QA context projection step
  --skip-merge-queue               Skip global merge queue build step
  --skip-qa-audit                 Skip post-merge QA structural audit
  --skip-db-bootstrap              Skip DB schema bootstrap
  --run-id <id>                    Optional run id prefix for logs
  --dry-run                        Show execution plan without mutating data
  -h, --help                       Show this help
USAGE
  return 0
}

apply_profile() {
  case "$PROFILE" in
    "")
      return 0
      ;;
    country-fast|europe-fast)
      SELECTED_STEPS=(
        "stop-topology"
        "qa-network-context"
        "global-stations"
        "qa-network-projection"
        "merge-queue"
      )
      ;;
    merge-only)
      SELECTED_STEPS=("merge-queue")
      ;;
    references-refresh)
      SELECTED_STEPS=("reference-data")
      ;;
    *)
      fail "Unknown --profile '$PROFILE'"
      ;;
  esac
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
  local row
  for row in "${STEP_SUMMARY[@]}"; do
    local name status elapsed
    IFS='|' read -r name status elapsed <<<"$row"
    printf '  - %-24s status=%-8s elapsed=%ss\n' "$name" "$status" "$elapsed"
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

normalize_step_id() {
  case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
    fetch)
      printf 'fetch\n'
      ;;
    ingest|stop-topology|stop_topology|topology)
      printf 'stop-topology\n'
      ;;
    qa-network-context|qa_network_context|network-context|network_context|qa-context)
      printf 'qa-network-context\n'
      ;;
    global-stations|global_stations|global|stations)
      printf 'global-stations\n'
      ;;
    reference-data|reference_data|reference|references)
      printf 'reference-data\n'
      ;;
    qa-network-projection|qa_network_projection|projection|project)
      printf 'qa-network-projection\n'
      ;;
    merge-queue|merge_queue|merge|queue|review)
      printf 'merge-queue\n'
      ;;
    *)
      printf '\n'
      ;;
  esac
}

require_step_id() {
  local resolved
  resolved="$(normalize_step_id "$1")"
  [[ -n "$resolved" ]] || fail "Unknown step '$1'"
  printf '%s\n' "$resolved"
  return 0
}

select_default_steps() {
  SELECTED_STEPS=("${STEP_IDS[@]}")
  return 0
}

remove_selected_step() {
  local target="$1"
  local next=()
  local step
  for step in "${SELECTED_STEPS[@]}"; do
    if [[ "$step" != "$target" ]]; then
      next+=("$step")
    fi
  done
  SELECTED_STEPS=("${next[@]}")
  return 0
}

apply_only_steps() {
  local raw_list="$1"
  local only=()
  local token resolved step
  IFS=',' read -r -a tokens <<<"$raw_list"
  for token in "${tokens[@]}"; do
    resolved="$(require_step_id "$token")"
    only+=("$resolved")
  done

  local filtered=()
  for step in "${STEP_IDS[@]}"; do
    local candidate
    for candidate in "${only[@]}"; do
      if [[ "$step" == "$candidate" ]]; then
        filtered+=("$step")
        break
      fi
    done
  done
  SELECTED_STEPS=("${filtered[@]}")
  return 0
}

apply_step_range() {
  local from_step="$1"
  local to_step="$2"
  local from_index=-1
  local to_index=-1
  local idx

  if [[ -n "$from_step" ]]; then
    for idx in "${!STEP_IDS[@]}"; do
      [[ "${STEP_IDS[$idx]}" == "$from_step" ]] && from_index="$idx"
    done
  fi
  if [[ -n "$to_step" ]]; then
    for idx in "${!STEP_IDS[@]}"; do
      [[ "${STEP_IDS[$idx]}" == "$to_step" ]] && to_index="$idx"
    done
  fi

  local filtered=()
  for idx in "${!SELECTED_STEPS[@]}"; do
    local step="${SELECTED_STEPS[$idx]}"
    local order_index
    for order_index in "${!STEP_IDS[@]}"; do
      if [[ "${STEP_IDS[$order_index]}" == "$step" ]]; then
        if [[ "$from_index" -ge 0 && "$order_index" -lt "$from_index" ]]; then
          break
        fi
        if [[ "$to_index" -ge 0 && "$order_index" -gt "$to_index" ]]; then
          break
        fi
        filtered+=("$step")
        break
      fi
    done
  done
  SELECTED_STEPS=("${filtered[@]}")
  return 0
}

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

build_step_args() {
  local step_name="$1"
  local args=()
  local index=0
  local key value
  while [[ $index -lt ${#COMMON_ARGS[@]} ]]; do
    key="${COMMON_ARGS[$index]}"
    value="${COMMON_ARGS[$((index + 1))]}"
    case "$step_name" in
      fetch|stop-topology|qa-network-context|global-stations|qa-network-projection)
        args+=("$key" "$value")
        ;;
      reference-data)
        if [[ "$key" != "--source-id" ]]; then
          args+=("$key" "$value")
        fi
        ;;
      merge-queue)
        if [[ "$key" != "--source-id" ]]; then
          args+=("$key" "$value")
        fi
        ;;
    esac
    index=$((index + 2))
  done
  if [[ ${#args[@]} -eq 0 ]]; then
    return 0
  fi
  printf '%s\n' "${args[@]}"
  return 0
}

parse_args() {
  local arg value from_step="" to_step="" only_steps=""

  select_default_steps

  while [[ $# -gt 0 ]]; do
    arg="$1"
    case "$arg" in
      --profile)
        [[ $# -ge 2 ]] || fail "Missing value for --profile"
        PROFILE="$2"
        shift 2
        ;;
      --country|--as-of|--source-id)
        [[ $# -ge 2 ]] || fail "Missing value for $arg"
        COMMON_ARGS+=("$arg" "$2")
        shift 2
        ;;
      --only)
        [[ $# -ge 2 ]] || fail "Missing value for --only"
        only_steps="$2"
        shift 2
        ;;
      --from-step)
        [[ $# -ge 2 ]] || fail "Missing value for --from-step"
        from_step="$(require_step_id "$2")"
        shift 2
        ;;
      --to-step)
        [[ $# -ge 2 ]] || fail "Missing value for --to-step"
        to_step="$(require_step_id "$2")"
        shift 2
        ;;
      --skip-fetch)
        SKIPPED_STEPS+=("fetch")
        shift
        ;;
      --skip-stop-topology|--skip-ingest)
        SKIPPED_STEPS+=("stop-topology")
        shift
        ;;
      --skip-qa-network-context)
        SKIPPED_STEPS+=("qa-network-context")
        shift
        ;;
      --skip-global-stations)
        SKIPPED_STEPS+=("global-stations")
        shift
        ;;
      --skip-reference-data)
        SKIPPED_STEPS+=("reference-data")
        shift
        ;;
      --skip-qa-network-projection)
        SKIPPED_STEPS+=("qa-network-projection")
        shift
        ;;
      --skip-merge-queue)
        SKIPPED_STEPS+=("merge-queue")
        shift
        ;;
      --skip-qa-audit)
        RUN_QA_AUDIT="false"
        shift
        ;;
      --skip-db-bootstrap)
        SKIP_DB_BOOTSTRAP="true"
        shift
        ;;
      --run-id)
        [[ $# -ge 2 ]] || fail "Missing value for --run-id"
        RUN_ID="$2"
        shift 2
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
        fail "Unknown argument: $arg"
        ;;
    esac
  done

  apply_profile

  if [[ -n "$only_steps" ]]; then
    apply_only_steps "$only_steps"
  fi

  local skipped_step
  for skipped_step in "${SKIPPED_STEPS[@]}"; do
    remove_selected_step "$skipped_step"
  done

  apply_step_range "$from_step" "$to_step"

  [[ ${#SELECTED_STEPS[@]} -gt 0 ]] || fail "No pipeline steps selected after applying filters"
  return 0
}

run_selected_steps() {
  local step
  for step in "${SELECTED_STEPS[@]}"; do
    mapfile -t step_args < <(build_step_args "$step")
    case "$step" in
      fetch)
        run_step "fetch" "${ROOT_DIR}/scripts/data/fetch-sources.sh" "${step_args[@]}"
        ;;
      stop-topology)
        run_step "stop-topology" "${ROOT_DIR}/scripts/data/ingest-netex.sh" --mode stop-topology "${step_args[@]}"
        ;;
      qa-network-context)
        run_step "qa-network-context" node "${ROOT_DIR}/services/orchestrator/src/cli/extract-qa-network-context.js" --root "${ROOT_DIR}" "${step_args[@]}"
        ;;
      global-stations)
        run_step "global-stations" "${ROOT_DIR}/scripts/data/build-global-stations.sh" "${step_args[@]}"
        ;;
      reference-data)
        run_step "reference-data" "${ROOT_DIR}/scripts/data/refresh-external-references.sh" "${step_args[@]}"
        ;;
      qa-network-projection)
        run_step "qa-network-projection" node "${ROOT_DIR}/services/orchestrator/src/cli/project-qa-network-context.js" --root "${ROOT_DIR}" "${step_args[@]}"
        ;;
      merge-queue)
        run_step "merge-queue" "${ROOT_DIR}/scripts/data/build-global-merge-queue.sh" "${step_args[@]}"
        ;;
      *)
        fail "Unsupported step: $step"
        ;;
    esac
  done
  return 0
}

should_run_qa_audit() {
  local step
  [[ "$RUN_QA_AUDIT" == "true" ]] || return 1
  for step in "${SELECTED_STEPS[@]}"; do
    if [[ "$step" == "merge-queue" ]]; then
      return 0
    fi
  done
  return 1
}

run_post_merge_audit() {
  mapfile -t step_args < <(build_step_args "merge-queue")
  run_step "qa-audit" "${ROOT_DIR}/scripts/data/qa-audit.sh" "${step_args[@]}"
  return 0
}

main() {
  parse_args "$@"

  if [[ -z "$RUN_ID" ]]; then
    RUN_ID="station-review-$(date -u +%Y%m%dT%H%M%SZ)"
  fi

  log "Root directory: ${ROOT_DIR}"
  log "Run ID: ${RUN_ID}"
  log "Profile: ${PROFILE:-default}"
  log "DB bootstrap step: ${SKIP_DB_BOOTSTRAP}"
  log "Post-merge QA audit: ${RUN_QA_AUDIT}"
  log "Dry run: ${DRY_RUN}"
  log "Selected steps: ${SELECTED_STEPS[*]}"
  log "Shared args: ${COMMON_ARGS[*]:-(none)}"

  if [[ "$DRY_RUN" == "true" ]]; then
    if [[ "$SKIP_DB_BOOTSTRAP" == "false" ]]; then
      record_step "$STEP_DB_BOOTSTRAP" "skipped" "0"
      log "DRY-RUN step=${STEP_DB_BOOTSTRAP} command=${ROOT_DIR}/scripts/data/db-bootstrap.sh"
    fi
    local step
    for step in "${SELECTED_STEPS[@]}"; do
      record_step "$step" "skipped" "0"
      log "DRY-RUN step=${step}"
    done
    if should_run_qa_audit; then
      record_step "qa-audit" "skipped" "0"
      log "DRY-RUN step=qa-audit"
    fi
    print_summary
    exit 0
  fi

  if [[ "$SKIP_DB_BOOTSTRAP" == "false" ]]; then
    run_step "$STEP_DB_BOOTSTRAP" "${ROOT_DIR}/scripts/data/db-bootstrap.sh"
  else
    record_step "$STEP_DB_BOOTSTRAP" "skipped" "0"
    log "SKIP step=${STEP_DB_BOOTSTRAP} (--skip-db-bootstrap)"
  fi

  run_selected_steps
  if should_run_qa_audit; then
    run_post_merge_audit
  fi

  print_summary
  log "Station review QA pipeline completed successfully."
  log "Next: open http://localhost:3000/curation.html or query /api/qa/global-clusters."
}

main "$@"
