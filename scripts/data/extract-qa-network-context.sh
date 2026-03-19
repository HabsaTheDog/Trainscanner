#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CONFIG_FILE="${ROOT_DIR}/config/europe-data-sources.json"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib-db.sh"

COUNTRY_FILTER=""
SOURCE_ID_FILTER=""
AS_OF=""
SKIP_CONFIG_VALIDATE="false"
SKIP_DB_BOOTSTRAP="false"
BULK_RESET_QA_PROVIDER_CONTEXT="false"
TMP_FILES=()
QA_NETWORK_EXTRACT_NICE_LEVEL="${QA_NETWORK_EXTRACT_NICE_LEVEL:-10}"
QA_NETWORK_EXTRACT_IONICE_CLASS="${QA_NETWORK_EXTRACT_IONICE_CLASS:-2}"
QA_NETWORK_EXTRACT_IONICE_LEVEL="${QA_NETWORK_EXTRACT_IONICE_LEVEL:-7}"

usage() {
  cat <<USAGE
Usage: scripts/data/extract-qa-network-context.sh [options]

Extract compact provider-level QA route and adjacency context from NeTEx snapshots.

Options:
  --country <ISO2>       Limit extract to one country
  --source-id <id>       Limit extract to one source id
  --as-of YYYY-MM-DD     Pick latest snapshot <= date
  --skip-config-validate Skip validate-config.sh --only sources preflight
  --skip-db-bootstrap    Skip db-bootstrap.sh preflight
  -h, --help             Show this help
USAGE
  return 0
}

log() {
  printf '[extract-qa-network] %s\n' "$*"
  return 0
}

fail() {
  printf '[extract-qa-network] ERROR: %s\n' "$*" >&2
  return 1
}

cleanup() {
  local file
  for file in "${TMP_FILES[@]}"; do
    [[ -n "$file" ]] && rm -f "$file" 2>/dev/null || true
  done
  return 0
}
trap cleanup EXIT

is_iso_date() {
  local d="$1"
  [[ "$d" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || return 1
  date -u -d "$d" +%F >/dev/null 2>&1
  return 0
}

slugify() {
  local value="$1"
  printf '%s' "$value" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//'
  return 0
}

make_tmp_file() {
  local base_dir="${TMPDIR:-}"
  if [[ -n "$base_dir" ]]; then
    mkdir -p "$base_dir"
    mktemp "${base_dir%/}/extract-qa-network.XXXXXX"
    return 0
  fi
  mktemp
  return 0
}

run_python_extractor() {
  local -a cmd=(python3 "${SCRIPT_DIR}/netex_extract_qa_network.py" "$@")
  if command -v ionice >/dev/null 2>&1; then
    cmd=(ionice -c "${QA_NETWORK_EXTRACT_IONICE_CLASS}" -n "${QA_NETWORK_EXTRACT_IONICE_LEVEL}" "${cmd[@]}")
  fi
  if command -v nice >/dev/null 2>&1; then
    cmd=(nice -n "${QA_NETWORK_EXTRACT_NICE_LEVEL}" "${cmd[@]}")
  fi
  "${cmd[@]}"
}

can_bulk_reset_qa_provider_context() {
  if [[ -n "$COUNTRY_FILTER" || -n "$SOURCE_ID_FILTER" || -n "$AS_OF" ]]; then
    return 1
  fi

  return 0
}

parse_args() {
  local arg
  local value
  while [[ $# -gt 0 ]]; do
    arg="$1"
    case "$arg" in
      --country)
        [[ $# -ge 2 ]] || fail "Missing value for --country"
        value="$2"
        COUNTRY_FILTER="$(printf '%s' "$value" | tr '[:lower:]' '[:upper:]')"
        shift 2
        ;;
      --source-id)
        [[ $# -ge 2 ]] || fail "Missing value for --source-id"
        SOURCE_ID_FILTER="$2"
        shift 2
        ;;
      --as-of)
        [[ $# -ge 2 ]] || fail "Missing value for --as-of"
        AS_OF="$2"
        shift 2
        ;;
      --skip-config-validate)
        SKIP_CONFIG_VALIDATE="true"
        shift
        ;;
      --skip-db-bootstrap)
        SKIP_DB_BOOTSTRAP="true"
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

  if [[ -n "$COUNTRY_FILTER" && ! "$COUNTRY_FILTER" =~ ^[A-Z]{2}$ ]]; then
    fail "Invalid --country '$COUNTRY_FILTER' (expected ISO-3166 alpha-2 code)"
  fi

  if [[ -n "$AS_OF" ]] && ! is_iso_date "$AS_OF"; then
    fail "Invalid --as-of value '$AS_OF' (expected YYYY-MM-DD)"
  fi
  return 0
}

resolve_snapshot_date() {
  local base_dir="$1"
  local selected=""

  [[ -d "$base_dir" ]] || return 1

  mapfile -t dates < <(find "$base_dir" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' | sort)
  [[ ${#dates[@]} -gt 0 ]] || return 1

  if [[ -z "$AS_OF" ]]; then
    selected="${dates[-1]}"
  else
    local d
    for d in "${dates[@]}"; do
      if [[ "$d" > "$AS_OF" ]]; then
        break
      fi
      selected="$d"
    done
  fi

  [[ -n "$selected" ]] || return 1
  printf '%s\n' "$selected"
  return 0
}

extract_source() {
  local source_json="$1"
  local source_id country provider provider_slug base_dir snapshot_date snapshot_dir
  local manifest_path manifest_sha file_name zip_path dataset_id run_id dataset_id_esc
  local tmp_routes_csv tmp_adjacencies_csv tmp_summary route_rows adjacency_rows
  local source_id_esc snapshot_date_esc

  source_id="$(jq -r '.id' <<<"$source_json")"
  country="$(jq -r '.country' <<<"$source_json" | tr '[:lower:]' '[:upper:]')"
  provider="$(jq -r '.provider' <<<"$source_json")"
  provider_slug="$(slugify "$provider")"
  base_dir="${ROOT_DIR}/data/raw/${country}/${provider_slug}/netex"

  snapshot_date="$(resolve_snapshot_date "$base_dir")" || fail "No snapshot directory found for '$source_id' in '$base_dir' (as-of='${AS_OF:-latest}')"
  snapshot_dir="${base_dir}/${snapshot_date}"
  manifest_path="${snapshot_dir}/manifest.json"
  [[ -f "$manifest_path" ]] || fail "Missing manifest for '$source_id': $manifest_path"

  file_name="$(jq -r '.fileName // empty' "$manifest_path")"
  manifest_sha="$(jq -r '.sha256 // empty' "$manifest_path")"
  if [[ -n "$file_name" ]]; then
    zip_path="${snapshot_dir}/${file_name}"
    if [[ ! -f "$zip_path" ]]; then
      zip_path="${snapshot_dir}/$(basename "$file_name")"
    fi
  else
    mapfile -t zip_candidates < <(find "$snapshot_dir" -maxdepth 1 -type f -name '*.zip' | sort)
    [[ ${#zip_candidates[@]} -ge 1 ]] || fail "No ZIP artifact found in snapshot dir: $snapshot_dir"
    zip_path="${zip_candidates[-1]}"
  fi
  [[ -f "$zip_path" ]] || fail "Expected NeTEx ZIP not found for '$source_id': $zip_path"

  source_id_esc="$(db_sql_escape "$source_id")"
  snapshot_date_esc="$(db_sql_escape "$snapshot_date")"
  dataset_id="$(db_psql -At -c "
    SELECT dataset_id
    FROM provider_datasets
    WHERE source_id = '${source_id_esc}'
      AND snapshot_date = '${snapshot_date_esc}'::date
    ORDER BY dataset_id DESC
    LIMIT 1;
  ")"
  dataset_id="$(printf '%s\n' "$dataset_id" | rg '^[0-9]+$' | head -n 1 | tr -d '[:space:]')"
  [[ -n "$dataset_id" ]] || fail "No provider_datasets row found for '$source_id' (${snapshot_date}); run stop-topology ingest first"

  tmp_routes_csv="$(make_tmp_file)"
  tmp_adjacencies_csv="$(make_tmp_file)"
  tmp_summary="$(make_tmp_file)"
  TMP_FILES+=("$tmp_routes_csv" "$tmp_adjacencies_csv" "$tmp_summary")

  run_id="qa-network-${source_id}-${snapshot_date}"
  log "Extracting compact QA context for '$source_id' snapshot '$snapshot_date' (dataset_id=${dataset_id})"

  if ! run_python_extractor \
    --zip-path "$zip_path" \
    --output-routes-csv "$tmp_routes_csv" \
    --output-adjacencies-csv "$tmp_adjacencies_csv" \
    --summary-json "$tmp_summary" \
    --dataset-id "$dataset_id" \
    --source-id "$source_id" \
    --country "$country" \
    --provider-slug "$provider_slug" \
    --snapshot-date "$snapshot_date" \
    --manifest-sha256 "$manifest_sha" \
    --import-run-id "$run_id"; then
    fail "QA network extractor failed for '$source_id' (${snapshot_date})"
  fi

  route_rows="$(jq -r '.routeRowsWritten // 0' "$tmp_summary")"
  adjacency_rows="$(jq -r '.adjacencyRowsWritten // 0' "$tmp_summary")"
  dataset_id_esc="$(db_sql_escape "$dataset_id")"

  if [[ "$BULK_RESET_QA_PROVIDER_CONTEXT" != "true" ]]; then
    db_psql -c "
      DELETE FROM qa_provider_stop_place_routes
      WHERE dataset_id = '${dataset_id_esc}'::bigint;
      DELETE FROM qa_provider_stop_place_adjacencies
      WHERE dataset_id = '${dataset_id_esc}'::bigint;
    " >/dev/null
  fi

  if [[ "$route_rows" != "0" ]]; then
    db_copy_csv_from_file "$tmp_routes_csv" "qa_provider_stop_place_routes (
      source_country,
      source_id,
      dataset_id,
      provider_stop_place_ref,
      route_label,
      transport_mode,
      pattern_hits,
      metadata
    )"
  fi

  if [[ "$adjacency_rows" != "0" ]]; then
    db_copy_csv_from_file "$tmp_adjacencies_csv" "qa_provider_stop_place_adjacencies (
      source_country,
      source_id,
      dataset_id,
      from_provider_stop_place_ref,
      to_provider_stop_place_ref,
      pattern_hits,
      metadata
    )"
  fi

  log "Completed '$source_id' (${snapshot_date}): route_rows=${route_rows} adjacency_rows=${adjacency_rows}"
  return 0
}

main() {
  parse_args "$@"

  [[ -f "$CONFIG_FILE" ]] || fail "Config file not found: $CONFIG_FILE"
  command -v jq >/dev/null 2>&1 || fail "Missing required command: jq"
  command -v python3 >/dev/null 2>&1 || fail "Missing required command: python3"

  if [[ "$SKIP_CONFIG_VALIDATE" != "true" ]]; then
    "${ROOT_DIR}/scripts/validate-config.sh" --only sources >/dev/null
  fi

  db_load_env
  db_resolve_connection
  db_ensure_ready

  if [[ "$SKIP_DB_BOOTSTRAP" != "true" ]]; then
    "${SCRIPT_DIR}/db-bootstrap.sh" --quiet --if-ready
  fi

  mapfile -t sources < <(jq -c \
    --arg country "$COUNTRY_FILTER" \
    --arg source_id "$SOURCE_ID_FILTER" \
    '.sources[]
     | select(.format == "netex")
     | select($country == "" or .country == $country)
     | select($source_id == "" or .id == $source_id)
     | select($source_id != "" or (.pipelineEnabled != false))' "$CONFIG_FILE")

  [[ ${#sources[@]} -gt 0 ]] || fail "No matching netex sources selected"

  if can_bulk_reset_qa_provider_context; then
    BULK_RESET_QA_PROVIDER_CONTEXT="true"
    log "Using bulk QA provider context reset fast path for full-scope run"
    db_psql -c "
      TRUNCATE TABLE qa_provider_stop_place_routes, qa_provider_stop_place_adjacencies;
    " >/dev/null
  fi

  local source_json
  for source_json in "${sources[@]}"; do
    extract_source "$source_json"
  done

  log "QA network context extraction completed successfully"
}

main "$@"
