#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CONFIG_FILE="${ROOT_DIR}/config/dach-data-sources.json"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib-db.sh"

COUNTRY_FILTER=""
SOURCE_ID_FILTER=""
AS_OF=""
TMP_FILES=()

usage() {
  cat <<USAGE
Usage: scripts/data/ingest-netex.sh [options]

Ingest NeTEx raw snapshots into PostGIS staging tables.

Options:
  --country DE|AT|CH   Limit ingest to one country
  --source-id ID       Limit ingest to one source id
  --as-of YYYY-MM-DD   Pick latest snapshot <= date
  -h, --help           Show this help
USAGE
  return 0
}

log() {
  printf '[ingest-netex] %s\n' "$*"
  return 0
}

fail() {
  printf '[ingest-netex] ERROR: %s\n' "$*" >&2
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

parse_args() {
  local arg
  while [[ $# -gt 0 ]]; do
    arg="$1"
    case "$arg" in
      --country)
        [[ $# -ge 2 ]] || fail "Missing value for --country"
        COUNTRY_FILTER="$2"
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
      -h|--help)
        usage
        exit 0
        ;;
      *)
        fail "Unknown argument: $arg"
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

create_run() {
  local source_id="$1"
  local country="$2"
  local snapshot_date="$3"
  local run_id
  local source_id_esc country_esc snapshot_date_esc run_id_esc

  run_id="$(python3 - <<'PY'
import uuid
print(uuid.uuid4())
PY
)"

  run_id_esc="$(db_sql_escape "$run_id")"
  source_id_esc="$(db_sql_escape "$source_id")"
  country_esc="$(db_sql_escape "$country")"
  snapshot_date_esc="$(db_sql_escape "$snapshot_date")"

  db_psql -c "
    INSERT INTO import_runs (run_id, pipeline, status, source_id, country, snapshot_date)
    VALUES ('${run_id_esc}'::uuid, 'netex_ingest', 'running', '${source_id_esc}', '${country_esc}', '${snapshot_date_esc}'::date);
  " >/dev/null

  printf '%s\n' "$run_id"
  return 0
}

ingest_source() {
  local source_json="$1"

  local source_id country provider provider_slug format base_dir snapshot_date snapshot_dir
  local manifest_path manifest_sha file_name zip_path
  local resolved_url retrieval_ts file_size detected_version requested_as_of
  local run_id tmp_csv tmp_summary parser_output parser_status stop_rows loaded_rows stats_json manifest_json
  local source_id_esc country_esc provider_slug_esc snapshot_date_esc manifest_path_esc manifest_sha_esc
  local manifest_json_esc resolved_url_esc file_name_esc file_size_esc retrieval_ts_esc detected_version_esc
  local requested_as_of_esc run_id_esc stats_json_esc

  source_id="$(jq -r '.id' <<<"$source_json")"
  country="$(jq -r '.country' <<<"$source_json")"
  provider="$(jq -r '.provider' <<<"$source_json")"
  format="$(jq -r '.format' <<<"$source_json")"

  [[ "$format" == "netex" ]] || fail "Source '$source_id' is format '$format', but this pipeline supports only netex"

  provider_slug="$(slugify "$provider")"
  base_dir="${ROOT_DIR}/data/raw/${country}/${provider_slug}/netex"

  snapshot_date="$(resolve_snapshot_date "$base_dir")" || fail "No snapshot directory found for '$source_id' in '$base_dir' (as-of='${AS_OF:-latest}')"
  snapshot_dir="${base_dir}/${snapshot_date}"
  manifest_path="${snapshot_dir}/manifest.json"

  [[ -f "$manifest_path" ]] || fail "Missing manifest for '$source_id': $manifest_path"

  file_name="$(jq -r '.fileName // empty' "$manifest_path")"
  manifest_sha="$(jq -r '.sha256 // empty' "$manifest_path")"
  resolved_url="$(jq -r '.resolvedDownloadUrl // empty' "$manifest_path")"
  retrieval_ts="$(jq -r '.retrievalTimestamp // empty' "$manifest_path")"
  file_size="$(jq -r '.fileSizeBytes // empty' "$manifest_path")"
  detected_version="$(jq -r '.detectedVersionOrDate // empty' "$manifest_path")"
  requested_as_of="$(jq -r '.requestedAsOf // empty' "$manifest_path")"

  if [[ -n "$file_name" ]]; then
    zip_path="${snapshot_dir}/${file_name}"
    if [[ ! -f "$zip_path" ]]; then
      zip_path="${snapshot_dir}/$(basename "$file_name")"
    fi
  else
    mapfile -t zip_candidates < <(find "$snapshot_dir" -maxdepth 1 -type f -name '*.zip' | sort)
    [[ ${#zip_candidates[@]} -ge 1 ]] || fail "No ZIP artifact found in snapshot dir: $snapshot_dir"
    zip_path="${zip_candidates[-1]}"
    file_name="$(basename "$zip_path")"
  fi

  [[ -f "$zip_path" ]] || fail "Expected NeTEx ZIP not found for '$source_id': $zip_path"

  run_id="$(create_run "$source_id" "$country" "$snapshot_date")"
  log "Ingesting source '$source_id' snapshot '$snapshot_date' from $(basename "$zip_path") (run $run_id)"

  tmp_csv="$(mktemp)"
  tmp_summary="$(mktemp)"
  TMP_FILES+=("$tmp_csv" "$tmp_summary")

  set +e
  parser_output="$(python3 "${SCRIPT_DIR}/netex_extract_stops.py" \
    --zip-path "$zip_path" \
    --output-csv "$tmp_csv" \
    --summary-json "$tmp_summary" \
    --source-id "$source_id" \
    --country "$country" \
    --provider-slug "$provider_slug" \
    --snapshot-date "$snapshot_date" \
    --manifest-sha256 "$manifest_sha" \
    --import-run-id "$run_id" 2>&1)"
  parser_status=$?
  set -e

  if [[ $parser_status -ne 0 ]]; then
    mark_run_failed "$run_id" "NeTEx parser failed for ${source_id} (${snapshot_date})"
    printf '%s\n' "$parser_output" >&2
    fail "Parser failed for '$source_id' (${snapshot_date}); hard-failing without fallback"
  fi

  stop_rows="$(jq -r '.stopPlacesWritten // 0' "$tmp_summary")"
  if [[ "$stop_rows" == "0" ]]; then
    mark_run_failed "$run_id" "No StopPlace rows extracted for ${source_id} (${snapshot_date})"
    fail "No StopPlace rows extracted for '$source_id' (${snapshot_date}); hard-failing"
  fi

  manifest_json="$(jq -c '.' "$manifest_path")"

  source_id_esc="$(db_sql_escape "$source_id")"
  country_esc="$(db_sql_escape "$country")"
  provider_slug_esc="$(db_sql_escape "$provider_slug")"
  snapshot_date_esc="$(db_sql_escape "$snapshot_date")"
  manifest_path_esc="$(db_sql_escape "$manifest_path")"
  manifest_sha_esc="$(db_sql_escape "$manifest_sha")"
  manifest_json_esc="$(db_sql_escape "$manifest_json")"
  resolved_url_esc="$(db_sql_escape "$resolved_url")"
  file_name_esc="$(db_sql_escape "$file_name")"
  file_size_esc="$(db_sql_escape "$file_size")"
  retrieval_ts_esc="$(db_sql_escape "$retrieval_ts")"
  detected_version_esc="$(db_sql_escape "$detected_version")"
  requested_as_of_esc="$(db_sql_escape "$requested_as_of")"

  set +e
  db_psql -c "
      INSERT INTO raw_snapshots (
        source_id, country, provider_slug, format, snapshot_date, manifest_path,
        manifest_sha256, manifest, resolved_download_url, file_name, file_size_bytes,
        retrieval_timestamp, detected_version_or_date, requested_as_of, updated_at
      )
      VALUES (
        '${source_id_esc}', '${country_esc}', '${provider_slug_esc}', 'netex', '${snapshot_date_esc}'::date, '${manifest_path_esc}',
        NULLIF('${manifest_sha_esc}', ''), '${manifest_json_esc}'::jsonb, NULLIF('${resolved_url_esc}', ''), '${file_name_esc}', NULLIF('${file_size_esc}', '')::bigint,
        NULLIF('${retrieval_ts_esc}', '')::timestamptz, NULLIF('${detected_version_esc}', ''), NULLIF('${requested_as_of_esc}', '')::date, now()
      )
      ON CONFLICT (source_id, snapshot_date)
      DO UPDATE SET
        country = EXCLUDED.country,
        provider_slug = EXCLUDED.provider_slug,
        format = EXCLUDED.format,
        manifest_path = EXCLUDED.manifest_path,
        manifest_sha256 = EXCLUDED.manifest_sha256,
        manifest = EXCLUDED.manifest,
        resolved_download_url = EXCLUDED.resolved_download_url,
        file_name = EXCLUDED.file_name,
        file_size_bytes = EXCLUDED.file_size_bytes,
        retrieval_timestamp = EXCLUDED.retrieval_timestamp,
        detected_version_or_date = EXCLUDED.detected_version_or_date,
        requested_as_of = EXCLUDED.requested_as_of,
        updated_at = now();
    " >/dev/null
  sql_status=$?
  set -e

  if [[ $sql_status -ne 0 ]]; then
    mark_run_failed "$run_id" "Failed upserting raw_snapshots for ${source_id} (${snapshot_date})"
    fail "Failed writing raw snapshot metadata for '$source_id'"
  fi

  db_psql -c "
    DELETE FROM netex_stops_staging
    WHERE source_id = '${source_id_esc}' AND snapshot_date = '${snapshot_date_esc}'::date;
  " >/dev/null

  set +e
  db_copy_csv_from_file "$tmp_csv" "netex_stops_staging (
    import_run_id,
    source_id,
    country,
    provider_slug,
    snapshot_date,
    manifest_sha256,
    source_stop_id,
    source_parent_stop_id,
    stop_name,
    latitude,
    longitude,
    grid_id,
    public_code,
    private_code,
    hard_id,
    source_file,
    raw_payload
  )"
  sql_status=$?
  set -e

  if [[ $sql_status -ne 0 ]]; then
    mark_run_failed "$run_id" "Failed loading staging rows for ${source_id} (${snapshot_date})"
    fail "Failed loading staging rows for '$source_id'; hard-failing"
  fi

  run_id_esc="$(db_sql_escape "$run_id")"
  loaded_rows="$(db_psql -At -c "SELECT COUNT(*) FROM netex_stops_staging WHERE import_run_id = '${run_id_esc}'::uuid;")"

  if [[ -z "$loaded_rows" || "$loaded_rows" == "0" ]]; then
    mark_run_failed "$run_id" "No rows loaded into staging for ${source_id} (${snapshot_date})"
    fail "No rows loaded into netex_stops_staging for '$source_id'; hard-failing"
  fi

  stats_json="$(jq -c --argjson loadedRows "$loaded_rows" '. + {loadedRows: $loadedRows}' "$tmp_summary")"

  stats_json_esc="$(db_sql_escape "$stats_json")"
  db_psql -c "
    UPDATE import_runs
    SET status = 'succeeded', ended_at = now(), stats = '${stats_json_esc}'::jsonb
    WHERE run_id = '${run_id_esc}'::uuid;
  " >/dev/null

  log "Completed '$source_id' (${snapshot_date}): loaded ${loaded_rows} staging rows"
  return 0
}

main() {
  parse_args "$@"
  "${ROOT_DIR}/scripts/validate-config.sh" --only dach >/dev/null

  [[ -f "$CONFIG_FILE" ]] || fail "Config file not found: $CONFIG_FILE"
  command -v jq >/dev/null 2>&1 || fail "Missing required command: jq"
  command -v python3 >/dev/null 2>&1 || fail "Missing required command: python3"

  db_load_env
  db_resolve_connection
  db_ensure_ready

  "${SCRIPT_DIR}/db-migrate.sh" --quiet

  if [[ -n "$SOURCE_ID_FILTER" ]]; then
    source_exists="$(jq -r --arg sid "$SOURCE_ID_FILTER" '.sources[] | select(.id == $sid) | .id' "$CONFIG_FILE" | head -n 1)"
    [[ -n "$source_exists" ]] || fail "Unknown --source-id '$SOURCE_ID_FILTER'"

    source_format="$(jq -r --arg sid "$SOURCE_ID_FILTER" '.sources[] | select(.id == $sid) | .format' "$CONFIG_FILE" | head -n 1)"
    [[ "$source_format" == "netex" ]] || fail "Source '$SOURCE_ID_FILTER' has format '$source_format'; this ingest supports only netex"
  fi

  mapfile -t sources < <(jq -c \
    --arg country "$COUNTRY_FILTER" \
    --arg source_id "$SOURCE_ID_FILTER" \
    '.sources[]
     | select(.format == "netex")
     | select($country == "" or .country == $country)
     | select($source_id == "" or .id == $source_id)' "$CONFIG_FILE")

  [[ ${#sources[@]} -gt 0 ]] || fail "No matching netex sources selected"

  for source_json in "${sources[@]}"; do
    ingest_source "$source_json"
  done

  log "All selected NeTEx sources ingested successfully"
  return 0
}

main "$@"
