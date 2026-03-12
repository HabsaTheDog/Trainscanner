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
TMP_FILES=()

usage() {
  cat <<USAGE
Usage: scripts/data/ingest-netex.sh [options]

Ingest NeTEx snapshots into pan-European raw provider tables.

Options:
  --country <ISO2>      Limit ingest to one country
  --source-id <id>      Limit ingest to one source id
  --as-of YYYY-MM-DD    Pick latest snapshot <= date
  --skip-config-validate Skip validate-config.sh --only sources preflight
  --skip-db-bootstrap   Skip db-bootstrap.sh preflight
  -h, --help            Show this help
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

warn() {
  printf '[ingest-netex] WARN: %s\n' "$*" >&2
  return 0
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

now_ms() {
  date +%s%3N
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

mark_run_failed() {
  local run_id="$1"
  local dataset_id="$2"
  local message="$3"
  local run_id_esc dataset_id_esc message_esc

  run_id_esc="$(db_sql_escape "$run_id")"
  dataset_id_esc="$(db_sql_escape "$dataset_id")"
  message_esc="$(db_sql_escape "$message")"

  db_psql -c "
    UPDATE import_runs
    SET status = 'failed', ended_at = now(), error_message = '${message_esc}'
    WHERE run_id = '${run_id_esc}'::uuid;
  " >/dev/null || true

  if [[ -n "$dataset_id" ]]; then
    db_psql -c "
      UPDATE provider_datasets
      SET
        ingestion_status = 'failed',
        ingestion_error = '${message_esc}',
        updated_at = now()
      WHERE dataset_id = '${dataset_id_esc}'::bigint;
    " >/dev/null || true
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
    VALUES (
      '${run_id_esc}'::uuid,
      'netex_ingest',
      'running',
      '${source_id_esc}',
      '${country_esc}',
      '${snapshot_date_esc}'::date
    );
  " >/dev/null

  printf '%s\n' "$run_id"
  return 0
}

transform_extracted_csv() {
  local raw_csv="$1"
  local places_csv="$2"
  local points_csv="$3"
  local dataset_id="$4"
  local source_id="$5"
  local country="$6"

  python3 - "$raw_csv" "$places_csv" "$points_csv" "$dataset_id" "$source_id" "$country" <<'PY'
import csv
import hashlib
import json
import sys
from pathlib import Path

raw_csv_path = Path(sys.argv[1])
places_csv_path = Path(sys.argv[2])
points_csv_path = Path(sys.argv[3])
dataset_id = str(sys.argv[4]).strip()
source_id = str(sys.argv[5]).strip()
country = str(sys.argv[6]).strip().upper()

place_header = [
    "stop_place_id",
    "dataset_id",
    "source_id",
    "provider_stop_place_ref",
    "country",
    "stop_name",
    "latitude",
    "longitude",
    "parent_stop_place_ref",
    "topographic_place_ref",
    "public_code",
    "private_code",
    "hard_id",
    "raw_payload",
]

point_header = [
    "stop_point_id",
    "dataset_id",
    "source_id",
    "provider_stop_point_ref",
    "provider_stop_place_ref",
    "stop_place_id",
    "country",
    "stop_name",
    "latitude",
    "longitude",
    "topographic_place_ref",
    "platform_code",
    "track_code",
    "raw_payload",
]

def stable_id(prefix: str, value: str) -> str:
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:24]
    return f"{prefix}_{digest}"

def to_json_map(raw_text: str, source_file: str) -> dict:
    text = str(raw_text or "").strip()
    try:
        payload = json.loads(text) if text else {}
        if not isinstance(payload, dict):
            payload = {"raw_payload": payload}
    except Exception:
        payload = {"raw_payload": text}
    payload["source_file"] = source_file
    return payload

def build_place_row(*, provider_stop_place_ref: str, stop_name: str, latitude: str, longitude: str, parent_ref: str, topographic_ref: str, public_code: str, private_code: str, hard_id: str, raw_payload: dict) -> dict:
    stop_place_id = stable_id(
        "rsp", f"{dataset_id}|{source_id}|{provider_stop_place_ref}"
    )
    return {
        "stop_place_id": stop_place_id,
        "dataset_id": dataset_id,
        "source_id": source_id,
        "provider_stop_place_ref": provider_stop_place_ref,
        "country": country,
        "stop_name": stop_name,
        "latitude": latitude,
        "longitude": longitude,
        "parent_stop_place_ref": parent_ref,
        "topographic_place_ref": topographic_ref,
        "public_code": public_code,
        "private_code": private_code,
        "hard_id": hard_id,
        "raw_payload": json.dumps(raw_payload, ensure_ascii=True, separators=(",", ":")),
    }

def build_point_row(*, provider_stop_point_ref: str, provider_stop_place_ref: str, stop_name: str, latitude: str, longitude: str, topographic_ref: str, raw_payload: dict, platform_code: str = "", track_code: str = "") -> dict:
    stop_point_id = stable_id(
        "rpp", f"{dataset_id}|{source_id}|{provider_stop_point_ref}"
    )
    stop_place_id = stable_id(
        "rsp", f"{dataset_id}|{source_id}|{provider_stop_place_ref}"
    )
    return {
        "stop_point_id": stop_point_id,
        "dataset_id": dataset_id,
        "source_id": source_id,
        "provider_stop_point_ref": provider_stop_point_ref,
        "provider_stop_place_ref": provider_stop_place_ref,
        "stop_place_id": stop_place_id,
        "country": country,
        "stop_name": stop_name,
        "latitude": latitude,
        "longitude": longitude,
        "topographic_place_ref": topographic_ref,
        "platform_code": platform_code,
        "track_code": track_code,
        "raw_payload": json.dumps(raw_payload, ensure_ascii=True, separators=(",", ":")),
    }

with raw_csv_path.open("r", encoding="utf-8", newline="") as rf, \
     places_csv_path.open("w", encoding="utf-8", newline="") as pf, \
     points_csv_path.open("w", encoding="utf-8", newline="") as qf:
    reader = csv.DictReader(rf)
    places_writer = csv.DictWriter(pf, fieldnames=place_header)
    points_writer = csv.DictWriter(qf, fieldnames=point_header)
    places_writer.writeheader()
    points_writer.writeheader()

    place_by_ref: dict[str, dict] = {}
    point_by_ref: dict[str, dict] = {}
    explicit_point_refs: set[str] = set()
    place_has_explicit_point: set[str] = set()

    for row in reader:
        entity_type = str(row.get("entity_type", "stop_place")).strip().lower()
        source_stop_id = str(row.get("source_stop_id", "")).strip()
        stop_name = str(row.get("stop_name", "")).strip()
        if not source_stop_id or not stop_name:
            continue

        provider_stop_place_ref = str(row.get("provider_stop_place_ref", "")).strip()
        if not provider_stop_place_ref:
            provider_stop_place_ref = str(row.get("source_parent_stop_id", "")).strip()
        if not provider_stop_place_ref:
            provider_stop_place_ref = source_stop_id

        provider_stop_point_ref = str(row.get("provider_stop_point_ref", "")).strip()
        if entity_type == "stop_point" and not provider_stop_point_ref:
            provider_stop_point_ref = source_stop_id

        latitude = str(row.get("latitude", "")).strip()
        longitude = str(row.get("longitude", "")).strip()
        parent_ref = str(row.get("source_parent_stop_id", "")).strip()
        topographic_ref = str(row.get("topographic_place_ref", "")).strip()
        public_code = str(row.get("public_code", "")).strip()
        private_code = str(row.get("private_code", "")).strip()
        hard_id = str(row.get("hard_id", "")).strip()
        source_file = str(row.get("source_file", "")).strip()
        raw_payload = to_json_map(str(row.get("raw_payload", "")), source_file)

        place_by_ref[provider_stop_place_ref] = build_place_row(
            provider_stop_place_ref=provider_stop_place_ref,
            stop_name=stop_name,
            latitude=latitude,
            longitude=longitude,
            parent_ref=parent_ref,
            topographic_ref=topographic_ref,
            public_code=public_code,
            private_code=private_code,
            hard_id=hard_id,
            raw_payload=raw_payload,
        )

        if entity_type == "stop_point":
            stop_point_ref = provider_stop_point_ref or f"{provider_stop_place_ref}::sp"
            point_payload = dict(raw_payload)
            point_payload["synthetic_point"] = False
            point_by_ref[stop_point_ref] = build_point_row(
                provider_stop_point_ref=stop_point_ref,
                provider_stop_place_ref=provider_stop_place_ref,
                stop_name=stop_name,
                latitude=latitude,
                longitude=longitude,
                topographic_ref=topographic_ref,
                raw_payload=point_payload,
                platform_code=public_code,
                track_code=private_code,
            )
            explicit_point_refs.add(stop_point_ref)
            place_has_explicit_point.add(provider_stop_place_ref)

    for place_ref, place_row in place_by_ref.items():
        if place_ref in place_has_explicit_point:
            continue
        synthetic_ref = f"{place_ref}::sp"
        if synthetic_ref in explicit_point_refs:
            continue
        payload = json.loads(place_row["raw_payload"] or "{}")
        if not isinstance(payload, dict):
            payload = {}
        payload["synthetic_point"] = True
        payload["source"] = "stop_place_fallback"
        point_by_ref[synthetic_ref] = build_point_row(
            provider_stop_point_ref=synthetic_ref,
            provider_stop_place_ref=place_ref,
            stop_name=place_row["stop_name"],
            latitude=place_row["latitude"],
            longitude=place_row["longitude"],
            topographic_ref=place_row["topographic_place_ref"],
            raw_payload=payload,
        )

    for place_ref in sorted(place_by_ref.keys()):
        places_writer.writerow(place_by_ref[place_ref])
    for point_ref in sorted(point_by_ref.keys()):
        points_writer.writerow(point_by_ref[point_ref])

print(json.dumps({
    "stopPlacesRows": len(place_by_ref),
    "stopPointsRows": len(point_by_ref),
    "explicitStopPointsRows": len(explicit_point_refs),
}))
PY
  return 0
}

ingest_source() {
  local source_json="$1"

  local source_id country provider provider_slug format base_dir snapshot_date snapshot_dir
  local manifest_path manifest_json manifest_sha file_name zip_path
  local resolved_url retrieval_ts file_size detected_version requested_as_of
  local run_id dataset_id
  local tmp_raw_csv tmp_places_csv tmp_points_csv tmp_summary tmp_transform_summary
  local tmp_timetable_trips_csv tmp_timetable_stop_times_csv tmp_timetable_summary
  local parser_output parser_status timetable_output timetable_status
  local stop_rows place_rows point_rows trip_rows trip_stop_time_rows stats_json
  local source_started_at source_elapsed_ms
  local parse_stops_started transform_started parse_timetable_started db_write_started finalize_started
  local parse_stops_ms transform_ms parse_timetable_ms db_write_ms finalize_ms
  local source_id_esc country_esc provider_slug_esc snapshot_date_esc manifest_path_esc manifest_sha_esc
  local manifest_json_esc resolved_url_esc file_name_esc file_size_esc retrieval_ts_esc detected_version_esc requested_as_of_esc
  local zip_path_esc dataset_id_esc run_id_esc stats_json_esc

  source_started_at="$(now_ms)"

  source_id="$(jq -r '.id' <<<"$source_json")"
  country="$(jq -r '.country' <<<"$source_json" | tr '[:lower:]' '[:upper:]')"
  provider="$(jq -r '.provider' <<<"$source_json")"
  format="$(jq -r '.format' <<<"$source_json")"

  [[ "$format" == "netex" ]] || fail "Source '$source_id' format is '$format' (expected netex)"

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
  log "Ingesting '$source_id' snapshot '$snapshot_date' from $(basename "$zip_path") (run=$run_id)"

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
  zip_path_esc="$(db_sql_escape "$zip_path")"

  db_psql -c "
    INSERT INTO raw_snapshots (
      source_id,
      country,
      provider_slug,
      format,
      snapshot_date,
      manifest_path,
      manifest_sha256,
      manifest,
      resolved_download_url,
      file_name,
      file_size_bytes,
      retrieval_timestamp,
      detected_version_or_date,
      requested_as_of,
      updated_at
    )
    VALUES (
      '${source_id_esc}',
      '${country_esc}',
      '${provider_slug_esc}',
      'netex',
      '${snapshot_date_esc}'::date,
      '${manifest_path_esc}',
      NULLIF('${manifest_sha_esc}', ''),
      '${manifest_json_esc}'::jsonb,
      NULLIF('${resolved_url_esc}', ''),
      '${file_name_esc}',
      NULLIF('${file_size_esc}', '')::bigint,
      NULLIF('${retrieval_ts_esc}', '')::timestamptz,
      NULLIF('${detected_version_esc}', ''),
      NULLIF('${requested_as_of_esc}', '')::date,
      now()
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

  dataset_id="$(db_psql -At -c "
    INSERT INTO provider_datasets (
      source_id,
      provider_slug,
      country,
      format,
      snapshot_date,
      manifest_path,
      manifest_sha256,
      manifest,
      raw_archive_path,
      ingestion_status,
      ingestion_error,
      updated_at
    )
    VALUES (
      '${source_id_esc}',
      '${provider_slug_esc}',
      '${country_esc}',
      'netex',
      '${snapshot_date_esc}'::date,
      '${manifest_path_esc}',
      NULLIF('${manifest_sha_esc}', ''),
      '${manifest_json_esc}'::jsonb,
      '${zip_path_esc}',
      'pending',
      NULL,
      now()
    )
    ON CONFLICT (source_id, snapshot_date)
    DO UPDATE SET
      provider_slug = EXCLUDED.provider_slug,
      country = EXCLUDED.country,
      format = EXCLUDED.format,
      manifest_path = EXCLUDED.manifest_path,
      manifest_sha256 = EXCLUDED.manifest_sha256,
      manifest = EXCLUDED.manifest,
      raw_archive_path = EXCLUDED.raw_archive_path,
      ingestion_status = 'pending',
      ingestion_error = NULL,
      updated_at = now()
    RETURNING dataset_id;
  ")"
  dataset_id="$(printf '%s\n' "$dataset_id" | rg '^[0-9]+$' | head -n 1 | tr -d '[:space:]')"
  [[ -n "$dataset_id" ]] || {
    mark_run_failed "$run_id" "" "Failed to resolve provider_datasets.dataset_id for ${source_id} (${snapshot_date})"
    fail "Failed to upsert provider_datasets for '$source_id'"
  }

  tmp_raw_csv="$(mktemp)"
  tmp_places_csv="$(mktemp)"
  tmp_points_csv="$(mktemp)"
  tmp_summary="$(mktemp)"
  tmp_transform_summary="$(mktemp)"
  tmp_timetable_trips_csv="$(mktemp)"
  tmp_timetable_stop_times_csv="$(mktemp)"
  tmp_timetable_summary="$(mktemp)"
  TMP_FILES+=(
    "$tmp_raw_csv"
    "$tmp_places_csv"
    "$tmp_points_csv"
    "$tmp_summary"
    "$tmp_transform_summary"
    "$tmp_timetable_trips_csv"
    "$tmp_timetable_stop_times_csv"
    "$tmp_timetable_summary"
  )

  parse_stops_started="$(now_ms)"
  set +e
  parser_output="$(python3 "${SCRIPT_DIR}/netex_extract_stops.py" \
    --zip-path "$zip_path" \
    --output-csv "$tmp_raw_csv" \
    --summary-json "$tmp_summary" \
    --source-id "$source_id" \
    --country "$country" \
    --provider-slug "$provider_slug" \
    --snapshot-date "$snapshot_date" \
    --manifest-sha256 "$manifest_sha" \
    --import-run-id "$run_id" 2>&1)"
  parser_status=$?
  set -e
  parse_stops_ms="$(( $(now_ms) - parse_stops_started ))"

  if [[ $parser_status -ne 0 ]]; then
    mark_run_failed "$run_id" "$dataset_id" "NeTEx parser failed for ${source_id} (${snapshot_date})"
    printf '%s\n' "$parser_output" >&2
    fail "Parser failed for '$source_id' (${snapshot_date})"
  fi

  stop_rows="$(jq -r '.stopPlacesWritten // 0' "$tmp_summary")"
  if [[ "$stop_rows" == "0" ]]; then
    mark_run_failed "$run_id" "$dataset_id" "No StopPlace rows extracted for ${source_id} (${snapshot_date})"
    fail "No StopPlace rows extracted for '$source_id' (${snapshot_date})"
  fi

  transform_started="$(now_ms)"
  transform_extracted_csv \
    "$tmp_raw_csv" \
    "$tmp_places_csv" \
    "$tmp_points_csv" \
    "$dataset_id" \
    "$source_id" \
    "$country" >"$tmp_transform_summary"
  transform_ms="$(( $(now_ms) - transform_started ))"

  place_rows="$(jq -r '.stopPlacesRows // 0' "$tmp_transform_summary")"
  point_rows="$(jq -r '.stopPointsRows // 0' "$tmp_transform_summary")"
  if [[ "$place_rows" == "0" ]]; then
    mark_run_failed "$run_id" "$dataset_id" "No rows prepared for raw_provider_stop_places (${source_id}, ${snapshot_date})"
    fail "No stop-place rows prepared for '$source_id' (${snapshot_date})"
  fi

  parse_timetable_started="$(now_ms)"
  set +e
  timetable_output="$(python3 "${SCRIPT_DIR}/netex_extract_timetable.py" \
    --zip-path "$zip_path" \
    --output-trips-csv "$tmp_timetable_trips_csv" \
    --output-stop-times-csv "$tmp_timetable_stop_times_csv" \
    --summary-json "$tmp_timetable_summary" \
    --dataset-id "$dataset_id" \
    --source-id "$source_id" \
    --country "$country" \
    --provider-slug "$provider_slug" \
    --snapshot-date "$snapshot_date" \
    --manifest-sha256 "$manifest_sha" \
    --import-run-id "$run_id" 2>&1)"
  timetable_status=$?
  set -e
  parse_timetable_ms="$(( $(now_ms) - parse_timetable_started ))"

  if [[ $timetable_status -ne 0 ]]; then
    mark_run_failed "$run_id" "$dataset_id" "NeTEx timetable parser failed for ${source_id} (${snapshot_date})"
    printf '%s\n' "$timetable_output" >&2
    fail "Timetable parser failed for '$source_id' (${snapshot_date})"
  fi

  trip_rows="$(jq -r '.tripsWritten // 0' "$tmp_timetable_summary")"
  trip_stop_time_rows="$(jq -r '.tripStopTimesWritten // 0' "$tmp_timetable_summary")"

  dataset_id_esc="$(db_sql_escape "$dataset_id")"
  db_write_started="$(now_ms)"
  db_psql -c "
    DELETE FROM timetable_trips
    WHERE dataset_id = '${dataset_id_esc}'::bigint;
    DELETE FROM raw_provider_stop_points
    WHERE dataset_id = '${dataset_id_esc}'::bigint;
    DELETE FROM raw_provider_stop_places
    WHERE dataset_id = '${dataset_id_esc}'::bigint;
  " >/dev/null

  db_copy_csv_from_file "$tmp_places_csv" "raw_provider_stop_places (
    stop_place_id,
    dataset_id,
    source_id,
    provider_stop_place_ref,
    country,
    stop_name,
    latitude,
    longitude,
    parent_stop_place_ref,
    topographic_place_ref,
    public_code,
    private_code,
    hard_id,
    raw_payload
  )"

  db_copy_csv_from_file "$tmp_points_csv" "raw_provider_stop_points (
    stop_point_id,
    dataset_id,
    source_id,
    provider_stop_point_ref,
    provider_stop_place_ref,
    stop_place_id,
    country,
    stop_name,
    latitude,
    longitude,
    topographic_place_ref,
    platform_code,
    track_code,
    raw_payload
  )"

  if [[ "$trip_rows" != "0" ]]; then
    db_copy_csv_from_file "$tmp_timetable_trips_csv" "timetable_trips (
      trip_fact_id,
      dataset_id,
      source_id,
      provider_trip_ref,
      service_id,
      route_id,
      route_short_name,
      route_long_name,
      trip_headsign,
      transport_mode,
      trip_start_date,
      trip_end_date,
      raw_payload
    )"
  fi

  if [[ "$trip_stop_time_rows" != "0" ]]; then
    db_copy_csv_from_file "$tmp_timetable_stop_times_csv" "timetable_trip_stop_times (
      trip_fact_id,
      stop_sequence,
      global_stop_point_id,
      arrival_time,
      departure_time,
      pickup_type,
      drop_off_type,
      metadata
    )"
    db_psql -c "
      UPDATE timetable_trip_stop_times tts
      SET global_stop_point_id = NULL
      WHERE tts.trip_fact_id IN (
        SELECT tt.trip_fact_id
        FROM timetable_trips tt
        WHERE tt.dataset_id = '${dataset_id_esc}'::bigint
      )
        AND tts.global_stop_point_id = '';
    " >/dev/null
  fi
  db_write_ms="$(( $(now_ms) - db_write_started ))"

  finalize_started="$(now_ms)"
  stats_json="$(jq -c \
    --argjson loadedStopPlaces "$place_rows" \
    --argjson loadedStopPoints "$point_rows" \
    --argjson loadedTrips "$trip_rows" \
    --argjson loadedTripStopTimes "$trip_stop_time_rows" \
    --slurpfile timetable "$tmp_timetable_summary" \
    '. + {
      loadedStopPlaces: $loadedStopPlaces,
      loadedStopPoints: $loadedStopPoints,
      loadedTrips: $loadedTrips,
      loadedTripStopTimes: $loadedTripStopTimes,
      timingsMs: {
        parseStops: $parseStopsMs,
        transformStops: $transformMs,
        parseTimetable: $parseTimetableMs,
        dbWrite: $dbWriteMs
      },
      timetableExtraction: ($timetable[0] // {})
    }' \
    --argjson parseStopsMs "$parse_stops_ms" \
    --argjson transformMs "$transform_ms" \
    --argjson parseTimetableMs "$parse_timetable_ms" \
    --argjson dbWriteMs "$db_write_ms" \
    "$tmp_summary")"

  stats_json_esc="$(db_sql_escape "$stats_json")"
  run_id_esc="$(db_sql_escape "$run_id")"
  db_psql -c "
    UPDATE import_runs
    SET
      status = 'succeeded',
      ended_at = now(),
      stats = '${stats_json_esc}'::jsonb,
      error_message = NULL
    WHERE run_id = '${run_id_esc}'::uuid;

    UPDATE provider_datasets
    SET
      ingestion_status = 'ingested',
      ingestion_error = NULL,
      updated_at = now()
    WHERE dataset_id = '${dataset_id_esc}'::bigint;
  " >/dev/null
  finalize_ms="$(( $(now_ms) - finalize_started ))"
  source_elapsed_ms="$(( $(now_ms) - source_started_at ))"

  log "Completed '$source_id' (${snapshot_date}): stop_places=${place_rows} stop_points=${point_rows} trips=${trip_rows} stop_times=${trip_stop_time_rows} total_ms=${source_elapsed_ms} parse_stops_ms=${parse_stops_ms} transform_ms=${transform_ms} parse_timetable_ms=${parse_timetable_ms} db_write_ms=${db_write_ms} finalize_ms=${finalize_ms}"
  return 0
}

main() {
  local setup_started setup_ms
  local validate_ms db_ready_ms bootstrap_ms
  local t0

  setup_started="$(now_ms)"
  parse_args "$@"

  [[ -f "$CONFIG_FILE" ]] || fail "Config file not found: $CONFIG_FILE"
  command -v jq >/dev/null 2>&1 || fail "Missing required command: jq"
  command -v python3 >/dev/null 2>&1 || fail "Missing required command: python3"

  if [[ "$SKIP_CONFIG_VALIDATE" != "true" ]]; then
    t0="$(now_ms)"
    "${ROOT_DIR}/scripts/validate-config.sh" --only sources >/dev/null
    validate_ms="$(( $(now_ms) - t0 ))"
  else
    validate_ms=0
  fi

  t0="$(now_ms)"
  db_load_env
  db_resolve_connection
  db_ensure_ready
  db_ready_ms="$(( $(now_ms) - t0 ))"

  if [[ "$SKIP_DB_BOOTSTRAP" != "true" ]]; then
    t0="$(now_ms)"
    "${SCRIPT_DIR}/db-bootstrap.sh" --quiet --if-ready
    bootstrap_ms="$(( $(now_ms) - t0 ))"
  else
    bootstrap_ms=0
  fi
  setup_ms="$(( $(now_ms) - setup_started ))"
  log "Setup complete: total_ms=${setup_ms} validate_ms=${validate_ms} db_ready_ms=${db_ready_ms} bootstrap_ms=${bootstrap_ms}"

  if [[ -n "$SOURCE_ID_FILTER" ]]; then
    source_exists="$(jq -r --arg sid "$SOURCE_ID_FILTER" '.sources[] | select(.id == $sid) | .id' "$CONFIG_FILE" | head -n 1)"
    [[ -n "$source_exists" ]] || fail "Unknown --source-id '$SOURCE_ID_FILTER'"

    source_format="$(jq -r --arg sid "$SOURCE_ID_FILTER" '.sources[] | select(.id == $sid) | .format' "$CONFIG_FILE" | head -n 1)"
    [[ "$source_format" == "netex" ]] || fail "Source '$SOURCE_ID_FILTER' format is '$source_format' (expected netex)"
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

  local source_json source_id
  local successful_sources=()
  local failed_sources=()
  for source_json in "${sources[@]}"; do
    source_id="$(jq -r '.id' <<<"$source_json")"
    if ingest_source "$source_json"; then
      successful_sources+=("$source_id")
    else
      failed_sources+=("$source_id")
      warn "Continuing after source failure: '$source_id'"
    fi
  done

  if [[ ${#successful_sources[@]} -eq 0 ]]; then
    fail "No selected NeTEx sources were ingested successfully"
  fi

  if [[ ${#failed_sources[@]} -gt 0 ]]; then
    warn "NeTEx ingest completed with warnings: succeeded=${#successful_sources[@]} failed=${#failed_sources[@]} failed_sources=${failed_sources[*]}"
  else
    log "All selected NeTEx sources ingested successfully"
  fi
  return 0
}

main "$@"
