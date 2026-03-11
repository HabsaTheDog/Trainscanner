#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

PROFILE=""
AS_OF=""
COUNTRY_FILTER=""
SOURCE_FILTER=""
BATCH_SIZE_TRIPS="25000"
OUTPUT_PATH=""
FORCE_REBUILD="false"
TIER="all"
LOW_MEMORY_MODE="false"
EXPORT_PGOPTIONS=""
QUERY_MODE="optimized"
BENCHMARK_MAX_SOURCES="0"
BENCHMARK_MAX_BATCHES="0"
BENCHMARK_MAX_TRIPS="0"
SQL_PROFILE_SAMPLE="false"
PROGRESS_INTERVAL_SEC="20"
PARALLEL_GATHER_WORKERS=""

usage() {
  cat <<USAGE
Usage: scripts/qa/build-profile.sh --profile <name> --as-of YYYY-MM-DD [options]

Build deterministic pan-European GTFS runtime artifact from global timetable facts.

Options:
  --profile <name>        Profile name in config/gtfs-profiles.json (required)
  --as-of YYYY-MM-DD      Snapshot cutoff date (required)
  --country <ISO2>        Optional ISO country filter
  --source-id <id>        Optional source id filter (for segmented exports)
  --batch-size-trips <n>  Trip IDs per DB batch (default: 25000)
  --tier <name>           high-speed|regional|local|all (default: all)
  --output <path>         Optional output zip path
  --low-memory            Apply conservative DB memory settings for export
  --pgoptions "<opts>"    Override Postgres PGOPTIONS passed to exporter
  --query-mode <mode>     legacy|optimized batch SQL mode (default: optimized)
  --benchmark-max-sources Benchmark cap for number of sources (default: 0 = disabled)
  --benchmark-max-batches Benchmark cap for number of fetched batches (default: 0 = disabled)
  --benchmark-max-trips   Benchmark cap for number of fetched trips (default: 0 = disabled)
  --sql-profile-sample    Run one EXPLAIN ANALYZE sample for batch query
  --progress-interval-sec Seconds between periodic exporter progress logs (default: 20, 0 disables periodic logs)
  --parallel-gather-workers <n>
                          Set Postgres max_parallel_workers_per_gather for exporter (default: unchanged)
  --force                 Rebuild even if existing artifact+manifest match
  -h, --help              Show this help
USAGE
  return 0
}

log() {
  printf '[build-profile] %s\n' "$*"
  return 0
}

fail() {
  printf '[build-profile] ERROR: %s\n' "$*" >&2
  return 1
}

is_iso_date() {
  local d="$1"
  [[ "$d" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || return 1
  date -u -d "$d" +%F >/dev/null 2>&1
}

sha256_file() {
  local file="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file" | awk '{print $1}'
    return
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$file" | awk '{print $1}'
    return
  fi
  fail "Neither sha256sum nor shasum is available"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="${2:-}"
      shift 2
      ;;
    --as-of)
      AS_OF="${2:-}"
      shift 2
      ;;
    --country)
      COUNTRY_FILTER="$(printf '%s' "${2:-}" | tr '[:lower:]' '[:upper:]')"
      shift 2
      ;;
    --source-id)
      SOURCE_FILTER="${2:-}"
      shift 2
      ;;
    --batch-size-trips)
      BATCH_SIZE_TRIPS="${2:-25000}"
      shift 2
      ;;
    --tier)
      TIER="${2:-all}"
      shift 2
      ;;
    --output)
      OUTPUT_PATH="${2:-}"
      shift 2
      ;;
    --low-memory)
      LOW_MEMORY_MODE="true"
      shift
      ;;
    --pgoptions)
      EXPORT_PGOPTIONS="${2:-}"
      shift 2
      ;;
    --query-mode)
      QUERY_MODE="${2:-optimized}"
      shift 2
      ;;
    --benchmark-max-sources)
      BENCHMARK_MAX_SOURCES="${2:-0}"
      shift 2
      ;;
    --benchmark-max-batches)
      BENCHMARK_MAX_BATCHES="${2:-0}"
      shift 2
      ;;
    --benchmark-max-trips)
      BENCHMARK_MAX_TRIPS="${2:-0}"
      shift 2
      ;;
    --sql-profile-sample)
      SQL_PROFILE_SAMPLE="true"
      shift
      ;;
    --progress-interval-sec)
      PROGRESS_INTERVAL_SEC="${2:-20}"
      shift 2
      ;;
    --parallel-gather-workers)
      PARALLEL_GATHER_WORKERS="${2:-}"
      shift 2
      ;;
    --force)
      FORCE_REBUILD="true"
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

[[ -n "$PROFILE" ]] || fail "Missing required --profile"
[[ -n "$AS_OF" ]] || fail "Missing required --as-of"
is_iso_date "$AS_OF" || fail "Invalid --as-of value '$AS_OF' (expected YYYY-MM-DD)"

if [[ -n "$COUNTRY_FILTER" && ! "$COUNTRY_FILTER" =~ ^[A-Z]{2}$ ]]; then
  fail "Invalid --country '$COUNTRY_FILTER' (expected ISO-3166 alpha-2)"
fi
if [[ -n "$SOURCE_FILTER" && ! "$SOURCE_FILTER" =~ ^[A-Za-z0-9._:-]+$ ]]; then
  fail "Invalid --source-id '$SOURCE_FILTER' (allowed: A-Z a-z 0-9 . _ : -)"
fi
if [[ ! "$BATCH_SIZE_TRIPS" =~ ^[0-9]+$ ]] || [[ "$BATCH_SIZE_TRIPS" -le 0 ]]; then
  fail "Invalid --batch-size-trips '$BATCH_SIZE_TRIPS' (expected integer > 0)"
fi

case "$TIER" in
  high-speed|regional|local|all) ;;
  *)
    fail "Invalid --tier '$TIER' (expected high-speed|regional|local|all)"
    ;;
esac

case "$QUERY_MODE" in
  legacy|optimized) ;;
  *)
    fail "Invalid --query-mode '$QUERY_MODE' (expected legacy|optimized)"
    ;;
esac

for _bench_value in "$BENCHMARK_MAX_SOURCES" "$BENCHMARK_MAX_BATCHES" "$BENCHMARK_MAX_TRIPS"; do
  if [[ ! "$_bench_value" =~ ^[0-9]+$ ]]; then
    fail "Benchmark limits must be integers >= 0"
  fi
done
if [[ ! "$PROGRESS_INTERVAL_SEC" =~ ^[0-9]+$ ]]; then
  fail "Invalid --progress-interval-sec '$PROGRESS_INTERVAL_SEC' (expected integer >= 0)"
fi
if [[ -n "$PARALLEL_GATHER_WORKERS" && ! "$PARALLEL_GATHER_WORKERS" =~ ^[0-9]+$ ]]; then
  fail "Invalid --parallel-gather-workers '$PARALLEL_GATHER_WORKERS' (expected integer >= 0)"
fi

command -v python3 >/dev/null 2>&1 || fail "python3 is required"
command -v jq >/dev/null 2>&1 || fail "jq is required"

if [[ -n "$OUTPUT_PATH" ]]; then
  if [[ -d "$OUTPUT_PATH" || "$OUTPUT_PATH" == */ ]]; then
    OUTPUT_ZIP_PATH="${OUTPUT_PATH%/}/active-gtfs.zip"
  else
    OUTPUT_ZIP_PATH="$OUTPUT_PATH"
  fi
else
  OUTPUT_ZIP_PATH="data/gtfs/runtime/${PROFILE}/${AS_OF}/active-gtfs.zip"
fi

if [[ "$OUTPUT_ZIP_PATH" != /* ]]; then
  OUTPUT_ZIP_PATH="${ROOT_DIR}/${OUTPUT_ZIP_PATH}"
fi

OUTPUT_DIR="$(dirname "$OUTPUT_ZIP_PATH")"
if [[ "$(basename "$OUTPUT_ZIP_PATH")" == "active-gtfs.zip" ]]; then
  MANIFEST_PATH="${OUTPUT_DIR}/manifest.json"
else
  MANIFEST_PATH="${OUTPUT_ZIP_PATH%.zip}.manifest.json"
fi
mkdir -p "$OUTPUT_DIR"

if [[ "$FORCE_REBUILD" != "true" && -f "$OUTPUT_ZIP_PATH" && -f "$MANIFEST_PATH" ]]; then
  EXISTING_PROFILE="$(jq -r '.profile // empty' "$MANIFEST_PATH")"
  EXISTING_AS_OF="$(jq -r '.asOf // empty' "$MANIFEST_PATH")"
  EXISTING_COUNTRY="$(jq -r '.countryScope // empty' "$MANIFEST_PATH")"
  EXISTING_SOURCE="$(jq -r '.sourceScope // empty' "$MANIFEST_PATH")"
  EXISTING_TIER="$(jq -r '.tier // empty' "$MANIFEST_PATH")"
  EXISTING_SHA="$(jq -r '.sha256 // empty' "$MANIFEST_PATH")"
  CURRENT_SHA="$(sha256_file "$OUTPUT_ZIP_PATH")"
  if [[ "$EXISTING_PROFILE" == "$PROFILE" && "$EXISTING_AS_OF" == "$AS_OF" && "$EXISTING_COUNTRY" == "${COUNTRY_FILTER}" && "$EXISTING_SOURCE" == "${SOURCE_FILTER}" && "$EXISTING_TIER" == "$TIER" && "$EXISTING_SHA" == "$CURRENT_SHA" ]]; then
    log "Idempotent export hit: existing artifact+manifest already match requested scope"
    log "artifact=${OUTPUT_ZIP_PATH}"
    log "manifest=${MANIFEST_PATH}"
    log "sha256=${CURRENT_SHA}"
    exit 0
  fi
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

SUMMARY_JSON="${TMP_DIR}/export-summary.json"

EXPORT_ARGS=(
  "${ROOT_DIR}/scripts/qa/export-canonical-gtfs.py"
  --from-db
  --profile "$PROFILE"
  --as-of "$AS_OF"
  --tier "$TIER"
  --query-mode "$QUERY_MODE"
  --batch-size-trips "$BATCH_SIZE_TRIPS"
  --benchmark-max-sources "$BENCHMARK_MAX_SOURCES"
  --benchmark-max-batches "$BENCHMARK_MAX_BATCHES"
  --benchmark-max-trips "$BENCHMARK_MAX_TRIPS"
  --progress-interval-sec "$PROGRESS_INTERVAL_SEC"
  --output-zip "$OUTPUT_ZIP_PATH"
  --summary-json "$SUMMARY_JSON"
)

if [[ -n "$COUNTRY_FILTER" ]]; then
  EXPORT_ARGS+=(--country "$COUNTRY_FILTER")
fi
if [[ -n "$SOURCE_FILTER" ]]; then
  EXPORT_ARGS+=(--source-id "$SOURCE_FILTER")
fi
if [[ "$SQL_PROFILE_SAMPLE" == "true" ]]; then
  EXPORT_ARGS+=(--sql-profile-sample)
fi

log "Exporting pan-European artifact profile='${PROFILE}' as-of='${AS_OF}' tier='${TIER}' country='${COUNTRY_FILTER:-ALL}' source='${SOURCE_FILTER:-ALL}' batch-size='${BATCH_SIZE_TRIPS}' query-mode='${QUERY_MODE}' bench[sources=${BENCHMARK_MAX_SOURCES},batches=${BENCHMARK_MAX_BATCHES},trips=${BENCHMARK_MAX_TRIPS}] progress-interval='${PROGRESS_INTERVAL_SEC}'"
if [[ -z "$EXPORT_PGOPTIONS" && "$LOW_MEMORY_MODE" == "true" ]]; then
  EXPORT_PGOPTIONS="-c work_mem=8MB -c maintenance_work_mem=32MB -c max_parallel_workers_per_gather=0 -c temp_buffers=8MB"
fi
if [[ -n "$PARALLEL_GATHER_WORKERS" ]]; then
  if [[ -n "$EXPORT_PGOPTIONS" ]]; then
    EXPORT_PGOPTIONS="${EXPORT_PGOPTIONS} -c max_parallel_workers_per_gather=${PARALLEL_GATHER_WORKERS}"
  else
    EXPORT_PGOPTIONS="-c max_parallel_workers_per_gather=${PARALLEL_GATHER_WORKERS} -c max_parallel_workers=8"
  fi
fi

if [[ -n "$EXPORT_PGOPTIONS" ]]; then
  log "Using exporter PGOPTIONS: ${EXPORT_PGOPTIONS}"
  EXPORT_GTFS_PGOPTIONS="$EXPORT_PGOPTIONS" python3 "${EXPORT_ARGS[@]}"
else
  python3 "${EXPORT_ARGS[@]}"
fi

[[ -f "$OUTPUT_ZIP_PATH" ]] || fail "Expected output zip missing: $OUTPUT_ZIP_PATH"
[[ -f "$SUMMARY_JSON" ]] || fail "Expected summary JSON missing: $SUMMARY_JSON"

ZIP_SHA="$(sha256_file "$OUTPUT_ZIP_PATH")"

jq -n \
  --arg profile "$PROFILE" \
  --arg asOf "$AS_OF" \
  --arg tier "$TIER" \
  --arg countryScope "$COUNTRY_FILTER" \
  --arg sourceScope "$SOURCE_FILTER" \
  --arg queryMode "$QUERY_MODE" \
  --arg batchSizeTrips "$BATCH_SIZE_TRIPS" \
  --arg benchmarkMaxSources "$BENCHMARK_MAX_SOURCES" \
  --arg benchmarkMaxBatches "$BENCHMARK_MAX_BATCHES" \
  --arg benchmarkMaxTrips "$BENCHMARK_MAX_TRIPS" \
  --arg progressIntervalSec "$PROGRESS_INTERVAL_SEC" \
  --arg parallelGatherWorkers "$PARALLEL_GATHER_WORKERS" \
  --arg zipPath "$OUTPUT_ZIP_PATH" \
  --arg summaryPath "$SUMMARY_JSON" \
  --arg sha256 "$ZIP_SHA" \
  --arg builtAt "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
  --argjson summary "$(cat "$SUMMARY_JSON")" \
  '{
    profile: $profile,
    asOf: $asOf,
    tier: $tier,
    countryScope: $countryScope,
    sourceScope: $sourceScope,
    queryMode: $queryMode,
    batchSizeTrips: $batchSizeTrips,
    benchmark: {
      maxSources: ($benchmarkMaxSources | tonumber),
      maxBatches: ($benchmarkMaxBatches | tonumber),
      maxTrips: ($benchmarkMaxTrips | tonumber)
    },
    progressIntervalSec: ($progressIntervalSec | tonumber),
    parallelGatherWorkers: (if $parallelGatherWorkers == "" then null else ($parallelGatherWorkers | tonumber) end),
    zipPath: $zipPath,
    summaryPath: $summaryPath,
    sha256: $sha256,
    builtAt: $builtAt,
    summary: $summary
  }' > "$MANIFEST_PATH"

log "artifact=${OUTPUT_ZIP_PATH}"
log "manifest=${MANIFEST_PATH}"
log "sha256=${ZIP_SHA}"
