#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ZIP_PATH=""

usage() {
  cat <<USAGE
Usage: scripts/qa/validate-export.sh --zip <path>

Validate GTFS runtime export artifact.

Options:
  --zip <path>             GTFS zip path (required)
  -h, --help               Show this help

Optional feed validator hook:
  Set GTFS_FEED_VALIDATOR_CMD with '{zip}' placeholder, e.g.
    GTFS_FEED_VALIDATOR_CMD='gtfs-validator --input {zip} --output /tmp/gtfs-report'
USAGE
  return 0
}

log() {
  printf '[validate-export] %s\n' "$*"
  return 0
}

fail() {
  printf '[validate-export] ERROR: %s\n' "$*" >&2
  return 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --zip)
      ZIP_PATH="${2:-}"
      shift 2
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

[[ -n "$ZIP_PATH" ]] || fail "Missing required --zip"

if [[ "$ZIP_PATH" != /* ]]; then
  ZIP_PATH="$(cd "${SCRIPT_DIR}/../.." && pwd)/${ZIP_PATH}"
fi

[[ -f "$ZIP_PATH" ]] || fail "Zip not found: $ZIP_PATH"
command -v python3 >/dev/null 2>&1 || fail "python3 is required"

python3 - <<'PY' "$ZIP_PATH"
import csv
import io
import sys
import zipfile

zip_path = sys.argv[1]

required_files = [
    "agency.txt",
    "stops.txt",
    "routes.txt",
    "trips.txt",
    "stop_times.txt",
]

required_columns = {
    "agency.txt": {"agency_id", "agency_name", "agency_url", "agency_timezone"},
    "stops.txt": {"stop_id", "stop_name", "stop_lat", "stop_lon"},
    "routes.txt": {"route_id", "agency_id", "route_type"},
    "trips.txt": {"route_id", "service_id", "trip_id"},
    "stop_times.txt": {"trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"},
    "calendar.txt": {
        "service_id",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
        "start_date",
        "end_date",
    },
    "calendar_dates.txt": {"service_id", "date", "exception_type"},
    "transfers.txt": {"from_stop_id", "to_stop_id", "transfer_type"},
}


def fail(msg: str):
    print(f"[validate-export] ERROR: {msg}", file=sys.stderr)
    raise SystemExit(1)


with zipfile.ZipFile(zip_path, "r") as zf:
    names = set(zf.namelist())

    for filename in required_files:
        if filename not in names:
            fail(f"missing required file in zip: {filename}")

    has_calendar = "calendar.txt" in names
    has_calendar_dates = "calendar_dates.txt" in names
    if not has_calendar and not has_calendar_dates:
        fail("missing service calendar file: require calendar.txt and/or calendar_dates.txt")

    table_rows = {}
    for filename in required_files + ["calendar.txt", "calendar_dates.txt", "transfers.txt"]:
        if filename not in names:
            continue

        raw = zf.read(filename)
        if not raw:
            fail(f"file is empty: {filename}")

        text = raw.decode("utf-8-sig")
        lines = [line for line in text.splitlines() if line.strip()]
        min_lines = 2
        if filename == "calendar_dates.txt" and has_calendar:
            min_lines = 1
        if len(lines) < min_lines:
            fail(f"file has no data rows: {filename}")

        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
        if not reader.fieldnames:
            fail(f"file has no CSV header: {filename}")

        cols = set(reader.fieldnames)
        missing_cols = required_columns.get(filename, set()) - cols
        if missing_cols:
            fail(f"file {filename} missing columns: {', '.join(sorted(missing_cols))}")

        table_rows[filename] = rows

    agencies = {r["agency_id"] for r in table_rows["agency.txt"] if r.get("agency_id")}
    stops = {r["stop_id"] for r in table_rows["stops.txt"] if r.get("stop_id")}
    routes = {r["route_id"] for r in table_rows["routes.txt"] if r.get("route_id")}
    trips = {r["trip_id"] for r in table_rows["trips.txt"] if r.get("trip_id")}

    if not agencies or not stops or not routes or not trips:
        fail("mandatory GTFS entities are empty")

    for route in table_rows["routes.txt"]:
        if route.get("agency_id") not in agencies:
            fail(f"routes.txt references missing agency_id '{route.get('agency_id')}'")

    service_ids = set()
    if "calendar.txt" in table_rows:
        service_ids.update(r["service_id"] for r in table_rows["calendar.txt"] if r.get("service_id"))
    if "calendar_dates.txt" in table_rows:
        service_ids.update(r["service_id"] for r in table_rows["calendar_dates.txt"] if r.get("service_id"))

    for trip in table_rows["trips.txt"]:
        if trip.get("route_id") not in routes:
            fail(f"trips.txt references missing route_id '{trip.get('route_id')}'")
        if trip.get("service_id") not in service_ids:
            fail(f"trips.txt references missing service_id '{trip.get('service_id')}'")

    seen_trip_stop_times = set()
    for st in table_rows["stop_times.txt"]:
        trip_id = st.get("trip_id")
        stop_id = st.get("stop_id")
        if trip_id not in trips:
            fail(f"stop_times.txt references missing trip_id '{trip_id}'")
        if stop_id not in stops:
            fail(f"stop_times.txt references missing stop_id '{stop_id}'")
        seen_trip_stop_times.add(trip_id)

    missing_stop_times = sorted(trips - seen_trip_stop_times)
    if missing_stop_times:
        fail(f"trip(s) without stop_times: {', '.join(missing_stop_times[:10])}")

    if "transfers.txt" in table_rows:
        for transfer in table_rows["transfers.txt"]:
            from_stop_id = transfer.get("from_stop_id")
            to_stop_id = transfer.get("to_stop_id")
            if from_stop_id not in stops:
                fail(f"transfers.txt references missing from_stop_id '{from_stop_id}'")
            if to_stop_id not in stops:
                fail(f"transfers.txt references missing to_stop_id '{to_stop_id}'")

print("[validate-export] Core GTFS checks passed")
PY

if [[ -n "${GTFS_FEED_VALIDATOR_CMD:-}" ]]; then
  VALIDATOR_CMD="${GTFS_FEED_VALIDATOR_CMD//\{zip\}/${ZIP_PATH}}"
  log "Running optional feed-validator hook"
  bash -lc "$VALIDATOR_CMD"
else
  log "No optional feed-validator hook configured (set GTFS_FEED_VALIDATOR_CMD to enable)"
fi

log "Validation completed: $ZIP_PATH"
