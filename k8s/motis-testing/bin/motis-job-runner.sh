#!/bin/sh
set -eu

WORK_DIR="${WORK_DIR:-/work}"
GTFS_FILE="${GTFS_FILE:-}"
OSM_FILE="${OSM_FILE:-}"
RESULT_FILE="${RESULT_FILE:-/work/test-result}"
TEST_TIMEOUT_SEC="${TEST_TIMEOUT_SEC:-900}"
SERVER_LOG="${SERVER_LOG:-/work/motis-server.log}"

log() {
  printf '[motis-job-runner] %s\n' "$*"
}

require_file() {
  path="$1"
  label="$2"
  if [ ! -f "$path" ]; then
    log "ERROR: missing ${label}: ${path}"
    exit 2
  fi
}

cleanup() {
  if [ "${MOTIS_PID:-}" != "" ]; then
    kill "$MOTIS_PID" >/dev/null 2>&1 || true
    wait "$MOTIS_PID" >/dev/null 2>&1 || true
  fi
}

trap cleanup EXIT INT TERM

if [ -z "$GTFS_FILE" ] || [ -z "$OSM_FILE" ]; then
  log "ERROR: GTFS_FILE and OSM_FILE are required"
  exit 2
fi

require_file "$GTFS_FILE" "GTFS zip"
require_file "$OSM_FILE" "OSM PBF"

mkdir -p "$WORK_DIR"
cp "$GTFS_FILE" "$WORK_DIR/active-gtfs.zip"
cp "$OSM_FILE" "$WORK_DIR/osm.pbf"
rm -f "$RESULT_FILE"

cd "$WORK_DIR"
log "Generating MOTIS config..."
/motis config "$WORK_DIR/osm.pbf" "$WORK_DIR/active-gtfs.zip"

if [ -f "$WORK_DIR/config.yml" ]; then
  log "Applying MVP-safe MOTIS config patch..."
  awk '
    BEGIN { skip_tiles = 0 }
    {
      if ($0 ~ /^tiles:[[:space:]]*$/) { skip_tiles = 1; next }
      if (skip_tiles == 1) {
        if ($0 ~ /^[^[:space:]].*:[[:space:]]*.*$/) { skip_tiles = 0 } else { next }
      }
      if ($0 ~ /^street_routing:[[:space:]]*true([[:space:]]*)$/) sub(/true/, "false")
      if ($0 ~ /^geocoding:[[:space:]]*true([[:space:]]*)$/) sub(/true/, "false")
      if ($0 ~ /^reverse_geocoding:[[:space:]]*true([[:space:]]*)$/) sub(/true/, "false")
      print
    }
  ' "$WORK_DIR/config.yml" > "$WORK_DIR/config.yml.tmp"
  mv "$WORK_DIR/config.yml.tmp" "$WORK_DIR/config.yml"
fi

log "Importing GTFS into MOTIS data directory..."
/motis import

log "Starting MOTIS server..."
/motis server "$WORK_DIR" >"$SERVER_LOG" 2>&1 &
MOTIS_PID="$!"

start_ts="$(date +%s)"
while [ ! -f "$RESULT_FILE" ]; do
  now_ts="$(date +%s)"
  elapsed="$((now_ts - start_ts))"
  if [ "$elapsed" -ge "$TEST_TIMEOUT_SEC" ]; then
    log "ERROR: timed out waiting for tester result (${TEST_TIMEOUT_SEC}s)"
    printf '1 timeout waiting for tester\n' > "$RESULT_FILE"
    break
  fi
  if ! kill -0 "$MOTIS_PID" >/dev/null 2>&1; then
    log "ERROR: MOTIS server exited unexpectedly before tests finished"
    printf '1 motis server exited unexpectedly\n' > "$RESULT_FILE"
    break
  fi
  sleep 2
done

result_code="$(awk 'NR==1 { print $1 }' "$RESULT_FILE" 2>/dev/null || true)"
if [ "$result_code" = "0" ]; then
  log "Tester signaled success."
  exit 0
fi

log "Tester signaled failure."
if [ -f "$RESULT_FILE" ]; then
  log "Result details: $(cat "$RESULT_FILE")"
fi
if [ -f "$SERVER_LOG" ]; then
  log "MOTIS log tail:"
  tail -n 120 "$SERVER_LOG" || true
fi
exit 1
