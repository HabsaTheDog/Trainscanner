#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OJP_CONFIG_FILE="${OJP_ENDPOINTS_CONFIG:-${ROOT_DIR}/config/ojp-endpoints.json}"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib-db.sh"

COUNTRY_FILTER=""
PROVIDER_ID=""
CASE_INDEX="0"
FROM_REF=""
TO_REF=""
DEPARTURE_TIME=""
FROM_CANONICAL_ID=""
TO_CANONICAL_ID=""
OUTPUT_FILE=""
TIMEOUT_OVERRIDE=""
TMP_FILES=()

usage() {
  cat <<USAGE
Usage: scripts/data/test-ojp-feeders.sh [options]

Probe configured OJP feeder endpoint for one test journey.

Options:
  --country DE|AT|CH          Restrict to one country (required if provider-id omitted)
  --provider-id ID            Probe one provider from config/ojp-endpoints.json
  --config PATH               Override OJP endpoints config file path
  --case-index N              Index into provider testCases array (default: 0)
  --from-ref REF              Override origin stop ref
  --to-ref REF                Override destination stop ref
  --departure-time ISO_TS     Override departure timestamp (e.g. 2026-02-20T08:00:00Z)
  --from-canonical-id ID      Resolve origin ref from ojp_stop_refs
  --to-canonical-id ID        Resolve destination ref from ojp_stop_refs
  --timeout-sec N             Override request timeout
  --output PATH               Write JSON report to file
  -h, --help                  Show this help

Auth env vars are read from each feeder's envPrefix in config/ojp-endpoints.json.
Examples (for envPrefix OJP_DE_PRIMARY):
  OJP_DE_PRIMARY_BEARER_TOKEN=...
  OJP_DE_PRIMARY_API_KEY=... (optional OJP_DE_PRIMARY_API_KEY_HEADER)
  OJP_DE_PRIMARY_USERNAME=... and OJP_DE_PRIMARY_PASSWORD=...
USAGE
  return 0
}

log() {
  printf '[test-ojp-feeders] %s\n' "$*"
  return 0
}

fail() {
  printf '[test-ojp-feeders] ERROR: %s\n' "$*" >&2
  exit 1
}

cleanup() {
  local f
  for f in "${TMP_FILES[@]}"; do
    [[ -n "$f" ]] && rm -f "$f" 2>/dev/null || true
  done
  return 0
}
trap cleanup EXIT

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "Missing required command: $1"
  return 0
}

is_iso_date_time() {
  local ts="$1"
  [[ "$ts" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$ ]] || return 1
  date -u -d "$ts" +"%Y-%m-%dT%H:%M:%SZ" >/dev/null 2>&1
  return 0
}

load_env() {
  local env_file
  for env_file in "${ROOT_DIR}/.env" "${ROOT_DIR}/.env.local"; do
    if [[ -f "$env_file" ]]; then
      set -a
      # shellcheck disable=SC1090,SC1091
      source "$env_file"
      set +a
    fi
  done
  return 0
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --country)
        [[ $# -ge 2 ]] || fail "Missing value for --country"
        COUNTRY_FILTER="$2"
        shift 2
        ;;
      --provider-id)
        [[ $# -ge 2 ]] || fail "Missing value for --provider-id"
        PROVIDER_ID="$2"
        shift 2
        ;;
      --config)
        [[ $# -ge 2 ]] || fail "Missing value for --config"
        OJP_CONFIG_FILE="$2"
        shift 2
        ;;
      --case-index)
        [[ $# -ge 2 ]] || fail "Missing value for --case-index"
        CASE_INDEX="$2"
        shift 2
        ;;
      --from-ref)
        [[ $# -ge 2 ]] || fail "Missing value for --from-ref"
        FROM_REF="$2"
        shift 2
        ;;
      --to-ref)
        [[ $# -ge 2 ]] || fail "Missing value for --to-ref"
        TO_REF="$2"
        shift 2
        ;;
      --departure-time)
        [[ $# -ge 2 ]] || fail "Missing value for --departure-time"
        DEPARTURE_TIME="$2"
        shift 2
        ;;
      --from-canonical-id)
        [[ $# -ge 2 ]] || fail "Missing value for --from-canonical-id"
        FROM_CANONICAL_ID="$2"
        shift 2
        ;;
      --to-canonical-id)
        [[ $# -ge 2 ]] || fail "Missing value for --to-canonical-id"
        TO_CANONICAL_ID="$2"
        shift 2
        ;;
      --output)
        [[ $# -ge 2 ]] || fail "Missing value for --output"
        OUTPUT_FILE="$2"
        shift 2
        ;;
      --timeout-sec)
        [[ $# -ge 2 ]] || fail "Missing value for --timeout-sec"
        TIMEOUT_OVERRIDE="$2"
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

  if [[ -n "$COUNTRY_FILTER" && "$COUNTRY_FILTER" != "DE" && "$COUNTRY_FILTER" != "AT" && "$COUNTRY_FILTER" != "CH" ]]; then
    fail "Invalid --country '$COUNTRY_FILTER' (expected DE, AT, or CH)"
  fi

  [[ "$CASE_INDEX" =~ ^[0-9]+$ ]] || fail "--case-index must be an integer"
  if [[ -n "$TIMEOUT_OVERRIDE" ]] && ! [[ "$TIMEOUT_OVERRIDE" =~ ^[0-9]+$ ]]; then
    fail "--timeout-sec must be an integer"
  fi

  if [[ -n "$DEPARTURE_TIME" ]] && ! is_iso_date_time "$DEPARTURE_TIME"; then
    fail "Invalid --departure-time '$DEPARTURE_TIME' (expected YYYY-MM-DDTHH:MM:SSZ)"
  fi
}

resolve_provider_json() {
  local provider_json

  [[ -f "$OJP_CONFIG_FILE" ]] || fail "OJP config file not found: $OJP_CONFIG_FILE"

  if [[ -n "$PROVIDER_ID" ]]; then
    provider_json="$(jq -ce --arg provider_id "$PROVIDER_ID" '
      .feeders[] | select(.providerId == $provider_id)
    ' "$OJP_CONFIG_FILE" || true)"
    [[ -n "$provider_json" ]] || fail "Provider '$PROVIDER_ID' not found in $OJP_CONFIG_FILE"
  else
    [[ -n "$COUNTRY_FILTER" ]] || fail "Provide either --provider-id or --country"
    provider_json="$(jq -ce --arg country "$COUNTRY_FILTER" '
      .feeders[] | select(.country == $country) | .
    ' "$OJP_CONFIG_FILE" | head -n 1 || true)"
    [[ -n "$provider_json" ]] || fail "No feeder provider configured for country '$COUNTRY_FILTER' in $OJP_CONFIG_FILE"
    PROVIDER_ID="$(jq -r '.providerId' <<<"$provider_json")"
  fi

  printf '%s\n' "$provider_json"
  return 0
}

resolve_ojp_ref_from_db() {
  local canonical_station_id="$1"
  local provider_id="$2"
  local country="$3"

  local canonical_esc provider_esc country_esc ref
  canonical_esc="$(db_sql_escape "$canonical_station_id")"
  provider_esc="$(db_sql_escape "$provider_id")"
  country_esc="$(db_sql_escape "$country")"

  ref="$(db_psql -At -c "
SELECT ojp_stop_ref
FROM ojp_stop_refs
WHERE canonical_station_id = '${canonical_esc}'
  AND provider_id = '${provider_esc}'
  AND country = '${country_esc}'
ORDER BY is_primary DESC, confidence_score DESC NULLS LAST, ojp_stop_ref ASC
LIMIT 1;
")"

  printf '%s\n' "$ref"
  return 0
}

build_auth_args() {
  local auth_mode="$1"
  local env_prefix="$2"
  AUTH_ARGS=()

  case "$auth_mode" in
    none)
      return 0
      ;;
    bearer)
      local token_var1 token_var2 token
      token_var1="${env_prefix}_BEARER_TOKEN"
      token_var2="${env_prefix}_TOKEN"
      token="${!token_var1:-${!token_var2:-}}"
      [[ -n "$token" ]] || fail "Missing auth token. Set ${token_var1} (or ${token_var2}) in .env.local (or .env)"
      AUTH_ARGS=(-H "Authorization: Bearer $token")
      ;;
    api_key)
      local key_var header_var api_key api_key_header
      key_var="${env_prefix}_API_KEY"
      header_var="${env_prefix}_API_KEY_HEADER"
      api_key="${!key_var:-}"
      api_key_header="${!header_var:-X-API-Key}"
      [[ -n "$api_key" ]] || fail "Missing API key. Set ${key_var} in .env.local (or .env)"
      AUTH_ARGS=(-H "${api_key_header}: ${api_key}")
      ;;
    basic)
      local user_var pass_var username password
      user_var="${env_prefix}_USERNAME"
      pass_var="${env_prefix}_PASSWORD"
      username="${!user_var:-}"
      password="${!pass_var:-}"
      [[ -n "$username" && -n "$password" ]] || fail "Missing basic auth credentials. Set ${user_var} and ${pass_var} in .env.local (or .env)"
      AUTH_ARGS=(-u "${username}:${password}")
      ;;
    header)
      local header_var header_value
      header_var="${env_prefix}_HEADER"
      header_value="${!header_var:-}"
      [[ -n "$header_value" ]] || fail "Missing header auth. Set ${header_var} in .env.local (or .env)"
      AUTH_ARGS=(-H "$header_value")
      ;;
    *)
      fail "Unsupported authMode '${auth_mode}' for provider '${PROVIDER_ID}'"
      ;;
  esac
  return 0
}

xml_escape() {
  local value="$1"
  value="${value//&/&amp;}"
  value="${value//</&lt;}"
  value="${value//>/&gt;}"
  value="${value//\"/&quot;}"
  value="${value//\'/&apos;}"
  printf '%s' "$value"
  return 0
}

main() {
  local provider_json provider_country endpoint_url auth_mode env_prefix timeout_sec
  local request_mode request_ts request_file response_file curl_status http_status
  local from_ref to_ref departure_time trip_count section_count error_text
  local report_json

  parse_args "$@"

  if [[ "$OJP_CONFIG_FILE" == "${ROOT_DIR}/config/ojp-endpoints.json" ]]; then
    "${ROOT_DIR}/scripts/validate-config.sh" --only ojp >/dev/null
  fi
  if [[ "$OJP_CONFIG_FILE" == "${ROOT_DIR}/config/ojp-endpoints.mock.json" ]]; then
    "${ROOT_DIR}/scripts/validate-config.sh" --only ojp-mock >/dev/null
  fi

  require_cmd jq
  require_cmd curl

  load_env

  provider_json="$(resolve_provider_json)"
  PROVIDER_ID="${PROVIDER_ID:-$(jq -r '.providerId // ""' <<<"$provider_json")}"
  provider_country="$(jq -r '.country' <<<"$provider_json")"
  endpoint_url="$(jq -r '.endpointUrl // ""' <<<"$provider_json")"
  auth_mode="$(jq -r '.authMode // "none"' <<<"$provider_json")"
  env_prefix="$(jq -r '.envPrefix // empty' <<<"$provider_json")"
  request_mode="$(jq -r '.requestMode // "ojp_xml_post"' <<<"$provider_json")"
  timeout_sec="$(jq -r '.timeoutSec // 25' <<<"$provider_json")"

  [[ "$provider_country" == "DE" || "$provider_country" == "AT" || "$provider_country" == "CH" ]] || fail "Provider '$PROVIDER_ID' has unsupported country '$provider_country' (must be DE|AT|CH)"
  [[ "$request_mode" == "ojp_xml_post" ]] || fail "Unsupported requestMode '$request_mode' for provider '$PROVIDER_ID'"

  if [[ -n "$COUNTRY_FILTER" && "$COUNTRY_FILTER" != "$provider_country" ]]; then
    fail "Requested country '$COUNTRY_FILTER' does not match provider country '$provider_country'"
  fi

  if [[ -z "$endpoint_url" || "$endpoint_url" == "null" ]]; then
    fail "Missing endpointUrl for provider '$PROVIDER_ID' in $OJP_CONFIG_FILE"
  fi
  if [[ "$endpoint_url" == *"example.invalid"* ]]; then
    fail "endpointUrl for provider '$PROVIDER_ID' is still a placeholder ('$endpoint_url')"
  fi

  [[ -n "$env_prefix" ]] || fail "Missing envPrefix for provider '$PROVIDER_ID' in $OJP_CONFIG_FILE"

  if [[ -n "$TIMEOUT_OVERRIDE" ]]; then
    timeout_sec="$TIMEOUT_OVERRIDE"
  fi

  from_ref="$FROM_REF"
  to_ref="$TO_REF"
  departure_time="$DEPARTURE_TIME"

  if [[ -n "$FROM_CANONICAL_ID" || -n "$TO_CANONICAL_ID" ]]; then
    [[ -n "$FROM_CANONICAL_ID" && -n "$TO_CANONICAL_ID" ]] || fail "Provide both --from-canonical-id and --to-canonical-id"
    db_load_env
    db_resolve_connection
    db_ensure_ready
    "${SCRIPT_DIR}/db-migrate.sh" --quiet
    from_ref="$(resolve_ojp_ref_from_db "$FROM_CANONICAL_ID" "$PROVIDER_ID" "$provider_country")"
    to_ref="$(resolve_ojp_ref_from_db "$TO_CANONICAL_ID" "$PROVIDER_ID" "$provider_country")"
    [[ -n "$from_ref" ]] || fail "No ojp_stop_refs mapping for from canonical station '$FROM_CANONICAL_ID' (provider '$PROVIDER_ID')"
    [[ -n "$to_ref" ]] || fail "No ojp_stop_refs mapping for to canonical station '$TO_CANONICAL_ID' (provider '$PROVIDER_ID')"
  fi

  if [[ -z "$from_ref" || -z "$to_ref" ]]; then
    local case_json
    case_json="$(jq -ce --argjson idx "$CASE_INDEX" '.testCases[$idx] // empty' <<<"$provider_json" || true)"
    [[ -n "$case_json" ]] || fail "No test case available for provider '$PROVIDER_ID' at index ${CASE_INDEX}; set --from-ref/--to-ref or update config"
    [[ -n "$from_ref" ]] || from_ref="$(jq -r '.fromRef // ""' <<<"$case_json")"
    [[ -n "$to_ref" ]] || to_ref="$(jq -r '.toRef // ""' <<<"$case_json")"
    [[ -n "$departure_time" ]] || departure_time="$(jq -r '.departureTime // ""' <<<"$case_json")"
  fi

  [[ -n "$from_ref" ]] || fail "Missing origin stop ref. Set --from-ref or configure testCases.fromRef"
  [[ -n "$to_ref" ]] || fail "Missing destination stop ref. Set --to-ref or configure testCases.toRef"

  if [[ -z "$departure_time" ]]; then
    departure_time="$(date -u -d '+1 hour' +%Y-%m-%dT%H:%M:%SZ)"
  fi
  is_iso_date_time "$departure_time" || fail "Invalid departure time '$departure_time' (expected YYYY-MM-DDTHH:MM:SSZ)"

  build_auth_args "$auth_mode" "$env_prefix"

  request_ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  request_file="$(mktemp)"
  response_file="$(mktemp)"
  TMP_FILES+=("$request_file" "$response_file")

  cat > "$request_file" <<XML
<?xml version="1.0" encoding="UTF-8"?>
<OJP xmlns="http://www.vdv.de/ojp" version="2.0">
  <OJPRequest>
    <ServiceRequest>
      <RequestTimestamp>$(xml_escape "$request_ts")</RequestTimestamp>
      <RequestorRef>trainscanner-mvp</RequestorRef>
      <OJPTripRequest>
        <RequestTimestamp>$(xml_escape "$request_ts")</RequestTimestamp>
        <Origin>
          <PlaceRef>
            <StopPlaceRef>$(xml_escape "$from_ref")</StopPlaceRef>
          </PlaceRef>
          <DepArrTime>$(xml_escape "$departure_time")</DepArrTime>
        </Origin>
        <Destination>
          <PlaceRef>
            <StopPlaceRef>$(xml_escape "$to_ref")</StopPlaceRef>
          </PlaceRef>
        </Destination>
        <Params>
          <NumberOfResults>5</NumberOfResults>
        </Params>
      </OJPTripRequest>
    </ServiceRequest>
  </OJPRequest>
</OJP>
XML

  log "Sending OJP feeder probe to provider=${PROVIDER_ID} country=${provider_country}"
  set +e
  http_status="$(curl -sS \
    --max-time "$timeout_sec" \
    -H 'Content-Type: application/xml; charset=utf-8' \
    "${AUTH_ARGS[@]}" \
    -o "$response_file" \
    -w '%{http_code}' \
    -X POST \
    --data-binary "@${request_file}" \
    "$endpoint_url" 2>&1)"
  curl_status=$?
  set -e

  if [[ $curl_status -ne 0 ]]; then
    fail "Request failed for provider '$PROVIDER_ID' (curl exit ${curl_status}): ${http_status}"
  fi

  if ! [[ "$http_status" =~ ^[0-9]{3}$ ]]; then
    fail "Unexpected HTTP status output for provider '$PROVIDER_ID': $http_status"
  fi

  if [[ "$http_status" == "401" || "$http_status" == "403" ]]; then
    fail "Auth failed for provider '$PROVIDER_ID' (HTTP ${http_status}). Check ${env_prefix} auth vars in .env.local (or .env)"
  fi

  if (( http_status >= 400 )); then
    error_text="$(tr -d '\n' < "$response_file" | sed -E 's/.*<ErrorText>([^<]+)<\/ErrorText>.*/\1/' | head -c 200)"
    [[ -n "$error_text" ]] || error_text="No <ErrorText> found in response"
    fail "Endpoint returned HTTP ${http_status} for provider '$PROVIDER_ID' (${error_text})"
  fi

  trip_count="$(grep -o '<TripResult' "$response_file" | wc -l | tr -d ' ')"
  section_count="$(grep -o '<TripSection' "$response_file" | wc -l | tr -d ' ')"
  error_text="$(tr -d '\n' < "$response_file" | sed -E 's/.*<ErrorText>([^<]+)<\/ErrorText>.*/\1/' | head -c 300)"
  if [[ "$error_text" == "" || "$error_text" == "$(tr -d '\n' < "$response_file" | head -c 300)" ]]; then
    error_text=""
  fi

  report_json="$(jq -cn \
    --arg providerId "$PROVIDER_ID" \
    --arg country "$provider_country" \
    --arg endpointUrl "$endpoint_url" \
    --arg authMode "$auth_mode" \
    --arg fromRef "$from_ref" \
    --arg toRef "$to_ref" \
    --arg departureTime "$departure_time" \
    --arg requestTimestamp "$request_ts" \
    --argjson timeoutSec "$timeout_sec" \
    --argjson httpStatus "$http_status" \
    --argjson tripResultCount "$trip_count" \
    --argjson tripSectionCount "$section_count" \
    --arg errorText "$error_text" \
    '{
      ok: true,
      providerId: $providerId,
      country: $country,
      endpointUrl: $endpointUrl,
      authMode: $authMode,
      timeoutSec: $timeoutSec,
      request: {
        fromRef: $fromRef,
        toRef: $toRef,
        departureTime: $departureTime,
        requestTimestamp: $requestTimestamp
      },
      response: {
        httpStatus: $httpStatus,
        tripResultCount: $tripResultCount,
        tripSectionCount: $tripSectionCount,
        errorText: (if $errorText == "" then null else $errorText end)
      }
    }')"

  if [[ -n "$OUTPUT_FILE" ]]; then
    mkdir -p "$(dirname "$OUTPUT_FILE")"
    printf '%s\n' "$report_json" > "$OUTPUT_FILE"
    log "Wrote JSON report: $OUTPUT_FILE"

    local response_copy
    response_copy="${OUTPUT_FILE%.json}.raw.xml"
    cp "$response_file" "$response_copy"
    log "Wrote raw response XML: $response_copy"
  fi

  printf '%s\n' "$report_json" | jq .
  return 0
}

main "$@"
