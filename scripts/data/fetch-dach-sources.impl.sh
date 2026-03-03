#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CONFIG_FILE="config/dach-data-sources.json"
AS_OF=""
COUNTRY_FILTER=""
SOURCE_ID_FILTER=""
TEMP_FILES=()
FETCH_PROGRESS_FILE="${FETCH_PROGRESS_FILE:-}"
FETCH_PROGRESS_SOURCE_ID=""
FETCH_PROGRESS_SOURCE_INDEX=0
FETCH_PROGRESS_TOTAL_SOURCES=0
FETCH_PROGRESS_FILE_NAME=""
FETCH_PROGRESS_DOWNLOADED_BYTES=0
FETCH_PROGRESS_TOTAL_BYTES=0

cleanup_temp_files() {
  local f
  for f in "${TEMP_FILES[@]}"; do
    [[ -n "$f" ]] && rm -f "$f" 2>/dev/null || true
  done
  return 0
}
trap cleanup_temp_files EXIT

write_fetch_progress() {
  local stage="${1:-running}"
  local source_id="${2:-$FETCH_PROGRESS_SOURCE_ID}"
  local source_index="${3:-$FETCH_PROGRESS_SOURCE_INDEX}"
  local total_sources="${4:-$FETCH_PROGRESS_TOTAL_SOURCES}"
  local file_name="${5:-$FETCH_PROGRESS_FILE_NAME}"
  local downloaded_bytes="${6:-$FETCH_PROGRESS_DOWNLOADED_BYTES}"
  local total_bytes="${7:-$FETCH_PROGRESS_TOTAL_BYTES}"
  local message="${8:-}"

  [[ -n "$FETCH_PROGRESS_FILE" ]] || return 0
  command -v jq >/dev/null 2>&1 || return 0

  [[ "$source_index" =~ ^[0-9]+$ ]] || source_index=0
  [[ "$total_sources" =~ ^[0-9]+$ ]] || total_sources=0
  [[ "$downloaded_bytes" =~ ^[0-9]+$ ]] || downloaded_bytes=0
  [[ "$total_bytes" =~ ^[0-9]+$ ]] || total_bytes=0

  local updated_at tmp_progress_file
  updated_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  tmp_progress_file="${FETCH_PROGRESS_FILE}.tmp.$$"

  jq -n \
    --arg stage "$stage" \
    --arg sourceId "$source_id" \
    --argjson sourceIndex "$source_index" \
    --argjson totalSources "$total_sources" \
    --arg fileName "$file_name" \
    --argjson downloadedBytes "$downloaded_bytes" \
    --argjson totalBytes "$total_bytes" \
    --arg message "$message" \
    --arg updatedAt "$updated_at" \
    '{
      stage: $stage,
      source_id: (if $sourceId == "" then null else $sourceId end),
      source_index: $sourceIndex,
      total_sources: $totalSources,
      file_name: (if $fileName == "" then null else $fileName end),
      downloaded_bytes: $downloadedBytes,
      total_bytes: $totalBytes,
      message: (if $message == "" then null else $message end),
      updated_at: $updatedAt
    }' > "$tmp_progress_file" 2>/dev/null || {
      rm -f "$tmp_progress_file" 2>/dev/null || true
      return 0
    }

  mv "$tmp_progress_file" "$FETCH_PROGRESS_FILE" 2>/dev/null || {
    rm -f "$tmp_progress_file" 2>/dev/null || true
  }
  return 0
}

usage() {
  cat <<USAGE
Usage: scripts/data/fetch-dach-sources.sh [options]

Fetch official DACH raw datasets into data/raw/.

Options:
  --as-of YYYY-MM-DD   Deterministic replay date (select latest artifact <= date)
  --country DE|AT|CH   Only fetch sources for one country
  --source-id ID       Only fetch one source id
  -h, --help           Show this help
USAGE
  return 0
}

log() {
  printf '[fetch-dach] %s\n' "$*"
  return 0
}

fail() {
  write_fetch_progress "failed" \
    "$FETCH_PROGRESS_SOURCE_ID" \
    "$FETCH_PROGRESS_SOURCE_INDEX" \
    "$FETCH_PROGRESS_TOTAL_SOURCES" \
    "$FETCH_PROGRESS_FILE_NAME" \
    "$FETCH_PROGRESS_DOWNLOADED_BYTES" \
    "$FETCH_PROGRESS_TOTAL_BYTES" \
    "$*" || true
  printf '[fetch-dach] ERROR: %s\n' "$*" >&2
  return 1
}

require_cmd() {
  local cmd="$1"
  command -v "$cmd" >/dev/null 2>&1 || fail "Missing required command: $cmd"
  return 0
}

slugify() {
  local value="$1"
  printf '%s' "$value" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//'
  return 0
}

is_iso_date() {
  local d="$1"
  [[ "$d" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || return 1
  date -u -d "$d" +%F >/dev/null 2>&1
  return 0
}

parse_args() {
  local arg
  while [[ $# -gt 0 ]]; do
    arg="$1"
    case "$arg" in
      --as-of)
        [[ $# -ge 2 ]] || fail "Missing value for --as-of"
        AS_OF="$2"
        shift 2
        ;;
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
      -h|--help)
        usage
        exit 0
        ;;
      *)
        fail "Unknown option: $arg"
        ;;
    esac
  done

  if [[ -n "$AS_OF" ]] && ! is_iso_date "$AS_OF"; then
    fail "Invalid --as-of value '$AS_OF' (expected YYYY-MM-DD)"
  fi

  if [[ -n "$COUNTRY_FILTER" && "$COUNTRY_FILTER" != "DE" && "$COUNTRY_FILTER" != "AT" && "$COUNTRY_FILTER" != "CH" ]]; then
    fail "Invalid --country '$COUNTRY_FILTER' (expected DE, AT, or CH)"
  fi
  return 0
}

load_env() {
  local env_file
  for env_file in .env .env.local; do
    if [[ -f "$env_file" ]]; then
      set -a
      # shellcheck disable=SC1090,SC1091
      source "$env_file"
      set +a
    fi
  done
  return 0
}

build_auth_args() {
  local source_id="$1"
  local access_type="$2"
  local auth_check_url="${3:-}"
  AUTH_ARGS=()

  case "$access_type" in
    public)
      ;;
    api_key)
      local var_name
      var_name="$(printf '%s' "$source_id" | tr '[:lower:]-' '[:upper:]_')_API_KEY"
      local api_key="${!var_name:-${DACH_API_KEY:-}}"
      [[ -n "$api_key" ]] || fail "Missing auth for '$source_id': set $var_name or DACH_API_KEY"
      AUTH_ARGS=(-H "X-API-Key: $api_key")
      ;;
    token)
      local token_var
      token_var="$(printf '%s' "$source_id" | tr '[:lower:]-' '[:upper:]_')_TOKEN"
      local token="${!token_var:-${DACH_TOKEN:-}}"
      [[ -n "$token" ]] || fail "Missing auth for '$source_id': set $token_var or DACH_TOKEN"
      AUTH_ARGS=(-H "Authorization: Bearer $token")
      ;;
    other)
      local source_key cookie_var cookie_file_var header_var user_var pass_var login_url_var
      source_key="$(printf '%s' "$source_id" | tr '[:lower:]-' '[:upper:]_')"
      cookie_var="${source_key}_COOKIE"
      cookie_file_var="${source_key}_COOKIE_FILE"
      header_var="${source_key}_HEADER"
      user_var="${source_key}_USERNAME"
      pass_var="${source_key}_PASSWORD"
      login_url_var="${source_key}_LOGIN_URL"

      local cookie cookie_file header username password login_url
      cookie="${!cookie_var:-${DACH_COOKIE:-}}"
      cookie_file="${!cookie_file_var:-${DACH_COOKIE_FILE:-}}"
      header="${!header_var:-${DACH_HEADER:-}}"
      username="${!user_var:-${DACH_USERNAME:-}}"
      password="${!pass_var:-${DACH_PASSWORD:-}}"
      login_url="${!login_url_var:-https://www.opendata-oepnv.de/ht/de/willkommen}"

      [[ "$cookie" == *PASTE_* || "$cookie" == *YOUR_SESSION_COOKIE* ]] && cookie=""
      [[ "$header" == *PASTE_* || "$header" == *YOUR_SESSION_COOKIE* ]] && header=""
      [[ "$username" == *PASTE_* || "$username" == *YOUR_USERNAME* ]] && username=""
      [[ "$password" == *PASTE_* || "$password" == *YOUR_PASSWORD* ]] && password=""

      if [[ -z "$cookie" && -z "$cookie_file" && -z "$header" ]]; then
        if [[ -n "$username" && -n "$password" ]]; then
          local session_cookie_file
          session_cookie_file="$(delfi_login_cookie_file "$source_id" "$login_url" "$username" "$password" "$auth_check_url")"
          AUTH_ARGS+=(--cookie "$session_cookie_file")
          return 0
        fi
        fail "Missing auth for '$source_id': set cookie/header ($cookie_var, $cookie_file_var, $header_var) or login credentials ($user_var, $pass_var)"
      fi

      if [[ -n "$cookie_file" ]]; then
        [[ -f "$cookie_file" ]] || fail "Missing auth for '$source_id': cookie file not found at $cookie_file"
        AUTH_ARGS+=(--cookie "$cookie_file")
      fi
      if [[ -n "$cookie" ]]; then
        AUTH_ARGS+=(--cookie "$cookie")
      fi
      if [[ -n "$header" ]]; then
        AUTH_ARGS+=(-H "$header")
      fi
      ;;
    *)
      fail "Invalid accessType '$access_type' for '$source_id'"
      ;;
  esac
}

extract_attr() {
  local input="$1"
  local attr="$2"
  printf '%s\n' "$input" | sed -nE "s/.*${attr}=\"([^\"]*)\".*/\\1/p" | head -1
  return 0
}

delfi_login_cookie_file() {
  local source_id="$1"
  local login_url="$2"
  local username="$3"
  local password="$4"
  local auth_check_url="$5"

  local cookie_file
  cookie_file="$(mktemp)"
  TEMP_FILES+=("$cookie_file")

  local html form_block form_line action_rel action_url
  html="$(curl -fsSL -c "$cookie_file" -b "$cookie_file" "$login_url")" || fail "Login bootstrap failed for '$source_id' at $login_url"
  form_block="$(awk 'BEGIN{IGNORECASE=1}/<form[^>]*tx_felogin_login%5Baction%5D=login/{f=1} f{print} /<\/form>/{if(f){exit}}' <<<"$html")"
  [[ -n "$form_block" ]] || fail "Could not find login form for '$source_id' at $login_url"

  form_line="$(printf '%s\n' "$form_block" | head -1)"
  action_rel="$(extract_attr "$form_line" "action")"
  [[ -n "$action_rel" ]] || fail "Could not parse login form action for '$source_id'"
  action_rel="${action_rel//&amp;/&}"
  action_url="$(normalize_url "$login_url" "$action_rel")"

  local post_args=()
  while IFS= read -r hidden; do
    local name value
    name="$(extract_attr "$hidden" "name")"
    value="$(extract_attr "$hidden" "value")"
    [[ -n "$name" ]] || continue
    post_args+=(--data-urlencode "$name=$value")
  done < <(printf '%s\n' "$form_block" | grep -oE '<input[^>]+type="hidden"[^>]*>')

  post_args+=(--data-urlencode "user=$username")
  post_args+=(--data-urlencode "pass=$password")
  post_args+=(--data-urlencode "submit=Anmelden")

  curl -fsSL -c "$cookie_file" -b "$cookie_file" -X POST "$action_url" "${post_args[@]}" >/dev/null \
    || fail "Login submit failed for '$source_id' at $action_url"

  if [[ -n "$auth_check_url" ]]; then
    local check_html has_download_links
    check_html="$(curl -fsSL -c "$cookie_file" -b "$cookie_file" "$auth_check_url")" \
      || fail "Post-login auth check failed for '$source_id'"
    has_download_links=0
    if printf '%s' "$check_html" | grep -Eiq 'href="[^"]+\.(zip|xml|xml\.gz|tgz|gz)"|data-download="[^"]+\.(zip|xml|xml\.gz|tgz|gz)"'; then
      has_download_links=1
    fi
    if [[ "$has_download_links" -eq 0 ]] && printf '%s' "$check_html" | grep -Eiq 'Bitte Anmelden|This Download is only available for registered Users'; then
      fail "Login appears unsuccessful for '$source_id' (still seeing login-required marker on dataset page)"
    fi
  fi

  printf '%s\n' "$cookie_file"
  return 0
}

normalize_url() {
  local base_url="$1"
  local maybe_relative="$2"

  if [[ "$maybe_relative" =~ ^https?:// ]]; then
    printf '%s\n' "$maybe_relative"
    return
  fi

  local proto host
  proto="${base_url%%://*}"
  host="${base_url#*://}"
  host="${host%%/*}"

  if [[ "$maybe_relative" == /* ]]; then
    printf '%s://%s%s\n' "$proto" "$host" "$maybe_relative"
  else
    printf '%s/%s\n' "${base_url%/}" "$maybe_relative"
  fi
  return 0
}

resolve_de_delfi_netex() {
  local endpoint="$1"
  local as_of="$2"

  local html
  html="$(curl -fsSL "${AUTH_ARGS[@]}" "$endpoint")" || fail "Source unavailable: could not open DE DELFI endpoint $endpoint"

  mapfile -t raw_urls < <(
    {
      printf '%s' "$html" | grep -oE 'href="[^"]+\.(zip|xml|xml\.gz|tgz|gz)"' | sed -E 's/^href="([^"]+)"$/\1/'
      printf '%s' "$html" | grep -oE 'data-download="[^"]+\.(zip|xml|xml\.gz|tgz|gz)"' | sed -E 's/^data-download="([^"]+)"$/\1/'
    } | sort -u
  )

  if [[ ${#raw_urls[@]} -eq 0 ]]; then
    if printf '%s' "$html" | grep -Eiq 'Bitte Anmelden|only available for registered users|Registration'; then
      fail "Missing auth for DE DELFI NeTEx source: login required and no download links visible"
    fi
    fail "No DE DELFI NeTEx download links found on dataset detail page"
  fi

  local cutoff="999999999999"
  if [[ -n "$as_of" ]]; then
    cutoff="${as_of//-/}2359"
  fi

  local best_url="" best_ts="0" fallback_url="" u abs_u ts name
  for u in "${raw_urls[@]}"; do
    abs_u="$(normalize_url "$endpoint" "$u")"
    [[ -n "$fallback_url" ]] || fallback_url="$abs_u"

    name="${abs_u##*/}"
    ts="$(printf '%s' "$name" | grep -oE '[0-9]{12}' | head -1 || true)"
    if [[ -z "$ts" ]]; then
      ts="$(printf '%s' "$name" | grep -oE '[0-9]{8}' | head -1 || true)"
      [[ -n "$ts" ]] && ts="${ts}0000"
    fi
    [[ -n "$ts" ]] || continue

    if [[ "$ts" -le "$cutoff" && "$ts" -ge "$best_ts" ]]; then
      best_ts="$ts"
      best_url="$abs_u"
    fi
  done

  if [[ -n "$best_url" ]]; then
    printf '%s\n' "$best_url"
    return
  fi

  [[ -n "$fallback_url" ]] || fail "No DE DELFI download URL could be resolved"
  printf '%s\n' "$fallback_url"
  return 0
}

resolve_at_netex() {
  local endpoint="$1"
  local as_of="$2"

  local html
  html="$(curl -fsSL "${AUTH_ARGS[@]}" "$endpoint")" || fail "Source unavailable: could not open AT endpoint $endpoint"

  mapfile -t raw_urls < <(printf '%s' "$html" | grep -oE 'data-download="[^"]+"' | sed -E 's/^data-download="([^"]+)"$/\1/' | grep -Ei 'netex.*\.zip|\.zip.*netex')
  [[ ${#raw_urls[@]} -gt 0 ]] || fail "No AT NetEx download URLs found in dataset page"

  local best_url=""
  local best_year="0"
  local as_of_year="9999"
  if [[ -n "$as_of" ]]; then
    as_of_year="${as_of:0:4}"
  fi

  local u abs_u year
  for u in "${raw_urls[@]}"; do
    abs_u="$(normalize_url "$endpoint" "$u")"
    year="$(printf '%s' "$abs_u" | grep -oE '20[0-9]{2}' | head -1 || true)"
    [[ -n "$year" ]] || year="0"
    if [[ "$year" -le "$as_of_year" && "$year" -ge "$best_year" ]]; then
      best_year="$year"
      best_url="$abs_u"
    fi
  done

  [[ -n "$best_url" ]] || fail "No AT NetEx URL matched as-of date '$as_of'"
  printf '%s\n' "$best_url"
  return 0
}

resolve_ch_netex() {
  local endpoint="$1"
  local as_of="$2"

  local html
  html="$(curl -fsSL "${AUTH_ARGS[@]}" "$endpoint")" || fail "Source unavailable: could not open CH endpoint $endpoint"

  mapfile -t urls < <(printf '%s' "$html" \
    | grep -oE 'href="https://data\.opentransportdata\.swiss/dataset/[^"]+/download/[^"]+\.zip"' \
    | sed -E 's/^href="([^"]+)"$/\1/' \
    | grep -Ei 'netex' \
    | sort -u)

  [[ ${#urls[@]} -gt 0 ]] || fail "No CH NetEx resource download URLs found in dataset page"

  local cutoff="999999999999"
  if [[ -n "$as_of" ]]; then
    cutoff="${as_of//-/}2359"
  fi

  local best_url=""
  local best_ts="0"
  local u name ts
  for u in "${urls[@]}"; do
    name="${u##*/}"
    ts="$(printf '%s' "$name" | grep -oE '[0-9]{12}' | head -1 || true)"
    if [[ -z "$ts" ]]; then
      ts="$(printf '%s' "$name" | grep -oE '[0-9]{8}' | head -1 || true)"
      [[ -n "$ts" ]] && ts="${ts}0000"
    fi
    [[ -n "$ts" ]] || continue

    if [[ "$ts" -le "$cutoff" && "$ts" -ge "$best_ts" ]]; then
      best_ts="$ts"
      best_url="$u"
    fi
  done

  [[ -n "$best_url" ]] || fail "No CH NetEx URL matched as-of date '$as_of'"
  printf '%s\n' "$best_url"
  return 0
}

resolve_download_url() {
  local source_json="$1"
  local source_id="$2"
  local method endpoint

  method="$(jq -r '.downloadMethod' <<<"$source_json")"
  endpoint="$(jq -r '.downloadUrlOrEndpoint' <<<"$source_json")"

  case "$method" in
    direct_url)
      printf '%s\n' "$endpoint"
      ;;
    api)
      fail "Source '$source_id' uses downloadMethod=api but API retrieval is not implemented yet"
      ;;
    manual_redirect)
      case "$source_id" in
        de_delfi_sollfahrplandaten_netex)
          resolve_de_delfi_netex "$endpoint" "$AS_OF"
          ;;
        at_oebb_mmtis_netex)
          resolve_at_netex "$endpoint" "$AS_OF"
          ;;
        ch_opentransportdata_timetable_netex)
          resolve_ch_netex "$endpoint" "$AS_OF"
          ;;
        *)
          fail "No manual_redirect resolver implemented for source '$source_id'"
          ;;
      esac
      ;;
    *)
      fail "Unknown downloadMethod '$method' for source '$source_id'"
      ;;
  esac
  return 0
}

check_format_match() {
  local file_path="$1"
  local format="$2"
  local resolved_url="$3"
  local source_id="${4:-}"

  local file_name
  file_name="${resolved_url##*/}"
  file_name="${file_name%%\?*}"

  case "$format" in
    netex)
      if printf '%s\n' "$file_name" | grep -Eiq 'netex|netx|\.xml$'; then
        return 0
      fi
      # DELFI's official NeTEx export uses a generic zip name without "netex".
      if [[ "$source_id" == "de_delfi_sollfahrplandaten_netex" ]] \
        && printf '%s\n' "$file_name" | grep -Eiq '^[0-9]{8}_fahrplaene_gesamtdeutschland\.zip$'; then
        return 0
      fi
      if command -v unzip >/dev/null 2>&1 \
        && printf '%s\n' "$file_name" | grep -Eiq '\.zip$' \
        && unzip -l "$file_path" 2>/dev/null | grep -Eiq 'netex|netx|\.xml'; then
        return 0
      fi
      return 1
      ;;
    gtfs)
      if printf '%s\n' "$file_name" | grep -Eiq 'gtfs|google_transit'; then
        return 0
      fi
      if command -v unzip >/dev/null 2>&1 \
        && printf '%s\n' "$file_name" | grep -Eiq '\.zip$' \
        && unzip -l "$file_path" 2>/dev/null | grep -Eq '(agency|stops|routes|trips|stop_times)\.txt'; then
        return 0
      fi
      return 1
      ;;
    *)
      return 1
      ;;
  esac
}

detect_version_hint() {
  local resolved_url="$1"
  local headers="$2"
  local file_name hint

  file_name="${resolved_url##*/}"
  file_name="${file_name%%\?*}"

  hint="$(printf '%s' "$file_name" | grep -oE '[0-9]{12}' | head -1 || true)"
  if [[ -z "$hint" ]]; then
    hint="$(printf '%s' "$file_name" | grep -oE '[0-9]{8}' | head -1 || true)"
  fi
  if [[ -z "$hint" ]]; then
    hint="$(printf '%s\n' "$headers" | awk -F': ' 'tolower($1)=="last-modified"{print $2; exit}' | tr -d '\r' || true)"
  fi

  printf '%s\n' "$hint"
  return 0
}

probe_http_code() {
  local url="$1"
  shift
  local code
  code="$(curl -sS -o /dev/null -w '%{http_code}' -L -I "$@" "$url" || true)"
  if [[ -z "$code" || "$code" == "000" || "$code" -ge 400 ]]; then
    code="$(curl -sS -o /dev/null -w '%{http_code}' -L -r 0-0 "$@" "$url" || true)"
  fi
  printf '%s\n' "$code"
  return 0
}

main() {
  parse_args "$@"
  "${ROOT_DIR}/scripts/validate-config.sh" --only dach >/dev/null

  require_cmd jq
  require_cmd curl
  require_cmd sha256sum
  require_cmd date
  require_cmd stat

  [[ -f "$CONFIG_FILE" ]] || fail "Config not found: $CONFIG_FILE"
  jq -e '.sources and (.sources | type == "array")' "$CONFIG_FILE" >/dev/null || fail "Invalid config format in $CONFIG_FILE"

  load_env

  local run_date
  run_date="${AS_OF:-$(date -u +%F)}"
  local retrieval_ts
  retrieval_ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

  # shellcheck disable=SC2016
  local jq_filter='(.sources[] | select((($country == "") or (.country == $country)) and (($source_id == "") or (.id == $source_id))))'
  mapfile -t selected_sources < <(jq -c --arg country "$COUNTRY_FILTER" --arg source_id "$SOURCE_ID_FILTER" "$jq_filter" "$CONFIG_FILE")

  [[ ${#selected_sources[@]} -gt 0 ]] || fail "No sources matched the provided filters"

  FETCH_PROGRESS_TOTAL_SOURCES="${#selected_sources[@]}"
  write_fetch_progress "starting" "" 0 "$FETCH_PROGRESS_TOTAL_SOURCES" "" 0 0 \
    "Selected ${FETCH_PROGRESS_TOTAL_SOURCES} source(s)"

  log "Selected ${#selected_sources[@]} source(s); run_date=$run_date${AS_OF:+ (as-of mode)}"

  local source_json source_index
  source_index=0
  for source_json in "${selected_sources[@]}"; do
    source_index=$((source_index + 1))
    local source_id country provider format access_type
    source_id="$(jq -r '.id' <<<"$source_json")"
    country="$(jq -r '.country' <<<"$source_json")"
    provider="$(jq -r '.provider' <<<"$source_json")"
    format="$(jq -r '.format' <<<"$source_json")"
    access_type="$(jq -r '.accessType' <<<"$source_json")"

    FETCH_PROGRESS_SOURCE_ID="$source_id"
    FETCH_PROGRESS_SOURCE_INDEX="$source_index"
    FETCH_PROGRESS_FILE_NAME=""
    FETCH_PROGRESS_DOWNLOADED_BYTES=0
    FETCH_PROGRESS_TOTAL_BYTES=0
    write_fetch_progress "resolving" \
      "$FETCH_PROGRESS_SOURCE_ID" \
      "$FETCH_PROGRESS_SOURCE_INDEX" \
      "$FETCH_PROGRESS_TOTAL_SOURCES" \
      "" \
      0 \
      0 \
      "Resolving source '$source_id'"

    log "Resolving source '$source_id' ($country/$format)"
    build_auth_args "$source_id" "$access_type" "$(jq -r '.downloadUrlOrEndpoint' <<<"$source_json")"

    local resolved_url
    resolved_url="$(resolve_download_url "$source_json" "$source_id")" || {
      if [[ "$format" == "netex" ]]; then
        fail "NeTEx source '$source_id' resolution failed (hard failure)"
      fi
      fail "Source '$source_id' resolution failed"
    }

    [[ -n "$resolved_url" ]] || fail "Source '$source_id' did not resolve a download URL"
    log "Resolved URL: $resolved_url"

    local http_code
    http_code="$(probe_http_code "$resolved_url" "${AUTH_ARGS[@]}")"
    if [[ -z "$http_code" || "$http_code" == "000" || "$http_code" -ge 400 ]]; then
      if [[ "$format" == "netex" ]]; then
        fail "NeTEx source '$source_id' HTTP error for $resolved_url (status=$http_code)"
      fi
      fail "HTTP error for source '$source_id' (status=$http_code, url=$resolved_url)"
    fi

    local head_headers
    head_headers="$(curl -sSI -L "${AUTH_ARGS[@]}" "$resolved_url" || true)"

    local file_name
    file_name="${resolved_url##*/}"
    file_name="${file_name%%\?*}"
    [[ -n "$file_name" && "$file_name" != */ ]] || file_name="${source_id}_${run_date}.bin"

    local provider_slug dest_dir out_file tmp_file
    provider_slug="$(slugify "$provider")"
    dest_dir="data/raw/${country}/${provider_slug}/${format}/${run_date}"
    mkdir -p "$dest_dir"

    out_file="${dest_dir}/${file_name}"
    tmp_file="${dest_dir}/.${file_name}.tmp.$$"
    FETCH_PROGRESS_FILE_NAME="$file_name"

    local expected_size
    expected_size="$(printf '%s\n' "$head_headers" | awk -F': ' 'tolower($1)=="content-length"{print $2; exit}' | tr -d '\r' || true)"
    [[ "$expected_size" =~ ^[0-9]+$ ]] || expected_size=0
    FETCH_PROGRESS_TOTAL_BYTES="$expected_size"
    FETCH_PROGRESS_DOWNLOADED_BYTES=0
    write_fetch_progress "downloading" \
      "$FETCH_PROGRESS_SOURCE_ID" \
      "$FETCH_PROGRESS_SOURCE_INDEX" \
      "$FETCH_PROGRESS_TOTAL_SOURCES" \
      "$FETCH_PROGRESS_FILE_NAME" \
      0 \
      "$FETCH_PROGRESS_TOTAL_BYTES" \
      "Downloading '$file_name'"

    local progress_watcher_pid=""
    (
      while true; do
        bytes=0
        if [[ -f "$tmp_file" ]]; then
          bytes="$(stat -c '%s' "$tmp_file" 2>/dev/null || printf '0')"
        fi
        [[ "$bytes" =~ ^[0-9]+$ ]] || bytes=0
        
        # Update progress file for external tools
        write_fetch_progress \
          "downloading" \
          "$source_id" \
          "$source_index" \
          "$FETCH_PROGRESS_TOTAL_SOURCES" \
          "$file_name" \
          "$bytes" \
          "$expected_size" \
          "Downloading '$file_name'"
          
        # Print terminal progress bar
        if [[ "$expected_size" -gt 0 ]]; then
          local percent=0
          if [[ "$bytes" -ge "$expected_size" ]]; then
            percent=100
          else
            percent=$((bytes * 100 / expected_size))
          fi
          
          local filled=$((percent / 5))
          local empty=$((20 - filled))
          local bar
          bar="$(printf "%${filled}s" | tr ' ' '#')$(printf "%${empty}s" | tr ' ' '-')"
          printf "\r[fetch-dach] [%-20s] %3d%% (%s/%s) " "$bar" "$percent" "$(numfmt --to=iec "$bytes")" "$(numfmt --to=iec "$expected_size")" >&2
        else
          printf "\r[fetch-dach] %s: %s downloaded " "$file_name" "$(numfmt --to=iec "$bytes")" >&2
        fi
        
        sleep 1
      done
    ) &
    progress_watcher_pid="$!"

    log "Downloading to $out_file"
    if ! curl -fsSL "${AUTH_ARGS[@]}" "$resolved_url" -o "$tmp_file"; then
      if [[ -n "$progress_watcher_pid" ]]; then
        kill "$progress_watcher_pid" >/dev/null 2>&1 || true
        wait "$progress_watcher_pid" >/dev/null 2>&1 || true
      fi
      printf "\n" >&2
      rm -f "$tmp_file"
      if [[ "$format" == "netex" ]]; then
        fail "NeTEx source '$source_id' download failed (hard failure): $resolved_url"
      fi
      fail "Download failed for source '$source_id'"
    fi
    if [[ -n "$progress_watcher_pid" ]]; then
      kill "$progress_watcher_pid" >/dev/null 2>&1 || true
      wait "$progress_watcher_pid" >/dev/null 2>&1 || true
    fi
    printf "\n" >&2

    if ! check_format_match "$tmp_file" "$format" "$resolved_url" "$source_id"; then
      rm -f "$tmp_file"
      fail "Format mismatch for source '$source_id': expected '$format', URL '$resolved_url'"
    fi

    mv "$tmp_file" "$out_file"

    local file_size sha256 version_hint
    file_size="$(stat -c '%s' "$out_file")"
    sha256="$(sha256sum "$out_file" | awk '{print $1}')"
    version_hint="$(detect_version_hint "$resolved_url" "$head_headers")"
    FETCH_PROGRESS_DOWNLOADED_BYTES="$file_size"
    FETCH_PROGRESS_TOTAL_BYTES="${FETCH_PROGRESS_TOTAL_BYTES:-0}"
    if [[ "$FETCH_PROGRESS_TOTAL_BYTES" -eq 0 ]]; then
      FETCH_PROGRESS_TOTAL_BYTES="$file_size"
    fi

    jq -n \
      --arg sourceId "$source_id" \
      --arg resolvedDownloadUrl "$resolved_url" \
      --arg retrievalTimestamp "$retrieval_ts" \
      --arg fileName "$file_name" \
      --argjson fileSizeBytes "$file_size" \
      --arg sha256 "$sha256" \
      --arg detectedVersionOrDate "$version_hint" \
      --arg requestedAsOf "$AS_OF" \
      '{
        sourceId: $sourceId,
        resolvedDownloadUrl: $resolvedDownloadUrl,
        retrievalTimestamp: $retrievalTimestamp,
        fileName: $fileName,
        fileSizeBytes: $fileSizeBytes,
        sha256: $sha256,
        detectedVersionOrDate: (if $detectedVersionOrDate == "" then null else $detectedVersionOrDate end),
        requestedAsOf: (if $requestedAsOf == "" then null else $requestedAsOf end)
      }' > "${dest_dir}/manifest.json"

    write_fetch_progress "source_completed" \
      "$FETCH_PROGRESS_SOURCE_ID" \
      "$FETCH_PROGRESS_SOURCE_INDEX" \
      "$FETCH_PROGRESS_TOTAL_SOURCES" \
      "$FETCH_PROGRESS_FILE_NAME" \
      "$FETCH_PROGRESS_DOWNLOADED_BYTES" \
      "$FETCH_PROGRESS_TOTAL_BYTES" \
      "Completed '$source_id'"

    log "Completed '$source_id': size=${file_size} sha256=${sha256}"
  done

  write_fetch_progress "completed" "" "$FETCH_PROGRESS_TOTAL_SOURCES" "$FETCH_PROGRESS_TOTAL_SOURCES" "" 0 0 \
    "All selected sources fetched successfully."
  log "All selected sources fetched successfully."
  return 0
}

main "$@"
