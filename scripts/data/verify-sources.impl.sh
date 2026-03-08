#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CONFIG_FILE="config/europe-data-sources.json"
COUNTRY_FILTER=""
SOURCE_ID_FILTER=""
AS_OF=""
TEMP_FILES=()

ERRORS=0
WARNINGS=0

cleanup_temp_files() {
  local f
  for f in "${TEMP_FILES[@]}"; do
    [[ -n "$f" ]] && rm -f "$f" 2>/dev/null || true
  done
  return 0
}
trap cleanup_temp_files EXIT

usage() {
  cat <<USAGE
Usage: scripts/data/verify-sources.sh [options]

Validate pan-European source config, policy consistency, and basic reachability.

Options:
  --country <ISO2>     Verify one country only
  --source-id ID       Verify one source id only
  --as-of YYYY-MM-DD   Resolve manual sources against this deterministic date
  -h, --help           Show this help
USAGE
  return 0
}

log() {
  printf '[verify-sources] %s\n' "$*"
  return 0
}

err() {
  printf '[verify-sources] ERROR: %s\n' "$*" >&2
  ERRORS=$((ERRORS + 1))
  return 0
}

warn() {
  printf '[verify-sources] WARN: %s\n' "$*" >&2
  WARNINGS=$((WARNINGS + 1))
  return 0
}

require_cmd() {
  local cmd="$1"
  command -v "$cmd" >/dev/null 2>&1 || {
    printf '[verify-sources] ERROR: Missing required command: %s\n' "$cmd" >&2
    exit 1
  }
  return 0
}

is_iso_date() {
  local d="$1"
  [[ "$d" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || return 1
  date -u -d "$d" +%F >/dev/null 2>&1
  return 0
}

is_iso_ts() {
  local ts="$1"
  [[ "$ts" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$ ]] || return 1
  date -u -d "$ts" +"%Y-%m-%dT%H:%M:%SZ" >/dev/null 2>&1
  return 0
}

parse_args() {
  local arg
  while [[ $# -gt 0 ]]; do
    arg="$1"
    case "$arg" in
      --country)
        [[ $# -ge 2 ]] || { err "Missing value for --country"; exit 1; }
        COUNTRY_FILTER="$2"
        shift 2
        ;;
      --source-id)
        [[ $# -ge 2 ]] || { err "Missing value for --source-id"; exit 1; }
        SOURCE_ID_FILTER="$2"
        shift 2
        ;;
      --as-of)
        [[ $# -ge 2 ]] || { err "Missing value for --as-of"; exit 1; }
        AS_OF="$2"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        err "Unknown option: $arg"
        exit 1
        ;;
    esac
  done

  if [[ -n "$COUNTRY_FILTER" && ! "$COUNTRY_FILTER" =~ ^[A-Z]{2}$ ]]; then
    err "Invalid --country '$COUNTRY_FILTER'"
    exit 1
  fi

  if [[ -n "$AS_OF" ]] && ! is_iso_date "$AS_OF"; then
    err "Invalid --as-of '$AS_OF' (expected YYYY-MM-DD)"
    exit 1
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
      return 0
      ;;
    api_key)
      local key_var
      key_var="$(printf '%s' "$source_id" | tr '[:lower:]-' '[:upper:]_')_API_KEY"
      local api_key="${!key_var:-${SOURCE_API_KEY:-${EUROPE_API_KEY:-}}}"
      if [[ -z "$api_key" ]]; then
        err "Missing auth for '$source_id': set $key_var or SOURCE_API_KEY/EUROPE_API_KEY"
        return 1
      fi
      AUTH_ARGS=(-H "X-API-Key: $api_key")
      return 0
      ;;
    token)
      local token_var
      token_var="$(printf '%s' "$source_id" | tr '[:lower:]-' '[:upper:]_')_TOKEN"
      local token="${!token_var:-${SOURCE_TOKEN:-${EUROPE_TOKEN:-}}}"
      if [[ -z "$token" ]]; then
        err "Missing auth for '$source_id': set $token_var or SOURCE_TOKEN/EUROPE_TOKEN"
        return 1
      fi
      AUTH_ARGS=(-H "Authorization: Bearer $token")
      return 0
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
      cookie="${!cookie_var:-${SOURCE_COOKIE:-${EUROPE_COOKIE:-}}}"
      cookie_file="${!cookie_file_var:-${SOURCE_COOKIE_FILE:-${EUROPE_COOKIE_FILE:-}}}"
      header="${!header_var:-${SOURCE_HEADER:-${EUROPE_HEADER:-}}}"
      username="${!user_var:-${SOURCE_USERNAME:-${EUROPE_USERNAME:-}}}"
      password="${!pass_var:-${SOURCE_PASSWORD:-${EUROPE_PASSWORD:-}}}"
      login_url="${!login_url_var:-https://www.opendata-oepnv.de/ht/de/willkommen}"

      [[ "$cookie" == *PASTE_* || "$cookie" == *YOUR_SESSION_COOKIE* ]] && cookie=""
      [[ "$header" == *PASTE_* || "$header" == *YOUR_SESSION_COOKIE* ]] && header=""
      [[ "$username" == *PASTE_* || "$username" == *YOUR_USERNAME* ]] && username=""
      [[ "$password" == *PASTE_* || "$password" == *YOUR_PASSWORD* ]] && password=""

      if [[ -z "$cookie" && -z "$cookie_file" && -z "$header" ]]; then
        if [[ -n "$username" && -n "$password" ]]; then
          local session_cookie_file
          if ! session_cookie_file="$(delfi_login_cookie_file "$source_id" "$login_url" "$username" "$password" "$auth_check_url")"; then
            err "Login failed for '$source_id' using $user_var/$pass_var"
            return 1
          fi
          AUTH_ARGS+=(--cookie "$session_cookie_file")
          return 0
        fi
        err "Missing auth for '$source_id': set cookie/header ($cookie_var, $cookie_file_var, $header_var) or login credentials ($user_var, $pass_var)"
        return 1
      fi
      if [[ -n "$cookie_file" ]]; then
        if [[ ! -f "$cookie_file" ]]; then
          err "Missing auth for '$source_id': cookie file not found at $cookie_file"
          return 1
        fi
        AUTH_ARGS+=(--cookie "$cookie_file")
      fi
      if [[ -n "$cookie" ]]; then
        AUTH_ARGS+=(--cookie "$cookie")
      fi
      if [[ -n "$header" ]]; then
        AUTH_ARGS+=(-H "$header")
      fi
      return 0
      ;;
    *)
      err "Invalid accessType '$access_type' for '$source_id'"
      return 1
      ;;
  esac
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

extract_attr() {
  local input="$1"
  local attr="$2"
  printf '%s\n' "$input" | sed -nE "s/.*${attr}=\"([^\"]*)\".*/\\1/p" | head -1
  return 0
}

delfi_login_cookie_file() {
  local login_url="$2"
  local username="$3"
  local password="$4"
  local auth_check_url="$5"

  local cookie_file
  cookie_file="$(mktemp)"
  TEMP_FILES+=("$cookie_file")

  local html form_block form_line action_rel action_url
  html="$(curl -fsSL -c "$cookie_file" -b "$cookie_file" "$login_url")" || return 1
  form_block="$(awk 'BEGIN{IGNORECASE=1}/<form[^>]*tx_felogin_login%5Baction%5D=login/{f=1} f{print} /<\/form>/{if(f){exit}}' <<<"$html")"
  [[ -n "$form_block" ]] || return 1

  form_line="$(printf '%s\n' "$form_block" | head -1)"
  action_rel="$(extract_attr "$form_line" "action")"
  [[ -n "$action_rel" ]] || return 1
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

  curl -fsSL -c "$cookie_file" -b "$cookie_file" -X POST "$action_url" "${post_args[@]}" >/dev/null || return 1

  if [[ -n "$auth_check_url" ]]; then
    local check_html has_download_links
    check_html="$(curl -fsSL -c "$cookie_file" -b "$cookie_file" "$auth_check_url")" || return 1
    has_download_links=0
    if printf '%s' "$check_html" | grep -Eiq 'href="[^"]+\.(zip|xml|xml\.gz|tgz|gz)"|data-download="[^"]+\.(zip|xml|xml\.gz|tgz|gz)"'; then
      has_download_links=1
    fi
    if [[ "$has_download_links" -eq 0 ]] && printf '%s' "$check_html" | grep -Eiq 'Bitte Anmelden|This Download is only available for registered Users'; then
      return 1
    fi
  fi

  printf '%s\n' "$cookie_file"
  return 0
}

resolve_de_delfi_netex() {
  local endpoint="$1"
  local as_of="$2"

  local html
  html="$(curl -fsSL "${AUTH_ARGS[@]}" "$endpoint")" || return 1

  mapfile -t raw_urls < <(
    {
      printf '%s' "$html" | grep -oE 'href="[^"]+\.(zip|xml|xml\.gz|tgz|gz)"' | sed -E 's/^href="([^"]+)"$/\1/'
      printf '%s' "$html" | grep -oE 'data-download="[^"]+\.(zip|xml|xml\.gz|tgz|gz)"' | sed -E 's/^data-download="([^"]+)"$/\1/'
    } | sort -u
  )
  [[ ${#raw_urls[@]} -gt 0 ]] || return 1

  local cutoff="999999999999"
  [[ -n "$as_of" ]] && cutoff="${as_of//-/}2359"

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
    return 0
  fi
  [[ -n "$fallback_url" ]] || return 1
  printf '%s\n' "$fallback_url"
  return 0
}

resolve_at_netex() {
  local endpoint="$1"
  local as_of="$2"

  local html
  html="$(curl -fsSL "${AUTH_ARGS[@]}" "$endpoint")" || return 1

  mapfile -t raw_urls < <(printf '%s' "$html" | grep -oE 'data-download="[^"]+"' | sed -E 's/^data-download="([^"]+)"$/\1/' | grep -Ei 'netex.*\.zip|\.zip.*netex')
  [[ ${#raw_urls[@]} -gt 0 ]] || return 1

  local as_of_year="9999"
  [[ -n "$as_of" ]] && as_of_year="${as_of:0:4}"

  local best_url=""
  local best_year="0"
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

  [[ -n "$best_url" ]] || return 1
  printf '%s\n' "$best_url"
  return 0
}

resolve_ch_netex() {
  local endpoint="$1"
  local as_of="$2"

  local html
  html="$(curl -fsSL "${AUTH_ARGS[@]}" "$endpoint")" || return 1

  mapfile -t urls < <(printf '%s' "$html" \
    | grep -oE 'href="https://data\.opentransportdata\.swiss/dataset/[^"]+/download/[^"]+\.zip"' \
    | sed -E 's/^href="([^"]+)"$/\1/' \
    | grep -Ei 'netex' \
    | sort -u)
  [[ ${#urls[@]} -gt 0 ]] || return 1

  local cutoff="999999999999"
  [[ -n "$as_of" ]] && cutoff="${as_of//-/}2359"

  local best_url=""
  local best_ts="0"
  local u ts name
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

  [[ -n "$best_url" ]] || return 1
  printf '%s\n' "$best_url"
  return 0
}

resolve_generic_manual_redirect() {
  local endpoint="$1"
  local as_of="$2"

  local html
  html="$(curl -fsSL "${AUTH_ARGS[@]}" "$endpoint")" || return 1

  mapfile -t raw_urls < <(
    {
      printf '%s' "$html" | grep -oE 'href="[^"]+\.(zip|xml|xml\.gz|tgz|gz)(\?[^"]*)?"' | sed -E 's/^href="([^"]+)"$/\1/'
      printf '%s' "$html" | grep -oE 'data-download="[^"]+\.(zip|xml|xml\.gz|tgz|gz)(\?[^"]*)?"' | sed -E 's/^data-download="([^"]+)"$/\1/'
    } | sort -u
  )
  [[ ${#raw_urls[@]} -gt 0 ]] || return 1

  local cutoff="999999999999"
  [[ -n "$as_of" ]] && cutoff="${as_of//-/}2359"

  local best_url="" best_ts="0" fallback_url="" u abs_u name ts
  for u in "${raw_urls[@]}"; do
    abs_u="$(normalize_url "$endpoint" "$u")"
    [[ -n "$fallback_url" ]] || fallback_url="$abs_u"
    name="${abs_u##*/}"
    name="${name%%\?*}"

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
    return 0
  fi

  [[ -n "$fallback_url" ]] || return 1
  printf '%s\n' "$fallback_url"
  return 0
}

resolve_url() {
  local source_id="$1"
  local method="$2"
  local endpoint="$3"

  case "$method" in
    direct|direct_url)
      printf '%s\n' "$endpoint"
      ;;
    api)
      return 1
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
          resolve_generic_manual_redirect "$endpoint" "$AS_OF"
          ;;
      esac
      ;;
    *)
      return 1
      ;;
  esac
  return 0
}

check_reachable() {
  local url="$1"
  local label="$2"
  shift 2

  local code
  code="$(curl -sS -o /dev/null -w '%{http_code}' -L -I "$@" "$url" || true)"
  if [[ -z "$code" || "$code" == "000" || "$code" -ge 400 ]]; then
    code="$(curl -sS -o /dev/null -w '%{http_code}' -L -r 0-0 "$@" "$url" || true)"
  fi
  if [[ -z "$code" || "$code" == "000" || "$code" -ge 400 ]]; then
    err "$label unreachable (status=$code): $url"
    return 1
  fi
  log "Reachable ($code): $label"
  return 0
}

main() {
  parse_args "$@"
  "${ROOT_DIR}/scripts/validate-config.sh" --only sources >/dev/null

  require_cmd jq
  require_cmd curl
  require_cmd date

  [[ -f "$CONFIG_FILE" ]] || { echo "Config not found: $CONFIG_FILE" >&2; exit 1; }
  if ! jq -e '.sources and (.sources | type == "array")' "$CONFIG_FILE" >/dev/null; then
    echo "Invalid config structure in $CONFIG_FILE" >&2
    exit 1
  fi

  load_env

  mapfile -t selected_sources < <(jq -c --arg country "$COUNTRY_FILTER" --arg source_id "$SOURCE_ID_FILTER" '(.sources[] | select((($country == "") or (.country == $country)) and (($source_id == "") or (.id == $source_id))))' "$CONFIG_FILE")
  [[ ${#selected_sources[@]} -gt 0 ]] || { echo "No sources matched filters" >&2; exit 1; }

  local netex_count=0
  local gtfs_count=0
  local gtfs_report=()

  local required_fields=(
    id country provider portalName portalUrl datasetName format accessType authSetupUrl
    licenseName licenseUrl attributionText updateCadence downloadMethod
    downloadUrlOrEndpoint notes lastVerifiedAt
  )

  log "Verifying ${#selected_sources[@]} source(s)${AS_OF:+ with as-of=$AS_OF}"

  local source_json
  for source_json in "${selected_sources[@]}"; do
    local id country format method portal_url endpoint access_type last_verified fallback_reason
    id="$(jq -r '.id // ""' <<<"$source_json")"
    country="$(jq -r '.country // ""' <<<"$source_json")"
    format="$(jq -r '.format // ""' <<<"$source_json")"
    method="$(jq -r '.downloadMethod // ""' <<<"$source_json")"
    portal_url="$(jq -r '.portalUrl // ""' <<<"$source_json")"
    endpoint="$(jq -r '.downloadUrlOrEndpoint // ""' <<<"$source_json")"
    access_type="$(jq -r '.accessType // ""' <<<"$source_json")"
    last_verified="$(jq -r '.lastVerifiedAt // ""' <<<"$source_json")"
    fallback_reason="$(jq -r '.fallbackReason // ""' <<<"$source_json")"

    log "--- Source: $id ($country/$format) ---"

    local f
    for f in "${required_fields[@]}"; do
      local value
      value="$(jq -r --arg f "$f" '.[$f] // ""' <<<"$source_json")"
      if [[ -z "$value" ]]; then
        err "Source '$id' missing required field '$f'"
      fi
    done

    if [[ ! "$country" =~ ^[A-Z]{2}$ ]]; then
      err "Source '$id' has invalid country '$country'"
    fi

    if [[ "$format" != "netex" && "$format" != "gtfs" ]]; then
      err "Source '$id' has invalid format '$format'"
    fi

    if [[ "$format" == "netex" ]]; then
      netex_count=$((netex_count + 1))
      if [[ -n "$fallback_reason" ]]; then
        warn "Source '$id' is netex but contains fallbackReason (ignored)"
      fi
    fi

    if [[ "$format" == "gtfs" ]]; then
      gtfs_count=$((gtfs_count + 1))
      if [[ -z "$fallback_reason" ]]; then
        err "Source '$id' is gtfs but missing required fallbackReason"
      else
        gtfs_report+=("$id: $fallback_reason")
      fi
    fi

    if [[ "$method" != "direct" && "$method" != "direct_url" && "$method" != "api" && "$method" != "manual_redirect" ]]; then
      err "Source '$id' has invalid downloadMethod '$method'"
    fi

    if [[ "$access_type" != "public" && "$access_type" != "api_key" && "$access_type" != "token" && "$access_type" != "other" ]]; then
      err "Source '$id' has invalid accessType '$access_type'"
    fi

    if ! is_iso_ts "$last_verified"; then
      err "Source '$id' has invalid lastVerifiedAt '$last_verified' (expected ISO UTC timestamp)"
    fi

    local auth_ok=1
    build_auth_args "$id" "$access_type" "$endpoint" || auth_ok=0

    check_reachable "$portal_url" "portalUrl for $id" "${AUTH_ARGS[@]}" || true

    local resolved_url
    if [[ "$auth_ok" -eq 0 && "$access_type" != "public" ]]; then
      warn "Skipping authenticated download resolution for '$id' until auth is configured."
      continue
    fi

    if resolved_url="$(resolve_url "$id" "$method" "$endpoint" 2>/dev/null)"; then
      log "Resolved download URL: $resolved_url"
      check_reachable "$resolved_url" "download URL for $id" "${AUTH_ARGS[@]}" || true
      if [[ "$format" == "netex" && ! "$resolved_url" =~ [Nn][Ee][Tt][Ee][Xx]|[Nn][Ee][Tt][Xx]|\.xml ]]; then
        warn "Source '$id' resolved URL does not clearly indicate NeTEx by name: $resolved_url"
      fi
      if [[ "$format" == "gtfs" && ! "$resolved_url" =~ [Gg][Tt][Ff][Ss]|google_transit ]]; then
        warn "Source '$id' resolved URL does not clearly indicate GTFS by name: $resolved_url"
      fi
    else
      if [[ "$method" == "manual_redirect" ]]; then
        err "Source '$id' manual_redirect resolution failed from endpoint: $endpoint"
      else
        check_reachable "$endpoint" "download endpoint for $id" "${AUTH_ARGS[@]}" || true
      fi
    fi
  done

  log "--- Policy Report ---"
  log "Official NeTEx entries: $netex_count"
  log "GTFS fallback entries: $gtfs_count"

  if [[ ${#gtfs_report[@]} -gt 0 ]]; then
    log "GTFS fallback reasons:"
    local row
    for row in "${gtfs_report[@]}"; do
      log "  - $row"
    done
  fi

  if [[ $ERRORS -gt 0 ]]; then
    log "Verification finished with $ERRORS error(s) and $WARNINGS warning(s)."
    exit 1
  fi

  log "Verification passed with $WARNINGS warning(s)."
  return 0
}

main "$@"
