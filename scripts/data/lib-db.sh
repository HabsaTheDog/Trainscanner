#!/usr/bin/env bash

set -euo pipefail

DATA_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${DATA_SCRIPT_DIR}/../.." && pwd)"

DB_MODE_EFFECTIVE=""
DB_URL=""
DB_HOST=""
DB_PORT=""
DB_USER=""
DB_NAME=""
DB_PASSWORD=""
DB_DOCKER_PROFILE=""
DB_DOCKER_SERVICE=""
DB_READY_TIMEOUT_SEC="${CANONICAL_DB_READY_TIMEOUT_SEC:-90}"
DB_MODE_DOCKER_COMPOSE="docker-compose"

db_log() {
  printf '[db] %s\n' "$*"
  return 0
}

db_fail() {
  printf '[db] ERROR: %s\n' "$*" >&2
  return 1
}

db_load_env() {
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

db_require_cmd() {
  local cmd="$1"
  command -v "$cmd" >/dev/null 2>&1 || db_fail "Missing required command: $cmd"
  return 0
}

db_sql_escape() {
  local value="$1"
  printf '%s' "$value" | sed "s/'/''/g"
  return 0
}

db_has_explicit_direct_target() {
  if [[ -n "${CANONICAL_DB_URL:-}" ]] \
    || [[ -n "${DATABASE_URL:-}" ]] \
    || [[ -n "${CANONICAL_DB_HOST:-}" ]] \
    || [[ -n "${CANONICAL_DB_PORT:-}" ]] \
    || [[ -n "${CANONICAL_DB_USER:-}" ]] \
    || [[ -n "${CANONICAL_DB_NAME:-}" ]] \
    || [[ -n "${PGHOST:-}" ]] \
    || [[ -n "${PGPORT:-}" ]] \
    || [[ -n "${PGUSER:-}" ]] \
    || [[ -n "${PGDATABASE:-}" ]]; then
    return 0
  fi
  return 1
}

db_probe_direct_connection() {
  local probe_status
  if [[ -n "$DB_URL" ]]; then
    if PGCONNECT_TIMEOUT=3 PGPASSWORD="$DB_PASSWORD" \
      psql "$DB_URL" -v ON_ERROR_STOP=1 -At -c 'SELECT 1' >/dev/null 2>&1; then
      probe_status=0
    else
      probe_status=$?
    fi
  else
    if PGCONNECT_TIMEOUT=3 PGPASSWORD="$DB_PASSWORD" \
      psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 -At -c 'SELECT 1' >/dev/null 2>&1; then
      probe_status=0
    else
      probe_status=$?
    fi
  fi
  return "$probe_status"
}

db_resolve_connection() {
  local requested_mode="${CANONICAL_DB_MODE:-auto}"
  local explicit_direct_target="false"

  DB_URL="${CANONICAL_DB_URL:-${DATABASE_URL:-}}"
  DB_HOST="${CANONICAL_DB_HOST:-${PGHOST:-localhost}}"
  DB_PORT="${CANONICAL_DB_PORT:-${PGPORT:-55432}}"
  DB_USER="${CANONICAL_DB_USER:-${PGUSER:-trainscanner}}"
  DB_NAME="${CANONICAL_DB_NAME:-${PGDATABASE:-trainscanner}}"
  DB_PASSWORD="${CANONICAL_DB_PASSWORD:-${PGPASSWORD:-trainscanner}}"
  DB_DOCKER_PROFILE="${CANONICAL_DB_DOCKER_PROFILE:-pan-europe-data}"
  DB_DOCKER_SERVICE="${CANONICAL_DB_DOCKER_SERVICE:-postgis}"
  if db_has_explicit_direct_target; then
    explicit_direct_target="true"
  fi

  case "$requested_mode" in
    auto)
      if command -v psql >/dev/null 2>&1; then
        if db_probe_direct_connection; then
          DB_MODE_EFFECTIVE="direct"
          db_log "Auto mode selected direct database connection"
        else
          if [[ "$explicit_direct_target" == "true" ]]; then
            db_fail "Auto mode detected explicit direct DB configuration, but direct connection failed. Fix connectivity or set CANONICAL_DB_MODE=docker-compose."
          fi
          DB_MODE_EFFECTIVE="$DB_MODE_DOCKER_COMPOSE"
          db_log "Auto mode falling back to docker-compose database service '${DB_DOCKER_SERVICE}'"
        fi
      else
        if [[ "$explicit_direct_target" == "true" ]]; then
          db_fail "Auto mode detected explicit direct DB configuration, but psql is unavailable. Install psql or set CANONICAL_DB_MODE=docker-compose."
        fi
        DB_MODE_EFFECTIVE="$DB_MODE_DOCKER_COMPOSE"
        db_log "Auto mode selected docker-compose database service '${DB_DOCKER_SERVICE}' because psql is unavailable"
      fi
      ;;
    direct)
      DB_MODE_EFFECTIVE="direct"
      ;;
    docker-compose)
      DB_MODE_EFFECTIVE="$DB_MODE_DOCKER_COMPOSE"
      ;;
    *)
      db_fail "Invalid CANONICAL_DB_MODE '$requested_mode' (expected auto, direct, docker-compose)"
      ;;
  esac

  if [[ "$DB_MODE_EFFECTIVE" == "direct" ]]; then
    db_require_cmd psql
  else
    db_require_cmd docker
  fi
  return 0
}

db_ensure_ready() {
  local started_at now elapsed

  if [[ "$DB_MODE_EFFECTIVE" == "$DB_MODE_DOCKER_COMPOSE" ]]; then
    db_log "Ensuring docker compose service '${DB_DOCKER_SERVICE}' (profile '${DB_DOCKER_PROFILE}') is running"
    (cd "$ROOT_DIR" && docker compose --profile "$DB_DOCKER_PROFILE" up -d "$DB_DOCKER_SERVICE") >/dev/null
  fi

  started_at="$(date +%s)"
  while true; do
    if db_psql -At -c 'SELECT 1' >/dev/null 2>&1; then
      return 0
    fi

    now="$(date +%s)"
    elapsed="$((now - started_at))"
    if [[ "$elapsed" -ge "$DB_READY_TIMEOUT_SEC" ]]; then
      db_fail "Database was not ready within ${DB_READY_TIMEOUT_SEC}s"
    fi
    sleep 2
  done
  return 0
}

db_psql() {
  if [[ "$DB_MODE_EFFECTIVE" == "$DB_MODE_DOCKER_COMPOSE" ]]; then
    (cd "$ROOT_DIR" && docker compose --profile "$DB_DOCKER_PROFILE" exec -T "$DB_DOCKER_SERVICE" \
      psql -v ON_ERROR_STOP=1 -U "$DB_USER" -d "$DB_NAME" "$@")
  else
    if [[ -n "$DB_URL" ]]; then
      PGPASSWORD="$DB_PASSWORD" psql "$DB_URL" -v ON_ERROR_STOP=1 "$@"
    else
      PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 "$@"
    fi
  fi
  return 0
}

db_copy_csv_from_file() {
  local csv_file="$1"
  local copy_target="$2"

  [[ -f "$csv_file" ]] || db_fail "CSV file not found: $csv_file"

  if [[ "$DB_MODE_EFFECTIVE" == "$DB_MODE_DOCKER_COMPOSE" ]]; then
    cat "$csv_file" | (cd "$ROOT_DIR" && docker compose --profile "$DB_DOCKER_PROFILE" exec -T "$DB_DOCKER_SERVICE" \
      psql -v ON_ERROR_STOP=1 -U "$DB_USER" -d "$DB_NAME" -c "\\copy ${copy_target} FROM STDIN WITH (FORMAT csv, HEADER true)")
  else
    if [[ -n "$DB_URL" ]]; then
      cat "$csv_file" | PGPASSWORD="$DB_PASSWORD" psql "$DB_URL" -v ON_ERROR_STOP=1 -c "\\copy ${copy_target} FROM STDIN WITH (FORMAT csv, HEADER true)"
    else
      cat "$csv_file" | PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 \
        -c "\\copy ${copy_target} FROM STDIN WITH (FORMAT csv, HEADER true)"
    fi
  fi
  return 0
}
