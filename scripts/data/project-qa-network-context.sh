#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/lib-db.sh"

COUNTRY_FILTER=""
SOURCE_ID_FILTER=""
AS_OF=""
SKIP_DB_BOOTSTRAP="false"

usage() {
  cat <<USAGE
Usage: scripts/data/project-qa-network-context.sh [options]

Project provider-level QA route and adjacency context onto global stations.

Options:
  --country <ISO2>       Restrict projection to one country
  --source-id <id>       Restrict projection to one source id
  --as-of YYYY-MM-DD     Use latest dataset snapshot <= date
  --skip-db-bootstrap    Skip db-bootstrap.sh preflight
  -h, --help             Show this help
USAGE
  return 0
}

log() {
  printf '[project-qa-network] %s\n' "$*"
  return 0
}

fail() {
  printf '[project-qa-network] ERROR: %s\n' "$*" >&2
  return 1
}

is_iso_date() {
  local d="$1"
  [[ "$d" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || return 1
  date -u -d "$d" +%F >/dev/null 2>&1
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

main() {
  local country_filter_esc source_id_filter_esc as_of_esc summary

  parse_args "$@"

  db_load_env
  db_resolve_connection
  db_ensure_ready

  if [[ "$SKIP_DB_BOOTSTRAP" != "true" ]]; then
    "${SCRIPT_DIR}/db-bootstrap.sh" --quiet --if-ready
  fi

  country_filter_esc="$(db_sql_escape "$COUNTRY_FILTER")"
  source_id_filter_esc="$(db_sql_escape "$SOURCE_ID_FILTER")"
  as_of_esc="$(db_sql_escape "$AS_OF")"

  log "Projecting QA network context country=${COUNTRY_FILTER:-ALL} source=${SOURCE_ID_FILTER:-ALL} as-of=${AS_OF:-latest}"

  db_psql -c "
    CREATE TEMP TABLE _qa_selected_datasets AS
    SELECT DISTINCT ON (pd.source_id)
      pd.dataset_id,
      pd.source_id,
      pd.country,
      pd.snapshot_date
    FROM provider_datasets pd
    WHERE pd.format = 'netex'
      AND (
        NULLIF('${country_filter_esc}', '') IS NULL
        OR pd.country = NULLIF('${country_filter_esc}', '')::char(2)
      )
      AND (
        NULLIF('${source_id_filter_esc}', '') IS NULL
        OR pd.source_id = NULLIF('${source_id_filter_esc}', '')
      )
      AND (
        NULLIF('${as_of_esc}', '') IS NULL
        OR pd.snapshot_date <= NULLIF('${as_of_esc}', '')::date
      )
    ORDER BY pd.source_id, pd.snapshot_date DESC, pd.dataset_id DESC;

    DELETE FROM qa_global_station_routes gr
    WHERE EXISTS (
      SELECT 1
      FROM _qa_selected_datasets scope
      WHERE scope.source_id = gr.source_id
        AND (
          gr.source_country IS NULL
          OR scope.country IS NULL
          OR gr.source_country = scope.country
        )
    );

    DELETE FROM qa_global_station_adjacencies ga
    WHERE EXISTS (
      SELECT 1
      FROM _qa_selected_datasets scope
      WHERE scope.source_id = ga.source_id
        AND (
          ga.source_country IS NULL
          OR scope.country IS NULL
          OR ga.source_country = scope.country
        )
    );

    INSERT INTO qa_global_station_routes (
      global_station_id,
      source_id,
      source_country,
      route_label,
      transport_mode,
      pattern_hits,
      metadata,
      updated_at
    )
    SELECT
      m.global_station_id,
      routes.source_id,
      scope.country,
      routes.route_label,
      routes.transport_mode,
      SUM(routes.pattern_hits)::integer AS pattern_hits,
      jsonb_build_object(
        'dataset_id', scope.dataset_id,
        'snapshot_date', scope.snapshot_date
      ),
      now()
    FROM _qa_selected_datasets scope
    JOIN qa_provider_stop_place_routes routes
      ON routes.dataset_id = scope.dataset_id
     AND routes.source_id = scope.source_id
    JOIN provider_global_station_mappings m
      ON m.source_id = routes.source_id
     AND m.provider_stop_place_ref = routes.provider_stop_place_ref
     AND m.is_active = true
    JOIN global_stations gs
      ON gs.global_station_id = m.global_station_id
     AND gs.is_active = true
    GROUP BY
      m.global_station_id,
      routes.source_id,
      scope.country,
      routes.route_label,
      routes.transport_mode,
      scope.dataset_id,
      scope.snapshot_date
    ON CONFLICT (global_station_id, source_id, route_label, transport_mode)
    DO UPDATE SET
      source_country = EXCLUDED.source_country,
      pattern_hits = EXCLUDED.pattern_hits,
      metadata = EXCLUDED.metadata,
      updated_at = EXCLUDED.updated_at;

    INSERT INTO qa_global_station_adjacencies (
      global_station_id,
      neighbor_global_station_id,
      direction,
      source_id,
      source_country,
      pattern_hits,
      metadata,
      updated_at
    )
    SELECT
      rows.global_station_id,
      rows.neighbor_global_station_id,
      rows.direction,
      rows.source_id,
      rows.source_country,
      rows.pattern_hits,
      rows.metadata,
      now()
    FROM (
      SELECT
        from_map.global_station_id,
        to_map.global_station_id AS neighbor_global_station_id,
        'outgoing'::text AS direction,
        adj.source_id,
        scope.country AS source_country,
        SUM(adj.pattern_hits)::integer AS pattern_hits,
        jsonb_build_object(
          'dataset_id', scope.dataset_id,
          'snapshot_date', scope.snapshot_date
        ) AS metadata
      FROM _qa_selected_datasets scope
      JOIN qa_provider_stop_place_adjacencies adj
        ON adj.dataset_id = scope.dataset_id
       AND adj.source_id = scope.source_id
      JOIN provider_global_station_mappings from_map
        ON from_map.source_id = adj.source_id
       AND from_map.provider_stop_place_ref = adj.from_provider_stop_place_ref
       AND from_map.is_active = true
      JOIN provider_global_station_mappings to_map
        ON to_map.source_id = adj.source_id
       AND to_map.provider_stop_place_ref = adj.to_provider_stop_place_ref
       AND to_map.is_active = true
      JOIN global_stations from_station
        ON from_station.global_station_id = from_map.global_station_id
       AND from_station.is_active = true
      JOIN global_stations to_station
        ON to_station.global_station_id = to_map.global_station_id
       AND to_station.is_active = true
      WHERE from_map.global_station_id <> to_map.global_station_id
      GROUP BY
        from_map.global_station_id,
        to_map.global_station_id,
        adj.source_id,
        scope.country,
        scope.dataset_id,
        scope.snapshot_date

      UNION ALL

      SELECT
        to_map.global_station_id AS global_station_id,
        from_map.global_station_id AS neighbor_global_station_id,
        'incoming'::text AS direction,
        adj.source_id,
        scope.country AS source_country,
        SUM(adj.pattern_hits)::integer AS pattern_hits,
        jsonb_build_object(
          'dataset_id', scope.dataset_id,
          'snapshot_date', scope.snapshot_date
        ) AS metadata
      FROM _qa_selected_datasets scope
      JOIN qa_provider_stop_place_adjacencies adj
        ON adj.dataset_id = scope.dataset_id
       AND adj.source_id = scope.source_id
      JOIN provider_global_station_mappings from_map
        ON from_map.source_id = adj.source_id
       AND from_map.provider_stop_place_ref = adj.from_provider_stop_place_ref
       AND from_map.is_active = true
      JOIN provider_global_station_mappings to_map
        ON to_map.source_id = adj.source_id
       AND to_map.provider_stop_place_ref = adj.to_provider_stop_place_ref
       AND to_map.is_active = true
      JOIN global_stations from_station
        ON from_station.global_station_id = from_map.global_station_id
       AND from_station.is_active = true
      JOIN global_stations to_station
        ON to_station.global_station_id = to_map.global_station_id
       AND to_station.is_active = true
      WHERE from_map.global_station_id <> to_map.global_station_id
      GROUP BY
        to_map.global_station_id,
        from_map.global_station_id,
        adj.source_id,
        scope.country,
        scope.dataset_id,
        scope.snapshot_date
    ) rows
    ON CONFLICT (
      global_station_id,
      neighbor_global_station_id,
      direction,
      source_id
    )
    DO UPDATE SET
      source_country = EXCLUDED.source_country,
      pattern_hits = EXCLUDED.pattern_hits,
      metadata = EXCLUDED.metadata,
      updated_at = EXCLUDED.updated_at;
  " >/dev/null

  summary="$(db_psql -At -c "
    WITH scope AS (
      SELECT source_id
      FROM provider_datasets pd
      WHERE pd.format = 'netex'
        AND (
          NULLIF('${country_filter_esc}', '') IS NULL
          OR pd.country = NULLIF('${country_filter_esc}', '')::char(2)
        )
        AND (
          NULLIF('${source_id_filter_esc}', '') IS NULL
          OR pd.source_id = NULLIF('${source_id_filter_esc}', '')
        )
        AND (
          NULLIF('${as_of_esc}', '') IS NULL
          OR pd.snapshot_date <= NULLIF('${as_of_esc}', '')::date
        )
      GROUP BY source_id
    )
    SELECT json_build_object(
      'sourceCount', (SELECT COUNT(*) FROM scope),
      'globalRouteRows', (
        SELECT COUNT(*)
        FROM qa_global_station_routes
        WHERE source_id IN (SELECT source_id FROM scope)
      ),
      'globalAdjacencyRows', (
        SELECT COUNT(*)
        FROM qa_global_station_adjacencies
        WHERE source_id IN (SELECT source_id FROM scope)
      )
    )::text;
  ")"

  log "${summary}"
}

main "$@"
