const { AppError } = require("../../../core/errors");
const { validateOrThrow } = require("../../../core/schema");
const {
  ensureMergeClusterEvidenceColumns,
} = require("./merge-evidence-schema");

const MERGE_QUEUE_SUMMARY_SCHEMA = {
  type: "object",
  required: ["scopeTag", "clusters", "candidates", "evidence"],
  properties: {
    scopeCountry: { type: "string" },
    scopeAsOf: { type: "string" },
    scopeTag: { type: "string", minLength: 1 },
    clusters: { type: "integer", minimum: 0 },
    candidates: { type: "integer", minimum: 0 },
    evidence: { type: "integer", minimum: 0 },
  },
  additionalProperties: true,
};

const DEFAULT_MERGE_QUEUE_MAX_PARALLEL_WORKERS = 4;
const DEFAULT_MERGE_QUEUE_WORK_MEM = "64MB";
const DEFAULT_MERGE_QUEUE_MAINTENANCE_WORK_MEM = "256MB";

function resolvePositiveIntegerEnv(name, fallback) {
  const rawValue = String(process.env[name] || "").trim();
  if (!rawValue) {
    return fallback;
  }

  const parsed = Number.parseInt(rawValue, 10);
  if (!Number.isInteger(parsed) || parsed < 0) {
    return fallback;
  }

  return parsed;
}

function resolveMemoryEnv(name, fallback) {
  const rawValue = String(process.env[name] || "").trim();
  if (!rawValue) {
    return fallback;
  }

  if (!/^\d+(kB|MB|GB)$/i.test(rawValue)) {
    return fallback;
  }

  return rawValue.toUpperCase();
}

const MERGE_QUEUE_MAX_PARALLEL_WORKERS = resolvePositiveIntegerEnv(
  "GLOBAL_MERGE_QUEUE_MAX_PARALLEL_WORKERS",
  DEFAULT_MERGE_QUEUE_MAX_PARALLEL_WORKERS,
);
const MERGE_QUEUE_WORK_MEM = resolveMemoryEnv(
  "GLOBAL_MERGE_QUEUE_WORK_MEM",
  DEFAULT_MERGE_QUEUE_WORK_MEM,
);
const MERGE_QUEUE_MAINTENANCE_WORK_MEM = resolveMemoryEnv(
  "GLOBAL_MERGE_QUEUE_MAINTENANCE_WORK_MEM",
  DEFAULT_MERGE_QUEUE_MAINTENANCE_WORK_MEM,
);

function extractJsonLine(stdout) {
  const lines = String(stdout || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);

  for (let i = lines.length - 1; i >= 0; i -= 1) {
    if (lines[i].startsWith("{") && lines[i].endsWith("}")) {
      return lines[i];
    }
  }

  return "";
}

function extractPhaseFromNotice(notice) {
  const message = String(notice?.message || notice || "").trim();
  if (!message.startsWith("merge_queue_phase:")) {
    return "";
  }
  return message.slice("merge_queue_phase:".length).trim();
}

function extractInfoFromNotice(notice) {
  const message = String(notice?.message || notice || "").trim();
  if (!message.startsWith("merge_queue_info:")) {
    return null;
  }

  const payload = message.slice("merge_queue_info:".length).trim();
  const separatorIndex = payload.indexOf("=");
  if (separatorIndex <= 0) {
    return null;
  }

  return {
    key: payload.slice(0, separatorIndex).trim(),
    value: payload.slice(separatorIndex + 1).trim(),
  };
}

function scopeParams(scope = {}) {
  return {
    country_filter: scope.country || "",
    as_of: scope.asOf || "",
  };
}

const CURRENT_MERGE_QUEUE_SUMMARY_SQL = `
WITH scope AS (
  SELECT COALESCE(NULLIF(:'as_of', ''), 'latest') AS scope_tag
)
SELECT json_build_object(
  'scopeCountry', COALESCE(NULLIF(:'country_filter', ''), ''),
  'scopeAsOf', COALESCE(NULLIF(:'as_of', ''), ''),
  'scopeTag', (SELECT scope_tag FROM scope),
  'clusters', (
    SELECT COUNT(*)
    FROM qa_merge_clusters c
    WHERE c.scope_tag = (SELECT scope_tag FROM scope)
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR NULLIF(:'country_filter', '') = ANY (COALESCE(c.country_tags, ARRAY[]::text[]))
      )
  ),
  'candidates', (
    SELECT COUNT(*)
    FROM qa_merge_cluster_candidates c
    JOIN qa_merge_clusters mc
      ON mc.merge_cluster_id = c.merge_cluster_id
    WHERE mc.scope_tag = (SELECT scope_tag FROM scope)
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR NULLIF(:'country_filter', '') = ANY (COALESCE(mc.country_tags, ARRAY[]::text[]))
      )
  ),
  'evidence', (
    SELECT COUNT(*)
    FROM qa_merge_cluster_evidence e
    JOIN qa_merge_clusters mc
      ON mc.merge_cluster_id = e.merge_cluster_id
    WHERE mc.scope_tag = (SELECT scope_tag FROM scope)
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR NULLIF(:'country_filter', '') = ANY (COALESCE(mc.country_tags, ARRAY[]::text[]))
      )
  )
)::text AS summary_json;
`;

const MERGE_QUEUE_FINGERPRINT_SQL = `
SELECT json_build_object(
  'stage', 'merge-queue',
  'country', COALESCE(NULLIF(:'country_filter', ''), ''),
  'asOf', COALESCE(NULLIF(:'as_of', ''), ''),
  'globalStationUpdatedAtMax', COALESCE(
    (
      SELECT to_char(MAX(gs.updated_at), 'YYYY-MM-DD"T"HH24:MI:SSOF')
      FROM global_stations gs
      WHERE gs.is_active = true
        AND (
          NULLIF(:'country_filter', '') IS NULL
          OR gs.country = NULLIF(:'country_filter', '')::char(2)
        )
    ),
    ''
  ),
  'activeStationCount', (
    SELECT COUNT(*)
    FROM global_stations gs
    WHERE gs.is_active = true
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR gs.country = NULLIF(:'country_filter', '')::char(2)
      )
  ),
  'activeStopPointCount', (
    SELECT COUNT(*)
    FROM global_stop_points sp
    WHERE sp.is_active = true
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR sp.country = NULLIF(:'country_filter', '')::char(2)
      )
  ),
  'activeStationMappingCount', (
    SELECT COUNT(*)
    FROM provider_global_station_mappings m
    JOIN global_stations gs
      ON gs.global_station_id = m.global_station_id
    WHERE m.is_active = true
      AND gs.is_active = true
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR gs.country = NULLIF(:'country_filter', '')::char(2)
      )
  )
) AS fingerprint;
`;

const LOOSE_NAME_TOKEN_SPLIT_REGEX_SQL = String.raw`\s+`;
const LOOSE_NAME_TOKEN_STOP_WORDS_SQL = [
  "bahnhof",
  "hauptbahnhof",
  "station",
  "halt",
  "haltestelle",
  "busbahnhof",
  "bus",
  "tram",
]
  .map((token) => `'${token}'`)
  .join(", ");

function buildLooseStationNameSql(inputExpression) {
  return String.raw`
trim(
  regexp_replace(
    regexp_replace(
      regexp_replace(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              lower(unaccent(coalesce(${inputExpression}, ''))),
              '\b(abzw|abzw\.)\b',
              ' abzweig ',
              'g'
            ),
            '\b(bhf|bf)\b',
            ' bahnhof ',
            'g'
          ),
          '\b(hbf)\b',
          ' hauptbahnhof ',
          'g'
        ),
        '\b(str|str\.)\b',
        ' strasse ',
        'g'
      ),
      '[^[:alnum:]]+',
      ' ',
      'g'
    ),
    '\s+',
    ' ',
    'g'
  )
)`;
}

const BUILD_MERGE_QUEUE_SQL = `
BEGIN;

SET LOCAL max_parallel_workers_per_gather = ${MERGE_QUEUE_MAX_PARALLEL_WORKERS};
SET LOCAL work_mem = '${MERGE_QUEUE_WORK_MEM}';
SET LOCAL maintenance_work_mem = '${MERGE_QUEUE_MAINTENANCE_WORK_MEM}';
SET LOCAL jit = off;

DO $$
BEGIN
  RAISE NOTICE 'merge_queue_phase:initializing';
END $$;

CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TEMP TABLE _scope AS
SELECT
  COALESCE(NULLIF(:'as_of', ''), 'latest') AS scope_tag,
  NULLIF(:'as_of', '')::date AS scope_as_of;

DELETE FROM qa_merge_clusters c
WHERE c.scope_tag = (SELECT scope_tag FROM _scope)
  AND (
    NULLIF(:'country_filter', '') IS NULL
    OR NULLIF(:'country_filter', '') = ANY (COALESCE(c.country_tags, ARRAY[]::text[]))
  );

DO $$
BEGIN
  RAISE NOTICE 'merge_queue_phase:building_station_context';
END $$;

CREATE TEMP TABLE _scope_stations AS
SELECT
  gs.global_station_id,
  gs.display_name,
  gs.normalized_name,
  ${buildLooseStationNameSql("gs.display_name")} AS loose_name,
  gs.country,
  gs.latitude,
  gs.longitude,
  gs.geom,
  CASE
    WHEN gs.geom IS NOT NULL THEN ST_Transform(gs.geom, 3857)::geometry(Point, 3857)
    ELSE NULL::geometry(Point, 3857)
  END AS geom_3857
FROM global_stations gs
WHERE gs.is_active = true
  AND EXISTS (
    SELECT 1
    FROM provider_global_station_mappings m
    WHERE m.global_station_id = gs.global_station_id
      AND m.is_active = true
  )
  AND (
    NULLIF(:'country_filter', '') IS NULL
    OR gs.country = NULLIF(:'country_filter', '')::char(2)
  );

CREATE INDEX _scope_stations_station_idx
  ON _scope_stations (global_station_id);

CREATE INDEX _scope_stations_country_idx
  ON _scope_stations (country, global_station_id);

ANALYZE _scope_stations;

CREATE TEMP TABLE _station_provider_context AS
SELECT
  ss.global_station_id,
  COALESCE(
    ARRAY_AGG(DISTINCT m.source_id ORDER BY m.source_id)
      FILTER (WHERE m.source_id IS NOT NULL),
    ARRAY[]::text[]
  ) AS provider_sources
FROM _scope_stations ss
LEFT JOIN provider_global_station_mappings m
  ON m.global_station_id = ss.global_station_id
 AND m.is_active = true
GROUP BY ss.global_station_id;

CREATE TEMP TABLE _station_stop_point_counts AS
SELECT
  sp.global_station_id,
  COUNT(*)::integer AS stop_point_count
FROM global_stop_points sp
JOIN _scope_stations ss
  ON ss.global_station_id = sp.global_station_id
WHERE sp.is_active = true
GROUP BY sp.global_station_id;

CREATE TEMP TABLE _station_context AS
SELECT
  base.global_station_id,
  base.display_name,
  base.normalized_name,
  base.loose_name,
  base.country,
  base.latitude,
  base.longitude,
  base.geom,
  base.geom_3857,
  CASE
    WHEN base.geom_3857 IS NOT NULL THEN floor(ST_X(base.geom_3857) / 2000)::integer
    ELSE NULL::integer
  END AS tile_2km_x,
  CASE
    WHEN base.geom_3857 IS NOT NULL THEN floor(ST_Y(base.geom_3857) / 2000)::integer
    ELSE NULL::integer
  END AS tile_2km_y,
  base.provider_sources,
  ARRAY[]::text[] AS aliases,
  ARRAY[]::text[] AS route_labels,
  ARRAY[]::text[] AS transport_modes,
  ARRAY[]::text[] AS incoming_labels,
  ARRAY[]::text[] AS outgoing_labels,
  ARRAY[]::text[] AS adjacent_labels,
  base.stop_point_count,
  cardinality(base.provider_sources) AS provider_source_count,
  CASE
    WHEN base.geom_3857 IS NOT NULL THEN 'coordinates_present'
    ELSE 'missing_coordinates'
  END AS coord_status,
  (
    base.loose_name ~
    '^(steig|gleis|platform|plattform|quai|bussteig|bahnsteig)( [[:alnum:]]+)?$'
  ) AS is_lexically_generic,
  false AS is_generic,
  0::integer AS name_frequency
FROM (
  SELECT
    ss.global_station_id,
    ss.display_name,
    ss.normalized_name,
    ss.loose_name,
    ss.country,
    ss.latitude,
    ss.longitude,
    ss.geom,
    ss.geom_3857,
    COALESCE(pc.provider_sources, ARRAY[]::text[]) AS provider_sources,
    COALESCE(spc.stop_point_count, 0) AS stop_point_count
  FROM _scope_stations ss
  LEFT JOIN _station_provider_context pc
    ON pc.global_station_id = ss.global_station_id
  LEFT JOIN _station_stop_point_counts spc
    ON spc.global_station_id = ss.global_station_id
) base;

CREATE INDEX _station_context_station_idx
  ON _station_context (global_station_id);

CREATE INDEX _station_context_normalized_name_idx
  ON _station_context (normalized_name);

CREATE INDEX _station_context_geo_tile_idx
  ON _station_context (country, tile_2km_x, tile_2km_y, global_station_id);

ANALYZE _station_context;

DO $$
BEGIN
  RAISE NOTICE 'merge_queue_phase:building_pair_seeds';
END $$;

CREATE TEMP TABLE _pair_seed_reasons (
  source_global_station_id text NOT NULL,
  target_global_station_id text NOT NULL,
  seed_reason text NOT NULL,
  PRIMARY KEY (
    source_global_station_id,
    target_global_station_id,
    seed_reason
  )
);

INSERT INTO _pair_seed_reasons (
  source_global_station_id,
  target_global_station_id,
  seed_reason
)
SELECT
  LEAST(a.global_station_id, b.global_station_id),
  GREATEST(a.global_station_id, b.global_station_id),
  'exact_name'::text
FROM _station_context a
JOIN _station_context b
  ON b.global_station_id > a.global_station_id
 AND a.normalized_name = b.normalized_name
WHERE
  (
    a.geom_3857 IS NOT NULL
    AND b.geom_3857 IS NOT NULL
    AND ST_DWithin(a.geom_3857, b.geom_3857, 5000)
  )
  OR (
    (a.geom_3857 IS NULL OR b.geom_3857 IS NULL)
    AND a.country IS NOT NULL
    AND a.country = b.country
  )
  OR (
    a.country IS NOT NULL
    AND a.country = b.country
    AND a.provider_sources && b.provider_sources
    AND similarity(a.loose_name, b.loose_name) >= 0.92
  )
ON CONFLICT DO NOTHING;

DO $$
DECLARE
  exact_name_count bigint;
BEGIN
  SELECT COUNT(*) INTO exact_name_count
  FROM _pair_seed_reasons
  WHERE seed_reason = 'exact_name';
  RAISE NOTICE 'merge_queue_info:pair_seeds_exact_name=%', exact_name_count;
END $$;

INSERT INTO _pair_seed_reasons (
  source_global_station_id,
  target_global_station_id,
  seed_reason
)
SELECT
  LEAST(a.global_station_id, b.global_station_id),
  GREATEST(a.global_station_id, b.global_station_id),
  'loose_name_geo'::text
FROM _station_context a
JOIN _station_context b
  ON b.global_station_id > a.global_station_id
 AND b.country = a.country
 AND b.tile_2km_x BETWEEN a.tile_2km_x - 1 AND a.tile_2km_x + 1
 AND b.tile_2km_y BETWEEN a.tile_2km_y - 1 AND a.tile_2km_y + 1
WHERE a.loose_name % b.loose_name
  AND a.geom_3857 IS NOT NULL
  AND b.geom_3857 IS NOT NULL
  AND similarity(a.loose_name, b.loose_name) >= 0.72
  AND ST_DWithin(a.geom_3857, b.geom_3857, 1500)
ON CONFLICT DO NOTHING;

DO $$
DECLARE
  loose_name_geo_count bigint;
BEGIN
  SELECT COUNT(*) INTO loose_name_geo_count
  FROM _pair_seed_reasons
  WHERE seed_reason = 'loose_name_geo';
  RAISE NOTICE 'merge_queue_info:pair_seeds_loose_name_geo=%', loose_name_geo_count;
END $$;

CREATE TEMP TABLE _missing_coord_subjects AS
SELECT
  sc.global_station_id,
  sc.country,
  sc.loose_name
FROM _station_context sc
WHERE sc.geom_3857 IS NULL
  AND sc.country IS NOT NULL;

CREATE INDEX _missing_coord_subjects_station_idx
  ON _missing_coord_subjects (global_station_id);

CREATE TEMP TABLE _missing_coord_subject_tokens AS
SELECT DISTINCT
  subject.global_station_id,
  subject.country,
  token
FROM _missing_coord_subjects subject
CROSS JOIN LATERAL regexp_split_to_table(
  subject.loose_name,
  '${LOOSE_NAME_TOKEN_SPLIT_REGEX_SQL}'
) AS token
WHERE token IS NOT NULL
  AND btrim(token) <> ''
  AND token NOT IN (${LOOSE_NAME_TOKEN_STOP_WORDS_SQL});

CREATE INDEX _missing_coord_subject_tokens_lookup_idx
  ON _missing_coord_subject_tokens (country, token, global_station_id);

CREATE TEMP TABLE _missing_coord_candidate_tokens AS
SELECT DISTINCT
  sc.global_station_id,
  sc.country,
  token
FROM _station_context sc
JOIN (
  SELECT DISTINCT country
  FROM _missing_coord_subjects
) missing_country
  ON missing_country.country = sc.country
CROSS JOIN LATERAL regexp_split_to_table(
  sc.loose_name,
  '${LOOSE_NAME_TOKEN_SPLIT_REGEX_SQL}'
) AS token
WHERE token IS NOT NULL
  AND btrim(token) <> ''
  AND token NOT IN (${LOOSE_NAME_TOKEN_STOP_WORDS_SQL});

CREATE INDEX _missing_coord_candidate_tokens_lookup_idx
  ON _missing_coord_candidate_tokens (country, token, global_station_id);

CREATE TEMP TABLE _missing_coord_token_frequency AS
SELECT
  rows.country,
  rows.token,
  COUNT(*)::integer AS station_count
FROM (
  SELECT DISTINCT
    global_station_id,
    country,
    token
  FROM _missing_coord_candidate_tokens
) rows
GROUP BY rows.country, rows.token;

CREATE TEMP TABLE _missing_coord_primary_tokens AS
SELECT
  ranked.global_station_id,
  ranked.token AS primary_rare_token
FROM (
  SELECT
    subject_token.global_station_id,
    subject_token.token,
    ROW_NUMBER() OVER (
      PARTITION BY subject_token.global_station_id
      ORDER BY tf.station_count ASC, subject_token.token ASC
    ) AS token_rank
  FROM _missing_coord_subject_tokens subject_token
  JOIN _missing_coord_token_frequency tf
    ON tf.country = subject_token.country
   AND tf.token = subject_token.token
  WHERE tf.station_count BETWEEN 2 AND 64
) ranked
WHERE ranked.token_rank = 1;

CREATE INDEX _missing_coord_primary_tokens_lookup_idx
  ON _missing_coord_primary_tokens (primary_rare_token, global_station_id);

ANALYZE _missing_coord_subjects;
ANALYZE _missing_coord_candidate_tokens;
ANALYZE _missing_coord_primary_tokens;

INSERT INTO _pair_seed_reasons (
  source_global_station_id,
  target_global_station_id,
  seed_reason
)
SELECT
  LEAST(subject.global_station_id, candidate.global_station_id),
  GREATEST(subject.global_station_id, candidate.global_station_id),
  'loose_name_missing_coords'::text
FROM _missing_coord_primary_tokens primary_token
JOIN _missing_coord_subjects subject
  ON subject.global_station_id = primary_token.global_station_id
JOIN _missing_coord_candidate_tokens candidate_token
  ON candidate_token.country = subject.country
 AND candidate_token.token = primary_token.primary_rare_token
JOIN _station_context subject_station
  ON subject_station.global_station_id = subject.global_station_id
JOIN _station_context candidate
  ON candidate.global_station_id = candidate_token.global_station_id
WHERE primary_token.primary_rare_token IS NOT NULL
  AND candidate.global_station_id <> subject.global_station_id
  AND subject_station.loose_name % candidate.loose_name
  AND similarity(subject_station.loose_name, candidate.loose_name) >= 0.88
ON CONFLICT DO NOTHING;

DO $$
DECLARE
  missing_coords_count bigint;
BEGIN
  SELECT COUNT(*) INTO missing_coords_count
  FROM _pair_seed_reasons
  WHERE seed_reason = 'loose_name_missing_coords';
  RAISE NOTICE 'merge_queue_info:pair_seeds_missing_coords=%', missing_coords_count;
END $$;

DO $$
DECLARE
  shared_route_count bigint := 0;
BEGIN
  RAISE NOTICE 'merge_queue_info:pair_seeds_shared_route=%', shared_route_count;
END $$;

DO $$
DECLARE
  shared_adjacent_count bigint := 0;
BEGIN
  RAISE NOTICE 'merge_queue_info:pair_seeds_shared_adjacent=%', shared_adjacent_count;
END $$;

CREATE TEMP TABLE _pair_seeds AS
SELECT
  source_global_station_id,
  target_global_station_id,
  ARRAY_AGG(DISTINCT seed_reason ORDER BY seed_reason) AS seed_reasons
FROM _pair_seed_reasons
GROUP BY source_global_station_id, target_global_station_id;

ANALYZE _pair_seeds;

DO $$
DECLARE
  pair_seed_total bigint;
BEGIN
  SELECT COUNT(*) INTO pair_seed_total
  FROM _pair_seeds;
  RAISE NOTICE 'merge_queue_info:pair_seeds_total=%', pair_seed_total;
END $$;

CREATE TEMP TABLE _pair_seed_features AS
SELECT
  ps.source_global_station_id,
  ps.target_global_station_id,
  ps.seed_reasons,
  a.display_name AS source_display_name,
  b.display_name AS target_display_name,
  a.normalized_name = b.normalized_name AS exact_name,
  similarity(a.loose_name, b.loose_name)::numeric(8,4) AS loose_similarity,
  0::numeric(8,4) AS token_overlap,
  CASE
    WHEN a.geom_3857 IS NULL OR b.geom_3857 IS NULL THEN NULL
    ELSE ST_Distance(a.geom_3857, b.geom_3857)
  END AS planar_distance_meters,
  CASE
    WHEN a.geom IS NULL OR b.geom IS NULL THEN NULL
    ELSE ST_DistanceSphere(a.geom, b.geom)
  END AS distance_meters,
  provider_overlap.provider_overlap_count,
  0::integer AS route_overlap_count,
  0::integer AS adjacent_overlap_count,
  CASE
    WHEN a.country IS NULL OR b.country IS NULL THEN NULL
    WHEN a.country = b.country THEN true
    ELSE false
  END AS same_country,
  (a.is_lexically_generic OR b.is_lexically_generic) AS generic_name_pair,
  0::integer AS generic_name_frequency
FROM _pair_seeds ps
JOIN _station_context a
  ON a.global_station_id = ps.source_global_station_id
JOIN _station_context b
  ON b.global_station_id = ps.target_global_station_id
CROSS JOIN LATERAL (
  SELECT COUNT(*)::integer AS provider_overlap_count
  FROM (
    SELECT DISTINCT item
    FROM unnest(a.provider_sources) item
    INTERSECT
    SELECT DISTINCT item
    FROM unnest(b.provider_sources) item
  ) overlap_items
) provider_overlap
;

CREATE TEMP TABLE _eligible_pairs AS
SELECT *
FROM _pair_seed_features ps
WHERE (
  (
    ps.exact_name = true
    AND (
      (ps.planar_distance_meters IS NOT NULL AND ps.planar_distance_meters <= 5000)
      OR (ps.planar_distance_meters IS NULL AND ps.same_country = true)
      OR ps.route_overlap_count > 0
      OR ps.adjacent_overlap_count > 0
      OR (
        ps.same_country = true
        AND ps.provider_overlap_count > 0
        AND ps.loose_similarity >= 0.92
      )
    )
  )
  OR (
    ps.loose_similarity >= 0.72
    AND ps.planar_distance_meters IS NOT NULL
    AND ps.planar_distance_meters <= 1500
  )
  OR (
    ps.loose_similarity >= 0.88
    AND ps.same_country = true
    AND ps.planar_distance_meters IS NULL
  )
  OR (
    (
      ps.route_overlap_count > 0
      OR ps.adjacent_overlap_count > 0
    )
    AND ps.loose_similarity >= 0.45
    AND (
      (ps.planar_distance_meters IS NOT NULL AND ps.planar_distance_meters <= 10000)
      OR (ps.planar_distance_meters IS NULL AND ps.same_country = true)
    )
  )
)
AND NOT (
  ps.generic_name_pair = true
  AND ps.provider_overlap_count = 0
  AND ps.route_overlap_count = 0
  AND ps.adjacent_overlap_count = 0
  AND COALESCE(ps.distance_meters, 999999) > 250
);

CREATE INDEX _eligible_pairs_source_idx
  ON _eligible_pairs (source_global_station_id);

CREATE INDEX _eligible_pairs_target_idx
  ON _eligible_pairs (target_global_station_id);

DO $$
BEGIN
  RAISE NOTICE 'merge_queue_phase:building_components';
END $$;

CREATE TEMP TABLE _cluster_nodes AS
SELECT source_global_station_id AS global_station_id
FROM _eligible_pairs
UNION
SELECT target_global_station_id AS global_station_id
FROM _eligible_pairs;

CREATE INDEX _cluster_nodes_station_idx
  ON _cluster_nodes (global_station_id);

CREATE TEMP TABLE _cluster_edges AS
SELECT
  source_global_station_id AS source_global_station_id,
  target_global_station_id AS target_global_station_id
FROM _eligible_pairs
UNION ALL
SELECT
  target_global_station_id AS source_global_station_id,
  source_global_station_id AS target_global_station_id
FROM _eligible_pairs;

CREATE INDEX _cluster_edges_source_idx
  ON _cluster_edges (source_global_station_id);

CREATE TEMP TABLE _components AS
WITH RECURSIVE walk(node_id, root_id) AS (
  SELECT
    n.global_station_id AS node_id,
    n.global_station_id AS root_id
  FROM _cluster_nodes n
  UNION
  SELECT
    e.target_global_station_id AS node_id,
    w.root_id
  FROM walk w
  JOIN _cluster_edges e
    ON e.source_global_station_id = w.node_id
)
SELECT
  node_id AS global_station_id,
  MIN(root_id) AS component_key
FROM walk
GROUP BY node_id;

CREATE TEMP TABLE _cluster_base AS
SELECT
  grouped.component_key,
  'qamc_' || substr(md5(array_to_string(grouped.station_ids, ',')), 1, 24) AS merge_cluster_id,
  md5(array_to_string(grouped.station_ids, ',')) AS cluster_key,
  grouped.station_ids,
  grouped.country_tags
FROM (
  SELECT
    c.component_key,
    ARRAY_AGG(c.global_station_id ORDER BY c.global_station_id) AS station_ids,
    ARRAY_AGG(DISTINCT sc.country) FILTER (WHERE sc.country IS NOT NULL) AS country_tags
  FROM _components c
  JOIN _station_context sc
    ON sc.global_station_id = c.global_station_id
  GROUP BY c.component_key
  HAVING COUNT(*) > 1
) grouped;

CREATE TEMP TABLE _cluster_station_ids AS
SELECT DISTINCT sid.global_station_id
FROM _cluster_base b
JOIN unnest(b.station_ids) sid(global_station_id) ON true;

CREATE INDEX _cluster_station_ids_station_idx
  ON _cluster_station_ids (global_station_id);

CREATE TEMP TABLE _cluster_station_route_counts AS
SELECT
  sp.global_station_id,
  COALESCE(
    NULLIF(tt.route_short_name, ''),
    NULLIF(tt.route_long_name, ''),
    NULLIF(tt.route_id, ''),
    NULLIF(tt.transport_mode, ''),
    'unlabeled'
  ) AS route_label,
  NULLIF(tt.transport_mode, '') AS transport_mode,
  COUNT(*)::integer AS hits
FROM _cluster_station_ids cs
JOIN global_stop_points sp
  ON sp.global_station_id = cs.global_station_id
 AND sp.is_active = true
JOIN timetable_trip_stop_times tts
  ON tts.global_stop_point_id = sp.global_stop_point_id
JOIN timetable_trips tt
  ON tt.trip_fact_id = tts.trip_fact_id
GROUP BY
  sp.global_station_id,
  COALESCE(
    NULLIF(tt.route_short_name, ''),
    NULLIF(tt.route_long_name, ''),
    NULLIF(tt.route_id, ''),
    NULLIF(tt.transport_mode, ''),
    'unlabeled'
  ),
  NULLIF(tt.transport_mode, '');

CREATE INDEX _cluster_station_route_counts_station_idx
  ON _cluster_station_route_counts (global_station_id, hits DESC, route_label);

CREATE TEMP TABLE _cluster_station_incoming_counts AS
SELECT
  sp.global_station_id,
  prev_gs.display_name AS station_label,
  COUNT(*)::integer AS hits
FROM _cluster_station_ids cs
JOIN global_stop_points sp
  ON sp.global_station_id = cs.global_station_id
 AND sp.is_active = true
JOIN timetable_trip_stop_times cur
  ON cur.global_stop_point_id = sp.global_stop_point_id
JOIN timetable_trip_stop_times prev
  ON prev.trip_fact_id = cur.trip_fact_id
 AND prev.stop_sequence = cur.stop_sequence - 1
JOIN global_stop_points prev_sp
  ON prev_sp.global_stop_point_id = prev.global_stop_point_id
 AND prev_sp.is_active = true
JOIN global_stations prev_gs
  ON prev_gs.global_station_id = prev_sp.global_station_id
 AND prev_gs.is_active = true
GROUP BY sp.global_station_id, prev_gs.display_name;

CREATE INDEX _cluster_station_incoming_counts_station_idx
  ON _cluster_station_incoming_counts (global_station_id, hits DESC, station_label);

CREATE TEMP TABLE _cluster_station_outgoing_counts AS
SELECT
  sp.global_station_id,
  next_gs.display_name AS station_label,
  COUNT(*)::integer AS hits
FROM _cluster_station_ids cs
JOIN global_stop_points sp
  ON sp.global_station_id = cs.global_station_id
 AND sp.is_active = true
JOIN timetable_trip_stop_times cur
  ON cur.global_stop_point_id = sp.global_stop_point_id
JOIN timetable_trip_stop_times nxt
  ON nxt.trip_fact_id = cur.trip_fact_id
 AND nxt.stop_sequence = cur.stop_sequence + 1
JOIN global_stop_points next_sp
  ON next_sp.global_stop_point_id = nxt.global_stop_point_id
 AND next_sp.is_active = true
JOIN global_stations next_gs
  ON next_gs.global_station_id = next_sp.global_station_id
 AND next_gs.is_active = true
GROUP BY sp.global_station_id, next_gs.display_name;

CREATE INDEX _cluster_station_outgoing_counts_station_idx
  ON _cluster_station_outgoing_counts (global_station_id, hits DESC, station_label);

CREATE TEMP TABLE _cluster_station_context AS
SELECT
  sc.global_station_id,
  sc.display_name,
  sc.normalized_name,
  sc.loose_name,
  COALESCE(
    ARRAY(
      SELECT DISTINCT token
      FROM regexp_split_to_table(sc.loose_name, '${LOOSE_NAME_TOKEN_SPLIT_REGEX_SQL}') AS token
      WHERE token IS NOT NULL
        AND btrim(token) <> ''
        AND token NOT IN (${LOOSE_NAME_TOKEN_STOP_WORDS_SQL})
      ORDER BY token
    ),
    ARRAY[]::text[]
  ) AS loose_tokens,
  sc.country,
  sc.latitude,
  sc.longitude,
  sc.geom,
  sc.geom_3857,
  sc.tile_2km_x,
  sc.tile_2km_y,
  sc.provider_sources,
  ARRAY[]::text[] AS aliases,
  COALESCE(
    ARRAY(
      SELECT rc.route_label
      FROM _cluster_station_route_counts rc
      WHERE rc.global_station_id = sc.global_station_id
      ORDER BY rc.hits DESC, rc.route_label ASC
      LIMIT 8
    ),
    ARRAY[]::text[]
  ) AS route_labels,
  COALESCE(
    (
      SELECT COUNT(*)::integer
      FROM _cluster_station_route_counts rc
      WHERE rc.global_station_id = sc.global_station_id
    ),
    0
  ) AS route_count,
  COALESCE(
    ARRAY(
      SELECT DISTINCT rc.transport_mode
      FROM _cluster_station_route_counts rc
      WHERE rc.global_station_id = sc.global_station_id
        AND rc.transport_mode IS NOT NULL
      ORDER BY rc.transport_mode ASC
      LIMIT 6
    ),
    ARRAY[]::text[]
  ) AS transport_modes,
  COALESCE(
    ARRAY(
      SELECT ic.station_label
      FROM _cluster_station_incoming_counts ic
      WHERE ic.global_station_id = sc.global_station_id
      ORDER BY ic.hits DESC, ic.station_label ASC
      LIMIT 8
    ),
    ARRAY[]::text[]
  ) AS incoming_labels,
  COALESCE(
    (
      SELECT COUNT(*)::integer
      FROM _cluster_station_incoming_counts ic
      WHERE ic.global_station_id = sc.global_station_id
    ),
    0
  ) AS incoming_count,
  COALESCE(
    ARRAY(
      SELECT oc.station_label
      FROM _cluster_station_outgoing_counts oc
      WHERE oc.global_station_id = sc.global_station_id
      ORDER BY oc.hits DESC, oc.station_label ASC
      LIMIT 8
    ),
    ARRAY[]::text[]
  ) AS outgoing_labels,
  COALESCE(
    (
      SELECT COUNT(*)::integer
      FROM _cluster_station_outgoing_counts oc
      WHERE oc.global_station_id = sc.global_station_id
    ),
    0
  ) AS outgoing_count,
  COALESCE(
    ARRAY(
      SELECT
        DISTINCT
        COALESCE(
          NULLIF(sp.display_name, ''),
          NULLIF(sp.metadata ->> 'provider_stop_point_ref', ''),
          sp.global_stop_point_id
        )
      FROM global_stop_points sp
      WHERE sp.global_station_id = sc.global_station_id
        AND sp.is_active = true
      ORDER BY
        COALESCE(
          NULLIF(sp.display_name, ''),
          NULLIF(sp.metadata ->> 'provider_stop_point_ref', ''),
          sp.global_stop_point_id
        ) ASC
      LIMIT 8
    ),
    ARRAY[]::text[]
  ) AS stop_point_labels,
  sc.adjacent_labels,
  sc.stop_point_count,
  sc.provider_source_count,
  sc.coord_status,
  sc.is_lexically_generic,
  0::integer AS name_frequency,
  sc.is_lexically_generic AS is_generic
FROM _station_context sc
JOIN _cluster_station_ids cs
  ON cs.global_station_id = sc.global_station_id;

UPDATE _cluster_station_context csc
SET adjacent_labels = COALESCE(
  ARRAY(
    SELECT DISTINCT label
    FROM unnest(csc.incoming_labels || csc.outgoing_labels) AS label
    WHERE label IS NOT NULL
      AND btrim(label) <> ''
    ORDER BY label
  ),
  ARRAY[]::text[]
);

CREATE TEMP TABLE _cluster_loose_name_keys AS
SELECT DISTINCT loose_name
FROM _cluster_station_context;

CREATE TEMP TABLE _cluster_name_frequency AS
SELECT
  sc.loose_name,
  COUNT(*)::integer AS name_frequency
FROM _station_context sc
JOIN _cluster_loose_name_keys keys
  ON keys.loose_name = sc.loose_name
GROUP BY sc.loose_name;

UPDATE _cluster_station_context csc
SET
  name_frequency = nf.name_frequency,
  is_generic = csc.is_lexically_generic OR nf.name_frequency >= 12
FROM _cluster_name_frequency nf
WHERE nf.loose_name = csc.loose_name;

ANALYZE _cluster_station_context;

DO $$
BEGIN
  RAISE NOTICE 'merge_queue_phase:writing_clusters';
END $$;

INSERT INTO qa_merge_clusters (
  merge_cluster_id,
  cluster_key,
  status,
  severity,
  scope_tag,
  scope_as_of,
  display_name,
  summary,
  country_tags,
  candidate_count,
  issue_count,
  created_at,
  updated_at
)
SELECT
  b.merge_cluster_id,
  b.cluster_key,
  'open',
  'low',
  (SELECT scope_tag FROM _scope),
  (SELECT scope_as_of FROM _scope),
  (
    SELECT sc.display_name
    FROM unnest(b.station_ids) sid(global_station_id)
    JOIN _station_context sc
      ON sc.global_station_id = sid.global_station_id
    ORDER BY
      sc.provider_source_count DESC,
      sc.stop_point_count DESC,
      sc.display_name ASC,
      sc.global_station_id ASC
    LIMIT 1
  ),
  jsonb_build_object(
    'station_ids', b.station_ids,
    'creation_reasons', '{}'::jsonb,
    'evidence_summary', '{}'::jsonb
  ),
  COALESCE(b.country_tags, ARRAY[]::text[]),
  cardinality(b.station_ids),
  0,
  now(),
  now()
FROM _cluster_base b
ON CONFLICT (merge_cluster_id)
DO UPDATE SET
  cluster_key = EXCLUDED.cluster_key,
  status = 'open',
  severity = EXCLUDED.severity,
  scope_tag = EXCLUDED.scope_tag,
  scope_as_of = EXCLUDED.scope_as_of,
  display_name = EXCLUDED.display_name,
  summary = EXCLUDED.summary,
  country_tags = EXCLUDED.country_tags,
  candidate_count = EXCLUDED.candidate_count,
  issue_count = EXCLUDED.issue_count,
  updated_at = now();

DELETE FROM qa_merge_cluster_candidates c
USING _cluster_base b
WHERE c.merge_cluster_id = b.merge_cluster_id;

DO $$
BEGIN
  RAISE NOTICE 'merge_queue_phase:writing_candidates';
END $$;

INSERT INTO qa_merge_cluster_candidates (
  merge_cluster_id,
  global_station_id,
  candidate_rank,
  display_name,
  latitude,
  longitude,
  country,
  provider_labels,
  metadata,
  created_at,
  updated_at
)
SELECT
  b.merge_cluster_id,
  gs.global_station_id,
  ROW_NUMBER() OVER (
    PARTITION BY b.merge_cluster_id
    ORDER BY
      sc.provider_source_count DESC,
      sc.stop_point_count DESC,
      CASE sc.coord_status WHEN 'coordinates_present' THEN 0 ELSE 1 END,
      gs.display_name ASC,
      gs.global_station_id ASC
  ),
  gs.display_name,
  gs.latitude,
  gs.longitude,
  gs.country,
  to_jsonb(COALESCE(sc.provider_sources, ARRAY[]::text[])),
  jsonb_build_object(
    'coord_status', sc.coord_status,
    'aliases', to_jsonb(COALESCE(sc.aliases, ARRAY[]::text[])),
    'service_context', jsonb_build_object(
      'lines', to_jsonb(COALESCE(sc.route_labels, ARRAY[]::text[])),
      'incoming', to_jsonb(COALESCE(sc.incoming_labels, ARRAY[]::text[])),
      'outgoing', to_jsonb(COALESCE(sc.outgoing_labels, ARRAY[]::text[])),
      'stop_points', to_jsonb(COALESCE(sc.stop_point_labels, ARRAY[]::text[])),
      'transport_modes', to_jsonb(COALESCE(sc.transport_modes, ARRAY[]::text[]))
    ),
    'context_summary', jsonb_build_object(
      'route_count', COALESCE(sc.route_count, 0),
      'incoming_count', COALESCE(sc.incoming_count, 0),
      'outgoing_count', COALESCE(sc.outgoing_count, 0),
      'stop_point_count', COALESCE(sc.stop_point_count, 0),
      'provider_source_count', COALESCE(sc.provider_source_count, 0)
    ),
    'generic_name', sc.is_generic
  ),
  now(),
  now()
FROM _cluster_base b
JOIN unnest(b.station_ids) sid(global_station_id) ON true
JOIN global_stations gs
  ON gs.global_station_id = sid.global_station_id
JOIN _cluster_station_context sc
  ON sc.global_station_id = gs.global_station_id;

DELETE FROM qa_merge_cluster_evidence e
USING _cluster_base b
WHERE e.merge_cluster_id = b.merge_cluster_id;

DO $$
BEGIN
  RAISE NOTICE 'merge_queue_phase:writing_evidence';
END $$;

CREATE TEMP TABLE _cluster_pair_metrics AS
SELECT
  cb.merge_cluster_id,
  left_station.global_station_id AS source_global_station_id,
  right_station.global_station_id AS target_global_station_id,
  left_station.display_name AS source_display_name,
  right_station.display_name AS target_display_name,
  left_station.normalized_name = right_station.normalized_name AS exact_name,
  similarity(left_station.loose_name, right_station.loose_name)::numeric(8,4) AS loose_similarity,
  CASE
    WHEN token_union.union_count = 0 THEN 0::numeric(8,4)
    ELSE ROUND((token_inter.intersection_count::numeric / token_union.union_count::numeric), 4)::numeric(8,4)
  END AS token_overlap,
  CASE
    WHEN left_station.geom IS NULL OR right_station.geom IS NULL THEN NULL
    ELSE ST_DistanceSphere(left_station.geom, right_station.geom)
  END AS distance_meters,
  CASE
    WHEN left_station.geom IS NULL OR right_station.geom IS NULL THEN 'missing_coordinates'
    WHEN ST_DistanceSphere(left_station.geom, right_station.geom) <= 50 THEN 'same_location'
    WHEN ST_DistanceSphere(left_station.geom, right_station.geom) <= 250 THEN 'nearby'
    WHEN ST_DistanceSphere(left_station.geom, right_station.geom) <= 1000 THEN 'far_apart'
    WHEN ST_DistanceSphere(left_station.geom, right_station.geom) <= 5000 THEN 'far_apart'
    ELSE 'too_far'
  END AS distance_status,
  CASE
    WHEN left_station.geom IS NOT NULL AND right_station.geom IS NOT NULL THEN 2
    WHEN left_station.geom IS NOT NULL OR right_station.geom IS NOT NULL THEN 1
    ELSE 0
  END AS coordinate_points_present,
  provider_overlap.provider_overlap_count,
  route_overlap.route_overlap_count,
  adjacent_overlap.adjacent_overlap_count,
  transport_overlap.transport_overlap_count,
  CASE
    WHEN left_station.country IS NULL OR right_station.country IS NULL THEN NULL
    WHEN left_station.country = right_station.country THEN true
    ELSE false
  END AS same_country,
  (left_station.is_generic OR right_station.is_generic) AS generic_name_pair,
  GREATEST(left_station.name_frequency, right_station.name_frequency) AS generic_name_frequency,
  COALESCE(seed.seed_reasons, ARRAY[]::text[]) AS seed_reasons
FROM _cluster_base cb
JOIN unnest(cb.station_ids) left_id(global_station_id) ON true
JOIN unnest(cb.station_ids) right_id(global_station_id)
  ON right_id.global_station_id > left_id.global_station_id
JOIN _cluster_station_context left_station
  ON left_station.global_station_id = left_id.global_station_id
JOIN _cluster_station_context right_station
  ON right_station.global_station_id = right_id.global_station_id
LEFT JOIN _eligible_pairs seed
  ON seed.source_global_station_id = left_id.global_station_id
 AND seed.target_global_station_id = right_id.global_station_id
CROSS JOIN LATERAL (
  SELECT COUNT(*)::integer AS provider_overlap_count
  FROM (
    SELECT DISTINCT item
    FROM unnest(left_station.provider_sources) item
    INTERSECT
    SELECT DISTINCT item
    FROM unnest(right_station.provider_sources) item
  ) overlap_items
) provider_overlap
CROSS JOIN LATERAL (
  SELECT COUNT(*)::integer AS route_overlap_count
  FROM (
    SELECT DISTINCT item
    FROM unnest(left_station.route_labels) item
    INTERSECT
    SELECT DISTINCT item
    FROM unnest(right_station.route_labels) item
  ) overlap_items
) route_overlap
CROSS JOIN LATERAL (
  SELECT COUNT(*)::integer AS adjacent_overlap_count
  FROM (
    SELECT DISTINCT item
    FROM (
      SELECT unnest(left_station.incoming_labels || left_station.outgoing_labels) AS item
    ) left_labels
    INTERSECT
    SELECT DISTINCT item
    FROM (
      SELECT unnest(right_station.incoming_labels || right_station.outgoing_labels) AS item
    ) right_labels
  ) overlap_items
) adjacent_overlap
CROSS JOIN LATERAL (
  SELECT COUNT(*)::integer AS transport_overlap_count
  FROM (
    SELECT DISTINCT item
    FROM unnest(left_station.transport_modes) item
    INTERSECT
    SELECT DISTINCT item
    FROM unnest(right_station.transport_modes) item
  ) overlap_items
) transport_overlap
CROSS JOIN LATERAL (
  SELECT COUNT(*)::integer AS intersection_count
  FROM (
    SELECT DISTINCT token
    FROM unnest(left_station.loose_tokens) token
    INTERSECT
    SELECT DISTINCT token
    FROM unnest(right_station.loose_tokens) token
  ) overlap_tokens
) token_inter
CROSS JOIN LATERAL (
  SELECT COUNT(*)::integer AS union_count
  FROM (
    SELECT DISTINCT token
    FROM unnest(left_station.loose_tokens) token
    UNION
    SELECT DISTINCT token
    FROM unnest(right_station.loose_tokens) token
  ) union_tokens
) token_union;

INSERT INTO qa_merge_cluster_evidence (
  merge_cluster_id,
  source_global_station_id,
  target_global_station_id,
  evidence_type,
  status,
  score,
  raw_value,
  details,
  created_at
)
SELECT
  rows.merge_cluster_id,
  rows.source_global_station_id,
  rows.target_global_station_id,
  rows.evidence_type,
  rows.status,
  rows.score,
  rows.raw_value,
  rows.details,
  now()
FROM (
  SELECT
    pm.merge_cluster_id,
    pm.source_global_station_id,
    pm.target_global_station_id,
    'name_exact'::text AS evidence_type,
    CASE WHEN pm.exact_name THEN 'supporting' ELSE 'informational' END AS status,
    CASE WHEN pm.exact_name THEN 1.0::numeric ELSE 0.0::numeric END AS score,
    CASE WHEN pm.exact_name THEN 1.0::numeric ELSE 0.0::numeric END AS raw_value,
    jsonb_build_object(
      'left', pm.source_display_name,
      'right', pm.target_display_name,
      'seed_reasons', to_jsonb(pm.seed_reasons)
    ) AS details
  FROM _cluster_pair_metrics pm

  UNION ALL

  SELECT
    pm.merge_cluster_id,
    pm.source_global_station_id,
    pm.target_global_station_id,
    'name_loose_similarity'::text AS evidence_type,
    CASE
      WHEN pm.loose_similarity >= 0.92 THEN 'supporting'
      WHEN pm.loose_similarity >= 0.72 THEN 'informational'
      ELSE 'warning'
    END AS status,
    pm.loose_similarity,
    pm.loose_similarity,
    jsonb_build_object(
      'left', pm.source_display_name,
      'right', pm.target_display_name,
      'seed_reasons', to_jsonb(pm.seed_reasons)
    ) AS details
  FROM _cluster_pair_metrics pm

  UNION ALL

  SELECT
    pm.merge_cluster_id,
    pm.source_global_station_id,
    pm.target_global_station_id,
    'token_overlap'::text AS evidence_type,
    CASE
      WHEN pm.token_overlap >= 0.75 THEN 'supporting'
      WHEN pm.token_overlap >= 0.40 THEN 'informational'
      ELSE 'warning'
    END AS status,
    pm.token_overlap,
    pm.token_overlap,
    jsonb_build_object(
      'left', pm.source_display_name,
      'right', pm.target_display_name,
      'seed_reasons', to_jsonb(pm.seed_reasons)
    ) AS details
  FROM _cluster_pair_metrics pm

  UNION ALL

  SELECT
    pm.merge_cluster_id,
    pm.source_global_station_id,
    pm.target_global_station_id,
    'geographic_distance'::text AS evidence_type,
    CASE
      WHEN pm.distance_meters IS NULL THEN 'missing'
      WHEN pm.distance_meters <= 250 THEN 'supporting'
      WHEN pm.distance_meters <= 1000 THEN 'informational'
      ELSE 'warning'
    END AS status,
    CASE
      WHEN pm.distance_meters IS NULL THEN 0.0::numeric
      WHEN pm.distance_meters <= 50 THEN 1.0::numeric
      WHEN pm.distance_meters <= 250 THEN 0.85::numeric
      WHEN pm.distance_meters <= 1000 THEN 0.60::numeric
      WHEN pm.distance_meters <= 5000 THEN 0.25::numeric
      ELSE 0.05::numeric
    END AS score,
    ROUND(COALESCE(pm.distance_meters, 0)::numeric, 2) AS raw_value,
    jsonb_build_object(
      'distance_meters', pm.distance_meters,
      'distance_status', pm.distance_status,
      'seed_reasons', to_jsonb(pm.seed_reasons)
    ) AS details
  FROM _cluster_pair_metrics pm

  UNION ALL

  SELECT
    pm.merge_cluster_id,
    pm.source_global_station_id,
    pm.target_global_station_id,
    'coordinate_quality'::text AS evidence_type,
    CASE
      WHEN pm.coordinate_points_present = 2 THEN 'supporting'
      WHEN pm.coordinate_points_present = 1 THEN 'informational'
      ELSE 'missing'
    END AS status,
    CASE
      WHEN pm.coordinate_points_present = 2 THEN 1.0::numeric
      WHEN pm.coordinate_points_present = 1 THEN 0.50::numeric
      ELSE 0.0::numeric
    END AS score,
    pm.coordinate_points_present::numeric AS raw_value,
    jsonb_build_object(
      'coordinate_points_present', pm.coordinate_points_present,
      'seed_reasons', to_jsonb(pm.seed_reasons)
    ) AS details
  FROM _cluster_pair_metrics pm

  UNION ALL

  SELECT
    pm.merge_cluster_id,
    pm.source_global_station_id,
    pm.target_global_station_id,
    'shared_provider_sources'::text AS evidence_type,
    CASE
      WHEN pm.provider_overlap_count > 0 THEN 'supporting'
      ELSE 'informational'
    END AS status,
    CASE
      WHEN pm.provider_overlap_count >= 2 THEN 1.0::numeric
      WHEN pm.provider_overlap_count = 1 THEN 0.65::numeric
      ELSE 0.0::numeric
    END AS score,
    pm.provider_overlap_count::numeric AS raw_value,
    jsonb_build_object(
      'shared_provider_source_count', pm.provider_overlap_count,
      'seed_reasons', to_jsonb(pm.seed_reasons)
    ) AS details
  FROM _cluster_pair_metrics pm

  UNION ALL

  SELECT
    pm.merge_cluster_id,
    pm.source_global_station_id,
    pm.target_global_station_id,
    'shared_route_context'::text AS evidence_type,
    CASE
      WHEN pm.route_overlap_count > 0 THEN 'supporting'
      ELSE 'informational'
    END AS status,
    LEAST(1.0::numeric, (pm.route_overlap_count::numeric / 3.0::numeric)) AS score,
    pm.route_overlap_count::numeric AS raw_value,
    jsonb_build_object(
      'shared_route_count', pm.route_overlap_count,
      'seed_reasons', to_jsonb(pm.seed_reasons)
    ) AS details
  FROM _cluster_pair_metrics pm

  UNION ALL

  SELECT
    pm.merge_cluster_id,
    pm.source_global_station_id,
    pm.target_global_station_id,
    'shared_adjacent_stations'::text AS evidence_type,
    CASE
      WHEN pm.adjacent_overlap_count > 0 THEN 'supporting'
      ELSE 'informational'
    END AS status,
    LEAST(1.0::numeric, (pm.adjacent_overlap_count::numeric / 3.0::numeric)) AS score,
    pm.adjacent_overlap_count::numeric AS raw_value,
    jsonb_build_object(
      'shared_adjacent_station_count', pm.adjacent_overlap_count,
      'seed_reasons', to_jsonb(pm.seed_reasons)
    ) AS details
  FROM _cluster_pair_metrics pm

  UNION ALL

  SELECT
    pm.merge_cluster_id,
    pm.source_global_station_id,
    pm.target_global_station_id,
    'country_relation'::text AS evidence_type,
    CASE
      WHEN pm.same_country = true THEN 'supporting'
      WHEN pm.same_country = false THEN 'informational'
      ELSE 'missing'
    END AS status,
    CASE
      WHEN pm.same_country = true THEN 1.0::numeric
      WHEN pm.same_country = false THEN 0.60::numeric
      ELSE 0.0::numeric
    END AS score,
    CASE
      WHEN pm.same_country = true THEN 1.0::numeric
      WHEN pm.same_country = false THEN 0.0::numeric
      ELSE NULL::numeric
    END AS raw_value,
    jsonb_build_object(
      'same_country', pm.same_country,
      'seed_reasons', to_jsonb(pm.seed_reasons)
    ) AS details
  FROM _cluster_pair_metrics pm

  UNION ALL

  SELECT
    pm.merge_cluster_id,
    pm.source_global_station_id,
    pm.target_global_station_id,
    'generic_name_penalty'::text AS evidence_type,
    CASE
      WHEN pm.generic_name_pair = true THEN 'warning'
      ELSE 'informational'
    END AS status,
    CASE
      WHEN pm.generic_name_pair = true THEN 0.15::numeric
      ELSE 1.0::numeric
    END AS score,
    pm.generic_name_frequency::numeric AS raw_value,
    jsonb_build_object(
      'generic_name_pair', pm.generic_name_pair,
      'name_frequency', pm.generic_name_frequency,
      'seed_reasons', to_jsonb(pm.seed_reasons)
    ) AS details
  FROM _cluster_pair_metrics pm
) rows;

CREATE TEMP TABLE _cluster_reason_summary AS
SELECT
  cluster_rows.merge_cluster_id,
  COALESCE(
    jsonb_object_agg(cluster_rows.seed_reason, cluster_rows.seed_count ORDER BY cluster_rows.seed_reason),
    '{}'::jsonb
  ) AS creation_reasons
FROM (
  SELECT
    cb.merge_cluster_id,
    reason.seed_reason,
    COUNT(*)::integer AS seed_count
  FROM _cluster_base cb
  JOIN _eligible_pairs ep
    ON ep.source_global_station_id = ANY (cb.station_ids)
   AND ep.target_global_station_id = ANY (cb.station_ids)
  CROSS JOIN LATERAL unnest(ep.seed_reasons) reason(seed_reason)
  GROUP BY cb.merge_cluster_id, reason.seed_reason
) cluster_rows
GROUP BY cluster_rows.merge_cluster_id;

CREATE TEMP TABLE _cluster_evidence_summary AS
SELECT
  e.merge_cluster_id,
  COUNT(*) FILTER (WHERE e.status = 'supporting')::integer AS supporting_count,
  COUNT(*) FILTER (WHERE e.status = 'warning')::integer AS warning_count,
  COUNT(*) FILTER (WHERE e.status = 'missing')::integer AS missing_count,
  COUNT(*) FILTER (WHERE e.status = 'informational')::integer AS informational_count,
  COUNT(DISTINCT (e.source_global_station_id || '|' || e.target_global_station_id))
    FILTER (WHERE e.status IN ('warning', 'missing'))::integer AS issue_count,
  jsonb_build_object(
    'supporting', COUNT(*) FILTER (WHERE e.status = 'supporting'),
    'warning', COUNT(*) FILTER (WHERE e.status = 'warning'),
    'missing', COUNT(*) FILTER (WHERE e.status = 'missing'),
    'informational', COUNT(*) FILTER (WHERE e.status = 'informational')
  ) AS evidence_summary
FROM qa_merge_cluster_evidence e
WHERE e.merge_cluster_id IN (SELECT merge_cluster_id FROM _cluster_base)
GROUP BY e.merge_cluster_id;

CREATE TEMP TABLE _cluster_pair_summary AS
SELECT
  pm.merge_cluster_id,
  COUNT(*)::integer AS pair_count,
  MAX(pm.distance_meters) AS max_distance_meters,
  COUNT(*) FILTER (WHERE pm.generic_name_pair = true)::integer AS generic_pair_count
FROM _cluster_pair_metrics pm
GROUP BY pm.merge_cluster_id;

UPDATE qa_merge_clusters c
SET
  issue_count = COALESCE(es.issue_count, 0),
  severity = CASE
    WHEN c.candidate_count >= 4
      OR COALESCE(es.warning_count, 0) >= 4
      OR COALESCE(es.missing_count, 0) >= 4
      OR COALESCE(ps.max_distance_meters, 0) > 5000
      OR COALESCE(ps.generic_pair_count, 0) > 0
    THEN 'high'
    WHEN c.candidate_count = 3
      OR COALESCE(es.warning_count, 0) > 0
      OR COALESCE(es.missing_count, 0) > 0
      OR COALESCE(ps.max_distance_meters, 0) > 1000
    THEN 'medium'
    ELSE 'low'
  END,
  summary = jsonb_strip_nulls(
    jsonb_build_object(
      'station_ids', cb.station_ids,
      'creation_reasons', COALESCE(rs.creation_reasons, '{}'::jsonb),
      'evidence_summary', COALESCE(es.evidence_summary, '{}'::jsonb),
      'pair_count', COALESCE(ps.pair_count, 0),
      'max_distance_meters', ps.max_distance_meters,
      'generic_pair_count', COALESCE(ps.generic_pair_count, 0)
    )
  ),
  updated_at = now()
FROM _cluster_base cb
LEFT JOIN _cluster_reason_summary rs
  ON rs.merge_cluster_id = cb.merge_cluster_id
LEFT JOIN _cluster_evidence_summary es
  ON es.merge_cluster_id = cb.merge_cluster_id
LEFT JOIN _cluster_pair_summary ps
  ON ps.merge_cluster_id = cb.merge_cluster_id
WHERE c.merge_cluster_id = cb.merge_cluster_id;

DO $$
BEGIN
  RAISE NOTICE 'merge_queue_phase:finalizing';
END $$;

COMMIT;

SELECT json_build_object(
  'scopeCountry', COALESCE(NULLIF(:'country_filter', ''), ''),
  'scopeAsOf', COALESCE(NULLIF(:'as_of', ''), ''),
  'scopeTag', (SELECT scope_tag FROM _scope),
  'clusters', (SELECT COUNT(*) FROM _cluster_base),
  'candidates', (
    SELECT COUNT(*)
    FROM qa_merge_cluster_candidates c
    WHERE c.merge_cluster_id IN (SELECT merge_cluster_id FROM _cluster_base)
  ),
  'evidence', (
    SELECT COUNT(*)
    FROM qa_merge_cluster_evidence e
    WHERE e.merge_cluster_id IN (SELECT merge_cluster_id FROM _cluster_base)
  )
)::text;
`;

function createMergeQueueRepo(client) {
  return {
    async rebuildMergeQueue(scope, options = {}) {
      const onPhase =
        typeof options.onPhase === "function" ? options.onPhase : null;
      const onInfo =
        typeof options.onInfo === "function" ? options.onInfo : null;
      await ensureMergeClusterEvidenceColumns(client);
      const result = await client.runScript(
        BUILD_MERGE_QUEUE_SQL,
        {
          country_filter: scope.country || "",
          as_of: scope.asOf || "",
        },
        {
          onNotice(notice) {
            const phase = extractPhaseFromNotice(notice);
            if (phase && onPhase) {
              onPhase(phase);
            }
            const info = extractInfoFromNotice(notice);
            if (info && onInfo) {
              onInfo(info);
            }
          },
        },
      );

      const line = extractJsonLine(result.stdout);
      if (!line) {
        throw new AppError({
          code: "MERGE_QUEUE_BUILD_FAILED",
          message: "Merge queue build did not return summary JSON",
        });
      }

      let parsed;
      try {
        parsed = JSON.parse(line);
      } catch (err) {
        throw new AppError({
          code: "MERGE_QUEUE_BUILD_FAILED",
          message: "Merge queue build returned invalid summary JSON",
          cause: err,
        });
      }

      validateOrThrow(parsed, MERGE_QUEUE_SUMMARY_SCHEMA, {
        code: "MERGE_QUEUE_BUILD_FAILED",
        message: "Merge queue summary failed schema validation",
      });
      return parsed;
    },

    async getRebuildFingerprint(scope = {}) {
      const row = await client.queryOne(
        MERGE_QUEUE_FINGERPRINT_SQL,
        scopeParams(scope),
      );
      return row?.fingerprint || null;
    },

    async getCurrentSummary(scope = {}) {
      const row = await client.queryOne(
        CURRENT_MERGE_QUEUE_SUMMARY_SQL,
        scopeParams(scope),
      );
      const raw = String(row?.summary_json || "");
      let parsed;
      try {
        parsed = JSON.parse(raw);
      } catch (err) {
        throw new AppError({
          code: "MERGE_QUEUE_BUILD_FAILED",
          message: "Current merge queue summary returned invalid summary JSON",
          cause: err,
        });
      }
      validateOrThrow(parsed, MERGE_QUEUE_SUMMARY_SCHEMA, {
        code: "MERGE_QUEUE_BUILD_FAILED",
        message: "Current merge queue summary failed schema validation",
      });
      return parsed;
    },

    getTuningConfig() {
      return {
        maxParallelWorkersPerGather: MERGE_QUEUE_MAX_PARALLEL_WORKERS,
        workMem: MERGE_QUEUE_WORK_MEM,
        maintenanceWorkMem: MERGE_QUEUE_MAINTENANCE_WORK_MEM,
      };
    },

    async sampleRebuildQueryPlans(scope = {}) {
      const params = scopeParams(scope);
      const queries = [
        {
          name: "scope_stations",
          sql: `
            EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
            SELECT
              gs.global_station_id,
              gs.display_name,
              gs.country
            FROM global_stations gs
            WHERE gs.is_active = true
              AND EXISTS (
                SELECT 1
                FROM provider_global_station_mappings m
                WHERE m.global_station_id = gs.global_station_id
                  AND m.is_active = true
              )
              AND (
                NULLIF(:'country_filter', '') IS NULL
                OR gs.country = NULLIF(:'country_filter', '')::char(2)
              )
            ORDER BY gs.global_station_id
            LIMIT 10000;
          `,
        },
        {
          name: "cluster_route_counts",
          sql: `
            EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
            SELECT
              sp.global_station_id,
              COUNT(*)::integer AS hits
            FROM global_stop_points sp
            JOIN timetable_trip_stop_times tts
              ON tts.global_stop_point_id = sp.global_stop_point_id
            WHERE sp.is_active = true
              AND (
                NULLIF(:'country_filter', '') IS NULL
                OR sp.country = NULLIF(:'country_filter', '')::char(2)
              )
            GROUP BY sp.global_station_id
            ORDER BY hits DESC
            LIMIT 1000;
          `,
        },
      ];

      const plans = [];
      for (const query of queries) {
        const rows = await client.queryRows(query.sql, params);
        plans.push({
          name: query.name,
          plan: rows[0]?.["QUERY PLAN"] || rows[0] || null,
        });
      }
      return plans;
    },
  };
}

module.exports = {
  BUILD_MERGE_QUEUE_SQL,
  DEFAULT_MERGE_QUEUE_MAINTENANCE_WORK_MEM,
  DEFAULT_MERGE_QUEUE_MAX_PARALLEL_WORKERS,
  DEFAULT_MERGE_QUEUE_WORK_MEM,
  createMergeQueueRepo,
  extractInfoFromNotice,
  extractPhaseFromNotice,
};
