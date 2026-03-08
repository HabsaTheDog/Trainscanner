const { AppError } = require("../../../core/errors");
const { validateOrThrow } = require("../../../core/schema");

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

const BUILD_MERGE_QUEUE_SQL = `
BEGIN;

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

CREATE TEMP TABLE _dup_groups AS
SELECT
  gs.normalized_name,
  ARRAY_AGG(gs.global_station_id ORDER BY gs.global_station_id) AS station_ids,
  ARRAY_AGG(DISTINCT gs.country) FILTER (WHERE gs.country IS NOT NULL) AS country_tags
FROM global_stations gs
WHERE gs.is_active = true
  AND (NULLIF(:'country_filter', '') IS NULL OR gs.country = NULLIF(:'country_filter', '')::char(2))
GROUP BY gs.normalized_name
HAVING COUNT(*) > 1;

CREATE TEMP TABLE _cluster_base AS
SELECT
  'qamc_' || substr(md5(normalized_name || '|' || array_to_string(station_ids, ',')), 1, 24) AS merge_cluster_id,
  md5(normalized_name || '|' || array_to_string(station_ids, ',')) AS cluster_key,
  normalized_name,
  station_ids,
  country_tags
FROM _dup_groups;

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
  CASE
    WHEN cardinality(b.station_ids) >= 4 THEN 'high'
    WHEN cardinality(b.station_ids) = 3 THEN 'medium'
    ELSE 'low'
  END,
  (SELECT scope_tag FROM _scope),
  (SELECT scope_as_of FROM _scope),
  (
    SELECT MIN(gs.display_name)
    FROM unnest(b.station_ids) sid(global_station_id)
    JOIN global_stations gs ON gs.global_station_id = sid.global_station_id
  ),
  jsonb_build_object(
    'normalized_name', b.normalized_name,
    'station_ids', b.station_ids
  ),
  COALESCE(b.country_tags, ARRAY[]::text[]),
  cardinality(b.station_ids),
  1,
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
  ROW_NUMBER() OVER (PARTITION BY b.merge_cluster_id ORDER BY gs.display_name, gs.global_station_id),
  gs.display_name,
  gs.latitude,
  gs.longitude,
  gs.country,
  (
    SELECT COALESCE(jsonb_agg(DISTINCT m.source_id ORDER BY m.source_id), '[]'::jsonb)
    FROM provider_global_station_mappings m
    WHERE m.global_station_id = gs.global_station_id
      AND m.is_active = true
  ),
  '{}'::jsonb,
  now(),
  now()
FROM _cluster_base b
JOIN unnest(b.station_ids) sid(global_station_id) ON true
JOIN global_stations gs
  ON gs.global_station_id = sid.global_station_id;

DELETE FROM qa_merge_cluster_evidence e
USING _cluster_base b
WHERE e.merge_cluster_id = b.merge_cluster_id;

INSERT INTO qa_merge_cluster_evidence (
  merge_cluster_id,
  source_global_station_id,
  target_global_station_id,
  evidence_type,
  score,
  details,
  created_at
)
SELECT
  a.merge_cluster_id,
  a.global_station_id,
  b.global_station_id,
  'name_similarity',
  1.0,
  jsonb_build_object('left', a.display_name, 'right', b.display_name),
  now()
FROM qa_merge_cluster_candidates a
JOIN qa_merge_cluster_candidates b
  ON b.merge_cluster_id = a.merge_cluster_id
 AND b.global_station_id > a.global_station_id
WHERE a.merge_cluster_id IN (SELECT merge_cluster_id FROM _cluster_base);

INSERT INTO qa_merge_cluster_evidence (
  merge_cluster_id,
  source_global_station_id,
  target_global_station_id,
  evidence_type,
  score,
  details,
  created_at
)
SELECT
  a.merge_cluster_id,
  a.global_station_id,
  b.global_station_id,
  'distance_proximity',
  CASE
    WHEN a.latitude IS NULL OR a.longitude IS NULL OR b.latitude IS NULL OR b.longitude IS NULL THEN NULL
    ELSE GREATEST(
      0::numeric,
      LEAST(
        1::numeric,
        1 - (
          ST_DistanceSphere(
            ST_SetSRID(ST_MakePoint(a.longitude, a.latitude), 4326),
            ST_SetSRID(ST_MakePoint(b.longitude, b.latitude), 4326)
          ) / 2000.0
        )
      )
    )
  END,
  jsonb_build_object(
    'distance_meters',
    CASE
      WHEN a.latitude IS NULL OR a.longitude IS NULL OR b.latitude IS NULL OR b.longitude IS NULL THEN NULL
      ELSE ST_DistanceSphere(
        ST_SetSRID(ST_MakePoint(a.longitude, a.latitude), 4326),
        ST_SetSRID(ST_MakePoint(b.longitude, b.latitude), 4326)
      )
    END
  ),
  now()
FROM qa_merge_cluster_candidates a
JOIN qa_merge_cluster_candidates b
  ON b.merge_cluster_id = a.merge_cluster_id
 AND b.global_station_id > a.global_station_id
WHERE a.merge_cluster_id IN (SELECT merge_cluster_id FROM _cluster_base);

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
    async rebuildMergeQueue(scope) {
      const result = await client.runScript(BUILD_MERGE_QUEUE_SQL, {
        country_filter: scope.country || "",
        as_of: scope.asOf || "",
      });

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
  };
}

module.exports = {
  BUILD_MERGE_QUEUE_SQL,
  createMergeQueueRepo,
};
