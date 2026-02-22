const { AppError } = require('../../../core/errors');
const { validateOrThrow } = require('../../../core/schema');
const { extractJsonLine } = require('./canonical-stations-repo');

const REVIEW_QUEUE_SUMMARY_SCHEMA = {
  type: 'object',
  required: ['scopeTag', 'detectedIssues', 'openItems', 'confirmedItems', 'resolvedItems'],
  properties: {
    scopeCountry: { type: 'string' },
    scopeAsOf: { type: 'string' },
    scopeTag: { type: 'string', minLength: 1 },
    detectedIssues: { type: 'integer', minimum: 0 },
    openItems: { type: 'integer', minimum: 0 },
    confirmedItems: { type: 'integer', minimum: 0 },
    resolvedItems: { type: 'integer', minimum: 0 }
  },
  additionalProperties: true
};

const BUILD_REVIEW_QUEUE_SQL = `
BEGIN;

CREATE TEMP TABLE _scoped_sources AS
SELECT
  css.canonical_station_id,
  css.source_id,
  css.source_stop_id,
  css.country,
  css.snapshot_date,
  css.match_method,
  css.hard_id,
  s.geom,
  s.stop_name,
  s.normalized_name
FROM canonical_station_sources css
LEFT JOIN netex_stops_staging s
  ON s.source_id = css.source_id
 AND s.source_stop_id = css.source_stop_id
 AND s.snapshot_date = css.snapshot_date
WHERE (NULLIF(:'country_filter', '') IS NULL OR css.country = NULLIF(:'country_filter', '')::char(2))
  AND (NULLIF(:'as_of', '') IS NULL OR css.snapshot_date <= NULLIF(:'as_of', '')::date);

DO $$
BEGIN
  IF (SELECT COUNT(*) FROM _scoped_sources) = 0 THEN
    RAISE EXCEPTION 'No canonical station mappings found for selected scope';
  END IF;
END $$;

CREATE TEMP TABLE _scoped_station_ids AS
SELECT DISTINCT canonical_station_id
FROM _scoped_sources;

CREATE TEMP TABLE _issues (
  issue_key text PRIMARY KEY,
  country char(2),
  canonical_station_id text,
  issue_type text,
  severity text,
  detected_as_of date,
  details jsonb
);

WITH scope_params AS (
  SELECT COALESCE(NULLIF(:'as_of', ''), 'latest') AS scope_tag
)
INSERT INTO _issues (issue_key, country, canonical_station_id, issue_type, severity, detected_as_of, details)
SELECT
  format('name_only_cluster|%s|%s', cs.canonical_station_id, sp.scope_tag) AS issue_key,
  cs.country,
  cs.canonical_station_id,
  'name_only_cluster'::text AS issue_type,
  CASE WHEN cs.member_count >= 4 THEN 'high' ELSE 'medium' END AS severity,
  NULLIF(:'as_of', '')::date AS detected_as_of,
  jsonb_build_object(
    'canonicalStationId', cs.canonical_station_id,
    'canonicalName', cs.canonical_name,
    'memberCount', cs.member_count,
    'matchMethod', cs.match_method
  ) AS details
FROM canonical_stations cs
JOIN _scoped_station_ids ss
  ON ss.canonical_station_id = cs.canonical_station_id
JOIN scope_params sp ON true
WHERE cs.match_method = 'name_only'
  AND cs.member_count > 1;

WITH scope_params AS (
  SELECT COALESCE(NULLIF(:'as_of', ''), 'latest') AS scope_tag
), geo_stats AS (
  SELECT
    s.canonical_station_id,
    s.country,
    COUNT(*) FILTER (WHERE s.geom IS NOT NULL) AS geom_members,
    ST_MaxDistance(ST_Transform(ST_Collect(s.geom), 3857), ST_Transform(ST_Collect(s.geom), 3857)) AS max_distance_m
  FROM _scoped_sources s
  WHERE s.geom IS NOT NULL
  GROUP BY s.canonical_station_id, s.country
)
INSERT INTO _issues (issue_key, country, canonical_station_id, issue_type, severity, detected_as_of, details)
SELECT
  format('suspicious_geo_spread|%s|%s', gs.canonical_station_id, sp.scope_tag) AS issue_key,
  gs.country,
  gs.canonical_station_id,
  'suspicious_geo_spread'::text AS issue_type,
  CASE WHEN gs.max_distance_m >= (NULLIF(:'geo_threshold_m', '')::double precision * 3) THEN 'high' ELSE 'medium' END AS severity,
  NULLIF(:'as_of', '')::date AS detected_as_of,
  jsonb_build_object(
    'canonicalStationId', gs.canonical_station_id,
    'maxDistanceMeters', ROUND(gs.max_distance_m::numeric, 2),
    'geomMembers', gs.geom_members,
    'thresholdMeters', NULLIF(:'geo_threshold_m', '')::integer
  ) AS details
FROM geo_stats gs
JOIN scope_params sp ON true
WHERE gs.geom_members > 1
  AND gs.max_distance_m > NULLIF(:'geo_threshold_m', '')::double precision
ON CONFLICT (issue_key) DO NOTHING;

WITH scope_params AS (
  SELECT COALESCE(NULLIF(:'as_of', ''), 'latest') AS scope_tag
), dup_hard AS (
  SELECT
    s.country,
    s.hard_id,
    COUNT(*) AS mapping_rows,
    COUNT(DISTINCT s.canonical_station_id) AS station_count,
    array_agg(DISTINCT s.canonical_station_id ORDER BY s.canonical_station_id) AS station_ids
  FROM _scoped_sources s
  WHERE s.hard_id IS NOT NULL
    AND btrim(s.hard_id) <> ''
  GROUP BY s.country, s.hard_id
  HAVING COUNT(DISTINCT s.canonical_station_id) > 1
)
INSERT INTO _issues (issue_key, country, canonical_station_id, issue_type, severity, detected_as_of, details)
SELECT
  format('duplicate_hard_id|%s|%s|%s', d.country, d.hard_id, sp.scope_tag) AS issue_key,
  d.country,
  NULL::text AS canonical_station_id,
  'duplicate_hard_id'::text AS issue_type,
  CASE WHEN d.station_count >= 3 THEN 'high' ELSE 'medium' END AS severity,
  NULLIF(:'as_of', '')::date AS detected_as_of,
  jsonb_build_object(
    'hardId', d.hard_id,
    'stationCount', d.station_count,
    'mappingRows', d.mapping_rows,
    'canonicalStationIds', d.station_ids
  ) AS details
FROM dup_hard d
JOIN scope_params sp ON true
ON CONFLICT (issue_key) DO NOTHING;

WITH scope_params AS (
  SELECT COALESCE(NULLIF(:'as_of', ''), 'latest') AS scope_tag
), dup_name AS (
  SELECT
    cs.country,
    cs.normalized_name,
    COUNT(*) AS station_count,
    array_agg(cs.canonical_station_id ORDER BY cs.canonical_station_id) AS station_ids
  FROM canonical_stations cs
  JOIN _scoped_station_ids ss
    ON ss.canonical_station_id = cs.canonical_station_id
  GROUP BY cs.country, cs.normalized_name
  HAVING COUNT(*) > 1
)
INSERT INTO _issues (issue_key, country, canonical_station_id, issue_type, severity, detected_as_of, details)
SELECT
  format('duplicate_normalized_name|%s|%s|%s', d.country, d.normalized_name, sp.scope_tag) AS issue_key,
  d.country,
  NULL::text AS canonical_station_id,
  'duplicate_normalized_name'::text AS issue_type,
  CASE WHEN d.station_count >= 4 THEN 'high' ELSE 'low' END AS severity,
  NULLIF(:'as_of', '')::date AS detected_as_of,
  jsonb_build_object(
    'normalizedName', d.normalized_name,
    'stationCount', d.station_count,
    'canonicalStationIds', d.station_ids
  ) AS details
FROM dup_name d
JOIN scope_params sp ON true
ON CONFLICT (issue_key) DO NOTHING;

SELECT COALESCE(NULLIF(:'as_of', ''), 'latest') AS scope_tag
INTO TEMP TABLE _scope;

INSERT INTO canonical_review_queue (
  issue_key,
  country,
  canonical_station_id,
  issue_type,
  severity,
  detected_as_of,
  status,
  details,
  provenance_source,
  provenance_run_tag,
  first_detected_at,
  last_detected_at,
  created_at,
  updated_at
)
SELECT
  i.issue_key,
  i.country,
  i.canonical_station_id,
  i.issue_type,
  i.severity,
  i.detected_as_of,
  'open'::text,
  i.details,
  'build-review-queue.sh'::text,
  (SELECT scope_tag FROM _scope),
  now(),
  now(),
  now(),
  now()
FROM _issues i
ON CONFLICT (issue_key)
DO UPDATE SET
  country = EXCLUDED.country,
  canonical_station_id = EXCLUDED.canonical_station_id,
  issue_type = EXCLUDED.issue_type,
  severity = EXCLUDED.severity,
  detected_as_of = EXCLUDED.detected_as_of,
  details = EXCLUDED.details,
  provenance_source = EXCLUDED.provenance_source,
  provenance_run_tag = EXCLUDED.provenance_run_tag,
  last_detected_at = now(),
  detected_count = canonical_review_queue.detected_count + 1,
  status = CASE
    WHEN canonical_review_queue.status IN ('resolved', 'auto_resolved') THEN 'open'
    ELSE canonical_review_queue.status
  END,
  resolved_at = CASE
    WHEN canonical_review_queue.status IN ('resolved', 'auto_resolved') THEN NULL
    ELSE canonical_review_queue.resolved_at
  END,
  resolved_by = CASE
    WHEN canonical_review_queue.status IN ('resolved', 'auto_resolved') THEN NULL
    ELSE canonical_review_queue.resolved_by
  END,
  resolution_note = CASE
    WHEN canonical_review_queue.status IN ('resolved', 'auto_resolved') THEN NULL
    ELSE canonical_review_queue.resolution_note
  END,
  updated_at = now();

UPDATE canonical_review_queue q
SET
  status = 'auto_resolved',
  resolved_at = now(),
  resolved_by = current_user,
  resolution_note = COALESCE(q.resolution_note, 'Auto-resolved: not redetected in latest queue build for scope.'),
  updated_at = now()
WHERE :'close_missing' = 'true'
  AND q.provenance_run_tag = (SELECT scope_tag FROM _scope)
  AND q.status IN ('open', 'confirmed')
  AND (NULLIF(:'country_filter', '') IS NULL OR q.country = NULLIF(:'country_filter', '')::char(2))
  AND NOT EXISTS (
    SELECT 1
    FROM _issues i
    WHERE i.issue_key = q.issue_key
  );

SELECT json_build_object(
  'scopeCountry', COALESCE(NULLIF(:'country_filter', ''), ''),
  'scopeAsOf', COALESCE(NULLIF(:'as_of', ''), ''),
  'scopeTag', (SELECT scope_tag FROM _scope),
  'detectedIssues', (SELECT COUNT(*) FROM _issues),
  'openItems', (
    SELECT COUNT(*)
    FROM canonical_review_queue q
    WHERE q.provenance_run_tag = (SELECT scope_tag FROM _scope)
      AND (NULLIF(:'country_filter', '') IS NULL OR q.country = NULLIF(:'country_filter', '')::char(2))
      AND q.status = 'open'
  ),
  'confirmedItems', (
    SELECT COUNT(*)
    FROM canonical_review_queue q
    WHERE q.provenance_run_tag = (SELECT scope_tag FROM _scope)
      AND (NULLIF(:'country_filter', '') IS NULL OR q.country = NULLIF(:'country_filter', '')::char(2))
      AND q.status = 'confirmed'
  ),
  'resolvedItems', (
    SELECT COUNT(*)
    FROM canonical_review_queue q
    WHERE q.provenance_run_tag = (SELECT scope_tag FROM _scope)
      AND (NULLIF(:'country_filter', '') IS NULL OR q.country = NULLIF(:'country_filter', '')::char(2))
      AND q.status IN ('resolved', 'auto_resolved')
  ),
  'clustersV2', qa_rebuild_station_clusters_v2(
    NULLIF(:'country_filter', ''),
    NULLIF(:'as_of', '')::date
  )
)::text;

COMMIT;
`;

function createReviewQueueRepo(client) {
  return {
    async buildReviewQueue(scope) {
      const result = await client.runScript(BUILD_REVIEW_QUEUE_SQL, {
        country_filter: scope.country || '',
        as_of: scope.asOf || '',
        geo_threshold_m: String(scope.geoThresholdMeters || 3000),
        close_missing: scope.closeMissing === false ? 'false' : 'true'
      });

      const line = extractJsonLine(result.stdout);
      if (!line) {
        throw new AppError({
          code: 'REVIEW_QUEUE_BUILD_FAILED',
          message: 'Review queue build did not return summary JSON'
        });
      }

      let parsed;
      try {
        parsed = JSON.parse(line);
      } catch (err) {
        throw new AppError({
          code: 'REVIEW_QUEUE_BUILD_FAILED',
          message: 'Review queue build returned invalid summary JSON',
          cause: err
        });
      }

      validateOrThrow(parsed, REVIEW_QUEUE_SUMMARY_SCHEMA, {
        code: 'REVIEW_QUEUE_BUILD_FAILED',
        message: 'Review queue build summary failed schema validation'
      });

      return parsed;
    },

    async fetchReportMetrics(scope) {
      const row = await client.queryOne(
        `
          SELECT
            COUNT(*)::integer AS total_items,
            COUNT(*) FILTER (WHERE status = 'open')::integer AS open_items,
            COUNT(*) FILTER (WHERE status = 'confirmed')::integer AS confirmed_items,
            COUNT(*) FILTER (WHERE status = 'dismissed')::integer AS dismissed_items,
            COUNT(*) FILTER (WHERE status = 'resolved')::integer AS resolved_items,
            COUNT(*) FILTER (WHERE status = 'auto_resolved')::integer AS auto_resolved_items
          FROM canonical_review_queue q
          WHERE (NULLIF(:'country', '') IS NULL OR q.country = NULLIF(:'country', '')::char(2))
            AND (
              :'all_scopes' = 'true'
              OR q.provenance_run_tag = :'scope_tag'
            );
        `,
        {
          country: scope.country || '',
          all_scopes: scope.allScopes ? 'true' : 'false',
          scope_tag: scope.scopeTag || 'latest'
        }
      );

      const metrics = {
        totalItems: Number.parseInt(String(row?.total_items || 0), 10) || 0,
        openItems: Number.parseInt(String(row?.open_items || 0), 10) || 0,
        confirmedItems: Number.parseInt(String(row?.confirmed_items || 0), 10) || 0,
        dismissedItems: Number.parseInt(String(row?.dismissed_items || 0), 10) || 0,
        resolvedItems: Number.parseInt(String(row?.resolved_items || 0), 10) || 0,
        autoResolvedItems: Number.parseInt(String(row?.auto_resolved_items || 0), 10) || 0
      };

      const reviewed = metrics.dismissedItems + metrics.resolvedItems + metrics.autoResolvedItems;
      metrics.reviewCoveragePercent = metrics.totalItems > 0 ? (reviewed / metrics.totalItems) * 100 : 0;
      return metrics;
    },

    async listCountsByIssueType(scope) {
      return client.queryRows(
        `
          SELECT issue_type, status, COUNT(*)::integer AS items
          FROM canonical_review_queue q
          WHERE (NULLIF(:'country', '') IS NULL OR q.country = NULLIF(:'country', '')::char(2))
            AND (
              :'all_scopes' = 'true'
              OR q.provenance_run_tag = :'scope_tag'
            )
          GROUP BY issue_type, status
          ORDER BY issue_type, status;
        `,
        {
          country: scope.country || '',
          all_scopes: scope.allScopes ? 'true' : 'false',
          scope_tag: scope.scopeTag || 'latest'
        }
      );
    },

    async listOpenOrConfirmed(scope) {
      return client.queryRows(
        `
          SELECT
            review_item_id,
            issue_type,
            severity,
            country,
            COALESCE(canonical_station_id, '-') AS canonical_station_id,
            provenance_run_tag,
            to_char(last_detected_at, 'YYYY-MM-DD HH24:MI:SSOF') AS last_detected_at,
            left(details::text, 180) AS details
          FROM canonical_review_queue q
          WHERE (NULLIF(:'country', '') IS NULL OR q.country = NULLIF(:'country', '')::char(2))
            AND (
              :'all_scopes' = 'true'
              OR q.provenance_run_tag = :'scope_tag'
            )
            AND q.status IN ('open', 'confirmed')
          ORDER BY
            CASE q.severity WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
            q.last_detected_at DESC,
            q.issue_type ASC,
            q.review_item_id DESC
          LIMIT NULLIF(:'limit_rows', '')::integer;
        `,
        {
          country: scope.country || '',
          all_scopes: scope.allScopes ? 'true' : 'false',
          scope_tag: scope.scopeTag || 'latest',
          limit_rows: String(scope.limitRows || 20)
        }
      );
    },

    async listResolved(scope) {
      return client.queryRows(
        `
          SELECT
            review_item_id,
            issue_type,
            status,
            country,
            COALESCE(canonical_station_id, '-') AS canonical_station_id,
            provenance_run_tag,
            COALESCE(to_char(resolved_at, 'YYYY-MM-DD HH24:MI:SSOF'), '-') AS resolved_at,
            COALESCE(resolved_by, '-') AS resolved_by,
            COALESCE(left(resolution_note, 140), '-') AS resolution_note
          FROM canonical_review_queue q
          WHERE (NULLIF(:'country', '') IS NULL OR q.country = NULLIF(:'country', '')::char(2))
            AND (
              :'all_scopes' = 'true'
              OR q.provenance_run_tag = :'scope_tag'
            )
            AND q.status IN ('dismissed', 'resolved', 'auto_resolved')
          ORDER BY q.resolved_at DESC NULLS LAST, q.updated_at DESC, q.review_item_id DESC
          LIMIT NULLIF(:'limit_rows', '')::integer;
        `,
        {
          country: scope.country || '',
          all_scopes: scope.allScopes ? 'true' : 'false',
          scope_tag: scope.scopeTag || 'latest',
          limit_rows: String(scope.limitRows || 20)
        }
      );
    }
  };
}

module.exports = {
  BUILD_REVIEW_QUEUE_SQL,
  createReviewQueueRepo
};
