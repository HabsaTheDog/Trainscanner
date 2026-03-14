const { AppError } = require("../../../core/errors");
const { validateOrThrow } = require("../../../core/schema");

const GLOBAL_BUILD_SUMMARY_SCHEMA = {
  type: "object",
  required: [
    "sourceRows",
    "globalStations",
    "stationMappings",
    "globalStopPoints",
    "stopPointMappings",
  ],
  properties: {
    sourceRows: { type: "integer", minimum: 0 },
    globalStations: { type: "integer", minimum: 0 },
    stationMappings: { type: "integer", minimum: 0 },
    globalStopPoints: { type: "integer", minimum: 0 },
    stopPointMappings: { type: "integer", minimum: 0 },
    countryFilter: { type: "string" },
    asOf: { type: "string" },
    sourceScope: { type: "string" },
  },
  additionalProperties: true,
};

const DEFAULT_GLOBAL_BUILD_MAX_PARALLEL_WORKERS = 4;
const DEFAULT_GLOBAL_BUILD_WORK_MEM = "64MB";
const DEFAULT_GLOBAL_BUILD_MAINTENANCE_WORK_MEM = "256MB";
const DEFAULT_GLOBAL_BUILD_SYNCHRONOUS_COMMIT = "OFF";

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

function resolveSynchronousCommitEnv(name, fallback) {
  const rawValue = String(process.env[name] || "")
    .trim()
    .toUpperCase();
  if (!rawValue) {
    return fallback;
  }

  if (
    ["ON", "OFF", "LOCAL", "REMOTE_WRITE", "REMOTE_APPLY"].includes(rawValue)
  ) {
    return rawValue;
  }

  return fallback;
}

const GLOBAL_BUILD_MAX_PARALLEL_WORKERS = resolvePositiveIntegerEnv(
  "GLOBAL_STATION_BUILD_MAX_PARALLEL_WORKERS",
  DEFAULT_GLOBAL_BUILD_MAX_PARALLEL_WORKERS,
);
const GLOBAL_BUILD_WORK_MEM = resolveMemoryEnv(
  "GLOBAL_STATION_BUILD_WORK_MEM",
  DEFAULT_GLOBAL_BUILD_WORK_MEM,
);
const GLOBAL_BUILD_MAINTENANCE_WORK_MEM = resolveMemoryEnv(
  "GLOBAL_STATION_BUILD_MAINTENANCE_WORK_MEM",
  DEFAULT_GLOBAL_BUILD_MAINTENANCE_WORK_MEM,
);
const GLOBAL_BUILD_SYNCHRONOUS_COMMIT = resolveSynchronousCommitEnv(
  "GLOBAL_STATION_BUILD_SYNCHRONOUS_COMMIT",
  DEFAULT_GLOBAL_BUILD_SYNCHRONOUS_COMMIT,
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
  if (!message.startsWith("global_build_phase:")) {
    return "";
  }
  return message.slice("global_build_phase:".length).trim();
}

function extractInfoFromNotice(notice) {
  const message = String(notice?.message || notice || "").trim();
  if (!message.startsWith("global_build_info:")) {
    return null;
  }

  const payload = message.slice("global_build_info:".length).trim();
  const separatorIndex = payload.indexOf("=");
  if (separatorIndex <= 0) {
    return null;
  }

  return {
    key: payload.slice(0, separatorIndex).trim(),
    value: payload.slice(separatorIndex + 1).trim(),
  };
}

function parseSummaryLine(line, code, messagePrefix) {
  if (!line) {
    throw new AppError({
      code,
      message: `${messagePrefix} did not return summary JSON`,
    });
  }

  let parsed;
  try {
    parsed = JSON.parse(line);
  } catch (err) {
    throw new AppError({
      code,
      message: `${messagePrefix} returned invalid summary JSON`,
      cause: err,
    });
  }

  validateOrThrow(parsed, GLOBAL_BUILD_SUMMARY_SCHEMA, {
    code,
    message: `${messagePrefix} summary failed schema validation`,
  });

  return parsed;
}

function scopeParams(scope = {}) {
  return {
    country_filter: scope.country || "",
    as_of: scope.asOf || "",
    source_id_scope: scope.sourceId || "",
  };
}

const GLOBAL_BUILD_CURRENT_SUMMARY_SQL = `
SELECT json_build_object(
  'sourceRows', (
    SELECT COUNT(*)
    FROM (
      SELECT 1
      FROM raw_provider_stop_places rp
      JOIN provider_datasets pd
        ON pd.dataset_id = rp.dataset_id
      WHERE (NULLIF(:'country_filter', '') IS NULL OR rp.country = NULLIF(:'country_filter', '')::char(2))
        AND (NULLIF(:'source_id_scope', '') IS NULL OR rp.source_id = NULLIF(:'source_id_scope', ''))
        AND (NULLIF(:'as_of', '') IS NULL OR pd.snapshot_date <= NULLIF(:'as_of', '')::date)
      GROUP BY rp.source_id, rp.provider_stop_place_ref
    ) latest
  ),
  'globalStations', (
    SELECT COUNT(*)
    FROM global_stations gs
    WHERE gs.is_active = true
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR gs.country = NULLIF(:'country_filter', '')::char(2)
      )
      AND (
        NULLIF(:'source_id_scope', '') IS NULL
        OR EXISTS (
          SELECT 1
          FROM provider_global_station_mappings m
          WHERE m.global_station_id = gs.global_station_id
            AND m.source_id = NULLIF(:'source_id_scope', '')
            AND m.is_active = true
        )
      )
  ),
  'stationMappings', (
    SELECT COUNT(*)
    FROM provider_global_station_mappings m
    WHERE m.is_active = true
      AND (
        NULLIF(:'source_id_scope', '') IS NULL
        OR m.source_id = NULLIF(:'source_id_scope', '')
      )
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR EXISTS (
          SELECT 1
          FROM global_stations gs
          WHERE gs.global_station_id = m.global_station_id
            AND gs.is_active = true
            AND gs.country = NULLIF(:'country_filter', '')::char(2)
        )
      )
  ),
  'globalStopPoints', (
    SELECT COUNT(*)
    FROM global_stop_points sp
    WHERE sp.is_active = true
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR sp.country = NULLIF(:'country_filter', '')::char(2)
      )
      AND (
        NULLIF(:'source_id_scope', '') IS NULL
        OR sp.metadata ->> 'source_id' = NULLIF(:'source_id_scope', '')
      )
  ),
  'stopPointMappings', (
    SELECT COUNT(*)
    FROM provider_global_stop_point_mappings m
    JOIN global_stop_points sp
      ON sp.global_stop_point_id = m.global_stop_point_id
    WHERE m.is_active = true
      AND sp.is_active = true
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR sp.country = NULLIF(:'country_filter', '')::char(2)
      )
      AND (
        NULLIF(:'source_id_scope', '') IS NULL
        OR m.source_id = NULLIF(:'source_id_scope', '')
      )
  ),
  'mappedTripStopTimes', (
    SELECT COUNT(*)
    FROM timetable_trip_stop_times tts
    WHERE tts.global_stop_point_id IS NOT NULL
  ),
  'transferEdges', (
    SELECT COUNT(*)
    FROM transfer_edges te
    JOIN global_stop_points fsp
      ON fsp.global_stop_point_id = te.from_global_stop_point_id
    JOIN global_stop_points tsp
      ON tsp.global_stop_point_id = te.to_global_stop_point_id
    WHERE (
      NULLIF(:'country_filter', '') IS NULL
      OR fsp.country = NULLIF(:'country_filter', '')::char(2)
      OR tsp.country = NULLIF(:'country_filter', '')::char(2)
    )
      AND (
        NULLIF(:'source_id_scope', '') IS NULL
        OR fsp.metadata ->> 'source_id' = NULLIF(:'source_id_scope', '')
        OR tsp.metadata ->> 'source_id' = NULLIF(:'source_id_scope', '')
      )
  ),
  'coordSourceSelf', (
    SELECT COUNT(*)
    FROM global_stations gs
    WHERE gs.is_active = true
      AND COALESCE(gs.metadata ->> 'coord_source', '') = 'self'
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR gs.country = NULLIF(:'country_filter', '')::char(2)
      )
  ),
  'coordSourceParentStopPlace', (
    SELECT COUNT(*)
    FROM global_stations gs
    WHERE gs.is_active = true
      AND COALESCE(gs.metadata ->> 'coord_source', '') = 'parent_stop_place'
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR gs.country = NULLIF(:'country_filter', '')::char(2)
      )
  ),
  'coordSourceChildStopPoints', (
    SELECT COUNT(*)
    FROM global_stations gs
    WHERE gs.is_active = true
      AND COALESCE(gs.metadata ->> 'coord_source', '') = 'child_stop_points'
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR gs.country = NULLIF(:'country_filter', '')::char(2)
      )
  ),
  'coordSourceSiblingStopPlaces', (
    SELECT COUNT(*)
    FROM global_stations gs
    WHERE gs.is_active = true
      AND COALESCE(gs.metadata ->> 'coord_source', '') = 'sibling_stop_places'
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR gs.country = NULLIF(:'country_filter', '')::char(2)
      )
  ),
  'coordSourceTopographicPlaceCluster', (
    SELECT COUNT(*)
    FROM global_stations gs
    WHERE gs.is_active = true
      AND COALESCE(gs.metadata ->> 'coord_source', '') = 'topographic_place_cluster'
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR gs.country = NULLIF(:'country_filter', '')::char(2)
      )
  ),
  'coordSourceMissing', (
    SELECT COUNT(*)
    FROM global_stations gs
    WHERE gs.is_active = true
      AND COALESCE(gs.metadata ->> 'coord_source', '') = 'missing'
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR gs.country = NULLIF(:'country_filter', '')::char(2)
      )
  ),
  'coordConfidenceHigh', (
    SELECT COUNT(*)
    FROM global_stations gs
    WHERE gs.is_active = true
      AND COALESCE(gs.metadata ->> 'coord_confidence', '') = 'high'
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR gs.country = NULLIF(:'country_filter', '')::char(2)
      )
  ),
  'coordConfidenceMedium', (
    SELECT COUNT(*)
    FROM global_stations gs
    WHERE gs.is_active = true
      AND COALESCE(gs.metadata ->> 'coord_confidence', '') = 'medium'
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR gs.country = NULLIF(:'country_filter', '')::char(2)
      )
  ),
  'coordConfidenceLow', (
    SELECT COUNT(*)
    FROM global_stations gs
    WHERE gs.is_active = true
      AND COALESCE(gs.metadata ->> 'coord_confidence', '') = 'low'
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR gs.country = NULLIF(:'country_filter', '')::char(2)
      )
  ),
  'coordConfidenceUnresolved', (
    SELECT COUNT(*)
    FROM global_stations gs
    WHERE gs.is_active = true
      AND COALESCE(gs.metadata ->> 'coord_confidence', '') = 'unresolved'
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR gs.country = NULLIF(:'country_filter', '')::char(2)
      )
  ),
  'coordConflictCount', (
    SELECT COUNT(*)
    FROM global_stations gs
    WHERE gs.is_active = true
      AND COALESCE((gs.metadata -> 'coord_validation' ->> 'conflict_signal_count')::integer, 0) > 0
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR gs.country = NULLIF(:'country_filter', '')::char(2)
      )
  ),
  'countryFilter', COALESCE(NULLIF(:'country_filter', ''), ''),
  'asOf', COALESCE(NULLIF(:'as_of', ''), ''),
  'sourceScope', COALESCE(NULLIF(:'source_id_scope', ''), '')
)::text AS summary_json;
`;

const GLOBAL_BUILD_FINGERPRINT_SQL = `
WITH relevant_datasets AS (
  SELECT DISTINCT rp.dataset_id, rp.source_id
  FROM raw_provider_stop_places rp
  JOIN provider_datasets pd
    ON pd.dataset_id = rp.dataset_id
  WHERE (NULLIF(:'country_filter', '') IS NULL OR rp.country = NULLIF(:'country_filter', '')::char(2))
    AND (NULLIF(:'source_id_scope', '') IS NULL OR rp.source_id = NULLIF(:'source_id_scope', ''))
    AND (NULLIF(:'as_of', '') IS NULL OR pd.snapshot_date <= NULLIF(:'as_of', '')::date)
  UNION
  SELECT DISTINCT rpp.dataset_id, rpp.source_id
  FROM raw_provider_stop_points rpp
  JOIN provider_datasets pd
    ON pd.dataset_id = rpp.dataset_id
  WHERE (NULLIF(:'country_filter', '') IS NULL OR rpp.country = NULLIF(:'country_filter', '')::char(2))
    AND (NULLIF(:'source_id_scope', '') IS NULL OR rpp.source_id = NULLIF(:'source_id_scope', ''))
    AND (NULLIF(:'as_of', '') IS NULL OR pd.snapshot_date <= NULLIF(:'as_of', '')::date)
)
SELECT json_build_object(
  'stage', 'global-stations',
  'country', COALESCE(NULLIF(:'country_filter', ''), ''),
  'asOf', COALESCE(NULLIF(:'as_of', ''), ''),
  'sourceScope', COALESCE(NULLIF(:'source_id_scope', ''), ''),
  'datasetIds', COALESCE(
    (SELECT jsonb_agg(dataset_id ORDER BY dataset_id) FROM relevant_datasets),
    '[]'::jsonb
  ),
  'sourceIds', COALESCE(
    (SELECT jsonb_agg(DISTINCT source_id ORDER BY source_id) FROM relevant_datasets),
    '[]'::jsonb
  ),
  'datasetCount', (SELECT COUNT(*) FROM relevant_datasets)
) AS fingerprint;
`;

const BUILD_GLOBAL_SQL = `
BEGIN;

SET LOCAL max_parallel_workers_per_gather = ${GLOBAL_BUILD_MAX_PARALLEL_WORKERS};
SET LOCAL work_mem = '${GLOBAL_BUILD_WORK_MEM}';
SET LOCAL maintenance_work_mem = '${GLOBAL_BUILD_MAINTENANCE_WORK_MEM}';
SET LOCAL synchronous_commit = '${GLOBAL_BUILD_SYNCHRONOUS_COMMIT}';
SET LOCAL jit = off;

DO $$
BEGIN
  RAISE NOTICE 'global_build_phase:initializing';
  RAISE NOTICE 'global_build_info:country_filter=%', COALESCE(NULLIF(:'country_filter', ''), 'ALL');
  RAISE NOTICE 'global_build_info:as_of=%', COALESCE(NULLIF(:'as_of', ''), 'latest');
  RAISE NOTICE 'global_build_info:source_scope=%', COALESCE(NULLIF(:'source_id_scope', ''), 'ALL');
END $$;

DO $$
BEGIN
  RAISE NOTICE 'global_build_phase:loading_latest_stop_places';
END $$;

CREATE TEMP TABLE _candidate AS
SELECT
  latest.source_id,
  latest.provider_stop_place_ref,
  latest.country,
  latest.stop_name,
  latest.normalized_name,
  latest.latitude,
  latest.longitude,
  latest.geom,
  latest.parent_stop_place_ref,
  latest.topographic_place_ref,
  latest.hard_key
FROM (
  SELECT
    rp.source_id,
    rp.provider_stop_place_ref,
    rp.country,
    rp.stop_name,
    rp.normalized_name,
    rp.latitude,
    rp.longitude,
    rp.geom,
    rp.parent_stop_place_ref,
    rp.topographic_place_ref,
    NULLIF(rp.hard_id, '') AS hard_key,
    ROW_NUMBER() OVER (
      PARTITION BY rp.source_id, rp.provider_stop_place_ref
      ORDER BY pd.snapshot_date DESC, rp.dataset_id DESC, rp.updated_at DESC, rp.stop_place_id DESC
    ) AS row_num
  FROM raw_provider_stop_places rp
  JOIN provider_datasets pd
    ON pd.dataset_id = rp.dataset_id
  WHERE (NULLIF(:'country_filter', '') IS NULL OR rp.country = NULLIF(:'country_filter', '')::char(2))
    AND (NULLIF(:'source_id_scope', '') IS NULL OR rp.source_id = NULLIF(:'source_id_scope', ''))
    AND (NULLIF(:'as_of', '') IS NULL OR pd.snapshot_date <= NULLIF(:'as_of', '')::date)
) latest
WHERE latest.row_num = 1;

DO $$
BEGIN
  IF (SELECT COUNT(*) FROM _candidate) = 0 THEN
    RAISE EXCEPTION 'No raw provider stop places found for selected scope';
  END IF;
  RAISE NOTICE 'global_build_info:source_row_count=%', (SELECT COUNT(*) FROM _candidate);
END $$;

CREATE INDEX _candidate_source_place_idx
  ON _candidate (source_id, provider_stop_place_ref);

CREATE INDEX _candidate_hard_key_idx
  ON _candidate (hard_key)
  WHERE hard_key IS NOT NULL;

CREATE INDEX _candidate_normalized_name_idx
  ON _candidate (normalized_name);

CREATE INDEX _candidate_parent_idx
  ON _candidate (source_id, parent_stop_place_ref)
  WHERE parent_stop_place_ref IS NOT NULL;

CREATE INDEX _candidate_topographic_idx
  ON _candidate (source_id, topographic_place_ref)
  WHERE topographic_place_ref IS NOT NULL;

DO $$
BEGIN
  RAISE NOTICE 'global_build_info:candidate_index_count=5';
END $$;

CREATE TEMP TABLE _candidate_child_counts AS
SELECT
  c.source_id,
  c.provider_stop_place_ref,
  COUNT(child.provider_stop_place_ref)::integer AS child_count
FROM _candidate c
LEFT JOIN _candidate child
  ON child.source_id = c.source_id
 AND child.parent_stop_place_ref = c.provider_stop_place_ref
GROUP BY c.source_id, c.provider_stop_place_ref;

DO $$
BEGIN
  RAISE NOTICE 'global_build_info:child_count_rows=%', (SELECT COUNT(*) FROM _candidate_child_counts);
END $$;

DO $$
BEGIN
  RAISE NOTICE 'global_build_phase:loading_latest_stop_points';
END $$;

CREATE TEMP TABLE _candidate_stop_points_latest AS
SELECT
  latest.source_id,
  latest.provider_stop_point_ref,
  latest.provider_stop_place_ref,
  latest.country,
  latest.stop_name,
  latest.normalized_name,
  latest.latitude,
  latest.longitude,
  latest.geom,
  latest.topographic_place_ref
FROM (
  SELECT
    rpp.source_id,
    rpp.provider_stop_point_ref,
    rpp.provider_stop_place_ref,
    rpp.country,
    rpp.stop_name,
    rpp.normalized_name,
    rpp.latitude,
    rpp.longitude,
    rpp.geom,
    rpp.topographic_place_ref,
    ROW_NUMBER() OVER (
      PARTITION BY rpp.source_id, rpp.provider_stop_point_ref
      ORDER BY pd.snapshot_date DESC, rpp.dataset_id DESC, rpp.updated_at DESC, rpp.stop_point_id DESC
    ) AS row_num
  FROM raw_provider_stop_points rpp
  JOIN provider_datasets pd
    ON pd.dataset_id = rpp.dataset_id
  WHERE (NULLIF(:'country_filter', '') IS NULL OR rpp.country = NULLIF(:'country_filter', '')::char(2))
    AND (NULLIF(:'source_id_scope', '') IS NULL OR rpp.source_id = NULLIF(:'source_id_scope', ''))
    AND (NULLIF(:'as_of', '') IS NULL OR pd.snapshot_date <= NULLIF(:'as_of', '')::date)
) latest
WHERE latest.row_num = 1;

CREATE INDEX _candidate_stop_points_latest_place_idx
  ON _candidate_stop_points_latest (source_id, provider_stop_place_ref);

DO $$
BEGIN
  RAISE NOTICE 'global_build_info:latest_stop_point_rows=%', (SELECT COUNT(*) FROM _candidate_stop_points_latest);
  RAISE NOTICE 'global_build_info:stop_point_latest_index_count=1';
END $$;

DO $$
BEGIN
  RAISE NOTICE 'global_build_phase:building_stop_point_evidence';
END $$;

CREATE TEMP TABLE _candidate_stop_point_evidence AS
SELECT
  grouped.source_id,
  grouped.provider_stop_place_ref,
  grouped.stop_point_count,
  grouped.latitude,
  grouped.longitude,
  grouped.geom,
  COALESCE(spread.max_distance_meters, 0)::numeric(12,2) AS max_distance_meters
FROM (
  SELECT
    source_id,
    provider_stop_place_ref,
    COUNT(*) FILTER (WHERE geom IS NOT NULL)::integer AS stop_point_count,
    AVG(latitude) FILTER (WHERE latitude IS NOT NULL) AS latitude,
    AVG(longitude) FILTER (WHERE longitude IS NOT NULL) AS longitude,
    CASE
      WHEN COUNT(*) FILTER (WHERE geom IS NOT NULL) > 0 THEN ST_SetSRID(ST_Centroid(ST_Collect(geom)), 4326)
      ELSE NULL::geometry(Point, 4326)
    END AS geom
  FROM _candidate_stop_points_latest
  GROUP BY source_id, provider_stop_place_ref
) grouped
LEFT JOIN LATERAL (
  SELECT
    MAX(ST_DistanceSphere(grouped.geom, sp.geom)) AS max_distance_meters
  FROM _candidate_stop_points_latest sp
  WHERE sp.source_id = grouped.source_id
    AND sp.provider_stop_place_ref = grouped.provider_stop_place_ref
    AND grouped.geom IS NOT NULL
    AND sp.geom IS NOT NULL
) spread ON true;

DO $$
BEGIN
  RAISE NOTICE 'global_build_info:stop_point_evidence_rows=%', (SELECT COUNT(*) FROM _candidate_stop_point_evidence);
END $$;

DO $$
BEGIN
  RAISE NOTICE 'global_build_phase:building_parent_evidence';
END $$;

CREATE TEMP TABLE _candidate_parent_evidence AS
SELECT
  c.source_id,
  c.provider_stop_place_ref,
  parent.provider_stop_place_ref AS parent_provider_stop_place_ref,
  parent.latitude AS parent_latitude,
  parent.longitude AS parent_longitude,
  parent.geom AS parent_geom,
  parent.topographic_place_ref AS parent_topographic_place_ref
FROM _candidate c
LEFT JOIN _candidate parent
  ON parent.source_id = c.source_id
 AND parent.provider_stop_place_ref = c.parent_stop_place_ref;

DO $$
BEGIN
  RAISE NOTICE 'global_build_info:parent_evidence_rows=%', (SELECT COUNT(*) FROM _candidate_parent_evidence);
END $$;

DO $$
BEGIN
  RAISE NOTICE 'global_build_phase:building_sibling_evidence';
END $$;

CREATE TEMP TABLE _candidate_sibling_evidence AS
SELECT
  stats.source_id,
  stats.provider_stop_place_ref,
  stats.sibling_count,
  stats.latitude,
  stats.longitude,
  stats.geom,
  COALESCE(spread.max_distance_meters, 0)::numeric(12,2) AS max_distance_meters
FROM (
  SELECT
    c.source_id,
    c.provider_stop_place_ref,
    COUNT(s.provider_stop_place_ref) FILTER (WHERE s.geom IS NOT NULL)::integer AS sibling_count,
    AVG(s.latitude) FILTER (WHERE s.latitude IS NOT NULL) AS latitude,
    AVG(s.longitude) FILTER (WHERE s.longitude IS NOT NULL) AS longitude,
    CASE
      WHEN COUNT(s.provider_stop_place_ref) FILTER (WHERE s.geom IS NOT NULL) > 0 THEN ST_SetSRID(ST_Centroid(ST_Collect(s.geom)), 4326)
      ELSE NULL::geometry(Point, 4326)
    END AS geom
  FROM _candidate c
  LEFT JOIN _candidate s
    ON s.source_id = c.source_id
   AND s.parent_stop_place_ref = c.parent_stop_place_ref
   AND c.parent_stop_place_ref IS NOT NULL
   AND s.provider_stop_place_ref <> c.provider_stop_place_ref
   AND s.geom IS NOT NULL
  GROUP BY c.source_id, c.provider_stop_place_ref
) stats
LEFT JOIN LATERAL (
  SELECT
    MAX(ST_DistanceSphere(stats.geom, s.geom)) AS max_distance_meters
  FROM _candidate base
  JOIN _candidate s
    ON s.source_id = base.source_id
   AND s.parent_stop_place_ref = base.parent_stop_place_ref
   AND base.parent_stop_place_ref IS NOT NULL
   AND s.provider_stop_place_ref <> base.provider_stop_place_ref
   AND s.geom IS NOT NULL
  WHERE base.source_id = stats.source_id
    AND base.provider_stop_place_ref = stats.provider_stop_place_ref
    AND stats.geom IS NOT NULL
) spread ON true;

DO $$
BEGIN
  RAISE NOTICE 'global_build_info:sibling_evidence_rows=%', (SELECT COUNT(*) FROM _candidate_sibling_evidence);
END $$;

CREATE TEMP TABLE _candidate_topographic_evidence AS
SELECT
  stats.source_id,
  stats.provider_stop_place_ref,
  stats.peer_count,
  stats.latitude,
  stats.longitude,
  stats.geom,
  COALESCE(spread.max_distance_meters, 0)::numeric(12,2) AS max_distance_meters
FROM (
  SELECT
    c.source_id,
    c.provider_stop_place_ref,
    COUNT(peer.provider_stop_place_ref) FILTER (WHERE peer.geom IS NOT NULL)::integer AS peer_count,
    AVG(peer.latitude) FILTER (WHERE peer.latitude IS NOT NULL) AS latitude,
    AVG(peer.longitude) FILTER (WHERE peer.longitude IS NOT NULL) AS longitude,
    CASE
      WHEN COUNT(peer.provider_stop_place_ref) FILTER (WHERE peer.geom IS NOT NULL) > 0 THEN ST_SetSRID(ST_Centroid(ST_Collect(peer.geom)), 4326)
      ELSE NULL::geometry(Point, 4326)
    END AS geom
  FROM _candidate c
  LEFT JOIN _candidate peer
    ON peer.source_id = c.source_id
   AND peer.topographic_place_ref = c.topographic_place_ref
   AND c.topographic_place_ref IS NOT NULL
   AND peer.provider_stop_place_ref <> c.provider_stop_place_ref
   AND peer.geom IS NOT NULL
  GROUP BY c.source_id, c.provider_stop_place_ref
) stats
LEFT JOIN LATERAL (
  SELECT
    MAX(ST_DistanceSphere(stats.geom, peer.geom)) AS max_distance_meters
  FROM _candidate base
  JOIN _candidate peer
    ON peer.source_id = base.source_id
   AND peer.topographic_place_ref = base.topographic_place_ref
   AND base.topographic_place_ref IS NOT NULL
   AND peer.provider_stop_place_ref <> base.provider_stop_place_ref
   AND peer.geom IS NOT NULL
  WHERE base.source_id = stats.source_id
    AND base.provider_stop_place_ref = stats.provider_stop_place_ref
    AND stats.geom IS NOT NULL
) spread ON true;

DO $$
BEGIN
  RAISE NOTICE 'global_build_info:topographic_evidence_rows=%', (SELECT COUNT(*) FROM _candidate_topographic_evidence);
END $$;

DO $$
BEGIN
  RAISE NOTICE 'global_build_phase:building_coord_resolution';
END $$;

CREATE TEMP TABLE _candidate_coord_choice AS
SELECT
  c.source_id,
  c.provider_stop_place_ref,
  c.country,
  c.stop_name,
  c.normalized_name,
  c.hard_key,
  c.parent_stop_place_ref,
  c.topographic_place_ref,
  c.latitude AS self_latitude,
  c.longitude AS self_longitude,
  c.geom AS self_geom,
  pe.parent_provider_stop_place_ref,
  pe.parent_latitude,
  pe.parent_longitude,
  pe.parent_geom,
  sp.stop_point_count,
  sp.latitude AS stop_point_latitude,
  sp.longitude AS stop_point_longitude,
  sp.geom AS stop_point_geom,
  sp.max_distance_meters AS stop_point_spread_meters,
  se.sibling_count,
  se.latitude AS sibling_latitude,
  se.longitude AS sibling_longitude,
  se.geom AS sibling_geom,
  se.max_distance_meters AS sibling_spread_meters,
  te.peer_count AS topographic_peer_count,
  te.latitude AS topographic_latitude,
  te.longitude AS topographic_longitude,
  te.geom AS topographic_geom,
  te.max_distance_meters AS topographic_spread_meters,
  COALESCE(cc.child_count, 0) AS child_count,
  CASE
    WHEN c.parent_stop_place_ref IS NOT NULL THEN 'child'
    WHEN COALESCE(cc.child_count, 0) > 0 THEN 'parent'
    ELSE 'standalone'
  END AS hierarchy_role,
  CASE
    WHEN c.geom IS NOT NULL THEN 'self'
    WHEN pe.parent_geom IS NOT NULL AND sp.geom IS NOT NULL THEN
      CASE
        WHEN ST_DistanceSphere(pe.parent_geom, sp.geom) <= 250 THEN 'child_stop_points'
        WHEN COALESCE(sp.stop_point_count, 0) >= 2 AND ST_DistanceSphere(pe.parent_geom, sp.geom) > 75 THEN 'child_stop_points'
        ELSE 'parent_stop_place'
      END
    WHEN pe.parent_geom IS NOT NULL THEN 'parent_stop_place'
    WHEN sp.geom IS NOT NULL THEN 'child_stop_points'
    WHEN se.geom IS NOT NULL AND COALESCE(se.sibling_count, 0) >= 2 AND COALESCE(se.max_distance_meters, 0) <= 1000 THEN 'sibling_stop_places'
    WHEN te.geom IS NOT NULL AND COALESCE(te.peer_count, 0) >= 2 AND COALESCE(te.max_distance_meters, 0) <= 1500 THEN 'topographic_place_cluster'
    ELSE 'missing'
  END AS coord_source
FROM _candidate c
LEFT JOIN _candidate_parent_evidence pe
  ON pe.source_id = c.source_id
 AND pe.provider_stop_place_ref = c.provider_stop_place_ref
LEFT JOIN _candidate_stop_point_evidence sp
  ON sp.source_id = c.source_id
 AND sp.provider_stop_place_ref = c.provider_stop_place_ref
LEFT JOIN _candidate_sibling_evidence se
  ON se.source_id = c.source_id
 AND se.provider_stop_place_ref = c.provider_stop_place_ref
LEFT JOIN _candidate_topographic_evidence te
  ON te.source_id = c.source_id
 AND te.provider_stop_place_ref = c.provider_stop_place_ref
LEFT JOIN _candidate_child_counts cc
  ON cc.source_id = c.source_id
 AND cc.provider_stop_place_ref = c.provider_stop_place_ref;

CREATE TEMP TABLE _candidate_coord_resolution AS
SELECT
  choice.*,
  chosen.resolved_latitude,
  chosen.resolved_longitude,
  chosen.resolved_geom,
  dist.parent_distance_meters,
  dist.stop_point_distance_meters,
  dist.sibling_distance_meters,
  dist.topographic_distance_meters,
  (
    CASE
      WHEN chosen.resolved_geom IS NOT NULL
        AND choice.coord_source <> 'parent_stop_place'
        AND choice.parent_geom IS NOT NULL
        AND dist.parent_distance_meters <= 250 THEN 1
      ELSE 0
    END +
    CASE
      WHEN chosen.resolved_geom IS NOT NULL
        AND choice.coord_source <> 'child_stop_points'
        AND choice.stop_point_geom IS NOT NULL
        AND dist.stop_point_distance_meters <= 250 THEN 1
      ELSE 0
    END +
    CASE
      WHEN chosen.resolved_geom IS NOT NULL
        AND choice.coord_source <> 'sibling_stop_places'
        AND choice.sibling_geom IS NOT NULL
        AND dist.sibling_distance_meters <= 250 THEN 1
      ELSE 0
    END +
    CASE
      WHEN chosen.resolved_geom IS NOT NULL
        AND choice.coord_source <> 'topographic_place_cluster'
        AND choice.topographic_geom IS NOT NULL
        AND dist.topographic_distance_meters <= 250 THEN 1
      ELSE 0
    END
  )::integer AS supporting_signal_count,
  (
    CASE
      WHEN chosen.resolved_geom IS NOT NULL
        AND choice.coord_source <> 'parent_stop_place'
        AND choice.parent_geom IS NOT NULL
        AND dist.parent_distance_meters > 1000 THEN 1
      ELSE 0
    END +
    CASE
      WHEN chosen.resolved_geom IS NOT NULL
        AND choice.coord_source <> 'child_stop_points'
        AND choice.stop_point_geom IS NOT NULL
        AND dist.stop_point_distance_meters > 1000 THEN 1
      ELSE 0
    END +
    CASE
      WHEN chosen.resolved_geom IS NOT NULL
        AND choice.coord_source <> 'sibling_stop_places'
        AND choice.sibling_geom IS NOT NULL
        AND dist.sibling_distance_meters > 1000 THEN 1
      ELSE 0
    END +
    CASE
      WHEN chosen.resolved_geom IS NOT NULL
        AND choice.coord_source <> 'topographic_place_cluster'
        AND choice.topographic_geom IS NOT NULL
        AND dist.topographic_distance_meters > 1000 THEN 1
      ELSE 0
    END
  )::integer AS conflict_signal_count,
  GREATEST(
    COALESCE(dist.parent_distance_meters, 0),
    COALESCE(dist.stop_point_distance_meters, 0),
    COALESCE(dist.sibling_distance_meters, 0),
    COALESCE(dist.topographic_distance_meters, 0)
  )::numeric(12,2) AS max_signal_distance_meters,
  CASE
    WHEN chosen.resolved_geom IS NULL THEN 'unresolved'
    WHEN choice.coord_source = 'self' THEN 'high'
    WHEN choice.coord_source IN ('parent_stop_place', 'child_stop_points')
      AND (
        CASE
          WHEN chosen.resolved_geom IS NOT NULL
            AND choice.coord_source <> 'parent_stop_place'
            AND choice.parent_geom IS NOT NULL
            AND dist.parent_distance_meters <= 250 THEN 1
          ELSE 0
        END +
        CASE
          WHEN chosen.resolved_geom IS NOT NULL
            AND choice.coord_source <> 'child_stop_points'
            AND choice.stop_point_geom IS NOT NULL
            AND dist.stop_point_distance_meters <= 250 THEN 1
          ELSE 0
        END +
        CASE
          WHEN chosen.resolved_geom IS NOT NULL
            AND choice.coord_source <> 'sibling_stop_places'
            AND choice.sibling_geom IS NOT NULL
            AND dist.sibling_distance_meters <= 250 THEN 1
          ELSE 0
        END +
        CASE
          WHEN chosen.resolved_geom IS NOT NULL
            AND choice.coord_source <> 'topographic_place_cluster'
            AND choice.topographic_geom IS NOT NULL
            AND dist.topographic_distance_meters <= 250 THEN 1
          ELSE 0
        END
      ) >= 1 THEN 'high'
    WHEN choice.coord_source IN ('parent_stop_place', 'child_stop_points')
      AND (
        CASE
          WHEN chosen.resolved_geom IS NOT NULL
            AND choice.coord_source <> 'parent_stop_place'
            AND choice.parent_geom IS NOT NULL
            AND dist.parent_distance_meters > 1000 THEN 1
          ELSE 0
        END +
        CASE
          WHEN chosen.resolved_geom IS NOT NULL
            AND choice.coord_source <> 'child_stop_points'
            AND choice.stop_point_geom IS NOT NULL
            AND dist.stop_point_distance_meters > 1000 THEN 1
          ELSE 0
        END +
        CASE
          WHEN chosen.resolved_geom IS NOT NULL
            AND choice.coord_source <> 'sibling_stop_places'
            AND choice.sibling_geom IS NOT NULL
            AND dist.sibling_distance_meters > 1000 THEN 1
          ELSE 0
        END +
        CASE
          WHEN chosen.resolved_geom IS NOT NULL
            AND choice.coord_source <> 'topographic_place_cluster'
            AND choice.topographic_geom IS NOT NULL
            AND dist.topographic_distance_meters > 1000 THEN 1
          ELSE 0
        END
      ) = 0 THEN 'medium'
    WHEN choice.coord_source = 'sibling_stop_places'
      AND COALESCE(choice.sibling_count, 0) >= 2
      AND COALESCE(choice.sibling_spread_meters, 0) <= 1000 THEN 'medium'
    WHEN choice.coord_source = 'missing' THEN 'unresolved'
    ELSE 'low'
  END AS coord_confidence,
  ARRAY_REMOVE(
    ARRAY[
      CASE WHEN choice.self_geom IS NULL THEN 'missing_self_geom' END,
      CASE WHEN choice.coord_source = 'parent_stop_place' THEN 'used_parent_geom' END,
      CASE WHEN choice.coord_source = 'child_stop_points' THEN 'used_child_stop_points' END,
      CASE WHEN choice.coord_source = 'sibling_stop_places' THEN 'used_sibling_centroid' END,
      CASE WHEN choice.coord_source = 'topographic_place_cluster' THEN 'used_topographic_place_centroid' END,
      CASE
        WHEN GREATEST(
          COALESCE(dist.parent_distance_meters, 0),
          COALESCE(dist.stop_point_distance_meters, 0),
          COALESCE(dist.sibling_distance_meters, 0),
          COALESCE(dist.topographic_distance_meters, 0)
        ) > 1000 THEN 'signal_conflict_gt_1000m'
      END,
      CASE
        WHEN chosen.resolved_geom IS NOT NULL
          AND (
            CASE
              WHEN chosen.resolved_geom IS NOT NULL
                AND choice.coord_source <> 'parent_stop_place'
                AND choice.parent_geom IS NOT NULL
                AND dist.parent_distance_meters <= 250 THEN 1
              ELSE 0
            END +
            CASE
              WHEN chosen.resolved_geom IS NOT NULL
                AND choice.coord_source <> 'child_stop_points'
                AND choice.stop_point_geom IS NOT NULL
                AND dist.stop_point_distance_meters <= 250 THEN 1
              ELSE 0
            END +
            CASE
              WHEN chosen.resolved_geom IS NOT NULL
                AND choice.coord_source <> 'sibling_stop_places'
                AND choice.sibling_geom IS NOT NULL
                AND dist.sibling_distance_meters <= 250 THEN 1
              ELSE 0
            END +
            CASE
              WHEN chosen.resolved_geom IS NOT NULL
                AND choice.coord_source <> 'topographic_place_cluster'
                AND choice.topographic_geom IS NOT NULL
                AND dist.topographic_distance_meters <= 250 THEN 1
              ELSE 0
            END
          ) = 0 THEN 'no_supporting_signal'
      END,
      CASE
        WHEN chosen.resolved_geom IS NOT NULL
          AND (
            CASE
              WHEN chosen.resolved_geom IS NOT NULL
                AND choice.coord_source <> 'parent_stop_place'
                AND choice.parent_geom IS NOT NULL
                AND dist.parent_distance_meters <= 250 THEN 1
              ELSE 0
            END +
            CASE
              WHEN chosen.resolved_geom IS NOT NULL
                AND choice.coord_source <> 'child_stop_points'
                AND choice.stop_point_geom IS NOT NULL
                AND dist.stop_point_distance_meters <= 250 THEN 1
              ELSE 0
            END +
            CASE
              WHEN chosen.resolved_geom IS NOT NULL
                AND choice.coord_source <> 'sibling_stop_places'
                AND choice.sibling_geom IS NOT NULL
                AND dist.sibling_distance_meters <= 250 THEN 1
              ELSE 0
            END +
            CASE
              WHEN chosen.resolved_geom IS NOT NULL
                AND choice.coord_source <> 'topographic_place_cluster'
                AND choice.topographic_geom IS NOT NULL
                AND dist.topographic_distance_meters <= 250 THEN 1
              ELSE 0
            END
          ) = 0
          AND (
            CASE WHEN choice.parent_geom IS NOT NULL AND choice.coord_source <> 'parent_stop_place' THEN 1 ELSE 0 END +
            CASE WHEN choice.stop_point_geom IS NOT NULL AND choice.coord_source <> 'child_stop_points' THEN 1 ELSE 0 END +
            CASE WHEN choice.sibling_geom IS NOT NULL AND choice.coord_source <> 'sibling_stop_places' THEN 1 ELSE 0 END +
            CASE WHEN choice.topographic_geom IS NOT NULL AND choice.coord_source <> 'topographic_place_cluster' THEN 1 ELSE 0 END
          ) <= 1 THEN 'single_weak_signal'
      END,
      CASE WHEN choice.coord_source = 'missing' THEN 'unresolved_missing_all_sources' END
    ],
    NULL
  )::text[] AS warning_codes
FROM _candidate_coord_choice choice
CROSS JOIN LATERAL (
  SELECT
    CASE choice.coord_source
      WHEN 'self' THEN choice.self_latitude
      WHEN 'parent_stop_place' THEN choice.parent_latitude
      WHEN 'child_stop_points' THEN choice.stop_point_latitude
      WHEN 'sibling_stop_places' THEN choice.sibling_latitude
      WHEN 'topographic_place_cluster' THEN choice.topographic_latitude
      ELSE NULL::double precision
    END AS resolved_latitude,
    CASE choice.coord_source
      WHEN 'self' THEN choice.self_longitude
      WHEN 'parent_stop_place' THEN choice.parent_longitude
      WHEN 'child_stop_points' THEN choice.stop_point_longitude
      WHEN 'sibling_stop_places' THEN choice.sibling_longitude
      WHEN 'topographic_place_cluster' THEN choice.topographic_longitude
      ELSE NULL::double precision
    END AS resolved_longitude,
    CASE choice.coord_source
      WHEN 'self' THEN choice.self_geom
      WHEN 'parent_stop_place' THEN choice.parent_geom
      WHEN 'child_stop_points' THEN choice.stop_point_geom
      WHEN 'sibling_stop_places' THEN choice.sibling_geom
      WHEN 'topographic_place_cluster' THEN choice.topographic_geom
      ELSE NULL::geometry(Point, 4326)
    END AS resolved_geom
) chosen
CROSS JOIN LATERAL (
  SELECT
    CASE
      WHEN chosen.resolved_geom IS NOT NULL AND choice.parent_geom IS NOT NULL THEN ST_DistanceSphere(chosen.resolved_geom, choice.parent_geom)
      ELSE NULL::numeric
    END AS parent_distance_meters,
    CASE
      WHEN chosen.resolved_geom IS NOT NULL AND choice.stop_point_geom IS NOT NULL THEN ST_DistanceSphere(chosen.resolved_geom, choice.stop_point_geom)
      ELSE NULL::numeric
    END AS stop_point_distance_meters,
    CASE
      WHEN chosen.resolved_geom IS NOT NULL AND choice.sibling_geom IS NOT NULL THEN ST_DistanceSphere(chosen.resolved_geom, choice.sibling_geom)
      ELSE NULL::numeric
    END AS sibling_distance_meters,
    CASE
      WHEN chosen.resolved_geom IS NOT NULL AND choice.topographic_geom IS NOT NULL THEN ST_DistanceSphere(chosen.resolved_geom, choice.topographic_geom)
      ELSE NULL::numeric
    END AS topographic_distance_meters
) dist;

DO $$
BEGIN
  RAISE NOTICE 'global_build_info:coord_resolution_rows=%', (SELECT COUNT(*) FROM _candidate_coord_resolution);
  RAISE NOTICE 'global_build_info:coord_conflict_count=%', (
    SELECT COUNT(*)
    FROM _candidate_coord_resolution
    WHERE conflict_signal_count > 0
  );
END $$;

CREATE TEMP TABLE _hard_groups AS
SELECT
  'gstn_' || substr(md5('hard|' || hard_key), 1, 24) AS global_station_id,
  hard_key,
  MIN(stop_name) AS display_name,
  MIN(normalized_name) AS normalized_name,
  MIN(country) AS country,
  AVG(resolved_latitude) FILTER (WHERE resolved_latitude IS NOT NULL) AS latitude,
  AVG(resolved_longitude) FILTER (WHERE resolved_longitude IS NOT NULL) AS longitude,
  CASE
    WHEN COUNT(*) FILTER (WHERE resolved_geom IS NOT NULL) > 0 THEN ST_SetSRID(ST_Centroid(ST_Collect(resolved_geom)), 4326)
    ELSE NULL::geometry(Point, 4326)
  END AS geom,
  jsonb_build_object(
    'coord_source',
    CASE
      WHEN COUNT(*) FILTER (WHERE coord_source = 'self') > 0 THEN 'self'
      WHEN COUNT(*) FILTER (WHERE coord_source = 'parent_stop_place') > 0 THEN 'parent_stop_place'
      WHEN COUNT(*) FILTER (WHERE coord_source = 'child_stop_points') > 0 THEN 'child_stop_points'
      WHEN COUNT(*) FILTER (WHERE coord_source = 'sibling_stop_places') > 0 THEN 'sibling_stop_places'
      WHEN COUNT(*) FILTER (WHERE coord_source = 'topographic_place_cluster') > 0 THEN 'topographic_place_cluster'
      ELSE 'missing'
    END,
    'coord_confidence',
    CASE
      WHEN COUNT(*) FILTER (WHERE resolved_geom IS NOT NULL) = 0 THEN 'unresolved'
      WHEN COUNT(*) FILTER (WHERE coord_confidence = 'low') > 0 THEN 'low'
      WHEN COUNT(*) FILTER (WHERE coord_confidence = 'medium') > 0 THEN 'medium'
      ELSE 'high'
    END,
    'coord_validation',
    jsonb_build_object(
      'supporting_signal_count', COALESCE(MAX(supporting_signal_count), 0),
      'conflict_signal_count', COALESCE(MAX(conflict_signal_count), 0),
      'max_signal_distance_meters', COALESCE(MAX(max_signal_distance_meters), 0),
      'validated_against', to_jsonb(ARRAY_REMOVE(ARRAY[
        CASE WHEN COUNT(*) FILTER (WHERE parent_geom IS NOT NULL) > 0 THEN 'parent_stop_place' END,
        CASE WHEN COUNT(*) FILTER (WHERE stop_point_geom IS NOT NULL) > 0 THEN 'child_stop_points' END,
        CASE WHEN COUNT(*) FILTER (WHERE sibling_geom IS NOT NULL) > 0 THEN 'sibling_stop_places' END,
        CASE WHEN COUNT(*) FILTER (WHERE topographic_geom IS NOT NULL) > 0 THEN 'topographic_place_cluster' END
      ], NULL)),
      'warning_codes', to_jsonb(ARRAY_REMOVE(ARRAY[
        CASE WHEN COUNT(*) FILTER (WHERE self_geom IS NULL) > 0 THEN 'missing_self_geom' END,
        CASE WHEN COUNT(*) FILTER (WHERE conflict_signal_count > 0) > 0 THEN 'signal_conflict_gt_1000m' END,
        CASE WHEN COUNT(*) FILTER (WHERE supporting_signal_count = 0 AND resolved_geom IS NOT NULL) > 0 THEN 'no_supporting_signal' END,
        CASE WHEN COUNT(*) FILTER (WHERE coord_source = 'missing') > 0 THEN 'unresolved_missing_all_sources' END
      ], NULL))
    ),
    'coord_inputs',
    jsonb_build_object(
      'provider_stop_place_refs',
      to_jsonb(ARRAY_AGG(provider_stop_place_ref ORDER BY provider_stop_place_ref))
    ),
    'hierarchy_role',
    CASE
      WHEN COUNT(*) FILTER (WHERE hierarchy_role = 'child') > 0 THEN 'child'
      WHEN COUNT(*) FILTER (WHERE hierarchy_role = 'parent') > 0 THEN 'parent'
      ELSE 'standalone'
    END,
    'parent_stop_place_ref',
    MIN(parent_stop_place_ref),
    'topographic_place_ref',
    MIN(topographic_place_ref)
  ) AS metadata
FROM _candidate_coord_resolution
WHERE hard_key IS NOT NULL
GROUP BY hard_key;

CREATE TEMP TABLE _soft_geo_clustered AS
SELECT
  c.*,
  ST_ClusterDBSCAN(ST_Transform(c.resolved_geom, 3857), eps := 500, minpoints := 1)
    OVER (PARTITION BY c.normalized_name ORDER BY c.source_id, c.provider_stop_place_ref) AS cluster_id
FROM _candidate_coord_resolution c
WHERE c.hard_key IS NULL
  AND c.resolved_geom IS NOT NULL;

CREATE TEMP TABLE _soft_geo_groups AS
SELECT
  'gstn_' || substr(md5('geo|' || normalized_name || '|' || cluster_id::text), 1, 24) AS global_station_id,
  normalized_name,
  cluster_id,
  MIN(stop_name) AS display_name,
  MIN(country) AS country,
  AVG(resolved_latitude) AS latitude,
  AVG(resolved_longitude) AS longitude,
  ST_SetSRID(ST_Centroid(ST_Collect(resolved_geom)), 4326) AS geom,
  jsonb_build_object(
    'coord_source',
    CASE
      WHEN COUNT(*) FILTER (WHERE coord_source = 'self') > 0 THEN 'self'
      WHEN COUNT(*) FILTER (WHERE coord_source = 'parent_stop_place') > 0 THEN 'parent_stop_place'
      WHEN COUNT(*) FILTER (WHERE coord_source = 'child_stop_points') > 0 THEN 'child_stop_points'
      WHEN COUNT(*) FILTER (WHERE coord_source = 'sibling_stop_places') > 0 THEN 'sibling_stop_places'
      WHEN COUNT(*) FILTER (WHERE coord_source = 'topographic_place_cluster') > 0 THEN 'topographic_place_cluster'
      ELSE 'missing'
    END,
    'coord_confidence',
    CASE
      WHEN COUNT(*) FILTER (WHERE coord_confidence = 'low') > 0 THEN 'low'
      WHEN COUNT(*) FILTER (WHERE coord_confidence = 'medium') > 0 THEN 'medium'
      ELSE 'high'
    END,
    'coord_validation',
    jsonb_build_object(
      'supporting_signal_count', COALESCE(MAX(supporting_signal_count), 0),
      'conflict_signal_count', COALESCE(MAX(conflict_signal_count), 0),
      'max_signal_distance_meters', COALESCE(MAX(max_signal_distance_meters), 0),
      'validated_against', to_jsonb(ARRAY_REMOVE(ARRAY[
        CASE WHEN COUNT(*) FILTER (WHERE parent_geom IS NOT NULL) > 0 THEN 'parent_stop_place' END,
        CASE WHEN COUNT(*) FILTER (WHERE stop_point_geom IS NOT NULL) > 0 THEN 'child_stop_points' END,
        CASE WHEN COUNT(*) FILTER (WHERE sibling_geom IS NOT NULL) > 0 THEN 'sibling_stop_places' END,
        CASE WHEN COUNT(*) FILTER (WHERE topographic_geom IS NOT NULL) > 0 THEN 'topographic_place_cluster' END
      ], NULL)),
      'warning_codes', to_jsonb(ARRAY_REMOVE(ARRAY[
        CASE WHEN COUNT(*) FILTER (WHERE self_geom IS NULL) > 0 THEN 'missing_self_geom' END,
        CASE WHEN COUNT(*) FILTER (WHERE conflict_signal_count > 0) > 0 THEN 'signal_conflict_gt_1000m' END,
        CASE WHEN COUNT(*) FILTER (WHERE supporting_signal_count = 0) > 0 THEN 'no_supporting_signal' END
      ], NULL))
    ),
    'coord_inputs',
    jsonb_build_object(
      'provider_stop_place_refs',
      to_jsonb(ARRAY_AGG(provider_stop_place_ref ORDER BY provider_stop_place_ref))
    ),
    'hierarchy_role',
    CASE
      WHEN COUNT(*) FILTER (WHERE hierarchy_role = 'child') > 0 THEN 'child'
      WHEN COUNT(*) FILTER (WHERE hierarchy_role = 'parent') > 0 THEN 'parent'
      ELSE 'standalone'
    END,
    'parent_stop_place_ref',
    MIN(parent_stop_place_ref),
    'topographic_place_ref',
    MIN(topographic_place_ref)
  ) AS metadata
FROM _soft_geo_clustered
GROUP BY normalized_name, cluster_id;

CREATE TEMP TABLE _soft_name_only_groups AS
SELECT
  'gstn_' || substr(md5('name|' || normalized_name), 1, 24) AS global_station_id,
  normalized_name,
  MIN(stop_name) AS display_name,
  MIN(country) AS country,
  AVG(resolved_latitude) FILTER (WHERE resolved_latitude IS NOT NULL) AS latitude,
  AVG(resolved_longitude) FILTER (WHERE resolved_longitude IS NOT NULL) AS longitude,
  CASE
    WHEN COUNT(*) FILTER (WHERE resolved_geom IS NOT NULL) > 0 THEN ST_SetSRID(ST_Centroid(ST_Collect(resolved_geom)), 4326)
    ELSE NULL::geometry(Point, 4326)
  END AS geom,
  jsonb_build_object(
    'coord_source',
    CASE
      WHEN COUNT(*) FILTER (WHERE coord_source = 'parent_stop_place') > 0 THEN 'parent_stop_place'
      WHEN COUNT(*) FILTER (WHERE coord_source = 'child_stop_points') > 0 THEN 'child_stop_points'
      WHEN COUNT(*) FILTER (WHERE coord_source = 'sibling_stop_places') > 0 THEN 'sibling_stop_places'
      WHEN COUNT(*) FILTER (WHERE coord_source = 'topographic_place_cluster') > 0 THEN 'topographic_place_cluster'
      ELSE 'missing'
    END,
    'coord_confidence',
    CASE
      WHEN COUNT(*) FILTER (WHERE resolved_geom IS NOT NULL) = 0 THEN 'unresolved'
      WHEN COUNT(*) FILTER (WHERE coord_confidence = 'low') > 0 THEN 'low'
      WHEN COUNT(*) FILTER (WHERE coord_confidence = 'medium') > 0 THEN 'medium'
      ELSE 'high'
    END,
    'coord_validation',
    jsonb_build_object(
      'supporting_signal_count', COALESCE(MAX(supporting_signal_count), 0),
      'conflict_signal_count', COALESCE(MAX(conflict_signal_count), 0),
      'max_signal_distance_meters', COALESCE(MAX(max_signal_distance_meters), 0),
      'validated_against', to_jsonb(ARRAY_REMOVE(ARRAY[
        CASE WHEN COUNT(*) FILTER (WHERE parent_geom IS NOT NULL) > 0 THEN 'parent_stop_place' END,
        CASE WHEN COUNT(*) FILTER (WHERE stop_point_geom IS NOT NULL) > 0 THEN 'child_stop_points' END,
        CASE WHEN COUNT(*) FILTER (WHERE sibling_geom IS NOT NULL) > 0 THEN 'sibling_stop_places' END,
        CASE WHEN COUNT(*) FILTER (WHERE topographic_geom IS NOT NULL) > 0 THEN 'topographic_place_cluster' END
      ], NULL)),
      'warning_codes', to_jsonb(ARRAY_REMOVE(ARRAY[
        CASE WHEN COUNT(*) FILTER (WHERE self_geom IS NULL) > 0 THEN 'missing_self_geom' END,
        CASE WHEN COUNT(*) FILTER (WHERE coord_source = 'missing') > 0 THEN 'unresolved_missing_all_sources' END,
        CASE WHEN COUNT(*) FILTER (WHERE conflict_signal_count > 0) > 0 THEN 'signal_conflict_gt_1000m' END
      ], NULL))
    ),
    'coord_inputs',
    jsonb_build_object(
      'provider_stop_place_refs',
      to_jsonb(ARRAY_AGG(provider_stop_place_ref ORDER BY provider_stop_place_ref))
    ),
    'hierarchy_role',
    CASE
      WHEN COUNT(*) FILTER (WHERE hierarchy_role = 'child') > 0 THEN 'child'
      WHEN COUNT(*) FILTER (WHERE hierarchy_role = 'parent') > 0 THEN 'parent'
      ELSE 'standalone'
    END,
    'parent_stop_place_ref',
    MIN(parent_stop_place_ref),
    'topographic_place_ref',
    MIN(topographic_place_ref)
  ) AS metadata
FROM _candidate_coord_resolution
WHERE hard_key IS NULL
GROUP BY normalized_name;

CREATE TEMP TABLE _new_global AS
SELECT
  global_station_id,
  display_name,
  normalized_name,
  country,
  latitude,
  longitude,
  geom,
  metadata
FROM _hard_groups
UNION ALL
SELECT
  global_station_id,
  display_name,
  normalized_name,
  country,
  latitude,
  longitude,
  geom,
  metadata
FROM _soft_geo_groups
UNION ALL
SELECT
  global_station_id,
  display_name,
  normalized_name,
  country,
  latitude,
  longitude,
  geom,
  metadata
FROM _soft_name_only_groups;

DO $$
BEGIN
  RAISE NOTICE 'global_build_phase:writing_global_stations';
  RAISE NOTICE 'global_build_info:new_global_station_rows=%', (SELECT COUNT(*) FROM _new_global);
END $$;

INSERT INTO global_stations (
  global_station_id,
  display_name,
  normalized_name,
  country,
  latitude,
  longitude,
  geom,
  station_kind,
  confidence_score,
  metadata,
  updated_at
)
SELECT
  global_station_id,
  display_name,
  normalized_name,
  country,
  latitude,
  longitude,
  geom,
  'station',
  1.0,
  metadata,
  now()
FROM _new_global
ON CONFLICT (global_station_id)
DO UPDATE SET
  display_name = EXCLUDED.display_name,
  normalized_name = EXCLUDED.normalized_name,
  country = EXCLUDED.country,
  latitude = EXCLUDED.latitude,
  longitude = EXCLUDED.longitude,
  geom = EXCLUDED.geom,
  metadata = EXCLUDED.metadata,
  is_active = true,
  updated_at = now();

CREATE TEMP TABLE _assign_hard AS
SELECT
  c.source_id,
  c.provider_stop_place_ref,
  h.global_station_id,
  'hard_id'::text AS mapping_method
FROM _candidate_coord_resolution c
JOIN _hard_groups h
  ON h.hard_key = c.hard_key
WHERE c.hard_key IS NOT NULL;

CREATE TEMP TABLE _assign_geo AS
SELECT
  c.source_id,
  c.provider_stop_place_ref,
  g.global_station_id,
  'name_geo'::text AS mapping_method
FROM _soft_geo_clustered c
JOIN _soft_geo_groups g
  ON g.normalized_name = c.normalized_name
 AND g.cluster_id = c.cluster_id;

CREATE TEMP TABLE _assign_name AS
SELECT
  c.source_id,
  c.provider_stop_place_ref,
  n.global_station_id,
  'name_only'::text AS mapping_method
FROM _candidate_coord_resolution c
JOIN _soft_name_only_groups n
  ON n.normalized_name = c.normalized_name
WHERE c.hard_key IS NULL
  AND c.resolved_geom IS NULL;

CREATE TEMP TABLE _assignments AS
SELECT * FROM _assign_hard
UNION ALL
SELECT * FROM _assign_geo
UNION ALL
SELECT * FROM _assign_name;

DO $$
BEGIN
  RAISE NOTICE 'global_build_phase:writing_station_mappings';
  RAISE NOTICE 'global_build_info:station_mapping_rows=%', (SELECT COUNT(*) FROM _assignments);
END $$;

UPDATE provider_global_station_mappings m
SET
  is_active = false,
  valid_to = COALESCE(valid_to, CURRENT_DATE),
  updated_at = now()
WHERE m.is_active = true
  AND EXISTS (
    SELECT 1
    FROM _candidate_coord_resolution c
    WHERE c.source_id = m.source_id
      AND c.provider_stop_place_ref = m.provider_stop_place_ref
  );

INSERT INTO provider_global_station_mappings (
  source_id,
  provider_stop_place_ref,
  global_station_id,
  confidence_score,
  mapping_method,
  is_active,
  valid_from,
  metadata,
  created_at,
  updated_at
)
SELECT
  a.source_id,
  a.provider_stop_place_ref,
  a.global_station_id,
  1.0,
  a.mapping_method,
  true,
  CURRENT_DATE,
  '{}'::jsonb,
  now(),
  now()
FROM _assignments a;

CREATE TEMP TABLE _candidate_stop_points AS
SELECT
  latest.source_id,
  latest.provider_stop_point_ref,
  latest.provider_stop_place_ref,
  latest.stop_name,
  latest.normalized_name,
  latest.country,
  COALESCE(latest.latitude, gs.latitude) AS latitude,
  COALESCE(latest.longitude, gs.longitude) AS longitude,
  COALESCE(latest.geom, gs.geom) AS geom,
  latest.topographic_place_ref,
  m.global_station_id,
  CASE
    WHEN latest.geom IS NOT NULL THEN 'self'
    WHEN gs.metadata ->> 'coord_source' <> '' THEN gs.metadata ->> 'coord_source'
    ELSE 'station_fallback'
  END AS coord_source
FROM _candidate_stop_points_latest latest
JOIN provider_global_station_mappings m
  ON m.source_id = latest.source_id
 AND m.provider_stop_place_ref = latest.provider_stop_place_ref
 AND m.is_active = true
JOIN global_stations gs
  ON gs.global_station_id = m.global_station_id
 AND gs.is_active = true;

CREATE INDEX _candidate_stop_points_source_point_idx
  ON _candidate_stop_points (source_id, provider_stop_point_ref);

CREATE INDEX _candidate_stop_points_station_idx
  ON _candidate_stop_points (global_station_id);

DO $$
BEGIN
  RAISE NOTICE 'global_build_phase:writing_global_stop_points';
  RAISE NOTICE 'global_build_info:candidate_stop_point_rows=%', (SELECT COUNT(*) FROM _candidate_stop_points);
  RAISE NOTICE 'global_build_info:candidate_stop_point_index_count=2';
END $$;

UPDATE global_stop_points sp
SET
  is_active = false,
  updated_at = now()
WHERE sp.is_active = true
  AND sp.metadata ? 'source_id'
  AND sp.metadata ? 'provider_stop_point_ref'
  AND EXISTS (
    SELECT 1
    FROM _candidate_stop_points c
    WHERE c.source_id = sp.metadata ->> 'source_id'
  )
  AND NOT EXISTS (
    SELECT 1
    FROM _candidate_stop_points c
    WHERE c.source_id = sp.metadata ->> 'source_id'
      AND c.provider_stop_point_ref = sp.metadata ->> 'provider_stop_point_ref'
  )
  AND (
    NULLIF(:'source_id_scope', '') IS NULL
    OR sp.metadata ->> 'source_id' = NULLIF(:'source_id_scope', '')
  )
  AND (
    NULLIF(:'country_filter', '') IS NULL
    OR sp.country = NULLIF(:'country_filter', '')::char(2)
  );

INSERT INTO global_stop_points (
  global_stop_point_id,
  global_station_id,
  display_name,
  normalized_name,
  country,
  latitude,
  longitude,
  geom,
  stop_point_kind,
  metadata,
  updated_at
)
SELECT
  'gsp_' || substr(md5(rpp.source_id || '|' || rpp.provider_stop_point_ref), 1, 24) AS global_stop_point_id,
  rpp.global_station_id,
  rpp.stop_name,
  rpp.normalized_name,
  rpp.country,
  rpp.latitude,
  rpp.longitude,
  rpp.geom,
  'platform',
  jsonb_build_object(
    'source_id',
    rpp.source_id,
    'provider_stop_place_ref',
    rpp.provider_stop_place_ref,
    'provider_stop_point_ref',
    rpp.provider_stop_point_ref,
    'coord_source',
    rpp.coord_source,
    'topographic_place_ref',
    rpp.topographic_place_ref
  ),
  now()
FROM _candidate_stop_points rpp
ON CONFLICT (global_stop_point_id)
DO UPDATE SET
  global_station_id = EXCLUDED.global_station_id,
  display_name = EXCLUDED.display_name,
  normalized_name = EXCLUDED.normalized_name,
  country = EXCLUDED.country,
  latitude = EXCLUDED.latitude,
  longitude = EXCLUDED.longitude,
  geom = EXCLUDED.geom,
  metadata = EXCLUDED.metadata,
  is_active = true,
  updated_at = now();

DO $$
BEGIN
  RAISE NOTICE 'global_build_info:active_global_stop_point_rows=%', (
    SELECT COUNT(*)
    FROM global_stop_points sp
    WHERE sp.is_active = true
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR sp.country = NULLIF(:'country_filter', '')::char(2)
      )
      AND (
        NULLIF(:'source_id_scope', '') IS NULL
        OR sp.metadata ->> 'source_id' = NULLIF(:'source_id_scope', '')
      )
  );
END $$;

DO $$
BEGIN
  RAISE NOTICE 'global_build_phase:writing_stop_point_mappings';
END $$;

UPDATE provider_global_stop_point_mappings m
SET
  is_active = false,
  valid_to = COALESCE(valid_to, CURRENT_DATE),
  updated_at = now()
WHERE m.is_active = true
  AND EXISTS (
    SELECT 1
    FROM _candidate_stop_points c
    WHERE c.source_id = m.source_id
      AND c.provider_stop_point_ref = m.provider_stop_point_ref
  );

INSERT INTO provider_global_stop_point_mappings (
  source_id,
  provider_stop_point_ref,
  global_stop_point_id,
  confidence_score,
  mapping_method,
  is_active,
  valid_from,
  metadata,
  created_at,
  updated_at
)
SELECT
  c.source_id,
  c.provider_stop_point_ref,
  sp.global_stop_point_id,
  1.0,
  'provider_stop_point',
  true,
  CURRENT_DATE,
  '{}'::jsonb,
  now(),
  now()
FROM _candidate_stop_points c
JOIN global_stop_points sp
  ON sp.global_stop_point_id =
    'gsp_' || substr(md5(c.source_id || '|' || c.provider_stop_point_ref), 1, 24);

DO $$
BEGIN
  RAISE NOTICE 'global_build_info:stop_point_mapping_rows=%', (
    SELECT COUNT(*)
    FROM provider_global_stop_point_mappings m
    JOIN global_stop_points sp
      ON sp.global_stop_point_id = m.global_stop_point_id
    WHERE m.is_active = true
      AND sp.is_active = true
      AND (
        NULLIF(:'country_filter', '') IS NULL
        OR sp.country = NULLIF(:'country_filter', '')::char(2)
      )
      AND (
        NULLIF(:'source_id_scope', '') IS NULL
        OR m.source_id = NULLIF(:'source_id_scope', '')
      )
  );
END $$;

CREATE TEMP TABLE _cleanup_station_candidates AS
SELECT DISTINCT gs.global_station_id
FROM global_stations gs
WHERE gs.is_active = true
  AND (
    NULLIF(:'country_filter', '') IS NULL
    OR gs.country = NULLIF(:'country_filter', '')::char(2)
  )
  AND (
    NULLIF(:'source_id_scope', '') IS NULL
    OR EXISTS (
      SELECT 1
      FROM provider_global_station_mappings m
      WHERE m.global_station_id = gs.global_station_id
        AND m.source_id = NULLIF(:'source_id_scope', '')
    )
    OR EXISTS (
      SELECT 1
      FROM global_stop_points sp
      WHERE sp.global_station_id = gs.global_station_id
        AND sp.metadata ? 'source_id'
        AND sp.metadata ->> 'source_id' = NULLIF(:'source_id_scope', '')
    )
  );

UPDATE global_stations gs
SET
  is_active = false,
  metadata = gs.metadata || jsonb_build_object(
    'deactivated_by',
    'global_station_build',
    'deactivation_reason',
    'orphaned_without_active_source_or_stop_points',
    'deactivated_at',
    now()
  ),
  updated_at = now()
WHERE gs.global_station_id IN (
    SELECT candidate.global_station_id
    FROM _cleanup_station_candidates candidate
  )
  AND NOT EXISTS (
    SELECT 1
    FROM provider_global_station_mappings m
    WHERE m.global_station_id = gs.global_station_id
      AND m.is_active = true
  )
  AND NOT EXISTS (
    SELECT 1
    FROM global_stop_points sp
    WHERE sp.global_station_id = gs.global_station_id
      AND sp.is_active = true
  );

DO $$
BEGIN
  RAISE NOTICE 'global_build_phase:writing_transfer_edges';
END $$;

CREATE TEMP TABLE _transfer_scope_station_ids AS
SELECT DISTINCT c.global_station_id
FROM _candidate_stop_points c;

CREATE INDEX _transfer_scope_station_ids_station_idx
  ON _transfer_scope_station_ids (global_station_id);

CREATE TEMP TABLE _transfer_scope_stop_points AS
SELECT
  sp.global_stop_point_id,
  sp.global_station_id,
  sp.geom
FROM global_stop_points sp
JOIN _transfer_scope_station_ids scope_station
  ON scope_station.global_station_id = sp.global_station_id
WHERE sp.is_active = true;

CREATE INDEX _transfer_scope_stop_points_station_idx
  ON _transfer_scope_stop_points (global_station_id, global_stop_point_id);

ANALYZE _transfer_scope_station_ids;
ANALYZE _transfer_scope_stop_points;

DO $$
BEGIN
  RAISE NOTICE 'global_build_info:transfer_scope_station_count=%', (
    SELECT COUNT(*) FROM _transfer_scope_station_ids
  );
  RAISE NOTICE 'global_build_info:transfer_scope_stop_point_rows=%', (
    SELECT COUNT(*) FROM _transfer_scope_stop_points
  );
END $$;

DELETE FROM transfer_edges te
WHERE te.metadata ->> 'generated_by' = 'global_station_build'
  AND te.metadata ->> 'global_station_id' IN (
    SELECT global_station_id
    FROM _transfer_scope_station_ids
  );

INSERT INTO transfer_edges (
  from_global_stop_point_id,
  to_global_stop_point_id,
  min_transfer_seconds,
  transfer_type,
  is_bidirectional,
  metadata,
  updated_at
)
SELECT
  a.global_stop_point_id,
  b.global_stop_point_id,
  CASE
    WHEN a.geom IS NOT NULL AND b.geom IS NOT NULL THEN
      GREATEST(
        30,
        LEAST(
          900,
          ROUND(ST_DistanceSphere(a.geom, b.geom) / 1.25)::integer
        )
      )
    ELSE 120
  END AS min_transfer_seconds,
  2,
  false,
  jsonb_build_object(
    'generated_by', 'global_station_build',
    'global_station_id', a.global_station_id
  ),
  now()
FROM _transfer_scope_stop_points a
JOIN _transfer_scope_stop_points b
  ON b.global_station_id = a.global_station_id
 AND b.global_stop_point_id <> a.global_stop_point_id
;

DO $$
BEGIN
  RAISE NOTICE 'global_build_info:transfer_edge_rows=%', (
    SELECT COUNT(*)
    FROM transfer_edges te
  );
END $$;

DO $$
BEGIN
  RAISE NOTICE 'global_build_phase:finalizing';
END $$;

COMMIT;

SELECT json_build_object(
  'sourceRows', (SELECT COUNT(*) FROM _candidate),
  'globalStations', (SELECT COUNT(*) FROM _new_global),
  'stationMappings', (SELECT COUNT(*) FROM _assignments),
  'globalStopPoints', (
    SELECT COUNT(*)
    FROM global_stop_points sp
    WHERE sp.is_active = true
  ),
  'stopPointMappings', (
    SELECT COUNT(*)
    FROM provider_global_stop_point_mappings m
    WHERE m.is_active = true
  ),
  'mappedTripStopTimes', (
    SELECT COUNT(*)
    FROM timetable_trip_stop_times tts
    WHERE tts.global_stop_point_id IS NOT NULL
  ),
  'transferEdges', (
    SELECT COUNT(*)
    FROM transfer_edges te
  ),
  'coordSourceSelf', (
    SELECT COUNT(*) FROM _candidate_coord_resolution WHERE coord_source = 'self'
  ),
  'coordSourceParentStopPlace', (
    SELECT COUNT(*) FROM _candidate_coord_resolution WHERE coord_source = 'parent_stop_place'
  ),
  'coordSourceChildStopPoints', (
    SELECT COUNT(*) FROM _candidate_coord_resolution WHERE coord_source = 'child_stop_points'
  ),
  'coordSourceSiblingStopPlaces', (
    SELECT COUNT(*) FROM _candidate_coord_resolution WHERE coord_source = 'sibling_stop_places'
  ),
  'coordSourceTopographicPlaceCluster', (
    SELECT COUNT(*) FROM _candidate_coord_resolution WHERE coord_source = 'topographic_place_cluster'
  ),
  'coordSourceMissing', (
    SELECT COUNT(*) FROM _candidate_coord_resolution WHERE coord_source = 'missing'
  ),
  'coordConfidenceHigh', (
    SELECT COUNT(*) FROM _candidate_coord_resolution WHERE coord_confidence = 'high'
  ),
  'coordConfidenceMedium', (
    SELECT COUNT(*) FROM _candidate_coord_resolution WHERE coord_confidence = 'medium'
  ),
  'coordConfidenceLow', (
    SELECT COUNT(*) FROM _candidate_coord_resolution WHERE coord_confidence = 'low'
  ),
  'coordConfidenceUnresolved', (
    SELECT COUNT(*) FROM _candidate_coord_resolution WHERE coord_confidence = 'unresolved'
  ),
  'coordConflictCount', (
    SELECT COUNT(*) FROM _candidate_coord_resolution WHERE conflict_signal_count > 0
  ),
  'countryFilter', COALESCE(NULLIF(:'country_filter', ''), ''),
  'asOf', COALESCE(NULLIF(:'as_of', ''), ''),
  'sourceScope', COALESCE(NULLIF(:'source_id_scope', ''), '')
)::text;
`;

function createGlobalStationsRepo(client) {
  return {
    async buildGlobalStations(scope, options = {}) {
      const onPhase =
        typeof options.onPhase === "function" ? options.onPhase : null;
      const onInfo =
        typeof options.onInfo === "function" ? options.onInfo : null;
      const result = await client.runScript(
        BUILD_GLOBAL_SQL,
        scopeParams(scope),
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

      return parseSummaryLine(
        extractJsonLine(result.stdout),
        "GLOBAL_BUILD_FAILED",
        "Global station build",
      );
    },

    async getBuildFingerprint(scope = {}) {
      const row = await client.queryOne(
        GLOBAL_BUILD_FINGERPRINT_SQL,
        scopeParams(scope),
      );
      return row?.fingerprint || null;
    },

    async getCurrentSummary(scope = {}) {
      const row = await client.queryOne(
        GLOBAL_BUILD_CURRENT_SUMMARY_SQL,
        scopeParams(scope),
      );
      return parseSummaryLine(
        String(row?.summary_json || ""),
        "GLOBAL_BUILD_FAILED",
        "Global station build current summary",
      );
    },

    getTuningConfig() {
      return {
        maxParallelWorkersPerGather: GLOBAL_BUILD_MAX_PARALLEL_WORKERS,
        workMem: GLOBAL_BUILD_WORK_MEM,
        maintenanceWorkMem: GLOBAL_BUILD_MAINTENANCE_WORK_MEM,
        synchronousCommit: GLOBAL_BUILD_SYNCHRONOUS_COMMIT,
      };
    },

    async sampleBuildQueryPlans(scope = {}) {
      const params = scopeParams(scope);
      const queries = [
        {
          name: "latest_stop_places",
          sql: `
            EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
            SELECT
              rp.source_id,
              rp.provider_stop_place_ref
            FROM raw_provider_stop_places rp
            JOIN provider_datasets pd
              ON pd.dataset_id = rp.dataset_id
            WHERE (NULLIF(:'country_filter', '') IS NULL OR rp.country = NULLIF(:'country_filter', '')::char(2))
              AND (NULLIF(:'source_id_scope', '') IS NULL OR rp.source_id = NULLIF(:'source_id_scope', ''))
              AND (NULLIF(:'as_of', '') IS NULL OR pd.snapshot_date <= NULLIF(:'as_of', '')::date)
            ORDER BY pd.snapshot_date DESC, rp.dataset_id DESC, rp.updated_at DESC, rp.stop_place_id DESC
            LIMIT 5000;
          `,
        },
        {
          name: "latest_stop_points",
          sql: `
            EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
            SELECT
              rpp.source_id,
              rpp.provider_stop_point_ref
            FROM raw_provider_stop_points rpp
            JOIN provider_datasets pd
              ON pd.dataset_id = rpp.dataset_id
            WHERE (NULLIF(:'country_filter', '') IS NULL OR rpp.country = NULLIF(:'country_filter', '')::char(2))
              AND (NULLIF(:'source_id_scope', '') IS NULL OR rpp.source_id = NULLIF(:'source_id_scope', ''))
              AND (NULLIF(:'as_of', '') IS NULL OR pd.snapshot_date <= NULLIF(:'as_of', '')::date)
            ORDER BY pd.snapshot_date DESC, rpp.dataset_id DESC, rpp.updated_at DESC, rpp.stop_point_id DESC
            LIMIT 5000;
          `,
        },
        {
          name: "transfer_edge_pairs",
          sql: `
            EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
            WITH transfer_scope_station_ids AS (
              SELECT DISTINCT sp.global_station_id
              FROM global_stop_points sp
              WHERE sp.is_active = true
                AND (
                  NULLIF(:'country_filter', '') IS NULL
                  OR sp.country = NULLIF(:'country_filter', '')::char(2)
                )
                AND (
                  NULLIF(:'source_id_scope', '') IS NULL
                  OR sp.metadata ->> 'source_id' = NULLIF(:'source_id_scope', '')
                )
            ),
            transfer_scope_stop_points AS (
              SELECT
                sp.global_stop_point_id,
                sp.global_station_id,
                sp.geom
              FROM global_stop_points sp
              JOIN transfer_scope_station_ids scope_station
                ON scope_station.global_station_id = sp.global_station_id
              WHERE sp.is_active = true
            )
            SELECT COUNT(*)
            FROM transfer_scope_stop_points a
            JOIN transfer_scope_stop_points b
              ON b.global_station_id = a.global_station_id
             AND b.global_stop_point_id <> a.global_stop_point_id;
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

    async listCoordinateAlerts(scope = {}, options = {}) {
      const limit = Number.isInteger(options.limit) ? options.limit : 50;
      return client.queryRows(
        `
        SELECT
          gs.global_station_id,
          gs.display_name,
          gs.country,
          gs.metadata ->> 'coord_source' AS coord_source,
          gs.metadata ->> 'coord_confidence' AS coord_confidence,
          gs.metadata -> 'coord_validation' -> 'warning_codes' AS warning_codes
        FROM global_stations gs
        WHERE gs.is_active = true
          AND (
            NULLIF(:'country_filter', '') IS NULL
            OR gs.country = NULLIF(:'country_filter', '')::char(2)
          )
          AND (
            NULLIF(:'source_id_scope', '') IS NULL
            OR EXISTS (
              SELECT 1
              FROM provider_global_station_mappings m
              WHERE m.global_station_id = gs.global_station_id
                AND m.source_id = NULLIF(:'source_id_scope', '')
                AND m.is_active = true
            )
          )
          AND (
            COALESCE(gs.metadata ->> 'coord_confidence', '') IN ('low', 'unresolved')
            OR COALESCE((gs.metadata -> 'coord_validation' ->> 'conflict_signal_count')::integer, 0) > 0
          )
        ORDER BY
          CASE gs.metadata ->> 'coord_confidence'
            WHEN 'unresolved' THEN 0
            WHEN 'low' THEN 1
            ELSE 2
          END,
          COALESCE((gs.metadata -> 'coord_validation' ->> 'conflict_signal_count')::integer, 0) DESC,
          gs.display_name ASC,
          gs.global_station_id ASC
        LIMIT :limit
        `,
        {
          country_filter: scope.country || "",
          source_id_scope: scope.sourceId || "",
          limit,
        },
      );
    },
  };
}

module.exports = {
  BUILD_GLOBAL_SQL,
  createGlobalStationsRepo,
  extractInfoFromNotice,
  extractPhaseFromNotice,
};
