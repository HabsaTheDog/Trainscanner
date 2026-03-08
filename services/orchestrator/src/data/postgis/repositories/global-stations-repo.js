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

const BUILD_GLOBAL_SQL = `
BEGIN;

CREATE TEMP TABLE _candidate AS
SELECT
  rp.source_id,
  rp.provider_stop_place_ref,
  rp.country,
  rp.stop_name,
  rp.normalized_name,
  rp.latitude,
  rp.longitude,
  rp.geom,
  NULLIF(rp.hard_id, '') AS hard_key
FROM raw_provider_stop_places rp
JOIN provider_datasets pd
  ON pd.dataset_id = rp.dataset_id
WHERE (NULLIF(:'country_filter', '') IS NULL OR rp.country = NULLIF(:'country_filter', '')::char(2))
  AND (NULLIF(:'source_id_scope', '') IS NULL OR rp.source_id = NULLIF(:'source_id_scope', ''))
  AND (NULLIF(:'as_of', '') IS NULL OR pd.snapshot_date <= NULLIF(:'as_of', '')::date);

DO $$
BEGIN
  IF (SELECT COUNT(*) FROM _candidate) = 0 THEN
    RAISE EXCEPTION 'No raw provider stop places found for selected scope';
  END IF;
END $$;

CREATE INDEX _candidate_source_place_idx
  ON _candidate (source_id, provider_stop_place_ref);

CREATE INDEX _candidate_hard_key_idx
  ON _candidate (hard_key)
  WHERE hard_key IS NOT NULL;

CREATE INDEX _candidate_normalized_name_idx
  ON _candidate (normalized_name);

CREATE TEMP TABLE _hard_groups AS
SELECT
  'gstn_' || substr(md5('hard|' || hard_key), 1, 24) AS global_station_id,
  hard_key,
  MIN(stop_name) AS display_name,
  MIN(normalized_name) AS normalized_name,
  MIN(country) AS country,
  AVG(latitude) FILTER (WHERE latitude IS NOT NULL) AS latitude,
  AVG(longitude) FILTER (WHERE longitude IS NOT NULL) AS longitude,
  CASE
    WHEN COUNT(*) FILTER (WHERE geom IS NOT NULL) > 0 THEN ST_SetSRID(ST_Centroid(ST_Collect(geom)), 4326)
    ELSE NULL
  END AS geom
FROM _candidate
WHERE hard_key IS NOT NULL
GROUP BY hard_key;

CREATE TEMP TABLE _soft_geo_clustered AS
SELECT
  c.*,
  ST_ClusterDBSCAN(ST_Transform(c.geom, 3857), eps := 500, minpoints := 1)
    OVER (PARTITION BY c.normalized_name ORDER BY c.source_id, c.provider_stop_place_ref) AS cluster_id
FROM _candidate c
WHERE c.hard_key IS NULL
  AND c.geom IS NOT NULL;

CREATE TEMP TABLE _soft_geo_groups AS
SELECT
  'gstn_' || substr(md5('geo|' || normalized_name || '|' || cluster_id::text), 1, 24) AS global_station_id,
  normalized_name,
  cluster_id,
  MIN(stop_name) AS display_name,
  MIN(country) AS country,
  AVG(latitude) AS latitude,
  AVG(longitude) AS longitude,
  ST_SetSRID(ST_Centroid(ST_Collect(geom)), 4326) AS geom
FROM _soft_geo_clustered
GROUP BY normalized_name, cluster_id;

CREATE TEMP TABLE _soft_name_only_groups AS
SELECT
  'gstn_' || substr(md5('name|' || normalized_name), 1, 24) AS global_station_id,
  normalized_name,
  MIN(stop_name) AS display_name,
  MIN(country) AS country,
  NULL::double precision AS latitude,
  NULL::double precision AS longitude,
  NULL::geometry(Point, 4326) AS geom
FROM _candidate
WHERE hard_key IS NULL
  AND geom IS NULL
GROUP BY normalized_name;

CREATE TEMP TABLE _new_global AS
SELECT
  global_station_id,
  display_name,
  normalized_name,
  country,
  latitude,
  longitude,
  geom
FROM _hard_groups
UNION ALL
SELECT
  global_station_id,
  display_name,
  normalized_name,
  country,
  latitude,
  longitude,
  geom
FROM _soft_geo_groups
UNION ALL
SELECT
  global_station_id,
  display_name,
  normalized_name,
  country,
  latitude,
  longitude,
  geom
FROM _soft_name_only_groups;

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
  '{}'::jsonb,
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
  is_active = true,
  updated_at = now();

CREATE TEMP TABLE _assign_hard AS
SELECT
  c.source_id,
  c.provider_stop_place_ref,
  h.global_station_id,
  'hard_id'::text AS mapping_method
FROM _candidate c
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
FROM _candidate c
JOIN _soft_name_only_groups n
  ON n.normalized_name = c.normalized_name
WHERE c.hard_key IS NULL
  AND c.geom IS NULL;

CREATE TEMP TABLE _assignments AS
SELECT * FROM _assign_hard
UNION ALL
SELECT * FROM _assign_geo
UNION ALL
SELECT * FROM _assign_name;

UPDATE provider_global_station_mappings m
SET
  is_active = false,
  valid_to = COALESCE(valid_to, CURRENT_DATE),
  updated_at = now()
WHERE m.is_active = true
  AND EXISTS (
    SELECT 1
    FROM _candidate c
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
  rpp.source_id,
  rpp.provider_stop_point_ref,
  rpp.provider_stop_place_ref,
  rpp.stop_name,
  rpp.normalized_name,
  rpp.country,
  COALESCE(rpp.latitude, gs.latitude) AS latitude,
  COALESCE(rpp.longitude, gs.longitude) AS longitude,
  COALESCE(rpp.geom, gs.geom) AS geom,
  m.global_station_id
FROM raw_provider_stop_points rpp
JOIN provider_datasets pd
  ON pd.dataset_id = rpp.dataset_id
JOIN provider_global_station_mappings m
  ON m.source_id = rpp.source_id
 AND m.provider_stop_place_ref = rpp.provider_stop_place_ref
 AND m.is_active = true
JOIN global_stations gs
  ON gs.global_station_id = m.global_station_id
 AND gs.is_active = true
WHERE (NULLIF(:'country_filter', '') IS NULL OR rpp.country = NULLIF(:'country_filter', '')::char(2))
  AND (NULLIF(:'source_id_scope', '') IS NULL OR rpp.source_id = NULLIF(:'source_id_scope', ''))
  AND (NULLIF(:'as_of', '') IS NULL OR pd.snapshot_date <= NULLIF(:'as_of', '')::date);

CREATE INDEX _candidate_stop_points_source_point_idx
  ON _candidate_stop_points (source_id, provider_stop_point_ref);

CREATE INDEX _candidate_stop_points_station_idx
  ON _candidate_stop_points (global_station_id);

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
    rpp.provider_stop_point_ref
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

DELETE FROM transfer_edges te
USING global_stop_points fsp, global_stop_points tsp
WHERE te.from_global_stop_point_id = fsp.global_stop_point_id
  AND te.to_global_stop_point_id = tsp.global_stop_point_id
  AND te.metadata ->> 'generated_by' = 'global_station_build'
  AND (
    NULLIF(:'country_filter', '') IS NULL
    OR fsp.country = NULLIF(:'country_filter', '')::char(2)
    OR tsp.country = NULLIF(:'country_filter', '')::char(2)
  )
  AND (
    NULLIF(:'source_id_scope', '') IS NULL
    OR fsp.metadata ->> 'source_id' = NULLIF(:'source_id_scope', '')
    OR tsp.metadata ->> 'source_id' = NULLIF(:'source_id_scope', '')
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
FROM global_stop_points a
JOIN global_stop_points b
  ON b.global_station_id = a.global_station_id
 AND b.global_stop_point_id <> a.global_stop_point_id
WHERE a.is_active = true
  AND b.is_active = true
  AND (
    NULLIF(:'country_filter', '') IS NULL
    OR a.country = NULLIF(:'country_filter', '')::char(2)
    OR b.country = NULLIF(:'country_filter', '')::char(2)
  )
  AND (
    NULLIF(:'source_id_scope', '') IS NULL
    OR a.metadata ->> 'source_id' = NULLIF(:'source_id_scope', '')
    OR b.metadata ->> 'source_id' = NULLIF(:'source_id_scope', '')
  )
ON CONFLICT (from_global_stop_point_id, to_global_stop_point_id)
DO UPDATE SET
  min_transfer_seconds = EXCLUDED.min_transfer_seconds,
  transfer_type = EXCLUDED.transfer_type,
  is_bidirectional = EXCLUDED.is_bidirectional,
  metadata = EXCLUDED.metadata,
  updated_at = now();

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
  'countryFilter', COALESCE(NULLIF(:'country_filter', ''), ''),
  'asOf', COALESCE(NULLIF(:'as_of', ''), ''),
  'sourceScope', COALESCE(NULLIF(:'source_id_scope', ''), '')
)::text;
`;

function createGlobalStationsRepo(client) {
  return {
    async buildGlobalStations(scope) {
      const result = await client.runScript(BUILD_GLOBAL_SQL, {
        country_filter: scope.country || "",
        as_of: scope.asOf || "",
        source_id_scope: scope.sourceId || "",
      });

      const line = extractJsonLine(result.stdout);
      if (!line) {
        throw new AppError({
          code: "GLOBAL_BUILD_FAILED",
          message: "Global station build did not return summary JSON",
        });
      }

      let parsed;
      try {
        parsed = JSON.parse(line);
      } catch (err) {
        throw new AppError({
          code: "GLOBAL_BUILD_FAILED",
          message: "Global station build returned invalid summary JSON",
          cause: err,
        });
      }

      validateOrThrow(parsed, GLOBAL_BUILD_SUMMARY_SCHEMA, {
        code: "GLOBAL_BUILD_FAILED",
        message: "Global station build summary failed schema validation",
      });

      return parsed;
    },
  };
}

module.exports = {
  BUILD_GLOBAL_SQL,
  createGlobalStationsRepo,
};
