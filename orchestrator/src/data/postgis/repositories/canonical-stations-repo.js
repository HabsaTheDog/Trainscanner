const { AppError } = require("../../../core/errors");
const { validateOrThrow } = require("../../../core/schema");

const CANONICAL_SUMMARY_SCHEMA = {
  type: "object",
  required: [
    "sourceRows",
    "canonicalRows",
    "inserted",
    "updated",
    "merged",
    "conflicts",
  ],
  properties: {
    sourceRows: { type: "integer", minimum: 0 },
    canonicalRows: { type: "integer", minimum: 0 },
    inserted: { type: "integer", minimum: 0 },
    updated: { type: "integer", minimum: 0 },
    merged: { type: "integer", minimum: 0 },
    conflicts: { type: "integer", minimum: 0 },
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

const BUILD_CANONICAL_SQL = `
BEGIN;

CREATE TEMP TABLE _selected_snapshots AS
SELECT rs.source_id, rs.country, MAX(rs.snapshot_date) AS snapshot_date
FROM raw_snapshots rs
WHERE rs.format = 'netex'
  AND (NULLIF(:'country_filter', '') IS NULL OR rs.country = :'country_filter')
  AND (NULLIF(:'source_id_scope', '') IS NULL OR rs.source_id = :'source_id_scope')
  AND (NULLIF(:'as_of', '') IS NULL OR rs.snapshot_date <= NULLIF(:'as_of', '')::date)
GROUP BY rs.source_id, rs.country;

DO $$
BEGIN
  IF (SELECT COUNT(*) FROM _selected_snapshots) = 0 THEN
    RAISE EXCEPTION 'No raw snapshots matched selected filters';
  END IF;
END $$;

CREATE TEMP TABLE _candidate AS
SELECT
  s.source_id,
  s.country,
  s.snapshot_date,
  s.source_stop_id,
  s.stop_name,
  s.normalized_name,
  s.latitude,
  s.longitude,
  s.geom,
  COALESCE(NULLIF(s.hard_id, ''), NULLIF(s.public_code, ''), NULLIF(s.private_code, '')) AS hard_key
FROM netex_stops_staging s
JOIN _selected_snapshots ss
  ON ss.source_id = s.source_id AND ss.snapshot_date = s.snapshot_date
WHERE s.stop_name IS NOT NULL
  AND btrim(s.stop_name) <> '';

DO $$
BEGIN
  IF (SELECT COUNT(*) FROM _candidate) = 0 THEN
    RAISE EXCEPTION 'No staging rows found for selected snapshots';
  END IF;
END $$;

CREATE TEMP TABLE _hard_groups AS
SELECT
  'cstn_' || substr(md5('hard|' || country || '|' || hard_key), 1, 20) AS canonical_station_id,
  country,
  hard_key,
  MIN(stop_name) AS canonical_name,
  MIN(normalized_name) AS normalized_name,
  AVG(latitude) FILTER (WHERE latitude IS NOT NULL) AS latitude,
  AVG(longitude) FILTER (WHERE longitude IS NOT NULL) AS longitude,
  CASE
    WHEN COUNT(*) FILTER (WHERE geom IS NOT NULL) > 0 THEN ST_SetSRID(ST_Centroid(ST_Collect(geom)), 4326)
    ELSE NULL
  END AS geom,
  'hard_id'::text AS match_method,
  COUNT(*)::integer AS member_count,
  MIN(snapshot_date) AS first_seen_snapshot_date,
  MAX(snapshot_date) AS last_seen_snapshot_date
FROM _candidate
WHERE hard_key IS NOT NULL
GROUP BY country, hard_key;

CREATE TEMP TABLE _soft_geo_clustered AS
SELECT
  c.*,
  ST_ClusterDBSCAN(ST_Transform(c.geom, 3857), eps := 250, minpoints := 1)
    OVER (PARTITION BY c.country, c.normalized_name ORDER BY c.source_id, c.source_stop_id) AS cluster_id
FROM _candidate c
WHERE c.hard_key IS NULL
  AND c.geom IS NOT NULL;

CREATE TEMP TABLE _soft_geo_groups AS
SELECT
  'cstn_' || substr(md5('geo|' || country || '|' || normalized_name || '|' || cluster_id::text), 1, 20) AS canonical_station_id,
  country,
  normalized_name,
  cluster_id,
  MIN(stop_name) AS canonical_name,
  AVG(latitude) AS latitude,
  AVG(longitude) AS longitude,
  ST_SetSRID(ST_Centroid(ST_Collect(geom)), 4326) AS geom,
  'name_geo'::text AS match_method,
  COUNT(*)::integer AS member_count,
  MIN(snapshot_date) AS first_seen_snapshot_date,
  MAX(snapshot_date) AS last_seen_snapshot_date
FROM _soft_geo_clustered
GROUP BY country, normalized_name, cluster_id;

CREATE TEMP TABLE _soft_name_only_groups AS
SELECT
  'cstn_' || substr(md5('name|' || country || '|' || normalized_name), 1, 20) AS canonical_station_id,
  country,
  normalized_name,
  MIN(stop_name) AS canonical_name,
  NULL::double precision AS latitude,
  NULL::double precision AS longitude,
  NULL::geometry(Point, 4326) AS geom,
  'name_only'::text AS match_method,
  COUNT(*)::integer AS member_count,
  MIN(snapshot_date) AS first_seen_snapshot_date,
  MAX(snapshot_date) AS last_seen_snapshot_date
FROM _candidate
WHERE hard_key IS NULL
  AND geom IS NULL
GROUP BY country, normalized_name;

CREATE TEMP TABLE _new_canonical AS
SELECT
  canonical_station_id,
  country,
  canonical_name,
  normalized_name,
  latitude,
  longitude,
  geom,
  match_method,
  member_count,
  first_seen_snapshot_date,
  last_seen_snapshot_date
FROM _hard_groups
UNION ALL
SELECT
  canonical_station_id,
  country,
  canonical_name,
  normalized_name,
  latitude,
  longitude,
  geom,
  match_method,
  member_count,
  first_seen_snapshot_date,
  last_seen_snapshot_date
FROM _soft_geo_groups
UNION ALL
SELECT
  canonical_station_id,
  country,
  canonical_name,
  normalized_name,
  latitude,
  longitude,
  geom,
  match_method,
  member_count,
  first_seen_snapshot_date,
  last_seen_snapshot_date
FROM _soft_name_only_groups;

CREATE TEMP TABLE _assign_hard AS
SELECT
  c.source_id,
  c.source_stop_id,
  c.country,
  c.snapshot_date,
  h.canonical_station_id,
  'hard_id'::text AS match_method,
  c.hard_key
FROM _candidate c
JOIN _hard_groups h
  ON h.country = c.country
 AND h.hard_key = c.hard_key
WHERE c.hard_key IS NOT NULL;

CREATE TEMP TABLE _assign_geo AS
SELECT
  c.source_id,
  c.source_stop_id,
  c.country,
  c.snapshot_date,
  g.canonical_station_id,
  'name_geo'::text AS match_method,
  c.hard_key
FROM _soft_geo_clustered c
JOIN _soft_geo_groups g
  ON g.country = c.country
 AND g.normalized_name = c.normalized_name
 AND g.cluster_id = c.cluster_id;

CREATE TEMP TABLE _assign_name AS
SELECT
  c.source_id,
  c.source_stop_id,
  c.country,
  c.snapshot_date,
  n.canonical_station_id,
  'name_only'::text AS match_method,
  c.hard_key
FROM _candidate c
JOIN _soft_name_only_groups n
  ON n.country = c.country
 AND n.normalized_name = c.normalized_name
WHERE c.hard_key IS NULL
  AND c.geom IS NULL;

CREATE TEMP TABLE _assignments AS
SELECT * FROM _assign_hard
UNION ALL
SELECT * FROM _assign_geo
UNION ALL
SELECT * FROM _assign_name;

CREATE TEMP TABLE _summary AS
SELECT
  (SELECT COUNT(*) FROM _candidate) AS source_rows,
  (SELECT COUNT(*) FROM _new_canonical) AS canonical_rows,
  (SELECT COUNT(*) FROM _soft_name_only_groups WHERE member_count > 1) AS conflicts,
  (SELECT COALESCE(SUM(member_count - 1), 0) FROM _new_canonical) AS merged,
  (
    SELECT COUNT(*)
    FROM _new_canonical nc
    LEFT JOIN canonical_stations cs ON cs.canonical_station_id = nc.canonical_station_id
    WHERE cs.canonical_station_id IS NULL
  ) AS inserted,
  (
    SELECT COUNT(*)
    FROM _new_canonical nc
    JOIN canonical_stations cs ON cs.canonical_station_id = nc.canonical_station_id
    WHERE cs.canonical_name IS DISTINCT FROM nc.canonical_name
       OR cs.latitude IS DISTINCT FROM nc.latitude
       OR cs.longitude IS DISTINCT FROM nc.longitude
       OR cs.match_method IS DISTINCT FROM nc.match_method
       OR cs.member_count IS DISTINCT FROM nc.member_count
       OR cs.last_seen_snapshot_date IS DISTINCT FROM nc.last_seen_snapshot_date
  ) AS updated;

DELETE FROM canonical_stations cs
USING (
  SELECT
    nc.canonical_station_id,
    compute_geo_grid_id(nc.country::text, nc.latitude, nc.longitude, nc.geom) AS expected_grid_id
  FROM _new_canonical nc
) expected
WHERE cs.canonical_station_id = expected.canonical_station_id
  AND cs.grid_id <> expected.expected_grid_id;

INSERT INTO canonical_stations (
  canonical_station_id,
  canonical_name,
  normalized_name,
  country,
  latitude,
  longitude,
  geom,
  grid_id,
  match_method,
  member_count,
  first_seen_snapshot_date,
  last_seen_snapshot_date,
  last_built_run_id,
  updated_at
)
SELECT
  canonical_station_id,
  canonical_name,
  normalized_name,
  country,
  latitude,
  longitude,
  geom,
  compute_geo_grid_id(country::text, latitude, longitude, geom) AS grid_id,
  match_method,
  member_count,
  first_seen_snapshot_date,
  last_seen_snapshot_date,
  :'run_id'::uuid,
  now()
FROM _new_canonical
ON CONFLICT (grid_id, canonical_station_id)
DO UPDATE SET
  canonical_name = EXCLUDED.canonical_name,
  normalized_name = EXCLUDED.normalized_name,
  country = EXCLUDED.country,
  latitude = EXCLUDED.latitude,
  longitude = EXCLUDED.longitude,
  geom = EXCLUDED.geom,
  match_method = EXCLUDED.match_method,
  member_count = EXCLUDED.member_count,
  first_seen_snapshot_date = EXCLUDED.first_seen_snapshot_date,
  last_seen_snapshot_date = EXCLUDED.last_seen_snapshot_date,
  last_built_run_id = EXCLUDED.last_built_run_id,
  updated_at = now();

DELETE FROM canonical_station_sources css
USING _selected_snapshots ss
WHERE css.source_id = ss.source_id;

INSERT INTO canonical_station_sources (
  canonical_station_id,
  source_id,
  source_stop_id,
  country,
  snapshot_date,
  match_method,
  hard_id,
  import_run_id,
  updated_at
)
SELECT
  a.canonical_station_id,
  a.source_id,
  a.source_stop_id,
  a.country,
  a.snapshot_date,
  a.match_method,
  a.hard_key,
  :'run_id'::uuid,
  now()
FROM _assignments a
ON CONFLICT (source_id, source_stop_id)
DO UPDATE SET
  canonical_station_id = EXCLUDED.canonical_station_id,
  country = EXCLUDED.country,
  snapshot_date = EXCLUDED.snapshot_date,
  match_method = EXCLUDED.match_method,
  hard_id = EXCLUDED.hard_id,
  import_run_id = EXCLUDED.import_run_id,
  updated_at = now();

DELETE FROM canonical_stations cs
WHERE cs.country IN (SELECT DISTINCT country FROM _selected_snapshots)
  AND NOT EXISTS (
    SELECT 1
    FROM canonical_station_sources css
    WHERE css.canonical_station_id = cs.canonical_station_id
  );

COMMIT;

SELECT json_build_object(
  'sourceRows', (SELECT source_rows FROM _summary),
  'canonicalRows', (SELECT canonical_rows FROM _summary),
  'inserted', (SELECT inserted FROM _summary),
  'updated', (SELECT updated FROM _summary),
  'merged', (SELECT merged FROM _summary),
  'conflicts', (SELECT conflicts FROM _summary),
  'countryFilter', COALESCE(NULLIF(:'country_filter', ''), ''),
  'asOf', COALESCE(NULLIF(:'as_of', ''), ''),
  'sourceScope', COALESCE(NULLIF(:'source_id_scope', ''), '')
)::text;
`;

function createCanonicalStationsRepo(client) {
  return {
    async buildCanonicalStations(scope) {
      const result = await client.runScript(BUILD_CANONICAL_SQL, {
        run_id: scope.runId,
        country_filter: scope.country || "",
        as_of: scope.asOf || "",
        source_id_scope: scope.sourceId || "",
      });

      const line = extractJsonLine(result.stdout);
      if (!line) {
        throw new AppError({
          code: "CANONICAL_BUILD_FAILED",
          message: "Canonical build did not return summary JSON",
        });
      }

      let parsed;
      try {
        parsed = JSON.parse(line);
      } catch (err) {
        throw new AppError({
          code: "CANONICAL_BUILD_FAILED",
          message: "Canonical build returned invalid summary JSON",
          cause: err,
        });
      }

      validateOrThrow(parsed, CANONICAL_SUMMARY_SCHEMA, {
        code: "CANONICAL_BUILD_FAILED",
        message: "Canonical build summary failed schema validation",
      });

      return parsed;
    },
  };
}

module.exports = {
  BUILD_CANONICAL_SQL,
  createCanonicalStationsRepo,
  extractJsonLine,
};
