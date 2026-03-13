const crypto = require("node:crypto");

const { AppError } = require("../../../core/errors");

function extractJsonLine(stdout) {
  const lines = String(stdout || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  for (let index = lines.length - 1; index >= 0; index -= 1) {
    if (lines[index].startsWith("{") && lines[index].endsWith("}")) {
      return lines[index];
    }
  }

  return "";
}

function parseSummaryJson(stdout, errorCode, message) {
  const line = extractJsonLine(stdout);
  if (!line) {
    throw new AppError({
      code: errorCode,
      message,
    });
  }

  try {
    return JSON.parse(line);
  } catch (error) {
    throw new AppError({
      code: errorCode,
      message,
      cause: error,
    });
  }
}

function normalizeRows(rows = []) {
  return (Array.isArray(rows) ? rows : []).map((row) => ({
    external_id: String(row?.external_id || "").trim(),
    display_name: String(row?.display_name || "").trim(),
    normalized_name: String(row?.normalized_name || "").trim(),
    country: String(row?.country || "")
      .trim()
      .toUpperCase(),
    latitude:
      row?.latitude === null ||
      row?.latitude === undefined ||
      row?.latitude === ""
        ? null
        : Number(row.latitude),
    longitude:
      row?.longitude === null ||
      row?.longitude === undefined ||
      row?.longitude === ""
        ? null
        : Number(row.longitude),
    category: String(row?.category || "").trim(),
    subtype: String(row?.subtype || "").trim(),
    source_url: String(row?.source_url || "").trim(),
    metadata:
      row?.metadata &&
      typeof row.metadata === "object" &&
      !Array.isArray(row.metadata)
        ? row.metadata
        : {},
  }));
}

const BUILD_MATCHES_SQL = `
BEGIN;

CREATE TEMP TABLE _reference_active_imports AS
SELECT DISTINCT ON (eri.source_id, COALESCE(eri.country::text, ''))
  eri.import_id,
  eri.source_id,
  eri.country,
  eri.snapshot_date,
  eri.snapshot_label
FROM external_reference_imports eri
WHERE eri.status = 'succeeded'
  AND (
    NULLIF(:'country_filter', '') IS NULL
    OR eri.country = NULLIF(:'country_filter', '')::char(2)
  )
  AND (
    NULLIF(:'as_of', '') IS NULL
    OR eri.snapshot_date IS NULL
    OR eri.snapshot_date <= NULLIF(:'as_of', '')::date
  )
ORDER BY
  eri.source_id,
  COALESCE(eri.country::text, ''),
  COALESCE(eri.snapshot_date, DATE '1900-01-01') DESC,
  eri.created_at DESC,
  eri.import_id DESC;

CREATE TEMP TABLE _reference_scope AS
SELECT
  ers.reference_station_id,
  ers.source_id,
  ers.external_id,
  ers.display_name,
  ers.normalized_name,
  qa_loose_station_name(ers.display_name) AS loose_name,
  ers.country,
  ers.latitude,
  ers.longitude,
  ers.geom,
  ers.category,
  ers.subtype,
  ers.source_url,
  ers.metadata
FROM external_reference_stations ers
JOIN _reference_active_imports active_imports
  ON active_imports.import_id = ers.import_id;

CREATE INDEX _reference_scope_source_idx
  ON _reference_scope (source_id, country, normalized_name);

CREATE INDEX _reference_scope_geom_idx
  ON _reference_scope USING gist (geom)
  WHERE geom IS NOT NULL;

CREATE TEMP TABLE _global_station_scope AS
SELECT
  gs.global_station_id,
  gs.display_name,
  gs.normalized_name,
  qa_loose_station_name(gs.display_name) AS loose_name,
  gs.country,
  gs.geom,
  gs.station_kind
FROM global_stations gs
WHERE gs.is_active = true
  AND (
    NULLIF(:'country_filter', '') IS NULL
    OR gs.country = NULLIF(:'country_filter', '')::char(2)
  );

CREATE INDEX _global_station_scope_name_idx
  ON _global_station_scope (country, normalized_name);

CREATE INDEX _global_station_scope_geom_idx
  ON _global_station_scope USING gist (geom)
  WHERE geom IS NOT NULL;

DELETE FROM global_station_reference_matches match_rows
USING global_stations gs
WHERE gs.global_station_id = match_rows.global_station_id
  AND (
    NULLIF(:'country_filter', '') IS NULL
    OR gs.country = NULLIF(:'country_filter', '')::char(2)
  )
  AND EXISTS (
    SELECT 1
    FROM _reference_active_imports active_imports
    WHERE active_imports.source_id = match_rows.source_id
  );

CREATE TEMP TABLE _reference_match_candidates AS
WITH candidate_pairs AS (
  SELECT
    gs.global_station_id,
    gs.display_name AS global_display_name,
    gs.normalized_name AS global_normalized_name,
    gs.loose_name AS global_loose_name,
    gs.country,
    gs.station_kind,
    gs.geom AS global_geom,
    refs.reference_station_id,
    refs.source_id,
    refs.external_id,
    refs.display_name AS reference_display_name,
    refs.normalized_name AS reference_normalized_name,
    refs.loose_name AS reference_loose_name,
    refs.category,
    refs.subtype,
    refs.geom AS reference_geom,
    refs.source_url,
    CASE
      WHEN gs.geom IS NOT NULL AND refs.geom IS NOT NULL
        THEN ST_DistanceSphere(gs.geom, refs.geom)
      ELSE NULL
    END AS distance_meters
  FROM _global_station_scope gs
  JOIN _reference_scope refs
    ON refs.country IS NOT DISTINCT FROM gs.country
   AND (
     (
       gs.geom IS NOT NULL
       AND refs.geom IS NOT NULL
       AND ST_DWithin(gs.geom::geography, refs.geom::geography, 1500)
     )
     OR (
       (gs.geom IS NULL OR refs.geom IS NULL)
       AND (
         gs.normalized_name = refs.normalized_name
         OR gs.loose_name = refs.loose_name
         OR similarity(gs.loose_name, refs.loose_name) >= 0.72
       )
     )
   )
)
SELECT
  pairs.*,
  (pairs.global_normalized_name = pairs.reference_normalized_name) AS normalized_name_equal,
  GREATEST(
    similarity(pairs.global_loose_name, pairs.reference_loose_name),
    similarity(pairs.global_normalized_name, pairs.reference_normalized_name)
  ) AS name_similarity,
  (
    SELECT COUNT(*)
    FROM (
      SELECT DISTINCT token
      FROM regexp_split_to_table(pairs.global_loose_name, '\\s+') AS token
      WHERE token <> ''
    ) left_tokens
    JOIN (
      SELECT DISTINCT token
      FROM regexp_split_to_table(pairs.reference_loose_name, '\\s+') AS token
      WHERE token <> ''
    ) right_tokens
      ON right_tokens.token = left_tokens.token
  )::integer AS token_overlap_count,
  CASE
    WHEN COALESCE(NULLIF(pairs.category, ''), 'station') IN (
      'station',
      'train_station',
      'rail_station',
      'railway',
      'transportation',
      'transport_hub',
      'transit',
      'transit_stop',
      'stop',
      'bus_station',
      'tram_stop',
      'metro_station',
      'subway_station'
    ) THEN 1.0
    WHEN COALESCE(NULLIF(pairs.category, ''), '') = '' THEN 0.9
    ELSE 0.7
  END AS category_compatibility
FROM candidate_pairs pairs;

CREATE TEMP TABLE _reference_match_ranked AS
SELECT
  candidates.*,
  (
    LEAST(GREATEST(candidates.name_similarity, 0), 1) * 0.55
    + LEAST(candidates.token_overlap_count / 4.0, 1) * 0.15
    + CASE
        WHEN candidates.distance_meters IS NULL AND candidates.normalized_name_equal THEN 0.20
        WHEN candidates.distance_meters IS NULL THEN 0.08
        WHEN candidates.distance_meters <= 50 THEN 0.20
        WHEN candidates.distance_meters <= 250 THEN 0.18
        WHEN candidates.distance_meters <= 500 THEN 0.14
        WHEN candidates.distance_meters <= 1000 THEN 0.09
        WHEN candidates.distance_meters <= 1500 THEN 0.04
        ELSE 0
      END
    + candidates.category_compatibility * 0.10
  )::numeric(6, 4) AS match_confidence,
  CASE
    WHEN
      (
        candidates.normalized_name_equal = true
        AND candidates.distance_meters IS NOT NULL
        AND candidates.distance_meters <= 250
      )
      OR (
        candidates.name_similarity >= 0.97
        AND COALESCE(candidates.distance_meters, 0) <= 100
      )
      THEN 'strong'
    WHEN
      (
        candidates.name_similarity >= 0.87
        AND candidates.distance_meters IS NOT NULL
        AND candidates.distance_meters <= 500
      )
      OR (
        candidates.distance_meters IS NULL
        AND (candidates.normalized_name_equal = true OR candidates.name_similarity >= 0.92)
        AND candidates.token_overlap_count >= 1
      )
      THEN 'probable'
    WHEN
      (
        candidates.token_overlap_count >= 1
        AND candidates.distance_meters IS NOT NULL
        AND candidates.distance_meters <= 1000
      )
      OR (
        candidates.distance_meters IS NULL
        AND candidates.name_similarity >= 0.80
        AND candidates.token_overlap_count >= 1
      )
      THEN 'weak'
    ELSE NULL
  END AS match_status
FROM _reference_match_candidates candidates;

CREATE TEMP TABLE _reference_match_filtered AS
SELECT
  ranked.*,
  ROW_NUMBER() OVER (
    PARTITION BY ranked.global_station_id, ranked.source_id
    ORDER BY
      ranked.match_confidence DESC,
      COALESCE(ranked.distance_meters, 1e12) ASC,
      ranked.name_similarity DESC,
      ranked.reference_station_id ASC
  ) AS per_source_rank
FROM _reference_match_ranked ranked
WHERE ranked.match_status IS NOT NULL;

INSERT INTO global_station_reference_matches (
  global_station_id,
  reference_station_id,
  source_id,
  match_status,
  match_confidence,
  name_similarity,
  distance_meters,
  token_overlap_count,
  is_primary,
  metadata,
  created_at,
  updated_at
)
SELECT
  filtered.global_station_id,
  filtered.reference_station_id,
  filtered.source_id,
  filtered.match_status,
  filtered.match_confidence,
  filtered.name_similarity,
  CASE
    WHEN filtered.distance_meters IS NULL THEN NULL
    ELSE ROUND(filtered.distance_meters::numeric, 2)
  END AS distance_meters,
  filtered.token_overlap_count,
  (filtered.per_source_rank = 1) AS is_primary,
  jsonb_build_object(
    'reference_display_name', filtered.reference_display_name,
    'reference_external_id', filtered.external_id,
    'reference_category', filtered.category,
    'reference_subtype', filtered.subtype,
    'normalized_name_equal', filtered.normalized_name_equal,
    'category_compatibility', filtered.category_compatibility
  ) AS metadata,
  now(),
  now()
FROM _reference_match_filtered filtered
WHERE filtered.per_source_rank <= 3;

SELECT json_build_object(
  'scopeCountry', COALESCE(NULLIF(:'country_filter', ''), ''),
  'asOf', COALESCE(NULLIF(:'as_of', ''), ''),
  'imports_considered', (SELECT COUNT(*) FROM _reference_active_imports),
  'matched_stations', (
    SELECT COUNT(DISTINCT global_station_id)
    FROM global_station_reference_matches
    WHERE EXISTS (
      SELECT 1
      FROM _reference_active_imports active_imports
      WHERE active_imports.source_id = global_station_reference_matches.source_id
    )
      AND EXISTS (
        SELECT 1
        FROM global_stations gs
        WHERE gs.global_station_id = global_station_reference_matches.global_station_id
          AND (
            NULLIF(:'country_filter', '') IS NULL
            OR gs.country = NULLIF(:'country_filter', '')::char(2)
          )
      )
  ),
  'rows_inserted', (
    SELECT COUNT(*)
    FROM global_station_reference_matches
    WHERE EXISTS (
      SELECT 1
      FROM _reference_active_imports active_imports
      WHERE active_imports.source_id = global_station_reference_matches.source_id
    )
      AND EXISTS (
        SELECT 1
        FROM global_stations gs
        WHERE gs.global_station_id = global_station_reference_matches.global_station_id
          AND (
            NULLIF(:'country_filter', '') IS NULL
            OR gs.country = NULLIF(:'country_filter', '')::char(2)
          )
      )
  ),
  'status_counts', (
    SELECT json_object_agg(match_status, row_count)
    FROM (
      SELECT
        match_status,
        COUNT(*)::integer AS row_count
      FROM global_station_reference_matches
      WHERE EXISTS (
        SELECT 1
        FROM _reference_active_imports active_imports
        WHERE active_imports.source_id = global_station_reference_matches.source_id
      )
        AND EXISTS (
          SELECT 1
          FROM global_stations gs
          WHERE gs.global_station_id = global_station_reference_matches.global_station_id
            AND (
              NULLIF(:'country_filter', '') IS NULL
              OR gs.country = NULLIF(:'country_filter', '')::char(2)
            )
        )
      GROUP BY match_status
    ) status_rows
  ),
  'source_counts', (
    SELECT json_object_agg(source_id, row_count)
    FROM (
      SELECT
        source_id,
        COUNT(*)::integer AS row_count
      FROM global_station_reference_matches
      WHERE EXISTS (
        SELECT 1
        FROM _reference_active_imports active_imports
        WHERE active_imports.source_id = global_station_reference_matches.source_id
      )
        AND EXISTS (
          SELECT 1
          FROM global_stations gs
          WHERE gs.global_station_id = global_station_reference_matches.global_station_id
            AND (
              NULLIF(:'country_filter', '') IS NULL
              OR gs.country = NULLIF(:'country_filter', '')::char(2)
            )
        )
      GROUP BY source_id
    ) source_rows
  )
)::text;

COMMIT;
`;

function buildStationIdArrayParam(stationIds = []) {
  return JSON.stringify(
    Array.from(
      new Set(
        (Array.isArray(stationIds) ? stationIds : [])
          .map((stationId) => String(stationId || "").trim())
          .filter(Boolean),
      ),
    ),
  );
}

function buildTextArrayParam(values = []) {
  return JSON.stringify(
    Array.from(
      new Set(
        (Array.isArray(values) ? values : [])
          .map((value) =>
            String(value || "")
              .trim()
              .toLowerCase(),
          )
          .filter(Boolean),
      ),
    ),
  );
}

function createExternalReferenceRepo(client) {
  return {
    async recordImportRun(input = {}) {
      const explicitImportId = String(input.importId || "").trim();
      if (explicitImportId) {
        const updatedRow = await client.queryOne(
          `
          UPDATE external_reference_imports
          SET
            snapshot_date = NULLIF(:'snapshot_date', '')::date,
            country = NULLIF(:'country', '')::char(2),
            status = :'status',
            metadata = COALESCE(external_reference_imports.metadata, '{}'::jsonb)
              || COALESCE(:'metadata'::jsonb, '{}'::jsonb),
            updated_at = now()
          WHERE import_id = :'import_id'::uuid
          RETURNING
            import_id,
            source_id,
            snapshot_label,
            snapshot_date,
            country,
            status,
            metadata,
            created_at,
            updated_at
          `,
          {
            import_id: explicitImportId,
            snapshot_date: String(input.snapshotDate || "").trim(),
            country: String(input.country || "")
              .trim()
              .toUpperCase(),
            status: String(input.status || "running").trim(),
            metadata: JSON.stringify(
              input.metadata &&
                typeof input.metadata === "object" &&
                !Array.isArray(input.metadata)
                ? input.metadata
                : {},
            ),
          },
        );

        if (updatedRow) {
          return updatedRow;
        }
      }

      const row = await client.queryOne(
        `
        INSERT INTO external_reference_imports (
          import_id,
          source_id,
          snapshot_label,
          snapshot_date,
          country,
          status,
          metadata,
          created_at,
          updated_at
        )
        VALUES (
          COALESCE(NULLIF(:'import_id', '')::uuid, :'generated_import_id'::uuid),
          :'source_id',
          :'snapshot_label',
          NULLIF(:'snapshot_date', '')::date,
          NULLIF(:'country', '')::char(2),
          :'status',
          COALESCE(:'metadata'::jsonb, '{}'::jsonb),
          now(),
          now()
        )
        ON CONFLICT (source_id, snapshot_label, country) DO UPDATE
        SET
          snapshot_date = EXCLUDED.snapshot_date,
          status = EXCLUDED.status,
          metadata = COALESCE(external_reference_imports.metadata, '{}'::jsonb)
            || COALESCE(EXCLUDED.metadata, '{}'::jsonb),
          updated_at = now()
        RETURNING
          import_id,
          source_id,
          snapshot_label,
          snapshot_date,
          country,
          status,
          metadata,
          created_at,
          updated_at
        `,
        {
          import_id: explicitImportId,
          generated_import_id: crypto.randomUUID(),
          source_id: String(input.sourceId || "").trim(),
          snapshot_label: String(input.snapshotLabel || "").trim(),
          snapshot_date: String(input.snapshotDate || "").trim(),
          country: String(input.country || "")
            .trim()
            .toUpperCase(),
          status: String(input.status || "running").trim(),
          metadata: JSON.stringify(
            input.metadata &&
              typeof input.metadata === "object" &&
              !Array.isArray(input.metadata)
              ? input.metadata
              : {},
          ),
        },
      );

      if (!row) {
        throw new AppError({
          code: "EXTERNAL_REFERENCE_IMPORT_FAILED",
          message: "Failed to create external reference import row",
        });
      }

      return row;
    },

    async replaceImportRows(input = {}) {
      const rows = normalizeRows(input.rows);
      await client.withTransaction(
        async (tx) => {
          tx.add(
            `
        DELETE FROM external_reference_stations
        WHERE import_id = :'import_id'::uuid;
        `,
          );

          tx.add(
            `
        INSERT INTO external_reference_stations (
          import_id,
          source_id,
          external_id,
          display_name,
          normalized_name,
          country,
          latitude,
          longitude,
          category,
          subtype,
          source_url,
          metadata,
          created_at,
          updated_at
        )
        SELECT
          :'import_id'::uuid,
          :'source_id',
          rows.external_id,
          rows.display_name,
          rows.normalized_name,
          NULLIF(rows.country, '')::char(2),
          rows.latitude,
          rows.longitude,
          NULLIF(rows.category, ''),
          NULLIF(rows.subtype, ''),
          NULLIF(rows.source_url, ''),
          COALESCE(rows.metadata, '{}'::jsonb),
          now(),
          now()
        FROM jsonb_to_recordset(:'rows_json'::jsonb) AS rows(
          external_id text,
          display_name text,
          normalized_name text,
          country text,
          latitude double precision,
          longitude double precision,
          category text,
          subtype text,
          source_url text,
          metadata jsonb
        )
        WHERE rows.external_id IS NOT NULL
          AND rows.external_id <> ''
          AND rows.display_name IS NOT NULL
          AND rows.display_name <> ''
          AND rows.normalized_name IS NOT NULL
          AND rows.normalized_name <> '';
        `,
          );
        },
        {
          import_id: String(input.importId || "").trim(),
          source_id: String(input.sourceId || "").trim(),
          rows_json: JSON.stringify(rows),
        },
      );

      return {
        import_id: String(input.importId || "").trim(),
        source_id: String(input.sourceId || "").trim(),
        row_count: rows.length,
      };
    },

    async buildStationReferenceMatches(scope = {}) {
      const result = await client.runScript(BUILD_MATCHES_SQL, {
        country_filter: String(scope.country || "")
          .trim()
          .toUpperCase(),
        as_of: String(scope.asOf || "").trim(),
      });

      return parseSummaryJson(
        result.stdout,
        "EXTERNAL_REFERENCE_MATCH_FAILED",
        "External reference match build did not return summary JSON",
      );
    },

    async loadMatchesByStationIds(stationIds = []) {
      const stationIdsJson = buildStationIdArrayParam(stationIds);
      if (stationIdsJson === "[]") {
        return [];
      }

      return client.queryRows(
        `
        WITH station_scope AS (
          SELECT jsonb_array_elements_text(:'station_ids_json'::jsonb) AS global_station_id
        )
        SELECT
          matches.match_id,
          matches.global_station_id,
          matches.reference_station_id,
          matches.source_id,
          matches.match_status,
          matches.match_confidence,
          matches.name_similarity,
          matches.distance_meters,
          matches.token_overlap_count,
          matches.is_primary,
          matches.metadata,
          refs.external_id,
          refs.display_name,
          refs.category,
          refs.subtype,
          refs.latitude,
          refs.longitude,
          refs.source_url
        FROM global_station_reference_matches matches
        JOIN station_scope scope
          ON scope.global_station_id = matches.global_station_id
        JOIN external_reference_stations refs
          ON refs.reference_station_id = matches.reference_station_id
        ORDER BY
          matches.global_station_id,
          matches.source_id,
          matches.is_primary DESC,
          matches.match_confidence DESC,
          COALESCE(matches.distance_meters, 1e12) ASC,
          refs.display_name ASC
        `,
        {
          station_ids_json: stationIdsJson,
        },
      );
    },

    async loadOverlayByStationIds(stationIds = []) {
      const stationIdsJson = buildStationIdArrayParam(stationIds);
      if (stationIdsJson === "[]") {
        return [];
      }

      return client.queryRows(
        `
        WITH station_scope AS (
          SELECT
            scope_ids.global_station_id,
            gs.country,
            gs.geom
          FROM jsonb_array_elements_text(:'station_ids_json'::jsonb) AS scope_ids(global_station_id)
          JOIN global_stations gs
            ON gs.global_station_id = scope_ids.global_station_id
          WHERE gs.geom IS NOT NULL
        ),
        active_imports AS (
          SELECT DISTINCT ON (eri.source_id, COALESCE(eri.country::text, ''))
            eri.import_id,
            eri.source_id,
            eri.country
          FROM external_reference_imports eri
          WHERE eri.status = 'succeeded'
            AND (
              eri.country IS NULL
              OR eri.country IN (
                SELECT DISTINCT country
                FROM station_scope
                WHERE country IS NOT NULL
              )
            )
          ORDER BY
            eri.source_id,
            COALESCE(eri.country::text, ''),
            COALESCE(eri.snapshot_date, DATE '1900-01-01') DESC,
            eri.created_at DESC,
            eri.import_id DESC
        ),
        bounds AS (
          SELECT
            ST_Transform(
              ST_SetSRID(
              ST_Expand(
                ST_Extent(ST_Transform(geom, 3857))::box2d::geometry,
                500
              ),
              3857
              ),
              4326
            ) AS bbox_geom
          FROM station_scope
        )
        SELECT
          refs.reference_station_id,
          refs.source_id,
          refs.external_id,
          refs.display_name,
          refs.category,
          refs.subtype,
          refs.latitude,
          refs.longitude,
          refs.source_url,
          COALESCE(
            ARRAY_AGG(DISTINCT matches.global_station_id ORDER BY matches.global_station_id)
              FILTER (WHERE matches.global_station_id IS NOT NULL),
            ARRAY[]::text[]
          ) AS matched_candidate_ids
        FROM external_reference_stations refs
        JOIN active_imports active_imports
          ON active_imports.import_id = refs.import_id
        JOIN bounds
          ON bounds.bbox_geom IS NOT NULL
         AND refs.geom IS NOT NULL
         AND ST_Intersects(refs.geom, bounds.bbox_geom)
        LEFT JOIN global_station_reference_matches matches
          ON matches.reference_station_id = refs.reference_station_id
         AND matches.global_station_id IN (
           SELECT global_station_id
           FROM station_scope
         )
        GROUP BY
          refs.reference_station_id,
          refs.source_id,
          refs.external_id,
          refs.display_name,
          refs.category,
          refs.subtype,
          refs.latitude,
          refs.longitude,
          refs.source_url
        ORDER BY refs.source_id, refs.display_name, refs.reference_station_id
        `,
        {
          station_ids_json: stationIdsJson,
        },
      );
    },

    async loadViewportPoints(input = {}) {
      const sourceIdsJson = buildTextArrayParam(input.sourceIds);
      const limit = Number.isFinite(Number(input.limit))
        ? Math.max(1, Math.min(5000, Number(input.limit)))
        : 1200;

      return client.queryRows(
        `
        WITH source_filter AS (
          SELECT jsonb_array_elements_text(:'source_ids_json'::jsonb) AS source_id
        ),
        active_imports AS (
          SELECT DISTINCT ON (eri.source_id, COALESCE(eri.country::text, ''))
            eri.import_id,
            eri.source_id,
            eri.country
          FROM external_reference_imports eri
          WHERE eri.status = 'succeeded'
            AND (
              jsonb_array_length(:'source_ids_json'::jsonb) = 0
              OR eri.source_id IN (SELECT source_id FROM source_filter)
            )
          ORDER BY
            eri.source_id,
            COALESCE(eri.country::text, ''),
            COALESCE(eri.snapshot_date, DATE '1900-01-01') DESC,
            eri.created_at DESC,
            eri.import_id DESC
        ),
        bbox AS (
          SELECT ST_MakeEnvelope(
            :'min_lon'::double precision,
            :'min_lat'::double precision,
            :'max_lon'::double precision,
            :'max_lat'::double precision,
            4326
          ) AS geom
        )
        SELECT
          refs.reference_station_id,
          refs.source_id,
          refs.external_id,
          refs.display_name,
          refs.category,
          refs.subtype,
          refs.latitude,
          refs.longitude,
          refs.source_url,
          COALESCE(
            ARRAY_AGG(DISTINCT matches.global_station_id ORDER BY matches.global_station_id)
              FILTER (WHERE matches.global_station_id IS NOT NULL),
            ARRAY[]::text[]
          ) AS matched_candidate_ids,
          COUNT(DISTINCT matches.global_station_id)::integer AS match_count
        FROM external_reference_stations refs
        JOIN active_imports active_imports
          ON active_imports.import_id = refs.import_id
        JOIN bbox
          ON refs.geom IS NOT NULL
         AND refs.geom && bbox.geom
         AND ST_Intersects(refs.geom, bbox.geom)
        LEFT JOIN global_station_reference_matches matches
          ON matches.reference_station_id = refs.reference_station_id
        GROUP BY
          refs.reference_station_id,
          refs.source_id,
          refs.external_id,
          refs.display_name,
          refs.category,
          refs.subtype,
          refs.latitude,
          refs.longitude,
          refs.source_url
        ORDER BY
          COUNT(DISTINCT matches.global_station_id) DESC,
          refs.source_id ASC,
          refs.display_name ASC,
          refs.reference_station_id ASC
        LIMIT :'limit'::integer
        `,
        {
          min_lat: Number(input.minLat),
          min_lon: Number(input.minLon),
          max_lat: Number(input.maxLat),
          max_lon: Number(input.maxLon),
          source_ids_json: sourceIdsJson,
          limit,
        },
      );
    },
  };
}

module.exports = {
  createExternalReferenceRepo,
  _internal: {
    buildTextArrayParam,
    normalizeRows,
    parseSummaryJson,
  },
};
