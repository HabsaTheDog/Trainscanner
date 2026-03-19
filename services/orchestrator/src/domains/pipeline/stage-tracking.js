const crypto = require("node:crypto");

const {
  createPipelineStageRepo,
} = require("../../data/postgis/repositories/pipeline-stage-repo");
const {
  collectTimingSummary,
  computeCodeFingerprint,
  normalizeStageScope,
} = require("./stage-runtime");

const ISO_TIMESTAMP_SQL = `YYYY-MM-DD"T"HH24:MI:SSOF`;

function buildScopeParams(scope = {}) {
  return {
    country_filter: scope.country || "",
    as_of: scope.asOf || "",
    source_id_scope: scope.sourceId || "",
  };
}

function stageInputFingerprintSql(stageId) {
  switch (stageId) {
    case "fetch":
      return `
      SELECT json_build_object(
        'stage', 'fetch',
        'country', COALESCE(NULLIF(:'country_filter', ''), ''),
        'asOf', COALESCE(NULLIF(:'as_of', ''), ''),
        'sourceId', COALESCE(NULLIF(:'source_id_scope', ''), ''),
        'rawSnapshotCount', COUNT(*),
        'latestSnapshotDate', COALESCE(MAX(snapshot_date)::text, ''),
        'latestUpdatedAt', COALESCE(to_char(MAX(updated_at), '${ISO_TIMESTAMP_SQL}'), '')
      ) AS fingerprint
      FROM raw_snapshots
      WHERE (
        NULLIF(:'country_filter', '') IS NULL
        OR country = NULLIF(:'country_filter', '')::char(2)
      )
        AND (
          NULLIF(:'source_id_scope', '') IS NULL
          OR source_id = NULLIF(:'source_id_scope', '')
        )
        AND (
          NULLIF(:'as_of', '') IS NULL
          OR snapshot_date <= NULLIF(:'as_of', '')::date
        );
      `;
    case "stop-topology":
      return `
      WITH selected_datasets AS (
        SELECT dataset_id, source_id, country, snapshot_date, updated_at
        FROM provider_datasets
        WHERE format = 'netex'
          AND (
            NULLIF(:'country_filter', '') IS NULL
            OR country = NULLIF(:'country_filter', '')::char(2)
          )
          AND (
            NULLIF(:'source_id_scope', '') IS NULL
            OR source_id = NULLIF(:'source_id_scope', '')
          )
          AND (
            NULLIF(:'as_of', '') IS NULL
            OR snapshot_date <= NULLIF(:'as_of', '')::date
          )
      )
      SELECT json_build_object(
        'stage', 'stop-topology',
        'country', COALESCE(NULLIF(:'country_filter', ''), ''),
        'asOf', COALESCE(NULLIF(:'as_of', ''), ''),
        'sourceId', COALESCE(NULLIF(:'source_id_scope', ''), ''),
        'datasetCount', (SELECT COUNT(*) FROM selected_datasets),
        'latestDatasetDate', COALESCE((SELECT MAX(snapshot_date)::text FROM selected_datasets), ''),
        'datasetUpdatedAtMax', COALESCE((SELECT to_char(MAX(updated_at), '${ISO_TIMESTAMP_SQL}') FROM selected_datasets), ''),
        'rawStopPlaceCount', (
          SELECT COUNT(*)
          FROM raw_provider_stop_places rp
          WHERE rp.dataset_id IN (SELECT dataset_id FROM selected_datasets)
        ),
        'rawStopPointCount', (
          SELECT COUNT(*)
          FROM raw_provider_stop_points rp
          WHERE rp.dataset_id IN (SELECT dataset_id FROM selected_datasets)
        )
      ) AS fingerprint;
      `;
    case "qa-network-context":
      return `
      WITH selected_datasets AS (
        SELECT dataset_id, source_id, country, snapshot_date, updated_at
        FROM provider_datasets
        WHERE format = 'netex'
          AND (
            NULLIF(:'country_filter', '') IS NULL
            OR country = NULLIF(:'country_filter', '')::char(2)
          )
          AND (
            NULLIF(:'source_id_scope', '') IS NULL
            OR source_id = NULLIF(:'source_id_scope', '')
          )
          AND (
            NULLIF(:'as_of', '') IS NULL
            OR snapshot_date <= NULLIF(:'as_of', '')::date
          )
      )
      SELECT json_build_object(
        'stage', 'qa-network-context',
        'country', COALESCE(NULLIF(:'country_filter', ''), ''),
        'asOf', COALESCE(NULLIF(:'as_of', ''), ''),
        'sourceId', COALESCE(NULLIF(:'source_id_scope', ''), ''),
        'datasetIds', COALESCE((SELECT jsonb_agg(dataset_id ORDER BY dataset_id) FROM selected_datasets), '[]'::jsonb),
        'datasetUpdatedAtMax', COALESCE((SELECT to_char(MAX(updated_at), '${ISO_TIMESTAMP_SQL}') FROM selected_datasets), ''),
        'rawStopPlaceCount', (
          SELECT COUNT(*)
          FROM raw_provider_stop_places rp
          WHERE rp.dataset_id IN (SELECT dataset_id FROM selected_datasets)
        ),
        'rawStopPointCount', (
          SELECT COUNT(*)
          FROM raw_provider_stop_points rp
          WHERE rp.dataset_id IN (SELECT dataset_id FROM selected_datasets)
        )
      ) AS fingerprint;
      `;
    case "qa-network-projection":
      return `
      SELECT json_build_object(
        'stage', 'qa-network-projection',
        'country', COALESCE(NULLIF(:'country_filter', ''), ''),
        'asOf', COALESCE(NULLIF(:'as_of', ''), ''),
        'sourceId', COALESCE(NULLIF(:'source_id_scope', ''), ''),
        'qaProviderRouteCount', (
          SELECT COUNT(*)
          FROM qa_provider_stop_place_routes qr
          WHERE (
            NULLIF(:'country_filter', '') IS NULL
            OR qr.source_country = NULLIF(:'country_filter', '')::char(2)
          )
            AND (
              NULLIF(:'source_id_scope', '') IS NULL
              OR qr.source_id = NULLIF(:'source_id_scope', '')
            )
        ),
        'qaProviderAdjacencyCount', (
          SELECT COUNT(*)
          FROM qa_provider_stop_place_adjacencies qa
          WHERE (
            NULLIF(:'country_filter', '') IS NULL
            OR qa.source_country = NULLIF(:'country_filter', '')::char(2)
          )
            AND (
              NULLIF(:'source_id_scope', '') IS NULL
              OR qa.source_id = NULLIF(:'source_id_scope', '')
            )
        ),
        'globalStationUpdatedAtMax', COALESCE(
          (
            SELECT to_char(MAX(updated_at), '${ISO_TIMESTAMP_SQL}')
            FROM global_stations gs
            WHERE gs.is_active = true
              AND (
                NULLIF(:'country_filter', '') IS NULL
                OR gs.country = NULLIF(:'country_filter', '')::char(2)
              )
          ),
          ''
        ),
        'stationMappingUpdatedAtMax', COALESCE(
          (
            SELECT to_char(MAX(updated_at), '${ISO_TIMESTAMP_SQL}')
            FROM provider_global_station_mappings m
            WHERE m.is_active = true
              AND (
                NULLIF(:'source_id_scope', '') IS NULL
                OR m.source_id = NULLIF(:'source_id_scope', '')
              )
          ),
          ''
        )
      ) AS fingerprint;
      `;
    case "reference-data":
      return `
      SELECT json_build_object(
        'stage', 'reference-data',
        'country', COALESCE(NULLIF(:'country_filter', ''), ''),
        'asOf', COALESCE(NULLIF(:'as_of', ''), ''),
        'sourceId', COALESCE(NULLIF(:'source_id_scope', ''), ''),
        'activeGlobalStations', (
          SELECT COUNT(*)
          FROM global_stations gs
          WHERE gs.is_active = true
            AND (
              NULLIF(:'country_filter', '') IS NULL
              OR gs.country = NULLIF(:'country_filter', '')::char(2)
            )
        ),
        'latestReferenceImportAt', COALESCE(
          (
            SELECT to_char(MAX(created_at), '${ISO_TIMESTAMP_SQL}')
            FROM external_reference_imports eri
            WHERE eri.status = 'succeeded'
              AND (
                NULLIF(:'country_filter', '') IS NULL
                OR eri.country = NULLIF(:'country_filter', '')::char(2)
                OR eri.country IS NULL
              )
              AND (
                NULLIF(:'source_id_scope', '') IS NULL
                OR eri.source_id = NULLIF(:'source_id_scope', '')
              )
          ),
          ''
        ),
        'successfulReferenceImports', (
          SELECT COUNT(*)
          FROM external_reference_imports eri
          WHERE eri.status = 'succeeded'
            AND (
              NULLIF(:'country_filter', '') IS NULL
              OR eri.country = NULLIF(:'country_filter', '')::char(2)
              OR eri.country IS NULL
            )
            AND (
              NULLIF(:'source_id_scope', '') IS NULL
              OR eri.source_id = NULLIF(:'source_id_scope', '')
            )
        )
      ) AS fingerprint;
      `;
    case "export-schedule":
      return `
      WITH selected_datasets AS (
        SELECT dataset_id, source_id, country, snapshot_date, updated_at
        FROM provider_datasets
        WHERE format = 'netex'
          AND (
            NULLIF(:'country_filter', '') IS NULL
            OR country = NULLIF(:'country_filter', '')::char(2)
          )
          AND (
            NULLIF(:'source_id_scope', '') IS NULL
            OR source_id = NULLIF(:'source_id_scope', '')
          )
          AND (
            NULLIF(:'as_of', '') IS NULL
            OR snapshot_date <= NULLIF(:'as_of', '')::date
          )
      )
      SELECT json_build_object(
        'stage', 'export-schedule',
        'country', COALESCE(NULLIF(:'country_filter', ''), ''),
        'asOf', COALESCE(NULLIF(:'as_of', ''), ''),
        'sourceId', COALESCE(NULLIF(:'source_id_scope', ''), ''),
        'datasetIds', COALESCE((SELECT jsonb_agg(dataset_id ORDER BY dataset_id) FROM selected_datasets), '[]'::jsonb),
        'datasetUpdatedAtMax', COALESCE((SELECT to_char(MAX(updated_at), '${ISO_TIMESTAMP_SQL}') FROM selected_datasets), ''),
        'tripCount', (
          SELECT COUNT(*)
          FROM timetable_trips tt
          WHERE (
            NULLIF(:'source_id_scope', '') IS NULL
            OR tt.source_id = NULLIF(:'source_id_scope', '')
          )
        ),
        'tripStopTimeCount', (
          SELECT COUNT(*)
          FROM timetable_trip_stop_times tts
          JOIN timetable_trips tt
            ON tt.trip_fact_id = tts.trip_fact_id
          WHERE (
            NULLIF(:'source_id_scope', '') IS NULL
            OR tt.source_id = NULLIF(:'source_id_scope', '')
          )
        )
      ) AS fingerprint;
      `;
    default:
      return `
      SELECT json_build_object(
        'stage', :'stage_id',
        'country', COALESCE(NULLIF(:'country_filter', ''), ''),
        'asOf', COALESCE(NULLIF(:'as_of', ''), ''),
        'sourceId', COALESCE(NULLIF(:'source_id_scope', ''), '')
      ) AS fingerprint;
      `;
  }
}

function stageSummarySql(stageId) {
  switch (stageId) {
    case "fetch":
      return `
      SELECT json_build_object(
        'rawSnapshots', COUNT(*),
        'latestSnapshotDate', COALESCE(MAX(snapshot_date)::text, ''),
        'scopeCountry', COALESCE(NULLIF(:'country_filter', ''), ''),
        'scopeAsOf', COALESCE(NULLIF(:'as_of', ''), ''),
        'scopeSourceId', COALESCE(NULLIF(:'source_id_scope', ''), '')
      ) AS summary
      FROM raw_snapshots
      WHERE (
        NULLIF(:'country_filter', '') IS NULL
        OR country = NULLIF(:'country_filter', '')::char(2)
      )
        AND (
          NULLIF(:'source_id_scope', '') IS NULL
          OR source_id = NULLIF(:'source_id_scope', '')
        )
        AND (
          NULLIF(:'as_of', '') IS NULL
          OR snapshot_date <= NULLIF(:'as_of', '')::date
        );
      `;
    case "stop-topology":
      return `
      SELECT json_build_object(
        'providerDatasets', (
          SELECT COUNT(*)
          FROM provider_datasets pd
          WHERE pd.format = 'netex'
            AND (
              NULLIF(:'country_filter', '') IS NULL
              OR pd.country = NULLIF(:'country_filter', '')::char(2)
            )
            AND (
              NULLIF(:'source_id_scope', '') IS NULL
              OR pd.source_id = NULLIF(:'source_id_scope', '')
            )
            AND (
              NULLIF(:'as_of', '') IS NULL
              OR pd.snapshot_date <= NULLIF(:'as_of', '')::date
            )
        ),
        'rawStopPlaces', (
          SELECT COUNT(*)
          FROM raw_provider_stop_places rp
          WHERE (
            NULLIF(:'country_filter', '') IS NULL
            OR rp.country = NULLIF(:'country_filter', '')::char(2)
          )
            AND (
              NULLIF(:'source_id_scope', '') IS NULL
              OR rp.source_id = NULLIF(:'source_id_scope', '')
            )
        ),
        'rawStopPoints', (
          SELECT COUNT(*)
          FROM raw_provider_stop_points rp
          WHERE (
            NULLIF(:'country_filter', '') IS NULL
            OR rp.country = NULLIF(:'country_filter', '')::char(2)
          )
            AND (
              NULLIF(:'source_id_scope', '') IS NULL
              OR rp.source_id = NULLIF(:'source_id_scope', '')
            )
        ),
        'timetableTripsTouched', 0,
        'timetableTripStopTimesTouched', 0
      ) AS summary;
      `;
    case "qa-network-context":
      return `
      SELECT json_build_object(
        'providerRouteRows', (
          SELECT COUNT(*)
          FROM qa_provider_stop_place_routes qr
          WHERE (
            NULLIF(:'country_filter', '') IS NULL
            OR qr.source_country = NULLIF(:'country_filter', '')::char(2)
          )
            AND (
              NULLIF(:'source_id_scope', '') IS NULL
              OR qr.source_id = NULLIF(:'source_id_scope', '')
            )
        ),
        'providerAdjacencyRows', (
          SELECT COUNT(*)
          FROM qa_provider_stop_place_adjacencies qa
          WHERE (
            NULLIF(:'country_filter', '') IS NULL
            OR qa.source_country = NULLIF(:'country_filter', '')::char(2)
          )
            AND (
              NULLIF(:'source_id_scope', '') IS NULL
              OR qa.source_id = NULLIF(:'source_id_scope', '')
            )
        ),
        'timetableTripsTouched', 0,
        'timetableTripStopTimesTouched', 0
      ) AS summary;
      `;
    case "qa-network-projection":
      return `
      SELECT json_build_object(
        'globalRouteRows', (
          SELECT COUNT(*)
          FROM qa_global_station_routes qr
          WHERE (
            NULLIF(:'country_filter', '') IS NULL
            OR qr.source_country = NULLIF(:'country_filter', '')::char(2)
          )
            AND (
              NULLIF(:'source_id_scope', '') IS NULL
              OR qr.source_id = NULLIF(:'source_id_scope', '')
            )
        ),
        'globalAdjacencyRows', (
          SELECT COUNT(*)
          FROM qa_global_station_adjacencies qa
          WHERE (
            NULLIF(:'country_filter', '') IS NULL
            OR qa.source_country = NULLIF(:'country_filter', '')::char(2)
          )
            AND (
              NULLIF(:'source_id_scope', '') IS NULL
              OR qa.source_id = NULLIF(:'source_id_scope', '')
            )
        ),
        'timetableTripsTouched', 0,
        'timetableTripStopTimesTouched', 0
      ) AS summary;
      `;
    case "export-schedule":
      return `
      SELECT json_build_object(
        'timetableTrips', (
          SELECT COUNT(*)
          FROM timetable_trips tt
          WHERE (
            NULLIF(:'source_id_scope', '') IS NULL
            OR tt.source_id = NULLIF(:'source_id_scope', '')
          )
        ),
        'timetableTripStopTimes', (
          SELECT COUNT(*)
          FROM timetable_trip_stop_times tts
          JOIN timetable_trips tt
            ON tt.trip_fact_id = tts.trip_fact_id
          WHERE (
            NULLIF(:'source_id_scope', '') IS NULL
            OR tt.source_id = NULLIF(:'source_id_scope', '')
          )
        )
      ) AS summary;
      `;
    default:
      return "";
  }
}

async function getStageInputFingerprint(client, stageId, scope = {}) {
  try {
    const sql = stageInputFingerprintSql(stageId);
    const row = await client.queryOne(sql, {
      ...buildScopeParams(scope),
      stage_id: stageId,
    });
    return row?.fingerprint || { stage: stageId };
  } catch {
    return { stage: stageId };
  }
}

async function getStageSummary(client, stageId, scope = {}) {
  try {
    const sql = stageSummarySql(stageId);
    if (!sql) {
      return {};
    }
    const row = await client.queryOne(sql, buildScopeParams(scope));
    return row?.summary || {};
  } catch {
    return {};
  }
}

async function runTrackedStage({
  client,
  stageRepo,
  rootDir,
  stageId,
  scope,
  codePaths,
  codeFingerprint,
  inputFingerprint,
  execute,
  summary,
  cacheHit = false,
  skippedUnchanged = false,
}) {
  const resolvedStageRepo = stageRepo || createPipelineStageRepo(client);
  const normalizedScope = normalizeStageScope(scope);
  const resolvedCodeFingerprint =
    codeFingerprint || (await computeCodeFingerprint(rootDir, codePaths));
  const resolvedInputFingerprint =
    inputFingerprint ||
    (await getStageInputFingerprint(client, stageId, normalizedScope));
  const runId = crypto.randomUUID();
  const startedAt = Date.now();
  let stagePersistenceEnabled = true;

  try {
    await resolvedStageRepo.startRun({
      runId,
      stageId,
      scope: normalizedScope,
      inputFingerprint: resolvedInputFingerprint,
      codeFingerprint: resolvedCodeFingerprint,
      startedAt,
    });
  } catch {
    stagePersistenceEnabled = false;
  }

  try {
    const result = await execute({
      runId,
      scope: normalizedScope,
      inputFingerprint: resolvedInputFingerprint,
      codeFingerprint: resolvedCodeFingerprint,
    });
    const finishedAt = Date.now();
    const resolvedSummary =
      summary ||
      result?.summary ||
      (await getStageSummary(client, stageId, normalizedScope));
    const timingSummary = collectTimingSummary({
      startedAt,
      finishedAt,
      result,
      cacheHit,
      skippedUnchanged,
    });
    const metrics = {
      ...(result?.metrics && typeof result.metrics === "object"
        ? result.metrics
        : {}),
      cacheHit: Boolean(cacheHit),
      skippedUnchanged: Boolean(skippedUnchanged),
    };

    if (stagePersistenceEnabled) {
      await resolvedStageRepo.finishRun({
        runId,
        stageId,
        scope: normalizedScope,
        status: "ready",
        inputFingerprint: resolvedInputFingerprint,
        codeFingerprint: resolvedCodeFingerprint,
        outputSummary: resolvedSummary,
        timingSummary,
        metrics,
        startedAt,
        finishedAt,
      });
    }

    return {
      ...(result && typeof result === "object" ? result : {}),
      summary: resolvedSummary,
      metrics,
      inputFingerprint: resolvedInputFingerprint,
      codeFingerprint: resolvedCodeFingerprint,
      stageRunId: runId,
      cacheHit: Boolean(cacheHit),
      skippedUnchanged: Boolean(skippedUnchanged),
    };
  } catch (error) {
    const finishedAt = Date.now();
    if (stagePersistenceEnabled) {
      await resolvedStageRepo.finishRun({
        runId,
        stageId,
        scope: normalizedScope,
        status: "failed",
        inputFingerprint: resolvedInputFingerprint,
        codeFingerprint: resolvedCodeFingerprint,
        outputSummary: {},
        timingSummary: {
          totalDurationMs: Math.max(0, finishedAt - startedAt),
          cacheHit: false,
          skippedUnchanged: false,
        },
        metrics: {
          errorMessage: error.message,
        },
        startedAt,
        finishedAt,
      });
    }
    throw error;
  }
}

module.exports = {
  getStageInputFingerprint,
  getStageSummary,
  runTrackedStage,
};
