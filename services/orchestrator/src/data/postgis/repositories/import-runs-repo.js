const { validateOrThrow } = require("../../../core/schema");

const IMPORT_RUN_SCHEMA = {
  type: "object",
  required: ["runId", "pipeline", "status"],
  properties: {
    runId: { type: "string", minLength: 1 },
    pipeline: { type: "string", minLength: 1 },
    status: { type: "string", enum: ["running", "succeeded", "failed"] },
    sourceId: { type: "string" },
    country: { type: "string" },
    snapshotDate: { type: "string" },
    startedAt: { type: "string" },
    endedAt: { type: "string" },
    errorCode: { type: "string" },
    errorMessage: { type: "string" },
    stats: { type: "object" },
  },
  additionalProperties: true,
};

function normalizeRunRow(row) {
  if (!row) {
    return null;
  }

  const normalized = {
    runId: row.run_id || row.runid || row.runId,
    pipeline: row.pipeline,
    status: row.status,
    sourceId: row.source_id || row.sourceid || row.sourceId || "",
    country: row.country || "",
    snapshotDate:
      row.snapshot_date || row.snapshotdate || row.snapshotDate || "",
    startedAt: row.started_at || row.startedat || row.startedAt || "",
    endedAt: row.ended_at || row.endedat || row.endedAt || "",
    errorCode: row.error_code || row.errorcode || row.errorCode || "",
    errorMessage:
      row.error_message || row.errormessage || row.errorMessage || "",
    stats: row.stats && typeof row.stats === "object" ? row.stats : {},
  };

  validateOrThrow(normalized, IMPORT_RUN_SCHEMA, {
    code: "INVALID_CONFIG",
    message: "Invalid import run row returned from repository",
  });

  return normalized;
}

function createImportRunsRepo(client) {
  return {
    async createRun(input) {
      const row = await client.queryOne(
        `
          INSERT INTO import_runs (run_id, pipeline, status, source_id, country, snapshot_date)
          VALUES (:'run_id'::uuid, :'pipeline', :'status', NULLIF(:'source_id', ''), NULLIF(:'country', '')::char(2), NULLIF(:'snapshot_date', '')::date)
          RETURNING
            run_id::text,
            pipeline,
            status,
            source_id,
            country::text,
            to_char(snapshot_date, 'YYYY-MM-DD') AS snapshot_date,
            to_char(started_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS started_at,
            to_char(ended_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS ended_at,
            error_message,
            stats;
        `,
        {
          run_id: input.runId,
          pipeline: input.pipeline,
          status: input.status || "running",
          source_id: input.sourceId || "",
          country: input.country || "",
          snapshot_date: input.snapshotDate || "",
        },
      );

      return normalizeRunRow(row);
    },

    async markFailed(input) {
      const row = await client.queryOne(
        `
          UPDATE import_runs
          SET
            status = 'failed',
            ended_at = now(),
            error_message = NULLIF(:'error_message', ''),
            stats = COALESCE(CASE WHEN NULLIF(:'stats_json', '') IS NULL THEN stats ELSE NULLIF(:'stats_json', '')::jsonb END, '{}'::jsonb)
          WHERE run_id = :'run_id'::uuid
          RETURNING
            run_id::text,
            pipeline,
            status,
            source_id,
            country::text,
            to_char(snapshot_date, 'YYYY-MM-DD') AS snapshot_date,
            to_char(started_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS started_at,
            to_char(ended_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS ended_at,
            error_message,
            stats;
        `,
        {
          run_id: input.runId,
          error_message: input.errorMessage || "",
          stats_json: input.stats ? JSON.stringify(input.stats) : "",
        },
      );

      return normalizeRunRow(row);
    },

    async markSucceeded(input) {
      const row = await client.queryOne(
        `
          UPDATE import_runs
          SET
            status = 'succeeded',
            ended_at = now(),
            error_message = NULL,
            stats = COALESCE(NULLIF(:'stats_json', '')::jsonb, stats)
          WHERE run_id = :'run_id'::uuid
          RETURNING
            run_id::text,
            pipeline,
            status,
            source_id,
            country::text,
            to_char(snapshot_date, 'YYYY-MM-DD') AS snapshot_date,
            to_char(started_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS started_at,
            to_char(ended_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS ended_at,
            error_message,
            stats;
        `,
        {
          run_id: input.runId,
          stats_json: input.stats ? JSON.stringify(input.stats) : "",
        },
      );

      return normalizeRunRow(row);
    },

    async getByRunId(runId) {
      const row = await client.queryOne(
        `
          SELECT
            run_id::text,
            pipeline,
            status,
            source_id,
            country::text,
            to_char(snapshot_date, 'YYYY-MM-DD') AS snapshot_date,
            to_char(started_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS started_at,
            to_char(ended_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS ended_at,
            error_message,
            stats
          FROM import_runs
          WHERE run_id = :'run_id'::uuid
          LIMIT 1;
        `,
        { run_id: runId },
      );

      return normalizeRunRow(row);
    },
  };
}

module.exports = {
  createImportRunsRepo,
  normalizeRunRow,
};
