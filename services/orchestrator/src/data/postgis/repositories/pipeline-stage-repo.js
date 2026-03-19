function normalizeStatus(status) {
  const value = String(status || "")
    .trim()
    .toLowerCase();
  if (["ready", "failed", "running", "stale"].includes(value)) {
    return value;
  }
  return "failed";
}

function normalizeJson(value, fallback = {}) {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value;
  }
  return fallback;
}

function coerceBigInt(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const parsed = Number.parseInt(String(value), 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function createPipelineStageRepo(client) {
  return {
    async getMaterialization(stageId, scopeKey) {
      return client.queryOne(
        `
        SELECT
          stage_id,
          scope_key,
          scope_country,
          scope_as_of,
          scope_source_id,
          status,
          input_fingerprint,
          code_fingerprint,
          output_summary,
          timing_summary,
          last_started_at,
          last_finished_at,
          updated_at
        FROM pipeline_stage_materializations
        WHERE stage_id = :'stage_id'
          AND scope_key = :'scope_key'
        `,
        {
          stage_id: stageId,
          scope_key: scopeKey,
        },
      );
    },

    async startRun({
      runId,
      stageId,
      scope,
      inputFingerprint,
      codeFingerprint,
      startedAt,
    }) {
      const normalizedScope = scope || {};
      const startedAtIso = new Date(startedAt || Date.now()).toISOString();
      const params = {
        run_id: runId,
        stage_id: stageId,
        scope_key: normalizedScope.scopeKey,
        scope_country: normalizedScope.country || "",
        scope_as_of: normalizedScope.asOf || "",
        scope_source_id: normalizedScope.sourceId || "",
        input_fingerprint: JSON.stringify(
          normalizeJson(inputFingerprint, { stage: stageId }),
        ),
        code_fingerprint: codeFingerprint,
        started_at: startedAtIso,
      };

      await client.runSql(
        `
        INSERT INTO pipeline_stage_runs (
          run_id,
          stage_id,
          scope_key,
          status,
          input_fingerprint,
          code_fingerprint,
          metrics,
          started_at
        )
        VALUES (
          :'run_id'::uuid,
          :'stage_id',
          :'scope_key',
          'running',
          :'input_fingerprint'::jsonb,
          :'code_fingerprint',
          '{}'::jsonb,
          :'started_at'::timestamptz
        )
        `,
        params,
      );

      await client.runSql(
        `
        INSERT INTO pipeline_stage_materializations (
          stage_id,
          scope_key,
          scope_country,
          scope_as_of,
          scope_source_id,
          status,
          input_fingerprint,
          code_fingerprint,
          output_summary,
          timing_summary,
          last_started_at,
          last_finished_at,
          updated_at
        )
        VALUES (
          :'stage_id',
          :'scope_key',
          NULLIF(:'scope_country', '')::iso_country_code,
          NULLIF(:'scope_as_of', '')::date,
          NULLIF(:'scope_source_id', ''),
          'running',
          :'input_fingerprint'::jsonb,
          :'code_fingerprint',
          '{}'::jsonb,
          '{}'::jsonb,
          :'started_at'::timestamptz,
          NULL,
          now()
        )
        ON CONFLICT (stage_id, scope_key)
        DO UPDATE SET
          scope_country = EXCLUDED.scope_country,
          scope_as_of = EXCLUDED.scope_as_of,
          scope_source_id = EXCLUDED.scope_source_id,
          status = 'running',
          input_fingerprint = EXCLUDED.input_fingerprint,
          code_fingerprint = EXCLUDED.code_fingerprint,
          last_started_at = EXCLUDED.last_started_at,
          updated_at = now()
        `,
        params,
      );
    },

    async finishRun({
      runId,
      stageId,
      scope,
      status,
      inputFingerprint,
      codeFingerprint,
      outputSummary,
      timingSummary,
      metrics,
      startedAt,
      finishedAt,
    }) {
      const normalizedScope = scope || {};
      const normalizedStatus = normalizeStatus(status);
      const finishedAtIso = new Date(finishedAt || Date.now()).toISOString();
      const startedAtIso = new Date(startedAt || Date.now()).toISOString();
      const normalizedMetrics = normalizeJson(metrics, {});
      const timing = normalizeJson(timingSummary, {});

      const params = {
        run_id: runId,
        stage_id: stageId,
        scope_key: normalizedScope.scopeKey,
        scope_country: normalizedScope.country || "",
        scope_as_of: normalizedScope.asOf || "",
        scope_source_id: normalizedScope.sourceId || "",
        status: normalizedStatus,
        input_fingerprint: JSON.stringify(
          normalizeJson(inputFingerprint, { stage: stageId }),
        ),
        code_fingerprint: codeFingerprint,
        output_summary: JSON.stringify(normalizeJson(outputSummary, {})),
        timing_summary: JSON.stringify(timing),
        metrics: JSON.stringify(normalizedMetrics),
        started_at: startedAtIso,
        finished_at: finishedAtIso,
        peak_rss_kb: coerceBigInt(
          normalizedMetrics.peakRssKb ?? normalizedMetrics.peak_rss_kb,
        ),
        disk_read_bytes: coerceBigInt(
          normalizedMetrics.diskReadBytes ?? normalizedMetrics.disk_read_bytes,
        ),
        disk_write_bytes: coerceBigInt(
          normalizedMetrics.diskWriteBytes ??
            normalizedMetrics.disk_write_bytes,
        ),
      };

      await client.runSql(
        `
        UPDATE pipeline_stage_runs
        SET
          status = :'status',
          input_fingerprint = :'input_fingerprint'::jsonb,
          code_fingerprint = :'code_fingerprint',
          metrics = :'metrics'::jsonb,
          finished_at = :'finished_at'::timestamptz,
          peak_rss_kb = NULLIF(:'peak_rss_kb', '')::bigint,
          disk_read_bytes = NULLIF(:'disk_read_bytes', '')::bigint,
          disk_write_bytes = NULLIF(:'disk_write_bytes', '')::bigint
        WHERE run_id = :'run_id'::uuid
        `,
        params,
      );

      await client.runSql(
        `
        INSERT INTO pipeline_stage_materializations (
          stage_id,
          scope_key,
          scope_country,
          scope_as_of,
          scope_source_id,
          status,
          input_fingerprint,
          code_fingerprint,
          output_summary,
          timing_summary,
          last_started_at,
          last_finished_at,
          updated_at
        )
        VALUES (
          :'stage_id',
          :'scope_key',
          NULLIF(:'scope_country', '')::iso_country_code,
          NULLIF(:'scope_as_of', '')::date,
          NULLIF(:'scope_source_id', ''),
          :'status',
          :'input_fingerprint'::jsonb,
          :'code_fingerprint',
          :'output_summary'::jsonb,
          :'timing_summary'::jsonb,
          :'started_at'::timestamptz,
          :'finished_at'::timestamptz,
          now()
        )
        ON CONFLICT (stage_id, scope_key)
        DO UPDATE SET
          scope_country = EXCLUDED.scope_country,
          scope_as_of = EXCLUDED.scope_as_of,
          scope_source_id = EXCLUDED.scope_source_id,
          status = EXCLUDED.status,
          input_fingerprint = EXCLUDED.input_fingerprint,
          code_fingerprint = EXCLUDED.code_fingerprint,
          output_summary = EXCLUDED.output_summary,
          timing_summary = EXCLUDED.timing_summary,
          last_started_at = EXCLUDED.last_started_at,
          last_finished_at = EXCLUDED.last_finished_at,
          updated_at = now()
        `,
        params,
      );
    },

    async listStageStatus(stageIds = []) {
      const stageFilterSql =
        Array.isArray(stageIds) && stageIds.length > 0
          ? "WHERE m.stage_id = ANY(:'stage_ids')"
          : "";
      return client.queryRows(
        `
        WITH latest_runs AS (
          SELECT DISTINCT ON (r.stage_id, r.scope_key)
            r.stage_id,
            r.scope_key,
            r.run_id,
            r.status AS run_status,
            r.metrics,
            r.started_at,
            r.finished_at
          FROM pipeline_stage_runs r
          ORDER BY r.stage_id, r.scope_key, r.started_at DESC, r.run_id DESC
        )
        SELECT
          m.stage_id,
          m.scope_key,
          m.scope_country,
          m.scope_as_of,
          m.scope_source_id,
          m.status,
          m.input_fingerprint,
          m.code_fingerprint,
          m.output_summary,
          m.timing_summary,
          m.last_started_at,
          m.last_finished_at,
          m.updated_at,
          lr.run_id AS last_run_id,
          lr.run_status,
          lr.metrics AS last_run_metrics,
          lr.started_at AS last_run_started_at,
          lr.finished_at AS last_run_finished_at
        FROM pipeline_stage_materializations m
        LEFT JOIN latest_runs lr
          ON lr.stage_id = m.stage_id
         AND lr.scope_key = m.scope_key
        ${stageFilterSql}
        ORDER BY
          m.stage_id ASC,
          m.scope_country NULLS FIRST,
          m.scope_as_of NULLS FIRST,
          m.scope_source_id NULLS FIRST,
          m.scope_key ASC
        `,
        {
          stage_ids: stageIds,
        },
      );
    },
  };
}

module.exports = {
  createPipelineStageRepo,
};
