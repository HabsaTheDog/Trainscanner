const { ensureAiEvaluationSchema } = require("../../data/postgis/repositories/ai-evaluation-schema");

function toTimestampString(value) {
  if (!value) {
    return "";
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  return String(value);
}

function mapConfigRow(row) {
  if (!row) {
    return null;
  }
  return {
    config_id: Number.parseInt(String(row.config_id || 0), 10) || 0,
    config_key: row.config_key,
    version: Number.parseInt(String(row.version || 0), 10) || 0,
    name: row.name,
    description: row.description || "",
    provider: row.provider || "litellm",
    model: row.model || "",
    model_params:
      row.model_params && typeof row.model_params === "object"
        ? row.model_params
        : {},
    system_prompt: row.system_prompt || "",
    context_sections: Array.isArray(row.context_sections)
      ? row.context_sections
      : [],
    context_preamble: row.context_preamble || "",
    created_by: row.created_by || "qa_operator",
    created_at: toTimestampString(row.created_at),
  };
}

function mapRunItemRow(row) {
  return {
    run_item_id: Number.parseInt(String(row.run_item_id || 0), 10) || 0,
    run_id: row.run_id || "",
    merge_cluster_id: row.merge_cluster_id || "",
    item_status: row.item_status || "queued",
    truth_snapshot:
      row.truth_snapshot && typeof row.truth_snapshot === "object"
        ? row.truth_snapshot
        : {},
    input_context_snapshot:
      row.input_context_snapshot && typeof row.input_context_snapshot === "object"
        ? row.input_context_snapshot
        : {},
    prompt_snapshot:
      row.prompt_snapshot && typeof row.prompt_snapshot === "object"
        ? row.prompt_snapshot
        : {},
    raw_model_response:
      row.raw_model_response && typeof row.raw_model_response === "object"
        ? row.raw_model_response
        : null,
    normalized_prediction:
      row.normalized_prediction && typeof row.normalized_prediction === "object"
        ? row.normalized_prediction
        : null,
    comparison:
      row.comparison && typeof row.comparison === "object" ? row.comparison : {},
    token_usage:
      row.token_usage && typeof row.token_usage === "object" ? row.token_usage : {},
    estimated_cost_usd:
      row.estimated_cost_usd === null || row.estimated_cost_usd === undefined
        ? null
        : Number(row.estimated_cost_usd),
    latency_ms:
      row.latency_ms === null || row.latency_ms === undefined
        ? null
        : Number.parseInt(String(row.latency_ms), 10),
    error_message: row.error_message || "",
    created_at: toTimestampString(row.created_at),
    updated_at: toTimestampString(row.updated_at),
  };
}

function mapRunRow(row, items = null) {
  if (!row) {
    return null;
  }
  return {
    run_id: row.run_id || "",
    mode: row.mode || "",
    status: row.status || "",
    dataset_source: row.dataset_source || null,
    gold_set_id:
      row.gold_set_id === null || row.gold_set_id === undefined
        ? null
        : Number.parseInt(String(row.gold_set_id), 10),
    config_id:
      row.config_id === null || row.config_id === undefined
        ? null
        : Number.parseInt(String(row.config_id), 10),
    config_snapshot:
      row.config_snapshot && typeof row.config_snapshot === "object"
        ? row.config_snapshot
        : {},
    filters: row.filters && typeof row.filters === "object" ? row.filters : {},
    summary_metrics:
      row.summary_metrics && typeof row.summary_metrics === "object"
        ? row.summary_metrics
        : {},
    progress:
      row.progress && typeof row.progress === "object" ? row.progress : {},
    requested_by: row.requested_by || "qa_operator",
    error_message: row.error_message || "",
    temporal_workflow_id: row.temporal_workflow_id || "",
    created_at: toTimestampString(row.created_at),
    started_at: toTimestampString(row.started_at),
    ended_at: toTimestampString(row.ended_at),
    items,
  };
}

function mapGoldSetRow(row, items = null) {
  if (!row) {
    return null;
  }
  return {
    gold_set_id: Number.parseInt(String(row.gold_set_id || 0), 10) || 0,
    slug: row.slug || "",
    name: row.name || "",
    description: row.description || "",
    is_frozen: row.is_frozen === true,
    created_by: row.created_by || "qa_operator",
    created_at: toTimestampString(row.created_at),
    updated_at: toTimestampString(row.updated_at),
    items,
  };
}

function createAiEvaluationRepo(client) {
  async function ensureReady() {
    await ensureAiEvaluationSchema(client);
  }

  return {
    ensureReady,

    async listConfigs() {
      await ensureReady();
      const rows = await client.queryRows(
        `
        SELECT *
        FROM ai_eval_configs
        ORDER BY config_key ASC, version DESC;
        `,
      );
      return rows.map(mapConfigRow);
    },

    async getConfig(configKey, version = null) {
      await ensureReady();
      const params = {
        config_key: configKey,
      };
      const row = await client.queryOne(
        version === null
          ? `
            SELECT *
            FROM ai_eval_configs
            WHERE config_key = :'config_key'
            ORDER BY version DESC
            LIMIT 1;
          `
          : `
            SELECT *
            FROM ai_eval_configs
            WHERE config_key = :'config_key'
              AND version = :'version'::integer
            LIMIT 1;
          `,
        version === null ? params : { ...params, version },
      );
      return mapConfigRow(row);
    },

    async createConfigVersion(input) {
      await ensureReady();
      const versionRow = await client.queryOne(
        `
        SELECT COALESCE(MAX(version), 0) + 1 AS next_version
        FROM ai_eval_configs
        WHERE config_key = :'config_key';
        `,
        {
          config_key: input.config_key,
        },
      );
      const row = await client.queryOne(
        `
        INSERT INTO ai_eval_configs (
          config_key,
          version,
          name,
          description,
          provider,
          model,
          model_params,
          system_prompt,
          context_sections,
          context_preamble,
          created_by
        )
        VALUES (
          :'config_key',
          :'version'::integer,
          :'name',
          :'description',
          :'provider',
          :'model',
          COALESCE(NULLIF(:'model_params', '')::jsonb, '{}'::jsonb),
          :'system_prompt',
          COALESCE(NULLIF(:'context_sections', '')::jsonb, '[]'::jsonb),
          :'context_preamble',
          :'created_by'
        )
        RETURNING *;
        `,
        {
          ...input,
          version: Number.parseInt(String(versionRow?.next_version || 1), 10) || 1,
          model_params: JSON.stringify(input.model_params || {}),
          context_sections: JSON.stringify(input.context_sections || []),
        },
      );
      return mapConfigRow(row);
    },

    async listGoldSets() {
      await ensureReady();
      const rows = await client.queryRows(
        `
        SELECT *
        FROM ai_eval_gold_sets
        ORDER BY updated_at DESC, gold_set_id DESC;
        `,
      );
      return rows.map((row) => mapGoldSetRow(row));
    },

    async getGoldSet(goldSetId) {
      await ensureReady();
      const row = await client.queryOne(
        `
        SELECT *
        FROM ai_eval_gold_sets
        WHERE gold_set_id = :'gold_set_id'::bigint
        LIMIT 1;
        `,
        { gold_set_id: goldSetId },
      );
      if (!row) {
        return null;
      }
      const itemRows = await client.queryRows(
        `
        SELECT
          gold_set_id,
          merge_cluster_id,
          note,
          truth_snapshot,
          to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS created_at
        FROM ai_eval_gold_set_items
        WHERE gold_set_id = :'gold_set_id'::bigint
        ORDER BY merge_cluster_id ASC;
        `,
        { gold_set_id: goldSetId },
      );
      return mapGoldSetRow(row, itemRows.map((item) => ({
        gold_set_id: Number.parseInt(String(item.gold_set_id || 0), 10) || 0,
        merge_cluster_id: item.merge_cluster_id || "",
        note: item.note || "",
        truth_snapshot:
          item.truth_snapshot && typeof item.truth_snapshot === "object"
            ? item.truth_snapshot
            : {},
        created_at: item.created_at || "",
      })));
    },

    async createGoldSet(input) {
      await ensureReady();
      const row = await client.queryOne(
        `
        INSERT INTO ai_eval_gold_sets (
          slug,
          name,
          description,
          created_by
        )
        VALUES (
          :'slug',
          :'name',
          :'description',
          :'created_by'
        )
        RETURNING *;
        `,
        input,
      );
      return mapGoldSetRow(row, []);
    },

    async replaceGoldSetItems(goldSetId, items) {
      await ensureReady();
      await client.runSql(
        `
        DELETE FROM ai_eval_gold_set_items
        WHERE gold_set_id = :'gold_set_id'::bigint;
        `,
        { gold_set_id: goldSetId },
      );
      for (const item of items) {
        await client.runSql(
          `
          INSERT INTO ai_eval_gold_set_items (
            gold_set_id,
            merge_cluster_id,
            note,
            truth_snapshot
          )
          VALUES (
            :'gold_set_id'::bigint,
            :'merge_cluster_id',
            :'note',
            COALESCE(NULLIF(:'truth_snapshot', '')::jsonb, '{}'::jsonb)
          );
          `,
          {
            gold_set_id: goldSetId,
            merge_cluster_id: item.merge_cluster_id,
            note: item.note || "",
            truth_snapshot: JSON.stringify(item.truth_snapshot || {}),
          },
        );
      }
      await client.runSql(
        `
        UPDATE ai_eval_gold_sets
        SET updated_at = now()
        WHERE gold_set_id = :'gold_set_id'::bigint;
        `,
        { gold_set_id: goldSetId },
      );
      return this.getGoldSet(goldSetId);
    },

    async createRun(input) {
      await ensureReady();
      const row = await client.queryOne(
        `
        INSERT INTO ai_eval_runs (
          run_id,
          mode,
          status,
          dataset_source,
          gold_set_id,
          config_id,
          config_snapshot,
          filters,
          summary_metrics,
          progress,
          requested_by,
          temporal_workflow_id
        )
        VALUES (
          :'run_id'::uuid,
          :'mode'::ai_eval_run_mode,
          :'status'::ai_eval_run_status,
          NULLIF(:'dataset_source', '')::ai_eval_dataset_source,
          NULLIF(:'gold_set_id', '')::bigint,
          NULLIF(:'config_id', '')::bigint,
          COALESCE(NULLIF(:'config_snapshot', '')::jsonb, '{}'::jsonb),
          COALESCE(NULLIF(:'filters', '')::jsonb, '{}'::jsonb),
          COALESCE(NULLIF(:'summary_metrics', '')::jsonb, '{}'::jsonb),
          COALESCE(NULLIF(:'progress', '')::jsonb, '{}'::jsonb),
          :'requested_by',
          NULLIF(:'temporal_workflow_id', '')
        )
        RETURNING *;
        `,
        {
          ...input,
          gold_set_id: input.gold_set_id ?? "",
          config_id: input.config_id ?? "",
          dataset_source: input.dataset_source || "",
          config_snapshot: JSON.stringify(input.config_snapshot || {}),
          filters: JSON.stringify(input.filters || {}),
          summary_metrics: JSON.stringify(input.summary_metrics || {}),
          progress: JSON.stringify(input.progress || {}),
          temporal_workflow_id: input.temporal_workflow_id || "",
        },
      );
      return mapRunRow(row, []);
    },

    async updateRun(runId, patch) {
      await ensureReady();
      const row = await client.queryOne(
        `
        UPDATE ai_eval_runs
        SET
          status = COALESCE(NULLIF(:'status', '')::ai_eval_run_status, status),
          summary_metrics = COALESCE(NULLIF(:'summary_metrics', '')::jsonb, summary_metrics),
          progress = COALESCE(NULLIF(:'progress', '')::jsonb, progress),
          error_message = COALESCE(NULLIF(:'error_message', ''), error_message),
          temporal_workflow_id = COALESCE(NULLIF(:'temporal_workflow_id', ''), temporal_workflow_id),
          started_at = CASE
            WHEN :'set_started' = 'true' THEN COALESCE(started_at, now())
            ELSE started_at
          END,
          ended_at = CASE
            WHEN :'set_ended' = 'true' THEN now()
            ELSE ended_at
          END
        WHERE run_id = :'run_id'::uuid
        RETURNING *;
        `,
        {
          run_id: runId,
          status: patch.status || "",
          summary_metrics: patch.summary_metrics
            ? JSON.stringify(patch.summary_metrics)
            : "",
          progress: patch.progress ? JSON.stringify(patch.progress) : "",
          error_message: patch.error_message || "",
          temporal_workflow_id: patch.temporal_workflow_id || "",
          set_started: patch.set_started === true ? "true" : "false",
          set_ended: patch.set_ended === true ? "true" : "false",
        },
      );
      return mapRunRow(row);
    },

    async listRuns(limit = 20, filters = {}) {
      await ensureReady();
      const rows = await client.queryRows(
        `
        SELECT *
        FROM ai_eval_runs
        WHERE (NULLIF(:'status', '') IS NULL OR status = NULLIF(:'status', '')::ai_eval_run_status)
          AND (NULLIF(:'mode', '') IS NULL OR mode = NULLIF(:'mode', '')::ai_eval_run_mode)
        ORDER BY created_at DESC
        LIMIT :'limit_rows'::integer;
        `,
        {
          status: filters.status || "",
          mode: filters.mode || "",
          limit_rows: String(limit),
        },
      );
      return rows.map((row) => mapRunRow(row));
    },

    async getRun(runId) {
      await ensureReady();
      const row = await client.queryOne(
        `
        SELECT *
        FROM ai_eval_runs
        WHERE run_id = :'run_id'::uuid
        LIMIT 1;
        `,
        { run_id: runId },
      );
      if (!row) {
        return null;
      }
      const itemRows = await client.queryRows(
        `
        SELECT
          run_item_id,
          run_id::text,
          merge_cluster_id,
          item_status,
          truth_snapshot,
          input_context_snapshot,
          prompt_snapshot,
          raw_model_response,
          normalized_prediction,
          comparison,
          token_usage,
          estimated_cost_usd,
          latency_ms,
          error_message,
          to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS created_at,
          to_char(updated_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS updated_at
        FROM ai_eval_run_items
        WHERE run_id = :'run_id'::uuid
        ORDER BY run_item_id ASC;
        `,
        { run_id: runId },
      );
      return mapRunRow(row, itemRows.map(mapRunItemRow));
    },

    async listRunItems(runId) {
      await ensureReady();
      const rows = await client.queryRows(
        `
        SELECT
          run_item_id,
          run_id::text,
          merge_cluster_id,
          item_status,
          truth_snapshot,
          input_context_snapshot,
          prompt_snapshot,
          raw_model_response,
          normalized_prediction,
          comparison,
          token_usage,
          estimated_cost_usd,
          latency_ms,
          error_message,
          to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS created_at,
          to_char(updated_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS updated_at
        FROM ai_eval_run_items
        WHERE run_id = :'run_id'::uuid
        ORDER BY run_item_id ASC;
        `,
        { run_id: runId },
      );
      return rows.map(mapRunItemRow);
    },

    async replaceRunItems(runId, items) {
      await ensureReady();
      await client.runSql(
        `
        DELETE FROM ai_eval_run_items
        WHERE run_id = :'run_id'::uuid;
        `,
        { run_id: runId },
      );
      for (const item of items) {
        await client.runSql(
          `
          INSERT INTO ai_eval_run_items (
            run_id,
            merge_cluster_id,
            item_status,
            truth_snapshot,
            input_context_snapshot,
            prompt_snapshot,
            comparison,
            token_usage
          )
          VALUES (
            :'run_id'::uuid,
            :'merge_cluster_id',
            :'item_status',
            COALESCE(NULLIF(:'truth_snapshot', '')::jsonb, '{}'::jsonb),
            COALESCE(NULLIF(:'input_context_snapshot', '')::jsonb, '{}'::jsonb),
            COALESCE(NULLIF(:'prompt_snapshot', '')::jsonb, '{}'::jsonb),
            '{}'::jsonb,
            '{}'::jsonb
          );
          `,
          {
            run_id: runId,
            merge_cluster_id: item.merge_cluster_id,
            item_status: item.item_status || "queued",
            truth_snapshot: JSON.stringify(item.truth_snapshot || {}),
            input_context_snapshot: JSON.stringify(item.input_context_snapshot || {}),
            prompt_snapshot: JSON.stringify(item.prompt_snapshot || {}),
          },
        );
      }
    },

    async updateRunItem(runItemId, patch) {
      await ensureReady();
      await client.runSql(
        `
        UPDATE ai_eval_run_items
        SET
          item_status = COALESCE(NULLIF(:'item_status', ''), item_status),
          raw_model_response = CASE
            WHEN :'raw_model_response' = '' THEN raw_model_response
            ELSE COALESCE(NULLIF(:'raw_model_response', '')::jsonb, raw_model_response)
          END,
          normalized_prediction = CASE
            WHEN :'normalized_prediction' = '' THEN normalized_prediction
            ELSE COALESCE(NULLIF(:'normalized_prediction', '')::jsonb, normalized_prediction)
          END,
          comparison = CASE
            WHEN :'comparison' = '' THEN comparison
            ELSE COALESCE(NULLIF(:'comparison', '')::jsonb, comparison)
          END,
          token_usage = CASE
            WHEN :'token_usage' = '' THEN token_usage
            ELSE COALESCE(NULLIF(:'token_usage', '')::jsonb, token_usage)
          END,
          estimated_cost_usd = COALESCE(NULLIF(:'estimated_cost_usd', '')::numeric, estimated_cost_usd),
          latency_ms = COALESCE(NULLIF(:'latency_ms', '')::integer, latency_ms),
          error_message = COALESCE(:'error_message', error_message),
          updated_at = now()
        WHERE run_item_id = :'run_item_id'::bigint;
        `,
        {
          run_item_id: runItemId,
          item_status: patch.item_status || "",
          raw_model_response: patch.raw_model_response
            ? JSON.stringify(patch.raw_model_response)
            : "",
          normalized_prediction: patch.normalized_prediction
            ? JSON.stringify(patch.normalized_prediction)
            : "",
          comparison: patch.comparison ? JSON.stringify(patch.comparison) : "",
          token_usage: patch.token_usage ? JSON.stringify(patch.token_usage) : "",
          estimated_cost_usd:
            patch.estimated_cost_usd === undefined ||
            patch.estimated_cost_usd === null
              ? ""
              : String(patch.estimated_cost_usd),
          latency_ms:
            patch.latency_ms === undefined || patch.latency_ms === null
              ? ""
              : String(patch.latency_ms),
          error_message:
            patch.error_message === undefined ? null : patch.error_message,
        },
      );
    },

    async listBenchmarkCandidates(filters = {}) {
      await ensureReady();
      const rows = await client.queryRows(
        `
        SELECT merge_cluster_id AS cluster_id
        FROM qa_merge_clusters
        WHERE status IN ('resolved', 'dismissed')
          AND (NULLIF(:'country', '') IS NULL OR NULLIF(:'country', '') = ANY (COALESCE(country_tags, ARRAY[]::text[])))
          AND (NULLIF(:'severity', '') IS NULL OR severity = NULLIF(:'severity', ''))
        ORDER BY resolved_at DESC NULLS LAST, updated_at DESC, merge_cluster_id ASC
        LIMIT :'limit_rows'::integer;
        `,
        {
          country: filters.country || "",
          severity: filters.severity || "",
          limit_rows: String(filters.limit || 100),
        },
      );
      return rows.map((row) => row.cluster_id).filter(Boolean);
    },

    async listGoldSetClusterIds(goldSetId) {
      await ensureReady();
      const rows = await client.queryRows(
        `
        SELECT merge_cluster_id
        FROM ai_eval_gold_set_items
        WHERE gold_set_id = :'gold_set_id'::bigint
        ORDER BY merge_cluster_id ASC;
        `,
        { gold_set_id: goldSetId },
      );
      return rows.map((row) => row.merge_cluster_id).filter(Boolean);
    },
  };
}

module.exports = {
  createAiEvaluationRepo,
};
