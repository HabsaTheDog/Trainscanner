const crypto = require("node:crypto");
const fs = require("node:fs");

const { Connection, Client } = require("@temporalio/client");

const { AppError } = require("../../core/errors");
const { resolveTemporalAddress } = require("../../core/runtime");
const { createPostgisClient } = require("../../data/postgis/client");
const { getGlobalClusterDetail } = require("../qa/api");
const {
  normalizeBenchmarkInput,
  normalizeClusterIdList,
  normalizeConfigDraft,
  normalizeGoldSetInput,
  normalizePreviewInput,
} = require("./contracts");
const { buildPromptContext, buildPromptSnapshot } = require("./context-builder");
const { aggregateRunMetrics, comparePrediction } = require("./metrics");
const { createAiEvaluationRepo } = require("./repo");
const { buildTruthSnapshot } = require("./truth");

function resolveAiServiceUrl(explicitUrl) {
  if (explicitUrl) {
    return String(explicitUrl).replace(/\/$/, "");
  }
  if (fs.existsSync("/.dockerenv")) {
    return "http://ai-scoring:8000";
  }
  return "http://localhost:8000";
}

function createAiEvaluationService(options = {}) {
  let dbClient = options.dbClient || null;
  let repo = options.repo || null;
  const aiServiceUrl = resolveAiServiceUrl(
    options.aiServiceUrl || process.env.AI_SCORING_URL,
  );
  const temporalAddress = options.temporalAddress || resolveTemporalAddress();

  async function getRepo() {
    if (repo) {
      return repo;
    }
    if (!dbClient) {
      dbClient = createPostgisClient({
        rootDir: options.rootDir || process.cwd(),
      });
      await dbClient.ensureReady();
    }
    repo = createAiEvaluationRepo(dbClient);
    await repo.ensureReady();
    return repo;
  }

  async function callAiService(payload) {
    const response = await fetch(`${aiServiceUrl}/evaluate-cluster`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new AppError({
        code: "INTERNAL_ERROR",
        statusCode: 502,
        message: body?.detail || body?.error || "AI evaluation request failed",
      });
    }
    return body;
  }

  async function resolveConfigSnapshot(previewInput) {
    if (previewInput.draft_config) {
      return normalizeConfigDraft(previewInput.draft_config);
    }
    const evaluationRepo = await getRepo();
    const config = await evaluationRepo.getConfig(
      previewInput.config_key,
      previewInput.version,
    );
    if (!config) {
      throw new AppError({
        code: "NOT_FOUND",
        statusCode: 404,
        message: "Evaluation config not found",
      });
    }
    return config;
  }

  async function buildEvaluationArtifacts(clusterId, configSnapshot) {
    const clusterDetail = await getGlobalClusterDetail(clusterId);
    if (!clusterDetail) {
      throw new AppError({
        code: "NOT_FOUND",
        statusCode: 404,
        message: "Cluster not found",
      });
    }
    const truthSnapshot = buildTruthSnapshot(clusterDetail);
    const inputContext = buildPromptContext(clusterDetail, configSnapshot);
    const promptSnapshot = buildPromptSnapshot(configSnapshot, inputContext);
    return {
      clusterDetail,
      truthSnapshot,
      inputContext,
      promptSnapshot,
    };
  }

  async function createPreviewRunRecord(configSnapshot, clusterId, requestedBy) {
    const evaluationRepo = await getRepo();
    const runId = crypto.randomUUID();
    return evaluationRepo.createRun({
      run_id: runId,
      mode: "preview",
      status: "queued",
      dataset_source: null,
      gold_set_id: null,
      config_id: configSnapshot.config_id || null,
      config_snapshot: configSnapshot,
      filters: { cluster_id: clusterId },
      requested_by: requestedBy,
      progress: { total_items: 1, completed_items: 0 },
    });
  }

  async function runPreview(clusterId, input) {
    const previewInput = normalizePreviewInput(input);
    const configSnapshot = await resolveConfigSnapshot(previewInput);
    const evaluationRepo = await getRepo();
    const run = await createPreviewRunRecord(
      configSnapshot,
      clusterId,
      previewInput.requested_by,
    );
    const { truthSnapshot, inputContext, promptSnapshot } =
      await buildEvaluationArtifacts(clusterId, configSnapshot);
    await evaluationRepo.replaceRunItems(run.run_id, [
      {
        merge_cluster_id: clusterId,
        truth_snapshot: truthSnapshot,
        input_context_snapshot: inputContext,
        prompt_snapshot: promptSnapshot,
      },
    ]);
    await evaluationRepo.updateRun(run.run_id, {
      status: "running",
      set_started: true,
      progress: { total_items: 1, completed_items: 0 },
    });

    try {
      const aiResult = await callAiService({
        mode: "preview",
        cluster_id: clusterId,
        config: configSnapshot,
        input_context: inputContext,
      });
      const comparison = comparePrediction(
        truthSnapshot,
        aiResult.normalized_prediction,
      );
      const items = await evaluationRepo.listRunItems(run.run_id);
      await evaluationRepo.updateRunItem(items[0].run_item_id, {
        item_status: "succeeded",
        raw_model_response: aiResult.raw_model_response || aiResult,
        normalized_prediction: aiResult.normalized_prediction,
        comparison,
        token_usage: aiResult.token_usage || {},
        estimated_cost_usd: aiResult.estimated_cost_usd ?? null,
        latency_ms: aiResult.latency_ms ?? null,
      });
      const updatedItems = await evaluationRepo.listRunItems(run.run_id);
      const summaryMetrics = aggregateRunMetrics(updatedItems);
      await evaluationRepo.updateRun(run.run_id, {
        status: "succeeded",
        set_ended: true,
        summary_metrics: summaryMetrics,
        progress: { total_items: 1, completed_items: 1 },
      });
      const fullRun = await evaluationRepo.getRun(run.run_id);
      return {
        run: fullRun,
        result: updatedItems[0],
      };
    } catch (error) {
      const items = await evaluationRepo.listRunItems(run.run_id);
      if (items[0]) {
        await evaluationRepo.updateRunItem(items[0].run_item_id, {
          item_status: "failed",
          error_message: error.message,
        });
      }
      await evaluationRepo.updateRun(run.run_id, {
        status: "failed",
        set_ended: true,
        error_message: error.message,
        progress: { total_items: 1, completed_items: 1 },
      });
      throw error;
    }
  }

  async function listConfigs() {
    return (await getRepo()).listConfigs();
  }

  async function getConfig(configKey, version) {
    return (await getRepo()).getConfig(configKey, version ?? null);
  }

  async function createConfigVersion(input) {
    return (await getRepo()).createConfigVersion(normalizeConfigDraft(input));
  }

  async function listRuns(filters = {}) {
    return (await getRepo()).listRuns(filters.limit || 20, filters);
  }

  async function getRun(runId) {
    return (await getRepo()).getRun(runId);
  }

  async function listGoldSets() {
    return (await getRepo()).listGoldSets();
  }

  async function getGoldSet(goldSetId) {
    return (await getRepo()).getGoldSet(goldSetId);
  }

  async function createGoldSet(input) {
    return (await getRepo()).createGoldSet(normalizeGoldSetInput(input));
  }

  async function replaceGoldSetItems(goldSetId, clusterIds) {
    const evaluationRepo = await getRepo();
    const normalizedIds = normalizeClusterIdList(clusterIds);
    const items = [];
    for (const clusterId of normalizedIds) {
      const detail = await getGlobalClusterDetail(clusterId);
      if (!detail) {
        throw new AppError({
          code: "NOT_FOUND",
          statusCode: 404,
          message: `Cluster '${clusterId}' not found`,
        });
      }
      items.push({
        merge_cluster_id: clusterId,
        truth_snapshot: buildTruthSnapshot(detail),
      });
    }
    return evaluationRepo.replaceGoldSetItems(goldSetId, items);
  }

  async function startBenchmark(input) {
    const normalizedInput = normalizeBenchmarkInput(input);
    const evaluationRepo = await getRepo();
    const configSnapshot = await evaluationRepo.getConfig(
      normalizedInput.config_key,
      normalizedInput.version,
    );
    if (!configSnapshot) {
      throw new AppError({
        code: "NOT_FOUND",
        statusCode: 404,
        message: "Benchmark config not found",
      });
    }
    const runId = crypto.randomUUID();
    const workflowId = `ai-evaluation-${runId}`;
    const run = await evaluationRepo.createRun({
      run_id: runId,
      mode: "benchmark",
      status: "queued",
      dataset_source: normalizedInput.dataset_source,
      gold_set_id: normalizedInput.gold_set_id,
      config_id: configSnapshot.config_id,
      config_snapshot: configSnapshot,
      filters: normalizedInput.filters,
      requested_by: normalizedInput.requested_by,
      progress: { total_items: 0, completed_items: 0 },
      temporal_workflow_id: workflowId,
    });
    try {
      const connection = await Connection.connect({
        address: temporalAddress,
      });
      const client = new Client({ connection });
      await client.workflow.start("aiEvaluationBenchmark", {
        taskQueue: "review-pipeline",
        workflowId,
        args: [{ runId }],
      });
    } catch (error) {
      // Fall back to an in-process async run so the feature still works when
      // Temporal is not reachable in local environments.
      queueMicrotask(() => {
        void processBenchmarkRun(runId);
      });
    }
    return evaluationRepo.getRun(run.run_id);
  }

  async function resolveBenchmarkClusterIds(run) {
    const evaluationRepo = await getRepo();
    const filters = run.filters || {};
    const datasetSource = run.dataset_source || "resolved_history";
    const ids = [];
    if (datasetSource === "resolved_history" || datasetSource === "combined") {
      ids.push(...(await evaluationRepo.listBenchmarkCandidates(filters)));
    }
    if (
      (datasetSource === "gold_set" || datasetSource === "combined") &&
      run.gold_set_id
    ) {
      ids.push(...(await evaluationRepo.listGoldSetClusterIds(run.gold_set_id)));
    }
    return Array.from(new Set(ids)).slice(0, Number(filters.limit || 100));
  }

  async function processBenchmarkRun(runId) {
    const evaluationRepo = await getRepo();
    let run = await evaluationRepo.getRun(runId);
    if (!run) {
      throw new AppError({
        code: "NOT_FOUND",
        statusCode: 404,
        message: "Benchmark run not found",
      });
    }
    await evaluationRepo.updateRun(runId, {
      status: "running",
      set_started: true,
    });
    run = await evaluationRepo.getRun(runId);
    let items = Array.isArray(run.items) ? run.items : [];
    if (items.length === 0) {
      const clusterIds = await resolveBenchmarkClusterIds(run);
      const newItems = [];
      for (const clusterId of clusterIds) {
        const { truthSnapshot, inputContext, promptSnapshot } =
          await buildEvaluationArtifacts(clusterId, run.config_snapshot);
        newItems.push({
          merge_cluster_id: clusterId,
          truth_snapshot: truthSnapshot,
          input_context_snapshot: inputContext,
          prompt_snapshot: promptSnapshot,
        });
      }
      await evaluationRepo.replaceRunItems(runId, newItems);
      await evaluationRepo.updateRun(runId, {
        progress: {
          total_items: newItems.length,
          completed_items: 0,
        },
      });
      items = await evaluationRepo.listRunItems(runId);
    }

    let completed = 0;
    for (const item of items) {
      try {
        const aiResult = await callAiService({
          mode: "benchmark",
          cluster_id: item.merge_cluster_id,
          config: run.config_snapshot,
          input_context: item.input_context_snapshot,
        });
        const comparison = comparePrediction(
          item.truth_snapshot,
          aiResult.normalized_prediction,
        );
        await evaluationRepo.updateRunItem(item.run_item_id, {
          item_status: "succeeded",
          raw_model_response: aiResult.raw_model_response || aiResult,
          normalized_prediction: aiResult.normalized_prediction,
          comparison,
          token_usage: aiResult.token_usage || {},
          estimated_cost_usd: aiResult.estimated_cost_usd ?? null,
          latency_ms: aiResult.latency_ms ?? null,
          error_message: "",
        });
      } catch (error) {
        await evaluationRepo.updateRunItem(item.run_item_id, {
          item_status: "failed",
          error_message: error.message,
        });
      }
      completed += 1;
      const updatedItems = await evaluationRepo.listRunItems(runId);
      await evaluationRepo.updateRun(runId, {
        progress: {
          total_items: items.length,
          completed_items: completed,
        },
        summary_metrics: aggregateRunMetrics(updatedItems),
      });
    }
    const finalItems = await evaluationRepo.listRunItems(runId);
    await evaluationRepo.updateRun(runId, {
      status: "succeeded",
      set_ended: true,
      progress: {
        total_items: items.length,
        completed_items: items.length,
      },
      summary_metrics: aggregateRunMetrics(finalItems),
    });
    return evaluationRepo.getRun(runId);
  }

  return {
    createConfigVersion,
    createGoldSet,
    getConfig,
    getGoldSet,
    getRun,
    listConfigs,
    listGoldSets,
    listRuns,
    processBenchmarkRun,
    replaceGoldSetItems,
    runPreview,
    startBenchmark,
  };
}

const defaultService = createAiEvaluationService();

module.exports = {
  createAiEvaluationService,
  defaultService,
};
