const crypto = require("node:crypto");

const { AppError } = require("../../core/errors");

const VALID_DATASET_SOURCES = new Set([
  "resolved_history",
  "gold_set",
  "combined",
]);
const VALID_RUN_MODES = new Set(["preview", "benchmark"]);
const VALID_CONTEXT_SECTIONS = new Set([
  "cluster_summary",
  "candidate_core",
  "aliases",
  "provenance",
  "network_context",
  "network_summary",
  "external_reference_summary",
  "external_reference_matches",
  "evidence_summary",
  "pair_summaries",
  "cluster_metadata",
]);

function invalid(message) {
  throw new AppError({
    code: "INVALID_REQUEST",
    statusCode: 400,
    message,
  });
}

function normalizeString(value) {
  return String(value || "").trim();
}

function normalizeStringArray(values) {
  const out = [];
  const seen = new Set();
  for (const value of Array.isArray(values) ? values : []) {
    const clean = normalizeString(value);
    if (!clean || seen.has(clean)) {
      continue;
    }
    seen.add(clean);
    out.push(clean);
  }
  return out;
}

function parseJsonObject(value, fallback = {}) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return fallback;
  }
  return value;
}

function normalizeContextSections(values) {
  const sections = normalizeStringArray(values);
  for (const section of sections) {
    if (!VALID_CONTEXT_SECTIONS.has(section)) {
      invalid(`Unsupported context section '${section}'`);
    }
  }
  return sections;
}

function slugify(value, fallback = "config") {
  const clean = normalizeString(value)
    .toLowerCase()
    .replaceAll(/[^a-z0-9]+/g, "-")
    .replaceAll(/^-+|-+$/g, "");
  return clean || fallback;
}

function normalizeConfigDraft(input = {}) {
  const payload = parseJsonObject(input, {});
  const name = normalizeString(payload.name);
  const model = normalizeString(payload.model);
  const systemPrompt = String(payload.system_prompt || payload.systemPrompt || "");

  if (!name) {
    invalid("Evaluation config name is required");
  }
  if (!model) {
    invalid("Evaluation config model is required");
  }
  if (!systemPrompt.trim()) {
    invalid("Evaluation config system_prompt is required");
  }

  const provider = normalizeString(payload.provider || "litellm") || "litellm";
  const configKey =
    normalizeString(payload.config_key || payload.configKey) || slugify(name);
  const contextSections = normalizeContextSections(
    payload.context_sections || payload.contextSections || [
      "cluster_summary",
      "candidate_core",
      "aliases",
      "provenance",
      "network_context",
      "network_summary",
      "external_reference_summary",
      "evidence_summary",
      "pair_summaries",
      "cluster_metadata",
    ],
  );

  return {
    config_key: configKey,
    name,
    description: String(payload.description || ""),
    provider,
    model,
    model_params: parseJsonObject(
      payload.model_params || payload.modelParams,
      { temperature: 0, top_p: 1 },
    ),
    system_prompt: systemPrompt,
    context_sections: contextSections,
    context_preamble: String(
      payload.context_preamble || payload.contextPreamble || "",
    ),
    created_by:
      normalizeString(payload.created_by || payload.createdBy) || "qa_operator",
  };
}

function normalizePreviewInput(input = {}) {
  const payload = parseJsonObject(input, {});
  const configKey = normalizeString(payload.config_key || payload.configKey);
  const versionValue = payload.version;
  const version =
    versionValue === undefined || versionValue === null || versionValue === ""
      ? null
      : Number.parseInt(String(versionValue), 10);
  const draftConfig = payload.draft_config || payload.draftConfig || null;

  if (!draftConfig && !configKey) {
    invalid("Preview requires either draft_config or config_key");
  }
  if (configKey && version !== null && (!Number.isInteger(version) || version <= 0)) {
    invalid("Preview version must be a positive integer");
  }

  return {
    config_key: configKey,
    version,
    draft_config: draftConfig && typeof draftConfig === "object" ? draftConfig : null,
    requested_by:
      normalizeString(payload.requested_by || payload.requestedBy) || "qa_operator",
  };
}

function normalizeBenchmarkInput(input = {}) {
  const payload = parseJsonObject(input, {});
  const configKey = normalizeString(payload.config_key || payload.configKey);
  const version = Number.parseInt(String(payload.version || ""), 10);
  const datasetSource = normalizeString(
    payload.dataset_source || payload.datasetSource || "resolved_history",
  );

  if (!configKey) {
    invalid("Benchmark requires config_key");
  }
  if (!Number.isInteger(version) || version <= 0) {
    invalid("Benchmark requires a positive config version");
  }
  if (!VALID_DATASET_SOURCES.has(datasetSource)) {
    invalid(
      "dataset_source must be one of resolved_history, gold_set, combined",
    );
  }
  const goldSetId =
    payload.gold_set_id === undefined || payload.gold_set_id === null
      ? null
      : Number.parseInt(String(payload.gold_set_id), 10);
  if (
    datasetSource === "gold_set" &&
    (!Number.isInteger(goldSetId) || goldSetId <= 0)
  ) {
    invalid("gold_set_id is required when dataset_source is gold_set");
  }

  const filters = parseJsonObject(payload.filters, {});
  const limit =
    filters.limit === undefined || filters.limit === null || filters.limit === ""
      ? 100
      : Number.parseInt(String(filters.limit), 10);
  if (!Number.isInteger(limit) || limit <= 0 || limit > 1000) {
    invalid("filters.limit must be between 1 and 1000");
  }

  return {
    config_key: configKey,
    version,
    dataset_source: datasetSource,
    gold_set_id: goldSetId,
    filters: {
      country: normalizeString(filters.country).toUpperCase(),
      severity: normalizeString(filters.severity).toLowerCase(),
      limit,
    },
    requested_by:
      normalizeString(payload.requested_by || payload.requestedBy) || "qa_operator",
  };
}

function normalizeGoldSetInput(input = {}) {
  const payload = parseJsonObject(input, {});
  const name = normalizeString(payload.name);
  if (!name) {
    invalid("Gold set name is required");
  }
  return {
    slug:
      normalizeString(payload.slug) ||
      `${slugify(name, "gold-set")}-${crypto.randomUUID().slice(0, 8)}`,
    name,
    description: String(payload.description || ""),
    created_by:
      normalizeString(payload.created_by || payload.createdBy) || "qa_operator",
  };
}

function normalizeClusterIdList(values) {
  const ids = normalizeStringArray(values);
  if (ids.length === 0) {
    invalid("clusterIds must contain at least one cluster id");
  }
  return ids;
}

module.exports = {
  VALID_CONTEXT_SECTIONS,
  VALID_DATASET_SOURCES,
  VALID_RUN_MODES,
  normalizeBenchmarkInput,
  normalizeClusterIdList,
  normalizeConfigDraft,
  normalizeGoldSetInput,
  normalizePreviewInput,
};
