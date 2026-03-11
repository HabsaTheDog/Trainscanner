const fs = require("node:fs");
const path = require("node:path");

const { normalizeProjectRoot } = require("../../cli/pipeline-common");
const { validateSourceDiscoveryConfig } = require("./contracts");

const sourceCatalogCache = new Map();

function formatSourceDisplayLabel(source) {
  const provider = String(source?.provider || "").trim();
  const datasetName = String(source?.datasetName || "").trim();

  if (provider && datasetName) {
    return `${provider} - ${datasetName}`;
  }
  return provider || datasetName || String(source?.id || "").trim();
}

function loadSourceCatalog(rootDir = process.cwd()) {
  const normalizedRoot = normalizeProjectRoot(rootDir);
  const configPath = path.join(
    normalizedRoot,
    "config",
    "europe-data-sources.json",
  );

  if (sourceCatalogCache.has(configPath)) {
    return sourceCatalogCache.get(configPath);
  }

  const raw = fs.readFileSync(configPath, "utf8");
  const config = validateSourceDiscoveryConfig(JSON.parse(raw));
  const labels = new Map();

  for (const source of config.sources || []) {
    labels.set(source.id, formatSourceDisplayLabel(source));
  }

  sourceCatalogCache.set(configPath, labels);
  return labels;
}

function resolveSourceLabel(sourceId, options = {}) {
  const normalizedSourceId = String(sourceId || "").trim();
  if (!normalizedSourceId) {
    return "";
  }

  try {
    const labels = loadSourceCatalog(options.rootDir);
    return labels.get(normalizedSourceId) || normalizedSourceId;
  } catch {
    return normalizedSourceId;
  }
}

function resolveSourceLabels(sourceIds, options = {}) {
  if (!Array.isArray(sourceIds)) {
    return [];
  }

  return Array.from(
    new Set(
      sourceIds
        .map((sourceId) => resolveSourceLabel(sourceId, options))
        .filter(Boolean),
    ),
  );
}

function resetSourceCatalogCache() {
  sourceCatalogCache.clear();
}

module.exports = {
  formatSourceDisplayLabel,
  loadSourceCatalog,
  resetSourceCatalogCache,
  resolveSourceLabel,
  resolveSourceLabels,
};
