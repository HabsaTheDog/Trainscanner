const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");

function normalizeScopeValue(value) {
  return String(value || "").trim();
}

function normalizeStageScope(scope = {}) {
  const country = normalizeScopeValue(scope.country).toUpperCase();
  const asOf = normalizeScopeValue(scope.asOf);
  const sourceId = normalizeScopeValue(scope.sourceId);
  return {
    country,
    asOf,
    sourceId,
    scopeKey: [country || "ALL", asOf || "latest", sourceId || "ALL"].join("|"),
  };
}

function parseStageScopeArgs(args = [], options = {}) {
  const parsed = {
    country: "",
    asOf: "",
    sourceId: "",
  };
  const allowSourceId = options.allowSourceId !== false;
  const tokens = Array.isArray(args) ? args : [];

  for (let index = 0; index < tokens.length; index += 1) {
    const token = String(tokens[index] || "");
    if (token === "--country") {
      parsed.country = String(tokens[index + 1] || "")
        .trim()
        .toUpperCase();
      index += 1;
      continue;
    }
    if (token === "--as-of") {
      parsed.asOf = String(tokens[index + 1] || "").trim();
      index += 1;
      continue;
    }
    if (allowSourceId && token === "--source-id") {
      parsed.sourceId = String(tokens[index + 1] || "").trim();
      index += 1;
    }
  }

  return normalizeStageScope(parsed);
}

async function computeCodeFingerprint(rootDir, filePaths = []) {
  const hash = crypto.createHash("sha256");
  const normalizedRoot = path.resolve(rootDir || process.cwd());
  const seen = new Set();

  for (const candidatePath of Array.isArray(filePaths) ? filePaths : []) {
    const relativePath = String(candidatePath || "").trim();
    if (!relativePath || seen.has(relativePath)) {
      continue;
    }
    seen.add(relativePath);
    const absolutePath = path.resolve(normalizedRoot, relativePath);
    hash.update(relativePath);
    hash.update("\0");
    try {
      const content = await fs.promises.readFile(absolutePath);
      hash.update(content);
    } catch {
      hash.update("<missing>");
    }
    hash.update("\0");
  }

  return hash.digest("hex");
}

function stableStringify(value) {
  return JSON.stringify(value, (_key, nestedValue) => {
    if (
      nestedValue &&
      typeof nestedValue === "object" &&
      !Array.isArray(nestedValue)
    ) {
      return Object.keys(nestedValue)
        .sort()
        .reduce((accumulator, key) => {
          accumulator[key] = nestedValue[key];
          return accumulator;
        }, {});
    }
    return nestedValue;
  });
}

function fingerprintsMatch(left, right) {
  return stableStringify(left || {}) === stableStringify(right || {});
}

function collectTimingSummary({
  startedAt,
  finishedAt,
  result,
  cacheHit,
  skippedUnchanged,
}) {
  const metrics =
    result?.metrics && typeof result.metrics === "object" ? result.metrics : {};
  return {
    totalDurationMs:
      metrics.totalDurationMs || Math.max(0, finishedAt - startedAt),
    cacheHit: Boolean(cacheHit),
    skippedUnchanged: Boolean(skippedUnchanged),
    phases: Array.isArray(metrics.phases) ? metrics.phases : [],
  };
}

module.exports = {
  collectTimingSummary,
  computeCodeFingerprint,
  fingerprintsMatch,
  normalizeStageScope,
  parseStageScopeArgs,
};
