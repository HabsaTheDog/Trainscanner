const fs = require("node:fs/promises");
const { validateOrThrow } = require("../../core/schema");
const { AppError } = require("../../core/errors");

const SOURCE_SCHEMA = {
  type: "object",
  required: [
    "id",
    "country",
    "provider",
    "datasetName",
    "format",
    "accessType",
    "downloadMethod",
    "downloadUrlOrEndpoint",
  ],
  properties: {
    id: { type: "string", minLength: 1 },
    country: { type: "string", enum: ["DE", "AT", "CH"] },
    provider: { type: "string", minLength: 1 },
    portalName: { type: "string" },
    portalUrl: { type: "string" },
    datasetName: { type: "string", minLength: 1 },
    format: { type: "string", enum: ["netex", "gtfs"] },
    accessType: {
      type: "string",
      enum: ["public", "api_key", "token", "other"],
    },
    authSetupUrl: { type: "string" },
    licenseName: { type: "string" },
    licenseUrl: { type: "string" },
    attributionText: { type: "string" },
    updateCadence: { type: "string" },
    downloadMethod: {
      type: "string",
      enum: ["manual_redirect", "direct", "api"],
    },
    downloadUrlOrEndpoint: { type: "string", minLength: 1 },
    fallbackReason: { type: "string" },
    notes: { type: "string" },
    lastVerifiedAt: { type: "string" },
  },
  additionalProperties: false,
};

const SOURCES_CONFIG_SCHEMA = {
  type: "object",
  required: ["schemaVersion", "sources"],
  properties: {
    schemaVersion: { type: "string", minLength: 1 },
    sources: {
      type: "array",
      minItems: 1,
      items: SOURCE_SCHEMA,
    },
  },
  additionalProperties: false,
};

function validateSourceDiscoveryConfig(raw) {
  validateOrThrow(raw, SOURCES_CONFIG_SCHEMA, {
    message: "Invalid source-discovery config",
    code: "INVALID_CONFIG",
  });

  const seen = new Set();
  for (const source of raw.sources) {
    if (seen.has(source.id)) {
      throw new AppError({
        code: "INVALID_CONFIG",
        message: `Duplicate source id '${source.id}' in source-discovery config`,
      });
    }
    seen.add(source.id);

    if (
      source.format === "gtfs" &&
      !String(source.fallbackReason || "").trim()
    ) {
      throw new AppError({
        code: "INVALID_CONFIG",
        message: `Source '${source.id}' uses GTFS and must declare fallbackReason (NeTEx-first policy)`,
      });
    }
  }

  return raw;
}

async function loadSourceDiscoveryConfig(filePath) {
  const content = await fs.readFile(filePath, "utf8");
  const parsed = JSON.parse(content);
  return validateSourceDiscoveryConfig(parsed);
}

module.exports = {
  loadSourceDiscoveryConfig,
  validateSourceDiscoveryConfig,
};
