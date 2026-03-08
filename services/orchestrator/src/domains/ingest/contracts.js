const { validateOrThrow } = require("../../core/schema");

const INGEST_OPTIONS_SCHEMA = {
  type: "object",
  properties: {
    country: { type: "string", pattern: /^[A-Z]{2}$/ },
    sourceId: { type: "string", minLength: 1 },
    asOf: { type: "string", pattern: /^\d{4}-\d{2}-\d{2}$/ },
    runId: { type: "string", minLength: 1 },
  },
  additionalProperties: false,
};

const INGEST_RUN_SCHEMA = {
  type: "object",
  required: ["runId", "pipeline", "status"],
  properties: {
    runId: { type: "string", minLength: 1 },
    pipeline: { type: "string", minLength: 1 },
    status: { type: "string", enum: ["running", "succeeded", "failed"] },
    sourceId: { type: "string" },
    country: { type: "string", pattern: /^[A-Z]{2}$/ },
    snapshotDate: { type: "string", pattern: /^\d{4}-\d{2}-\d{2}$/ },
    startedAt: { type: "string" },
    endedAt: { type: "string" },
    errorCode: { type: "string" },
    errorMessage: { type: "string" },
  },
  additionalProperties: false,
};

function validateIngestOptions(options) {
  return validateOrThrow(options || {}, INGEST_OPTIONS_SCHEMA, {
    message: "Invalid ingest options",
    code: "INVALID_CONFIG",
  });
}

function validateIngestRun(payload) {
  return validateOrThrow(payload || {}, INGEST_RUN_SCHEMA, {
    message: "Invalid ingest run metadata",
    code: "INVALID_CONFIG",
  });
}

module.exports = {
  validateIngestOptions,
  validateIngestRun,
};
