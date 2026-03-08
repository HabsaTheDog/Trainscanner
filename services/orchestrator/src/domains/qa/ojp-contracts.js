const { validateOrThrow } = require("../../core/schema");

const OJP_TEST_CASE_SCHEMA = {
  type: "object",
  required: ["fromRef", "toRef", "departureTime"],
  properties: {
    fromRef: { type: "string" },
    toRef: { type: "string" },
    departureTime: { type: "string", minLength: 1 },
  },
  additionalProperties: false,
};

const OJP_FEEDER_SCHEMA = {
  type: "object",
  required: ["providerId", "country", "endpointUrl", "authMode", "requestMode"],
  properties: {
    providerId: { type: "string", minLength: 1 },
    country: { type: "string", pattern: /^[A-Z]{2}$/ },
    endpointUrl: { type: "string" },
    authMode: { type: "string", enum: ["bearer", "api_key", "none"] },
    envPrefix: { type: "string" },
    requestMode: { type: "string", minLength: 1 },
    timeoutSec: { type: "integer", minimum: 1 },
    notes: { type: "string" },
    testCases: { type: "array", items: OJP_TEST_CASE_SCHEMA },
  },
  additionalProperties: false,
};

const OJP_CONFIG_SCHEMA = {
  type: "object",
  required: ["schemaVersion", "feeders"],
  properties: {
    schemaVersion: { type: "string", minLength: 1 },
    feeders: { type: "array", minItems: 1, items: OJP_FEEDER_SCHEMA },
  },
  additionalProperties: false,
};

function validateOjpEndpointsConfig(raw) {
  return validateOrThrow(raw, OJP_CONFIG_SCHEMA, {
    message: "Invalid OJP endpoints config",
    code: "INVALID_CONFIG",
  });
}

module.exports = {
  validateOjpEndpointsConfig,
};
