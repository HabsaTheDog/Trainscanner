const test = require("node:test");
const assert = require("node:assert/strict");

const {
  parseBooleanEnv,
  parseIntegerEnv,
  readCircuitBreakerConfig,
  readJobExecutionConfig,
  resolveTemporalAddress,
} = require("../../src/core/runtime");

test("parseBooleanEnv recognizes common truthy and falsy values", () => {
  assert.equal(parseBooleanEnv("true", false), true);
  assert.equal(parseBooleanEnv("OFF", true), false);
  assert.equal(parseBooleanEnv(undefined, true), true);
  assert.equal(parseBooleanEnv("maybe", false), false);
});

test("parseIntegerEnv enforces numeric bounds and fallbacks", () => {
  assert.equal(parseIntegerEnv("5", 1, { min: 1 }), 5);
  assert.equal(parseIntegerEnv("0", 3, { min: 1 }), 3);
  assert.equal(parseIntegerEnv("nope", 7, { min: 1 }), 7);
});

test("readJobExecutionConfig normalizes orchestration defaults", () => {
  assert.deepEqual(
    readJobExecutionConfig({
      PIPELINE_JOB_ORCHESTRATION_ENABLED: "false",
      PIPELINE_JOB_MAX_ATTEMPTS: "4",
      PIPELINE_JOB_MAX_CONCURRENT: "2",
    }),
    {
      jobOrchestrationEnabled: false,
      maxAttempts: 4,
      maxConcurrent: 2,
    },
  );

  assert.deepEqual(readJobExecutionConfig({}), {
    jobOrchestrationEnabled: true,
    maxAttempts: 3,
    maxConcurrent: 1,
  });
});

test("readCircuitBreakerConfig normalizes failure threshold and cooldown", () => {
  assert.deepEqual(
    readCircuitBreakerConfig(
      {
        SOURCE_FETCH_CIRCUIT_THRESHOLD: "5",
        SOURCE_FETCH_CIRCUIT_COOLDOWN_MS: "25000",
      },
      {
        thresholdKey: "SOURCE_FETCH_CIRCUIT_THRESHOLD",
        cooldownKey: "SOURCE_FETCH_CIRCUIT_COOLDOWN_MS",
      },
    ),
    {
      failureThreshold: 5,
      cooldownMs: 25000,
    },
  );

  assert.deepEqual(
    readCircuitBreakerConfig(
      {
        SOURCE_FETCH_CIRCUIT_THRESHOLD: "0",
        SOURCE_FETCH_CIRCUIT_COOLDOWN_MS: "bad",
      },
      {
        thresholdKey: "SOURCE_FETCH_CIRCUIT_THRESHOLD",
        cooldownKey: "SOURCE_FETCH_CIRCUIT_COOLDOWN_MS",
      },
    ),
    {
      failureThreshold: 3,
      cooldownMs: 15000,
    },
  );
});

test("resolveTemporalAddress trims env overrides and falls back to localhost", () => {
  assert.equal(
    resolveTemporalAddress({ TEMPORAL_ADDRESS: " temporal.example:7233 " }),
    "temporal.example:7233",
  );
  assert.equal(resolveTemporalAddress({}), "localhost:7233");
});
