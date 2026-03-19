const test = require("node:test");
const assert = require("node:assert/strict");

const { createIngestService } = require("../../src/domains/ingest/service");

const SAFE_REPO_ROOT = `${process.cwd()}/.test-fixtures/repo`;

function createStageAwareClient() {
  return {
    async ensureReady() {},
    async queryOne(sql) {
      if (String(sql).includes(" AS fingerprint")) {
        return { fingerprint: { stage: "stop-topology" } };
      }
      if (String(sql).includes(" AS summary")) {
        return { summary: {} };
      }
      return null;
    },
    async runSql() {},
    async end() {},
  };
}

test("ingestNetex delegates to ingest script with explicit error code", async () => {
  const calls = [];
  const service = createIngestService({
    runLegacyDataScript: async (options) => {
      calls.push(options);
      return { ok: true, runId: options.runId || "run-1" };
    },
    createPostgisClient: () => createStageAwareClient(),
  });

  await service.ingestNetex({
    rootDir: SAFE_REPO_ROOT,
    runId: "run-ingest-1",
    args: ["--country", "DE", "--as-of", "2026-02-19"],
    jobOrchestrationEnabled: false,
  });

  assert.equal(calls.length, 1);
  assert.equal(calls[0].scriptFile, "ingest-netex.impl.sh");
  assert.equal(calls[0].errorCode, "INGEST_FAILED");
  assert.equal(calls[0].service, "ingest.netex");
  assert.deepEqual(calls[0].args, ["--country", "DE", "--as-of", "2026-02-19"]);
});

test("ingestNetex skips unchanged reruns when fingerprints match", async () => {
  let scriptCalls = 0;
  const service = createIngestService({
    runLegacyDataScript: async () => {
      scriptCalls += 1;
      return { ok: true };
    },
    createPostgisClient: () => ({
      async ensureReady() {},
      async end() {},
    }),
    createPipelineStageRepo: () => ({
      async getMaterialization() {
        return {
          status: "ready",
          code_fingerprint: "code-1",
          input_fingerprint: {
            stage: "stop-topology",
            sourceId: "",
          },
        };
      },
      async startRun() {},
      async finishRun() {},
    }),
    computeCodeFingerprint: async () => "code-1",
    getStageInputFingerprint: async () => ({
      stage: "stop-topology",
      sourceId: "",
    }),
    getStageSummary: async () => ({
      providerDatasets: 3,
      rawStopPlaces: 10,
    }),
  });

  const result = await service.ingestNetex({
    rootDir: SAFE_REPO_ROOT,
    args: [],
    jobOrchestrationEnabled: false,
  });

  assert.equal(scriptCalls, 0);
  assert.equal(result.cacheHit, true);
  assert.equal(result.skippedUnchanged, true);
  assert.deepEqual(result.summary, {
    providerDatasets: 3,
    rawStopPlaces: 10,
  });
});
