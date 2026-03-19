const test = require("node:test");
const assert = require("node:assert/strict");

const {
  createQaPipelineStageService,
} = require("../../src/domains/qa/pipeline-stage-service");

const SAFE_REPO_ROOT = `${process.cwd()}/.test-fixtures/repo`;

test("extractQaNetworkContext skips unchanged reruns when fingerprints match", async () => {
  let scriptCalls = 0;
  const service = createQaPipelineStageService({
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
          code_fingerprint: "code-qa-1",
          input_fingerprint: {
            stage: "qa-network-context",
            sourceId: "",
          },
        };
      },
      async startRun() {},
      async finishRun() {},
    }),
    computeCodeFingerprint: async () => "code-qa-1",
    getStageInputFingerprint: async () => ({
      stage: "qa-network-context",
      sourceId: "",
    }),
    getStageSummary: async () => ({
      qaProviderRouteCount: 100,
      qaProviderAdjacencyCount: 50,
    }),
  });

  const result = await service.extractQaNetworkContext({
    rootDir: SAFE_REPO_ROOT,
    args: [],
  });

  assert.equal(scriptCalls, 0);
  assert.equal(result.cacheHit, true);
  assert.equal(result.skippedUnchanged, true);
  assert.deepEqual(result.summary, {
    qaProviderRouteCount: 100,
    qaProviderAdjacencyCount: 50,
  });
});
