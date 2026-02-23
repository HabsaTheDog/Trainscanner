const test = require("node:test");
const assert = require("node:assert/strict");

const { createIngestService } = require("../../src/domains/ingest/service");

test("ingestNetex delegates to ingest script with explicit error code", async () => {
  const calls = [];
  const service = createIngestService({
    runLegacyDataScript: async (options) => {
      calls.push(options);
      return { ok: true, runId: options.runId || "run-1" };
    },
  });

  await service.ingestNetex({
    rootDir: "/tmp/repo",
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
