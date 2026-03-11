const test = require("node:test");
const assert = require("node:assert/strict");

const { createQaService } = require("../../src/domains/qa/service");

const SAFE_REPO_ROOT = `${process.cwd()}/.test-fixtures/repo`;

test("reportReviewQueue runs repository-backed report generator", async () => {
  const ensureReadyCalls = [];
  const queryOneCalls = [];
  const queryRowsCalls = [];
  const stdoutWrites = [];
  const originalStdoutWrite = process.stdout.write;
  process.stdout.write = (chunk, encoding, callback) => {
    stdoutWrites.push(String(chunk));
    if (typeof encoding === "function") {
      encoding();
    } else if (typeof callback === "function") {
      callback();
    }
    return true;
  };

  try {
    const service = createQaService({
      createPostgisClient: () => ({
        ensureReady: async () => {
          ensureReadyCalls.push("ready");
        },
        queryOne: async (_sql, params) => {
          queryOneCalls.push(params);
          return {
            total_clusters: 5,
            open_clusters: 2,
            in_review_clusters: 1,
            resolved_clusters: 1,
            dismissed_clusters: 1,
          };
        },
        queryRows: async (_sql, params) => {
          queryRowsCalls.push(params);
          return [];
        },
      }),
    });

    await service.reportReviewQueue({
      rootDir: SAFE_REPO_ROOT,
      runId: "run-review-report-1",
      args: ["--country", "DE", "--limit", "10"],
      jobOrchestrationEnabled: false,
    });
  } finally {
    process.stdout.write = originalStdoutWrite;
  }

  assert.equal(ensureReadyCalls.length, 1);
  assert.equal(queryOneCalls.length, 1);
  assert.deepEqual(queryOneCalls[0], {
    country: "DE",
    all_scopes: "false",
    scope_tag: "latest",
  });
  assert.equal(queryRowsCalls.length, 3);
  assert.match(stdoutWrites.join(""), /"total_clusters":5/);
});

test("reportReviewQueue rejects invalid calendar dates for --as-of", async () => {
  const service = createQaService();

  await assert.rejects(
    () =>
      service.reportReviewQueue({
        args: ["--as-of", "2026-02-30"],
        jobOrchestrationEnabled: false,
      }),
    /Invalid --as-of value/,
  );
});
