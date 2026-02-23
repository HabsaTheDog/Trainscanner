const test = require("node:test");
const assert = require("node:assert/strict");

const {
  createCanonicalService,
} = require("../../src/domains/canonical/service");

test("buildCanonicalStations runs modern repository-backed implementation", async () => {
  const ensureReadyCalls = [];
  const createRunCalls = [];
  const canonicalBuildCalls = [];
  const markSucceededCalls = [];
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
    const service = createCanonicalService({
      createPostgisClient: () => ({
        ensureReady: async () => {
          ensureReadyCalls.push("ready");
        },
      }),
      createCanonicalStationsRepo: () => ({
        buildCanonicalStations: async (scope) => {
          canonicalBuildCalls.push(scope);
          return {
            sourceRows: 200,
            canonicalRows: 120,
            inserted: 10,
            updated: 5,
            merged: 65,
            conflicts: 2,
          };
        },
      }),
      createImportRunsRepo: () => ({
        createRun: async (input) => {
          createRunCalls.push(input);
        },
        markFailed: async () => {},
        markSucceeded: async (input) => {
          markSucceededCalls.push(input);
        },
      }),
    });

    await service.buildCanonicalStations({
      rootDir: "/tmp/repo",
      runId: "run-canonical-1",
      args: [
        "--country",
        "AT",
        "--as-of",
        "2026-02-19",
        "--source-id",
        "at_source",
      ],
      jobOrchestrationEnabled: false,
    });
  } finally {
    process.stdout.write = originalStdoutWrite;
  }

  assert.equal(ensureReadyCalls.length, 1);
  assert.equal(createRunCalls.length, 1);
  assert.equal(canonicalBuildCalls.length, 1);
  assert.equal(markSucceededCalls.length, 1);

  assert.match(createRunCalls[0].runId, /^[0-9a-f-]{36}$/);
  assert.equal(createRunCalls[0].pipeline, "canonical_build");
  assert.equal(createRunCalls[0].status, "running");
  assert.equal(createRunCalls[0].country, "AT");
  assert.equal(createRunCalls[0].snapshotDate, "2026-02-19");
  assert.equal(createRunCalls[0].sourceId, "at_source");

  assert.equal(canonicalBuildCalls[0].runId, createRunCalls[0].runId);
  assert.equal(canonicalBuildCalls[0].country, "AT");
  assert.equal(canonicalBuildCalls[0].asOf, "2026-02-19");
  assert.equal(canonicalBuildCalls[0].sourceId, "at_source");

  assert.equal(markSucceededCalls[0].runId, createRunCalls[0].runId);
  assert.equal(markSucceededCalls[0].stats.canonicalRows, 120);
  assert.match(stdoutWrites.join(""), /"canonicalRows":120/);
});

test("buildReviewQueue runs modern repository-backed implementation", async () => {
  const repoCalls = [];
  const ensureReadyCalls = [];
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
    const service = createCanonicalService({
      createPostgisClient: () => ({
        ensureReady: async () => {
          ensureReadyCalls.push("ready");
        },
      }),
      createReviewQueueRepo: () => ({
        buildReviewQueue: async (scope) => {
          repoCalls.push(scope);
          return {
            scopeCountry: scope.country,
            scopeAsOf: scope.asOf,
            scopeTag: scope.asOf || "latest",
            detectedIssues: 3,
            openItems: 2,
            confirmedItems: 1,
            resolvedItems: 0,
          };
        },
      }),
    });

    await service.buildReviewQueue({
      rootDir: "/tmp/repo",
      runId: "run-review-build-1",
      args: [
        "--country",
        "CH",
        "--as-of",
        "2026-02-19",
        "--geo-threshold-m",
        "4000",
      ],
      jobOrchestrationEnabled: false,
    });
  } finally {
    process.stdout.write = originalStdoutWrite;
  }

  assert.equal(ensureReadyCalls.length, 1);
  assert.equal(repoCalls.length, 1);
  assert.deepEqual(repoCalls[0], {
    country: "CH",
    asOf: "2026-02-19",
    geoThresholdMeters: 4000,
    closeMissing: true,
  });
  assert.match(stdoutWrites.join(""), /"detectedIssues":3/);
});

test("buildCanonicalStations rejects invalid calendar dates for --as-of", async () => {
  const service = createCanonicalService();

  await assert.rejects(
    () =>
      service.buildCanonicalStations({
        args: ["--as-of", "2026-02-30"],
        jobOrchestrationEnabled: false,
      }),
    /Invalid --as-of value/,
  );
});
