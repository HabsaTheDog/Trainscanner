const test = require("node:test");
const assert = require("node:assert/strict");

const { createGlobalService } = require("../../src/domains/global/service");
const {
  computeCodeFingerprint,
} = require("../../src/domains/pipeline/stage-runtime");

const SAFE_REPO_ROOT = `${process.cwd()}/.test-fixtures/repo`;

function createStageRepoStub(overrides = {}) {
  return {
    async getMaterialization() {
      return null;
    },
    async startRun() {},
    async finishRun() {},
    ...overrides,
  };
}

function captureStdout() {
  const writes = [];
  const originalStdoutWrite = process.stdout.write;

  process.stdout.write = (chunk, encoding, callback) => {
    writes.push(String(chunk));
    if (typeof encoding === "function") {
      encoding();
    } else if (typeof callback === "function") {
      callback();
    }
    return true;
  };

  return {
    writes,
    restore() {
      process.stdout.write = originalStdoutWrite;
    },
  };
}

test("buildGlobalStations closes the db client and prints summary JSON", async () => {
  const clientCalls = [];
  const stdout = captureStdout();

  try {
    const service = createGlobalService({
      createPostgisClient: () => ({
        async ensureReady() {
          clientCalls.push("ensureReady");
        },
        async end() {
          clientCalls.push("end");
        },
      }),
      createGlobalStationsRepo: () => ({
        async buildGlobalStations(scope) {
          assert.deepEqual(scope, {
            country: "DE",
            asOf: "2026-03-09",
            sourceId: "db-source",
          });
          return {
            sourceRows: 1,
            globalStations: 1,
            stationMappings: 1,
            globalStopPoints: 1,
            stopPointMappings: 1,
          };
        },
      }),
      createPipelineStageRepo: () => createStageRepoStub(),
    });

    await service.buildGlobalStations({
      rootDir: SAFE_REPO_ROOT,
      args: [
        "--country",
        "DE",
        "--as-of",
        "2026-03-09",
        "--source-id",
        "db-source",
      ],
      jobOrchestrationEnabled: false,
    });
  } finally {
    stdout.restore();
  }

  assert.deepEqual(clientCalls, ["ensureReady", "end"]);
  assert.match(stdout.writes.join(""), /"sourceRows":1/);
  assert.match(stdout.writes.join(""), /\[global-stations\] metrics=/);
});

test("buildGlobalMergeQueue prints phase and info notices and closes the client", async () => {
  const clientCalls = [];
  const stdout = captureStdout();

  try {
    const service = createGlobalService({
      createPostgisClient: () => ({
        async ensureReady() {
          clientCalls.push("ensureReady");
        },
        async end() {
          clientCalls.push("end");
        },
      }),
      createMergeQueueRepo: () => ({
        async rebuildMergeQueue(scope, options) {
          assert.deepEqual(scope, {
            country: "AT",
            asOf: "2026-03-10",
          });
          options.onPhase("building_pair_seeds");
          options.onInfo({
            key: "pair_seeds_total",
            value: "42",
          });
          return {
            scopeCountry: "AT",
            scopeAsOf: "2026-03-10",
            scopeTag: "2026-03-10",
            clusters: 1,
            candidates: 2,
            evidence: 3,
          };
        },
      }),
      createPipelineStageRepo: () => createStageRepoStub(),
    });

    await service.buildGlobalMergeQueue({
      rootDir: SAFE_REPO_ROOT,
      args: ["--country", "AT", "--as-of", "2026-03-10"],
      jobOrchestrationEnabled: false,
    });
  } finally {
    stdout.restore();
  }

  const output = stdout.writes.join("");
  assert.deepEqual(clientCalls, ["ensureReady", "end"]);
  assert.match(
    output,
    /\[merge-queue\] phase=building_pair_seeds country=AT scope=2026-03-10/,
  );
  assert.match(
    output,
    /\[merge-queue\] pair_seeds_total=42 country=AT scope=2026-03-10/,
  );
  assert.match(output, /"clusters":1/);
  assert.match(output, /\[merge-queue\] metrics=/);
});

test("buildGlobalStations skips unchanged scopes when input and code fingerprint match", async () => {
  const stdout = captureStdout();
  const codeFingerprint = await computeCodeFingerprint(SAFE_REPO_ROOT, [
    "services/orchestrator/src/domains/global/service.js",
    "services/orchestrator/src/data/postgis/repositories/global-stations-repo.js",
    "scripts/data/build-global-stations.sh",
  ]);

  try {
    const service = createGlobalService({
      createPostgisClient: () => ({
        async ensureReady() {},
        async end() {},
      }),
      createGlobalStationsRepo: () => ({
        async getBuildFingerprint() {
          return { datasetIds: [101] };
        },
        async getCurrentSummary() {
          return {
            sourceRows: 10,
            globalStations: 4,
            stationMappings: 4,
            globalStopPoints: 5,
            stopPointMappings: 5,
          };
        },
      }),
      createPipelineStageRepo: () =>
        createStageRepoStub({
          async getMaterialization() {
            return {
              status: "ready",
              input_fingerprint: { datasetIds: [101] },
              code_fingerprint: codeFingerprint,
            };
          },
        }),
    });

    const result = await service.buildGlobalStations({
      rootDir: SAFE_REPO_ROOT,
      args: ["--country", "DE"],
      jobOrchestrationEnabled: false,
    });

    assert.equal(result.cacheHit, true);
    assert.equal(result.skippedUnchanged, true);
  } finally {
    stdout.restore();
  }

  assert.match(stdout.writes.join(""), /\[global-stations\] cache_hit=true/);
  assert.match(
    stdout.writes.join(""),
    /\[global-stations\] skipped_unchanged=true/,
  );
});

test("job-orchestrated global build closes both orchestration and execution clients", async () => {
  const createdClients = [];
  const stdout = captureStdout();

  try {
    const service = createGlobalService({
      createPostgisClient: () => {
        const client = {
          async ensureReady() {},
          async end() {
            client.ended = true;
          },
          ended: false,
        };
        createdClients.push(client);
        return client;
      },
      createPipelineJobsRepo: () => ({
        async getByIdempotency() {
          return null;
        },
      }),
      createJobOrchestrator: () => ({
        async runJob({ execute }) {
          const summary = await execute({
            async updateCheckpoint() {},
          });
          return {
            job: { status: "succeeded" },
            reused: false,
            ...summary,
          };
        },
      }),
      createMergeQueueRepo: () => ({
        async rebuildMergeQueue() {
          return {
            scopeCountry: "",
            scopeAsOf: "",
            scopeTag: "latest",
            clusters: 0,
            candidates: 0,
            evidence: 0,
          };
        },
      }),
      createPipelineStageRepo: () => createStageRepoStub(),
    });

    await service.buildGlobalMergeQueue({
      rootDir: SAFE_REPO_ROOT,
      args: [],
      jobOrchestrationEnabled: true,
    });
  } finally {
    stdout.restore();
  }

  assert.equal(createdClients.length, 2);
  assert.equal(createdClients[0].ended, true);
  assert.equal(createdClients[1].ended, true);
});

test("all-scope merge queue rebuild runs once globally", async () => {
  const stdout = captureStdout();
  const seenScopes = [];

  try {
    const service = createGlobalService({
      createPostgisClient: () => ({
        async ensureReady() {},
        async end() {},
      }),
      createMergeQueueRepo: () => ({
        async rebuildMergeQueue(scope, options) {
          seenScopes.push(scope);
          options.onPhase("building_pair_seeds");
          return {
            scopeCountry: "",
            scopeAsOf: scope.asOf || "",
            scopeTag: "latest",
            clusters: 4,
            candidates: 7,
            evidence: 11,
          };
        },
      }),
      createPipelineStageRepo: () => createStageRepoStub(),
    });

    await service.buildGlobalMergeQueue({
      rootDir: SAFE_REPO_ROOT,
      args: [],
      jobOrchestrationEnabled: false,
    });
  } finally {
    stdout.restore();
  }

  assert.deepEqual(seenScopes, [{ country: "", asOf: "" }]);
  assert.doesNotMatch(
    stdout.writes.join(""),
    /\[merge-queue\] batching countries=/,
  );
  assert.match(stdout.writes.join(""), /"clusters":4/);
  assert.match(stdout.writes.join(""), /"candidates":7/);
  assert.match(stdout.writes.join(""), /"evidence":11/);
});
