const test = require("node:test");
const assert = require("node:assert/strict");

const { createGlobalService } = require("../../src/domains/global/service");

const SAFE_REPO_ROOT = `${process.cwd()}/.test-fixtures/repo`;

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
});

test("job-orchestrated global build closes both orchestration and execution clients", async () => {
  const createdClients = [];
  const stdout = captureStdout();

  try {
    const service = createGlobalService({
      createPostgisClient: () => {
        const client = {
          async ensureReady() {},
          async queryRows() {
            return [{ country: "DE" }];
          },
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
    });

    await service.buildGlobalMergeQueue({
      rootDir: SAFE_REPO_ROOT,
      args: [],
      jobOrchestrationEnabled: true,
    });
  } finally {
    stdout.restore();
  }

  assert.equal(createdClients.length, 3);
  assert.equal(createdClients[0].ended, true);
  assert.equal(createdClients[1].ended, true);
  assert.equal(createdClients[2].ended, true);
});

test("all-scope merge queue batches by country and aggregates summaries", async () => {
  const stdout = captureStdout();
  const seenScopes = [];

  try {
    const service = createGlobalService({
      createPostgisClient: () => ({
        async ensureReady() {},
        async queryRows() {
          return [{ country: "AT" }, { country: "DE" }];
        },
        async end() {},
      }),
      createMergeQueueRepo: () => ({
        async rebuildMergeQueue(scope, options) {
          seenScopes.push(scope);
          options.onPhase("building_pair_seeds");
          return {
            scopeCountry: scope.country,
            scopeAsOf: scope.asOf || "",
            scopeTag: "latest",
            clusters: scope.country === "DE" ? 3 : 1,
            candidates: scope.country === "DE" ? 5 : 2,
            evidence: scope.country === "DE" ? 8 : 3,
          };
        },
      }),
    });

    await service.buildGlobalMergeQueue({
      rootDir: SAFE_REPO_ROOT,
      args: [],
      jobOrchestrationEnabled: false,
    });
  } finally {
    stdout.restore();
  }

  assert.deepEqual(seenScopes, [
    { country: "AT", asOf: "" },
    { country: "DE", asOf: "" },
  ]);
  assert.match(
    stdout.writes.join(""),
    /\[merge-queue\] batching countries=AT,DE concurrency=1 scope=latest/,
  );
  assert.match(stdout.writes.join(""), /"clusters":4/);
  assert.match(stdout.writes.join(""), /"candidates":7/);
  assert.match(stdout.writes.join(""), /"evidence":11/);
});
