const test = require("node:test");
const assert = require("node:assert/strict");

const { createIngestService } = require("../../src/domains/ingest/service");

function cloneJob(job) {
  return job ? structuredClone(job) : null;
}

function createInMemoryJobsRepo() {
  const byId = new Map();
  const byKey = new Map();

  function save(job) {
    byId.set(job.jobId, cloneJob(job));
    byKey.set(`${job.jobType}|${job.idempotencyKey}`, job.jobId);
    return cloneJob(job);
  }

  return {
    async getByIdempotency(jobType, idempotencyKey) {
      const id = byKey.get(`${jobType}|${idempotencyKey}`);
      return id ? cloneJob(byId.get(id)) : null;
    },
    async getById(jobId) {
      return cloneJob(byId.get(jobId));
    },
    async createQueuedJob(input) {
      return save({
        jobId: input.jobId,
        jobType: input.jobType,
        idempotencyKey: input.idempotencyKey,
        status: "queued",
        attempt: 0,
        runContext: input.runContext || {},
        checkpoint: {},
        resultContext: {},
      });
    },
    async markRunning(input) {
      const job = byId.get(input.jobId);
      job.status = "running";
      job.attempt = input.attempt;
      return save(job);
    },
    async markRetryWait(input) {
      const job = byId.get(input.jobId);
      job.status = "retry_wait";
      job.errorCode = input.errorCode;
      job.errorMessage = input.errorMessage;
      return save(job);
    },
    async markSucceeded(input) {
      const job = byId.get(input.jobId);
      job.status = "succeeded";
      job.resultContext = input.resultContext || {};
      return save(job);
    },
    async markFailed(input) {
      const job = byId.get(input.jobId);
      job.status = "failed";
      job.errorCode = input.errorCode;
      job.errorMessage = input.errorMessage;
      return save(job);
    },
    async updateCheckpoint(input) {
      const job = byId.get(input.jobId);
      job.checkpoint = input.checkpoint || {};
      return save(job);
    },
    async countRunningByType(jobType) {
      let count = 0;
      for (const job of byId.values()) {
        if (job.jobType === jobType && job.status === "running") {
          count += 1;
        }
      }
      return count;
    },
  };
}

async function waitFor(fn, timeoutMs = 2000, intervalMs = 20) {
  const deadline = Date.now() + timeoutMs;
  async function check() {
    if (Date.now() >= deadline) {
      throw new Error("Timed out waiting for condition");
    }
    if (await fn()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
    return check();
  }
  return check();
}

test("ingest service job orchestration reuses completed job for same args", async () => {
  const calls = [];
  const jobsRepo = createInMemoryJobsRepo();

  const ingestService = createIngestService({
    runLegacyDataScript: async (opts) => {
      calls.push(opts);
      return {
        ok: true,
        runId: opts.runId || "run-1",
      };
    },
    createPostgisClient: () => ({
      async ensureReady() {},
    }),
    createPipelineJobsRepo: () => jobsRepo,
  });

  const first = await ingestService.ingestNetex({
    rootDir: "/tmp/repo",
    args: ["--country", "DE", "--as-of", "2026-02-19"],
    jobOrchestrationEnabled: true,
  });
  const second = await ingestService.ingestNetex({
    rootDir: "/tmp/repo",
    args: ["--country", "DE", "--as-of", "2026-02-19"],
    jobOrchestrationEnabled: true,
  });

  assert.equal(first.reused, false);
  assert.equal(second.reused, true);
  assert.equal(calls.length, 1);
});

test("ingest service enforces backpressure on concurrent start attempts", async () => {
  const jobsRepo = createInMemoryJobsRepo();
  let releaseFirst;
  const firstGate = new Promise((resolve) => {
    releaseFirst = resolve;
  });

  const ingestService = createIngestService({
    runLegacyDataScript: async () => {
      await firstGate;
      return {
        ok: true,
        runId: "run-concurrent",
      };
    },
    createPostgisClient: () => ({
      async ensureReady() {},
    }),
    createPipelineJobsRepo: () => jobsRepo,
  });

  const firstRunPromise = ingestService.ingestNetex({
    rootDir: "/tmp/repo",
    args: ["--country", "DE", "--as-of", "2026-02-19"],
    jobOrchestrationEnabled: true,
  });

  await waitFor(async () => {
    const running = await jobsRepo.countRunningByType("ingest.netex");
    return running === 1;
  });

  await assert.rejects(
    ingestService.ingestNetex({
      rootDir: "/tmp/repo",
      args: ["--country", "AT", "--as-of", "2026-02-19"],
      jobOrchestrationEnabled: true,
    }),
    (err) => {
      assert.equal(err.code, "JOB_BACKPRESSURE");
      return true;
    },
  );

  releaseFirst();
  const first = await firstRunPromise;
  assert.equal(first.reused, false);
  assert.equal(first.job.status, "succeeded");
});
