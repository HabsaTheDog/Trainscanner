const test = require("node:test");
const assert = require("node:assert/strict");

const {
  computeBackoffMs,
  createJobOrchestrator,
  isRunningSlotConflictError,
} = require("../../src/core/job-orchestrator");
const { AppError } = require("../../src/core/errors");

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
    async getById(jobId) {
      return cloneJob(byId.get(jobId));
    },
    async getByIdempotency(jobType, idempotencyKey) {
      const id = byKey.get(`${jobType}|${idempotencyKey}`);
      return id ? cloneJob(byId.get(id)) : null;
    },
    async createQueuedJob(input) {
      return save({
        jobId: input.jobId,
        jobType: input.jobType,
        idempotencyKey: input.idempotencyKey,
        status: "queued",
        attempt: 0,
        runContext: input.runContext || {},
        checkpoint: input.checkpoint || {},
        resultContext: {},
      });
    },
    async markRunning(input) {
      const current = byId.get(input.jobId);
      current.status = "running";
      current.attempt = input.attempt;
      return save(current);
    },
    async markRetryWait(input) {
      const current = byId.get(input.jobId);
      current.status = "retry_wait";
      current.errorCode = input.errorCode;
      current.errorMessage = input.errorMessage;
      return save(current);
    },
    async markSucceeded(input) {
      const current = byId.get(input.jobId);
      current.status = "succeeded";
      current.resultContext = input.resultContext || {};
      return save(current);
    },
    async markFailed(input) {
      const current = byId.get(input.jobId);
      current.status = "failed";
      current.errorCode = input.errorCode;
      current.errorMessage = input.errorMessage;
      return save(current);
    },
    async updateCheckpoint(input) {
      const current = byId.get(input.jobId);
      current.checkpoint = input.checkpoint || {};
      return save(current);
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
    _patch(jobId, patch) {
      const current = byId.get(jobId);
      if (!current) {
        return;
      }
      const patchData = patch && typeof patch === "object" ? patch : null;
      save({
        ...current,
        ...(patchData || undefined),
      });
    },
    _seed(job) {
      save(job);
    },
  };
}

test("computeBackoffMs returns bounded exponential values", () => {
  for (let i = 1; i <= 10; i += 1) {
    const value = computeBackoffMs(i, {
      baseMs: 100,
      maxMs: 500,
      jitterPct: 0,
    });
    assert.ok(value >= 100);
    assert.ok(value <= 500);
  }
});

test("isRunningSlotConflictError detects running-slot unique constraint conflicts", () => {
  assert.equal(
    isRunningSlotConflictError(
      new Error(
        'duplicate key value violates unique constraint "idx_pipeline_jobs_one_running_per_type"',
      ),
    ),
    true,
  );

  assert.equal(
    isRunningSlotConflictError(
      new Error(
        'duplicate key value violates unique constraint "pipeline_jobs_job_type_idempotency_key_key"',
      ),
    ),
    false,
  );
});

test("job orchestrator retries transient failures and resumes from checkpoint", async () => {
  const jobsRepo = createInMemoryJobsRepo();
  const sleeps = [];
  const orchestrator = createJobOrchestrator({
    jobsRepo,
    sleep: async (ms) => {
      sleeps.push(ms);
    },
  });

  let callCount = 0;
  const result = await orchestrator.runJob({
    jobType: "ingest.netex",
    idempotencyKey: "key-1",
    maxAttempts: 3,
    execute: async ({ attempt, checkpoint, updateCheckpoint }) => {
      callCount += 1;
      if (attempt === 1) {
        await updateCheckpoint({ loadedRows: 120 });
        const err = new Error("temporary outage");
        err.code = "MOTIS_UNAVAILABLE";
        err.transient = true;
        throw err;
      }

      assert.equal(checkpoint.loadedRows, 120);
      return { loadedRows: 120, completed: true };
    },
  });

  assert.equal(callCount, 2);
  assert.equal(result.reused, false);
  assert.equal(result.job.status, "succeeded");
  assert.equal(result.job.resultContext.completed, true);
  assert.equal(sleeps.length, 1);
});

test("job orchestrator resumes queued job for the same idempotency key", async () => {
  const jobsRepo = createInMemoryJobsRepo();
  jobsRepo._seed({
    jobId: "65f20a5f-3ebd-4f44-8097-f52d492e8be4",
    jobType: "canonical.build-stations",
    idempotencyKey: "queued-key",
    status: "queued",
    attempt: 0,
    runContext: { args: ["--as-of", "2026-02-19"] },
    checkpoint: { importedRows: 21 },
    resultContext: {},
  });

  const orchestrator = createJobOrchestrator({
    jobsRepo,
    sleep: async () => {},
  });
  let executeCalls = 0;
  const result = await orchestrator.runJob({
    jobType: "canonical.build-stations",
    idempotencyKey: "queued-key",
    execute: async ({ checkpoint }) => {
      executeCalls += 1;
      assert.equal(checkpoint.importedRows, 21);
      return { ok: true };
    },
  });

  assert.equal(executeCalls, 1);
  assert.equal(result.reused, false);
  assert.equal(result.job.status, "succeeded");
});

test("job orchestrator resumes retry_wait job from the last attempt/checkpoint", async () => {
  const jobsRepo = createInMemoryJobsRepo();
  jobsRepo._seed({
    jobId: "52977e20-da63-4b69-920d-f8e4ea5e09be",
    jobType: "ingest.netex",
    idempotencyKey: "retry-key",
    status: "retry_wait",
    attempt: 1,
    runContext: { args: ["--country", "DE"] },
    checkpoint: { chunk: 3 },
    resultContext: {},
  });

  const orchestrator = createJobOrchestrator({
    jobsRepo,
    sleep: async () => {},
  });
  let observedAttempt = 0;
  const result = await orchestrator.runJob({
    jobType: "ingest.netex",
    idempotencyKey: "retry-key",
    maxAttempts: 3,
    execute: async ({ attempt, checkpoint }) => {
      observedAttempt = attempt;
      assert.equal(checkpoint.chunk, 3);
      return { ok: true, chunk: checkpoint.chunk };
    },
  });

  assert.equal(observedAttempt, 2);
  assert.equal(result.reused, false);
  assert.equal(result.job.status, "succeeded");
});

test("job orchestrator reuses completed job for same idempotency key", async () => {
  const jobsRepo = createInMemoryJobsRepo();
  const orchestrator = createJobOrchestrator({
    jobsRepo,
    sleep: async () => {},
  });

  let calls = 0;
  const first = await orchestrator.runJob({
    jobType: "canonical.build-stations",
    idempotencyKey: "key-2",
    execute: async () => {
      calls += 1;
      return { ok: true };
    },
  });

  const second = await orchestrator.runJob({
    jobType: "canonical.build-stations",
    idempotencyKey: "key-2",
    execute: async () => {
      calls += 1;
      return { ok: true };
    },
  });

  assert.equal(first.reused, false);
  assert.equal(second.reused, true);
  assert.equal(second.job.status, "succeeded");
  assert.equal(calls, 1);
});

test("job orchestrator maps running-slot claim races to JOB_BACKPRESSURE", async () => {
  const jobsRepo = createInMemoryJobsRepo();
  jobsRepo.markRunning = async () => {
    throw new Error(
      'duplicate key value violates unique constraint "idx_pipeline_jobs_one_running_per_type"',
    );
  };

  const orchestrator = createJobOrchestrator({
    jobsRepo,
    sleep: async () => {},
  });
  let executeCalls = 0;

  await assert.rejects(
    orchestrator.runJob({
      jobType: "qa.report-review-queue",
      idempotencyKey: "race-claim-key",
      maxConcurrent: 1,
      execute: async () => {
        executeCalls += 1;
        return { ok: true };
      },
    }),
    (err) => {
      assert.equal(err.code, "JOB_BACKPRESSURE");
      return true;
    },
  );

  assert.equal(executeCalls, 0);
});

test("job orchestrator reuses same-key running job when claim loses race", async () => {
  const jobsRepo = createInMemoryJobsRepo();
  const jobId = "f2d41327-778e-4f9c-b7bf-cbf5f4d8e651";
  jobsRepo._seed({
    jobId,
    jobType: "ingest.netex",
    idempotencyKey: "same-key-race",
    status: "queued",
    attempt: 0,
    runContext: {},
    checkpoint: {},
    resultContext: {},
  });

  jobsRepo.claimRunning = async () => {
    jobsRepo._patch(jobId, {
      status: "running",
      attempt: 1,
    });
    return null;
  };

  const orchestrator = createJobOrchestrator({
    jobsRepo,
    sleep: async () => {},
  });
  let executeCalls = 0;
  const result = await orchestrator.runJob({
    jobType: "ingest.netex",
    idempotencyKey: "same-key-race",
    maxConcurrent: 1,
    execute: async () => {
      executeCalls += 1;
      return { ok: true };
    },
  });

  assert.equal(result.reused, true);
  assert.equal(result.inFlight, true);
  assert.equal(result.job.jobId, jobId);
  assert.equal(result.job.status, "running");
  assert.equal(executeCalls, 0);
});

test("job orchestrator enforces backpressure per job type", async () => {
  const jobsRepo = createInMemoryJobsRepo();
  jobsRepo._seed({
    jobId: "53d36f88-a8f0-4f15-beb1-79322c6be50e",
    jobType: "qa.report-review-queue",
    idempotencyKey: "already-running",
    status: "running",
    attempt: 1,
    runContext: {},
    checkpoint: {},
    resultContext: {},
  });

  const orchestrator = createJobOrchestrator({
    jobsRepo,
    sleep: async () => {},
  });

  await assert.rejects(
    orchestrator.runJob({
      jobType: "qa.report-review-queue",
      idempotencyKey: "new-job",
      maxConcurrent: 1,
      execute: async () => ({ ok: true }),
    }),
    (err) => {
      assert.equal(err.code, "JOB_BACKPRESSURE");
      return true;
    },
  );
});

test("job orchestrator reuses failed terminal outcome for same idempotency key", async () => {
  const jobsRepo = createInMemoryJobsRepo();
  const orchestrator = createJobOrchestrator({
    jobsRepo,
    sleep: async () => {},
  });

  let calls = 0;
  await assert.rejects(
    orchestrator.runJob({
      jobType: "ingest.netex",
      idempotencyKey: "failed-key",
      maxAttempts: 1,
      execute: async () => {
        calls += 1;
        throw new AppError({
          code: "INGEST_FAILED",
          message: "hard failure",
        });
      },
    }),
    (err) => {
      assert.equal(err.code, "INGEST_FAILED");
      return true;
    },
  );

  await assert.rejects(
    orchestrator.runJob({
      jobType: "ingest.netex",
      idempotencyKey: "failed-key",
      execute: async () => {
        calls += 1;
        return { ok: true };
      },
    }),
    (err) => {
      assert.equal(err.code, "INGEST_FAILED");
      assert.equal(err.details?.reused, true);
      assert.equal(err.details?.terminalStatus, "failed");
      return true;
    },
  );

  assert.equal(calls, 1);
});

test("job orchestrator handles duplicate-idempotency insert race by reusing existing job", async () => {
  const jobsRepo = createInMemoryJobsRepo();
  const existingJobId = "e5f706c6-b82f-4a86-b88e-f12359d49290";
  jobsRepo._seed({
    jobId: existingJobId,
    jobType: "ingest.netex",
    idempotencyKey: "race-key",
    status: "running",
    attempt: 1,
    runContext: { args: ["--country", "DE"] },
    checkpoint: {},
    resultContext: {},
  });

  const originalGetByIdempotency = jobsRepo.getByIdempotency.bind(jobsRepo);
  let readCount = 0;
  jobsRepo.getByIdempotency = async (jobType, idempotencyKey) => {
    readCount += 1;
    if (readCount === 1) {
      return null;
    }
    return originalGetByIdempotency(jobType, idempotencyKey);
  };
  jobsRepo.createQueuedJob = async () => {
    const err = new Error("duplicate key value violates unique constraint");
    err.code = "23505";
    throw err;
  };

  const orchestrator = createJobOrchestrator({
    jobsRepo,
    sleep: async () => {},
  });
  let executeCalls = 0;
  const result = await orchestrator.runJob({
    jobType: "ingest.netex",
    idempotencyKey: "race-key",
    maxConcurrent: 2,
    execute: async () => {
      executeCalls += 1;
      return { ok: true };
    },
  });

  assert.equal(result.reused, true);
  assert.equal(result.inFlight, true);
  assert.equal(result.job.jobId, existingJobId);
  assert.equal(executeCalls, 0);
});
