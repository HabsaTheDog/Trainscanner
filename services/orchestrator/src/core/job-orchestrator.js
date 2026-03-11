const crypto = require("node:crypto");
const { AppError, toAppError } = require("./errors");

function defaultSleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isTransientError(err, transientCodes) {
  if (!err) {
    return false;
  }
  if (err.transient === true) {
    return true;
  }
  const appErr = toAppError(err);
  return transientCodes.has(appErr.code);
}

function computeBackoffMs(attempt, options = {}) {
  const baseMs = Number.isFinite(options.baseMs) ? options.baseMs : 500;
  const maxMs = Number.isFinite(options.maxMs) ? options.maxMs : 10_000;
  const jitterPct = Number.isFinite(options.jitterPct)
    ? options.jitterPct
    : 0.15;

  const exp = Math.max(0, attempt - 1);
  const raw = Math.min(maxMs, baseMs * 2 ** exp);
  const jitter = raw * jitterPct;
  const lower = Math.max(0, raw - jitter);
  const upper = raw + jitter;

  const randomVal = crypto.randomBytes(4).readUInt32LE() / 0x100000000;
  return Math.floor(lower + randomVal * (upper - lower));
}

function isDuplicateIdempotencyError(err) {
  if (!err) {
    return false;
  }

  if (String(err.code || "") === "23505") {
    return true;
  }

  const appErr = toAppError(err);
  const message =
    `${String(appErr.message || "")} ${String(err.message || "")}`.toLowerCase();

  return (
    message.includes("duplicate key value") &&
    (message.includes("idempotency") ||
      message.includes("(job_type, idempotency_key)") ||
      message.includes("pipeline_jobs_job_type_idempotency_key_key"))
  );
}

function isRunningSlotConflictError(err) {
  if (!err) {
    return false;
  }

  if (String(err.code || "") === "23505") {
    return true;
  }

  const appErr = toAppError(err);
  const message = [
    String(appErr.message || ""),
    String(err.message || ""),
    String(err?.details?.stderr || ""),
    String(err?.details?.stdout || ""),
  ]
    .join(" ")
    .toLowerCase();

  return (
    message.includes("duplicate key value") &&
    (message.includes("idx_pipeline_jobs_one_running_per_type") ||
      (message.includes("pipeline_jobs") &&
        message.includes("status") &&
        message.includes("running")))
  );
}

function validateRunJobInput(input = {}) {
  const jobType = String(input.jobType || "").trim();
  const idempotencyKey = String(input.idempotencyKey || "").trim();
  const runContext =
    input.runContext && typeof input.runContext === "object"
      ? input.runContext
      : {};
  const maxAttempts = Number.isFinite(input.maxAttempts)
    ? Math.max(1, input.maxAttempts)
    : 3;
  const maxConcurrent = Number.isFinite(input.maxConcurrent)
    ? Math.max(1, input.maxConcurrent)
    : 1;
  const execute = input.execute;

  if (!jobType) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: "jobType is required",
    });
  }

  if (!idempotencyKey) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: "idempotencyKey is required",
    });
  }

  if (typeof execute !== "function") {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: "execute callback is required",
    });
  }

  return {
    jobType,
    idempotencyKey,
    runContext,
    maxAttempts,
    maxConcurrent,
    execute,
    backoff: input.backoff || {},
    jobId: input.jobId || crypto.randomUUID(),
  };
}

function createJobOrchestrator(options = {}) {
  const jobsRepo = options.jobsRepo;
  const logger = options.logger || {
    info() {},
    warn() {},
    error() {},
  };
  const sleep = options.sleep || defaultSleep;

  if (!jobsRepo) {
    throw new AppError({
      code: "INVALID_CONFIG",
      message: "Job orchestrator requires jobsRepo",
    });
  }

  const transientCodes = new Set(
    Array.isArray(options.transientErrorCodes)
      ? options.transientErrorCodes
      : [
          "MOTIS_UNAVAILABLE",
          "INTERNAL_ERROR",
          "SOURCE_FETCH_FAILED",
          "SOURCE_VERIFY_FAILED",
          "INGEST_FAILED",
        ],
  );

  function resolveExistingJob(existingJob, context) {
    if (!existingJob) {
      return null;
    }
    const { jobType, idempotencyKey } = context;

    if (existingJob.status === "running") {
      logger.info("job idempotency reused in-flight job", {
        jobType,
        jobId: existingJob.jobId,
        idempotencyKey,
        status: existingJob.status,
      });
      return {
        accepted: true,
        reused: true,
        inFlight: true,
        job: existingJob,
      };
    }

    if (
      existingJob.status === "queued" ||
      existingJob.status === "retry_wait"
    ) {
      logger.info("job idempotency resumed pending job", {
        jobType,
        jobId: existingJob.jobId,
        idempotencyKey,
        status: existingJob.status,
      });
      return null;
    }

    if (existingJob.status === "succeeded") {
      logger.info("job idempotency reused completed job", {
        jobType,
        jobId: existingJob.jobId,
        idempotencyKey,
      });
      return {
        accepted: true,
        reused: true,
        inFlight: false,
        job: existingJob,
      };
    }

    if (existingJob.status !== "failed") {
      return null;
    }

    logger.info("job idempotency reused failed job", {
      jobType,
      jobId: existingJob.jobId,
      idempotencyKey,
      errorCode: existingJob.errorCode || null,
    });

    throw new AppError({
      code: existingJob.errorCode || "INTERNAL_ERROR",
      message: existingJob.errorMessage || `Job '${jobType}' previously failed`,
      details: {
        jobId: existingJob.jobId,
        jobType,
        idempotencyKey,
        attempt: existingJob.attempt || 0,
        reused: true,
        terminalStatus: existingJob.status,
      },
    });
  }

  async function assertJobTypeCapacity(jobType, maxConcurrent) {
    const runningForType = await jobsRepo.countRunningByType(jobType);
    if (runningForType < maxConcurrent) {
      return;
    }
    throw new AppError({
      code: "JOB_BACKPRESSURE",
      message: `Backpressure active for jobType '${jobType}' (running=${runningForType}, limit=${maxConcurrent})`,
      details: {
        jobType,
        running: runningForType,
        limit: maxConcurrent,
      },
    });
  }

  async function findOrCreateJob(context) {
    let job = await jobsRepo.getByIdempotency(
      context.jobType,
      context.idempotencyKey,
    );
    const existingOutcome = resolveExistingJob(job, context);
    if (existingOutcome) {
      return { outcome: existingOutcome, job: null };
    }

    await assertJobTypeCapacity(context.jobType, context.maxConcurrent);
    if (job) {
      return { outcome: null, job };
    }

    try {
      job = await jobsRepo.createQueuedJob({
        jobId: context.jobId,
        jobType: context.jobType,
        idempotencyKey: context.idempotencyKey,
        runContext: context.runContext,
        checkpoint: {},
      });
      return { outcome: null, job };
    } catch (err) {
      if (!isDuplicateIdempotencyError(err)) {
        throw err;
      }
      job = await jobsRepo.getByIdempotency(
        context.jobType,
        context.idempotencyKey,
      );
      const racedOutcome = resolveExistingJob(job, context);
      if (racedOutcome) {
        return { outcome: racedOutcome, job: null };
      }
      if (job) {
        return { outcome: null, job };
      }
      throw err;
    }
  }

  async function claimRunningJob(job, context, attempt) {
    try {
      return typeof jobsRepo.claimRunning === "function"
        ? await jobsRepo.claimRunning({
            jobId: job.jobId,
            jobType: context.jobType,
            attempt,
            maxConcurrent: context.maxConcurrent,
          })
        : await jobsRepo.markRunning({ jobId: job.jobId, attempt });
    } catch (err) {
      if (!isRunningSlotConflictError(err)) {
        throw err;
      }
      return null;
    }
  }

  async function throwBackpressureForClaim(job, context) {
    if (typeof jobsRepo.getById === "function") {
      let latest = null;
      try {
        latest = await jobsRepo.getById(job.jobId);
      } catch {
        latest = null;
      }
      const latestOutcome = resolveExistingJob(latest, context);
      if (latestOutcome) {
        return latestOutcome;
      }
    }

    let currentRunning = context.maxConcurrent;
    try {
      currentRunning = await jobsRepo.countRunningByType(context.jobType);
    } catch {
      currentRunning = context.maxConcurrent;
    }
    throw new AppError({
      code: "JOB_BACKPRESSURE",
      message: `Backpressure active for jobType '${context.jobType}' (running=${currentRunning}, limit=${context.maxConcurrent})`,
      details: {
        jobId: job.jobId,
        jobType: context.jobType,
        running: currentRunning,
        limit: context.maxConcurrent,
      },
    });
  }

  async function runSingleAttempt(job, context, attempt, checkpoint) {
    logger.info("job attempt started", {
      jobType: context.jobType,
      jobId: job.jobId,
      idempotencyKey: context.idempotencyKey,
      attempt,
    });

    const result = await context.execute({
      job,
      attempt,
      runContext: context.runContext,
      checkpoint,
      async updateCheckpoint(nextCheckpoint) {
        const next =
          nextCheckpoint && typeof nextCheckpoint === "object"
            ? nextCheckpoint
            : checkpoint;
        await jobsRepo.updateCheckpoint({
          jobId: job.jobId,
          checkpoint: next,
        });
        if (next !== checkpoint) {
          for (const key of Object.keys(checkpoint)) {
            delete checkpoint[key];
          }
          Object.assign(checkpoint, next);
        }
      },
    });

    const succeededJob = await jobsRepo.markSucceeded({
      jobId: job.jobId,
      resultContext:
        result && typeof result === "object"
          ? result
          : {
              result,
            },
    });

    logger.info("job attempt succeeded", {
      jobType: context.jobType,
      jobId: succeededJob.jobId,
      idempotencyKey: context.idempotencyKey,
      attempt,
    });

    return {
      accepted: true,
      reused: false,
      inFlight: false,
      job: succeededJob,
    };
  }

  async function handleAttemptFailure(err, job, context, attempt) {
    const appErr = toAppError(err);
    const transient = isTransientError(err, transientCodes);

    if (transient && attempt < context.maxAttempts) {
      const retryJob = await jobsRepo.markRetryWait({
        jobId: job.jobId,
        errorCode: appErr.code,
        errorMessage: appErr.message,
      });

      const delayMs = computeBackoffMs(attempt, context.backoff);
      logger.warn("job attempt failed with transient error; retry scheduled", {
        jobType: context.jobType,
        jobId: retryJob.jobId,
        idempotencyKey: context.idempotencyKey,
        attempt,
        errorCode: appErr.code,
        delayMs,
      });
      await sleep(delayMs);
      return {
        shouldRetry: true,
        job: retryJob,
      };
    }

    const failedJob = await jobsRepo.markFailed({
      jobId: job.jobId,
      errorCode: appErr.code,
      errorMessage: appErr.message,
    });

    logger.error("job failed", {
      jobType: context.jobType,
      jobId: failedJob.jobId,
      idempotencyKey: context.idempotencyKey,
      attempt,
      errorCode: appErr.code,
    });

    throw new AppError({
      code: appErr.code,
      message: appErr.message,
      details: {
        jobId: failedJob.jobId,
        jobType: context.jobType,
        idempotencyKey: context.idempotencyKey,
        attempt,
      },
      cause: appErr,
    });
  }

  async function runJob(input = {}) {
    const context = validateRunJobInput(input);
    const { outcome, job: initialJob } = await findOrCreateJob(context);
    if (outcome) {
      return outcome;
    }

    let job = initialJob;
    let attempt = Math.max(0, job.attempt || 0);
    const checkpoint =
      job.checkpoint && typeof job.checkpoint === "object"
        ? { ...job.checkpoint }
        : {};

    while (attempt < context.maxAttempts) {
      attempt += 1;
      const runningJob = await claimRunningJob(job, context, attempt);

      if (!runningJob) {
        const backpressureOutcome = await throwBackpressureForClaim(
          job,
          context,
        );
        if (backpressureOutcome) {
          return backpressureOutcome;
        }
      }

      job = runningJob;

      try {
        return await runSingleAttempt(job, context, attempt, checkpoint);
      } catch (err) {
        const retry = await handleAttemptFailure(err, job, context, attempt);
        if (retry?.shouldRetry) {
          job = retry.job;
        }
      }
    }

    throw new AppError({
      code: "INTERNAL_ERROR",
      message: `Job '${context.jobType}' exceeded max attempts without terminal result`,
    });
  }

  return {
    runJob,
  };
}

module.exports = {
  computeBackoffMs,
  createJobOrchestrator,
  isTransientError,
  isDuplicateIdempotencyError,
  isRunningSlotConflictError,
};
