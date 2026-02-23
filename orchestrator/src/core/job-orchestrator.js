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

  return Math.floor(lower + Math.random() * (upper - lower));
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

  async function runJob(input = {}) {
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

    function resolveExistingJob(existingJob) {
      if (!existingJob) {
        return null;
      }

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

      if (existingJob.status === "failed") {
        logger.info("job idempotency reused failed job", {
          jobType,
          jobId: existingJob.jobId,
          idempotencyKey,
          errorCode: existingJob.errorCode || null,
        });

        throw new AppError({
          code: existingJob.errorCode || "INTERNAL_ERROR",
          message:
            existingJob.errorMessage || `Job '${jobType}' previously failed`,
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

      return null;
    }

    let job = await jobsRepo.getByIdempotency(jobType, idempotencyKey);
    const existingOutcome = resolveExistingJob(job);
    if (existingOutcome) {
      return existingOutcome;
    }

    const runningForType = await jobsRepo.countRunningByType(jobType);
    if (runningForType >= maxConcurrent) {
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

    if (!job) {
      const jobId = input.jobId || crypto.randomUUID();
      try {
        job = await jobsRepo.createQueuedJob({
          jobId,
          jobType,
          idempotencyKey,
          runContext,
          checkpoint: {},
        });
      } catch (err) {
        if (!isDuplicateIdempotencyError(err)) {
          throw err;
        }

        job = await jobsRepo.getByIdempotency(jobType, idempotencyKey);
        const racedOutcome = resolveExistingJob(job);
        if (racedOutcome) {
          return racedOutcome;
        }

        if (!job) {
          throw err;
        }
      }
    }

    let attempt = Math.max(0, job.attempt || 0);
    let checkpoint = job.checkpoint || {};

    while (attempt < maxAttempts) {
      attempt += 1;
      const jobId = job.jobId;
      let runningJob = null;

      try {
        runningJob =
          typeof jobsRepo.claimRunning === "function"
            ? await jobsRepo.claimRunning({
                jobId,
                jobType,
                attempt,
                maxConcurrent,
              })
            : await jobsRepo.markRunning({ jobId, attempt });
      } catch (err) {
        if (!isRunningSlotConflictError(err)) {
          throw err;
        }
      }

      if (!runningJob) {
        if (typeof jobsRepo.getById === "function") {
          const latest = await jobsRepo.getById(jobId).catch(() => null);
          const latestOutcome = resolveExistingJob(latest);
          if (latestOutcome) {
            return latestOutcome;
          }
        }

        const currentRunning = await jobsRepo
          .countRunningByType(jobType)
          .catch(() => maxConcurrent);
        throw new AppError({
          code: "JOB_BACKPRESSURE",
          message: `Backpressure active for jobType '${jobType}' (running=${currentRunning}, limit=${maxConcurrent})`,
          details: {
            jobId,
            jobType,
            running: currentRunning,
            limit: maxConcurrent,
          },
        });
      }

      job = runningJob;

      try {
        logger.info("job attempt started", {
          jobType,
          jobId: job.jobId,
          idempotencyKey,
          attempt,
        });

        const result = await execute({
          job,
          attempt,
          runContext,
          checkpoint,
          async updateCheckpoint(nextCheckpoint) {
            checkpoint =
              nextCheckpoint && typeof nextCheckpoint === "object"
                ? nextCheckpoint
                : checkpoint;
            await jobsRepo.updateCheckpoint({
              jobId: job.jobId,
              checkpoint,
            });
          },
        });

        job = await jobsRepo.markSucceeded({
          jobId: job.jobId,
          resultContext:
            result && typeof result === "object"
              ? result
              : {
                  result,
                },
        });

        logger.info("job attempt succeeded", {
          jobType,
          jobId: job.jobId,
          idempotencyKey,
          attempt,
        });

        return {
          accepted: true,
          reused: false,
          inFlight: false,
          job,
        };
      } catch (err) {
        const appErr = toAppError(err);
        const transient = isTransientError(err, transientCodes);

        if (transient && attempt < maxAttempts) {
          job = await jobsRepo.markRetryWait({
            jobId: job.jobId,
            errorCode: appErr.code,
            errorMessage: appErr.message,
          });

          const delayMs = computeBackoffMs(attempt, input.backoff || {});
          logger.warn(
            "job attempt failed with transient error; retry scheduled",
            {
              jobType,
              jobId: job.jobId,
              idempotencyKey,
              attempt,
              errorCode: appErr.code,
              delayMs,
            },
          );
          await sleep(delayMs);
          continue;
        }

        job = await jobsRepo.markFailed({
          jobId: job.jobId,
          errorCode: appErr.code,
          errorMessage: appErr.message,
        });

        logger.error("job failed", {
          jobType,
          jobId: job.jobId,
          idempotencyKey,
          attempt,
          errorCode: appErr.code,
        });

        throw new AppError({
          code: appErr.code,
          message: appErr.message,
          details: {
            jobId: job.jobId,
            jobType,
            idempotencyKey,
            attempt,
          },
          cause: appErr,
        });
      }
    }

    throw new AppError({
      code: "INTERNAL_ERROR",
      message: `Job '${jobType}' exceeded max attempts without terminal result`,
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
