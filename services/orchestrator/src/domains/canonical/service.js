const crypto = require("node:crypto");
const {
  runLegacyDataScript,
  buildIdempotencyKey,
  createPipelineLogger,
} = require("../../core/pipeline-runner");
const { AppError } = require("../../core/errors");
const { createPostgisClient } = require("../../data/postgis/client");
const {
  createPipelineJobsRepo,
} = require("../../data/postgis/repositories/pipeline-jobs-repo");
const { createJobOrchestrator } = require("../../core/job-orchestrator");
const {
  createReviewQueueRepo,
} = require("../../data/postgis/repositories/review-queue-repo");
const {
  createCanonicalStationsRepo,
} = require("../../data/postgis/repositories/canonical-stations-repo");
const {
  createImportRunsRepo,
} = require("../../data/postgis/repositories/import-runs-repo");
const { isStrictIsoDate } = require("../../core/date");

function printBuildReviewQueueUsage() {
  process.stdout.write("Usage: scripts/data/build-review-queue.sh [options]\n");
  process.stdout.write("\n");
  process.stdout.write(
    "Build deterministic canonical-station QA review queue items.\n",
  );
  process.stdout.write("\n");
  process.stdout.write("Options:\n");
  process.stdout.write("  --country DE|AT|CH    Restrict to one country\n");
  process.stdout.write(
    "  --as-of YYYY-MM-DD    Restrict canonical mappings to snapshot_date <= date\n",
  );
  process.stdout.write(
    "  --geo-threshold-m N   Suspicious spread threshold in meters (default: 3000)\n",
  );
  process.stdout.write(
    "  --close-missing       Mark open/confirmed items as auto_resolved when not redetected (default)\n",
  );
  process.stdout.write(
    "  --no-close-missing    Keep previously open items untouched\n",
  );
  process.stdout.write("  -h, --help            Show this help\n");
}

function printBuildCanonicalUsage() {
  process.stdout.write(
    "Usage: scripts/data/build-canonical-stations.sh [options]\n",
  );
  process.stdout.write("\n");
  process.stdout.write("Build canonical stations from NeTEx staging rows.\n");
  process.stdout.write("\n");
  process.stdout.write("Options:\n");
  process.stdout.write(
    "  --country DE|AT|CH   Restrict build scope to one country\n",
  );
  process.stdout.write("  --as-of YYYY-MM-DD   Use latest snapshots <= date\n");
  process.stdout.write(
    "  --source-id ID       Restrict build scope to one source id\n",
  );
  process.stdout.write("  -h, --help           Show this help\n");
}

function readRequiredTokenValue(tokens, index, flagName) {
  const value = String(tokens[index + 1] || "").trim();
  if (!value) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: `Missing value for ${flagName}`,
    });
  }
  return value;
}

function parseCountryScope(value) {
  const normalized = String(value || "")
    .trim()
    .toUpperCase();
  if (!["DE", "AT", "CH"].includes(normalized)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: "Invalid --country value (expected 'DE', 'AT', or 'CH')",
    });
  }
  return normalized;
}

function parseAsOfScope(value) {
  if (!isStrictIsoDate(value)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: "Invalid --as-of value (expected YYYY-MM-DD)",
    });
  }
  return value;
}

function parseGeoThreshold(value) {
  const parsedInt = Number.parseInt(value, 10);
  if (!Number.isFinite(parsedInt) || parsedInt < 0) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: "--geo-threshold-m must be a non-negative integer",
    });
  }
  return parsedInt;
}

function parseBuildCanonicalArgs(args = []) {
  const parsed = {
    helpRequested: false,
    scope: {
      country: "",
      asOf: "",
      sourceId: "",
    },
  };
  const tokens = Array.isArray(args) ? args : [];

  for (let i = 0; i < tokens.length; i += 1) {
    const token = String(tokens[i] || "");

    switch (token) {
      case "-h":
      case "--help":
        parsed.helpRequested = true;
        break;
      case "--country":
        parsed.scope.country = parseCountryScope(
          readRequiredTokenValue(tokens, i, "--country"),
        );
        i += 1;
        break;
      case "--as-of":
        parsed.scope.asOf = parseAsOfScope(
          readRequiredTokenValue(tokens, i, "--as-of"),
        );
        i += 1;
        break;
      case "--source-id":
        parsed.scope.sourceId = readRequiredTokenValue(tokens, i, "--source-id");
        i += 1;
        break;
      default:
        throw new AppError({
          code: "INVALID_REQUEST",
          message: `Unknown argument: ${token}`,
        });
    }
  }

  return parsed;
}

function parseBuildReviewQueueArgs(args = []) {
  const parsed = {
    helpRequested: false,
    scope: {
      country: "",
      asOf: "",
      geoThresholdMeters: 3000,
      closeMissing: true,
    },
  };
  const tokens = Array.isArray(args) ? args : [];

  for (let i = 0; i < tokens.length; i += 1) {
    const token = String(tokens[i] || "");

    switch (token) {
      case "-h":
      case "--help":
        parsed.helpRequested = true;
        break;
      case "--country":
        parsed.scope.country = parseCountryScope(
          readRequiredTokenValue(tokens, i, "--country"),
        );
        i += 1;
        break;
      case "--as-of":
        parsed.scope.asOf = parseAsOfScope(
          readRequiredTokenValue(tokens, i, "--as-of"),
        );
        i += 1;
        break;
      case "--geo-threshold-m":
        parsed.scope.geoThresholdMeters = parseGeoThreshold(
          readRequiredTokenValue(tokens, i, "--geo-threshold-m"),
        );
        i += 1;
        break;
      case "--close-missing":
        parsed.scope.closeMissing = true;
        break;
      case "--no-close-missing":
        parsed.scope.closeMissing = false;
        break;
      default:
        throw new AppError({
          code: "INVALID_REQUEST",
          message: `Unknown argument: ${token}`,
        });
    }
  }

  return parsed;
}

function createCanonicalService(deps = {}) {
  const runScript = deps.runLegacyDataScript || runLegacyDataScript;
  const createClient = deps.createPostgisClient || createPostgisClient;
  const createJobsRepo = deps.createPipelineJobsRepo || createPipelineJobsRepo;
  const createOrchestrator =
    deps.createJobOrchestrator || createJobOrchestrator;
  const createQueueRepo = deps.createReviewQueueRepo || createReviewQueueRepo;
  const createCanonicalRepo =
    deps.createCanonicalStationsRepo || createCanonicalStationsRepo;
  const createRunsRepo = deps.createImportRunsRepo || createImportRunsRepo;

  async function runWithJobOrchestration(options, config) {
    const rootDir = options.rootDir || process.cwd();
    const args = Array.isArray(options.args) ? options.args : [];
    const runId = options.runId || "";
    const defaultJobOrchestrationEnabled =
      String(process.env.PIPELINE_JOB_ORCHESTRATION_ENABLED || "true")
        .toLowerCase() !== "false";
    const jobOrchestrationEnabled =
      options.jobOrchestrationEnabled === undefined
        ? defaultJobOrchestrationEnabled
        : Boolean(options.jobOrchestrationEnabled);
    const helpRequested = args.includes("--help") || args.includes("-h");

    const runCall =
      typeof config.execute === "function"
        ? () =>
            config.execute({
              rootDir,
              runId,
              args,
              options,
            })
        : () =>
            runScript({
              rootDir,
              runId,
              args,
              service: config.service,
              scriptFile: config.scriptFile,
              errorCode: config.errorCode,
              runCommand: options.runCommand,
              logger: options.logger,
              loggerFactory: options.loggerFactory,
            });

    if (!jobOrchestrationEnabled || helpRequested) {
      return runCall();
    }

    const client = createClient({ rootDir });
    await client.ensureReady();
    const jobsRepo = createJobsRepo(client);
    const logger =
      options.logger ||
      createPipelineLogger(rootDir, config.jobType, runId || "job");
    const jobOrchestrator = createOrchestrator({
      jobsRepo,
      logger,
    });

    return jobOrchestrator.runJob({
      jobType: config.jobType,
      idempotencyKey:
        options.idempotencyKey || buildIdempotencyKey(config.jobType, args),
      runContext: {
        args,
      },
      maxAttempts: Number.parseInt(
        process.env.PIPELINE_JOB_MAX_ATTEMPTS || "3",
        10,
      ),
      maxConcurrent: Number.parseInt(
        process.env.PIPELINE_JOB_MAX_CONCURRENT || "1",
        10,
      ),
      execute: async ({ updateCheckpoint }) => {
        const result = await runCall();
        await updateCheckpoint({
          completedAt: new Date().toISOString(),
          script: config.scriptFile,
        });
        return result;
      },
    });
  }

  return {
    buildCanonicalStations(options = {}) {
      return runWithJobOrchestration(options, {
        service: "canonical.build-stations",
        scriptFile: "build-canonical-stations.js",
        errorCode: "CANONICAL_BUILD_FAILED",
        jobType: "canonical.build-stations",
        execute: async ({ rootDir, args }) => {
          const parsed = parseBuildCanonicalArgs(args);
          if (parsed.helpRequested) {
            printBuildCanonicalUsage();
            return {
              ok: true,
              help: true,
            };
          }

          const runId = crypto.randomUUID();
          const client = createClient({ rootDir });
          await client.ensureReady();
          const canonicalRepo = createCanonicalRepo(client);
          const runsRepo = createRunsRepo(client);

          await runsRepo.createRun({
            runId,
            pipeline: "canonical_build",
            status: "running",
            sourceId: parsed.scope.sourceId || "",
            country: parsed.scope.country || "",
            snapshotDate: parsed.scope.asOf || "",
          });

          let summary;
          try {
            summary = await canonicalRepo.buildCanonicalStations({
              runId,
              country: parsed.scope.country,
              asOf: parsed.scope.asOf,
              sourceId: parsed.scope.sourceId,
            });
          } catch (err) {
            await runsRepo
              .markFailed({
                runId,
                errorMessage: "Canonical build failed",
              })
              .catch(() => {});
            throw err;
          }

          if (
            !summary ||
            Number.parseInt(String(summary.canonicalRows || 0), 10) <= 0
          ) {
            await runsRepo
              .markFailed({
                runId,
                errorMessage: "Canonical build produced 0 rows",
                stats: summary || {},
              })
              .catch(() => {});
            throw new AppError({
              code: "CANONICAL_BUILD_FAILED",
              message: "Canonical build produced 0 rows",
            });
          }

          await runsRepo.markSucceeded({
            runId,
            stats: summary,
          });

          process.stdout.write(`${JSON.stringify(summary)}\n`);
          return {
            ok: true,
            summary,
          };
        },
      });
    },

    buildReviewQueue(options = {}) {
      return runWithJobOrchestration(options, {
        service: "canonical.build-review-queue",
        scriptFile: "build-review-queue.js",
        errorCode: "REVIEW_QUEUE_BUILD_FAILED",
        jobType: "canonical.build-review-queue",
        execute: async ({ rootDir, args }) => {
          const parsed = parseBuildReviewQueueArgs(args);
          if (parsed.helpRequested) {
            printBuildReviewQueueUsage();
            return {
              ok: true,
              help: true,
            };
          }

          const client = createClient({ rootDir });
          await client.ensureReady();
          const queueRepo = createQueueRepo(client);
          const summary = await queueRepo.buildReviewQueue(parsed.scope);
          process.stdout.write(`${JSON.stringify(summary)}\n`);
          return {
            ok: true,
            summary,
          };
        },
      });
    },
  };
}

const defaultService = createCanonicalService();

function buildCanonicalStations(options) {
  return defaultService.buildCanonicalStations(options);
}

function buildReviewQueue(options) {
  return defaultService.buildReviewQueue(options);
}

module.exports = {
  buildCanonicalStations,
  buildReviewQueue,
  createCanonicalService,
};
