const crypto = require('node:crypto');
const { runLegacyDataScript, buildIdempotencyKey, createPipelineLogger } = require('../../core/pipeline-runner');
const { AppError } = require('../../core/errors');
const { createPostgisClient } = require('../../data/postgis/client');
const { createPipelineJobsRepo } = require('../../data/postgis/repositories/pipeline-jobs-repo');
const { createJobOrchestrator } = require('../../core/job-orchestrator');
const { createReviewQueueRepo } = require('../../data/postgis/repositories/review-queue-repo');
const { createCanonicalStationsRepo } = require('../../data/postgis/repositories/canonical-stations-repo');
const { createImportRunsRepo } = require('../../data/postgis/repositories/import-runs-repo');

function isIsoDate(value) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return false;
  }
  const parsed = new Date(`${value}T00:00:00Z`);
  return Number.isFinite(parsed.getTime());
}

function printBuildReviewQueueUsage() {
  process.stdout.write('Usage: scripts/data/build-review-queue.sh [options]\n');
  process.stdout.write('\n');
  process.stdout.write('Build deterministic canonical-station QA review queue items.\n');
  process.stdout.write('\n');
  process.stdout.write('Options:\n');
  process.stdout.write('  --country DE|AT|CH    Restrict to one country\n');
  process.stdout.write('  --as-of YYYY-MM-DD    Restrict canonical mappings to snapshot_date <= date\n');
  process.stdout.write('  --geo-threshold-m N   Suspicious spread threshold in meters (default: 3000)\n');
  process.stdout.write('  --close-missing       Mark open/confirmed items as auto_resolved when not redetected (default)\n');
  process.stdout.write('  --no-close-missing    Keep previously open items untouched\n');
  process.stdout.write('  -h, --help            Show this help\n');
}

function printBuildCanonicalUsage() {
  process.stdout.write('Usage: scripts/data/build-canonical-stations.sh [options]\n');
  process.stdout.write('\n');
  process.stdout.write('Build canonical stations from NeTEx staging rows.\n');
  process.stdout.write('\n');
  process.stdout.write('Options:\n');
  process.stdout.write('  --country DE|AT|CH   Restrict build scope to one country\n');
  process.stdout.write('  --as-of YYYY-MM-DD   Use latest snapshots <= date\n');
  process.stdout.write('  --source-id ID       Restrict build scope to one source id\n');
  process.stdout.write('  -h, --help           Show this help\n');
}

function parseBuildCanonicalArgs(args = []) {
  const parsed = {
    helpRequested: false,
    scope: {
      country: '',
      asOf: '',
      sourceId: ''
    }
  };
  const tokens = Array.isArray(args) ? args : [];

  for (let i = 0; i < tokens.length; i += 1) {
    const token = String(tokens[i] || '');

    if (token === '-h' || token === '--help') {
      parsed.helpRequested = true;
      continue;
    }

    if (token === '--country') {
      const value = String(tokens[i + 1] || '').trim().toUpperCase();
      if (!value) {
        throw new AppError({
          code: 'INVALID_REQUEST',
          message: 'Missing value for --country'
        });
      }
      if (!['DE', 'AT', 'CH'].includes(value)) {
        throw new AppError({
          code: 'INVALID_REQUEST',
          message: "Invalid --country value (expected 'DE', 'AT', or 'CH')"
        });
      }
      parsed.scope.country = value;
      i += 1;
      continue;
    }

    if (token === '--as-of') {
      const value = String(tokens[i + 1] || '').trim();
      if (!value) {
        throw new AppError({
          code: 'INVALID_REQUEST',
          message: 'Missing value for --as-of'
        });
      }
      if (!isIsoDate(value)) {
        throw new AppError({
          code: 'INVALID_REQUEST',
          message: 'Invalid --as-of value (expected YYYY-MM-DD)'
        });
      }
      parsed.scope.asOf = value;
      i += 1;
      continue;
    }

    if (token === '--source-id') {
      const value = String(tokens[i + 1] || '').trim();
      if (!value) {
        throw new AppError({
          code: 'INVALID_REQUEST',
          message: 'Missing value for --source-id'
        });
      }
      parsed.scope.sourceId = value;
      i += 1;
      continue;
    }

    throw new AppError({
      code: 'INVALID_REQUEST',
      message: `Unknown argument: ${token}`
    });
  }

  return parsed;
}

function parseBuildReviewQueueArgs(args = []) {
  const parsed = {
    helpRequested: false,
    scope: {
      country: '',
      asOf: '',
      geoThresholdMeters: 3000,
      closeMissing: true
    }
  };
  const tokens = Array.isArray(args) ? args : [];

  for (let i = 0; i < tokens.length; i += 1) {
    const token = String(tokens[i] || '');

    if (token === '-h' || token === '--help') {
      parsed.helpRequested = true;
      continue;
    }

    if (token === '--country') {
      const value = String(tokens[i + 1] || '').trim().toUpperCase();
      if (!value) {
        throw new AppError({
          code: 'INVALID_REQUEST',
          message: 'Missing value for --country'
        });
      }
      if (!['DE', 'AT', 'CH'].includes(value)) {
        throw new AppError({
          code: 'INVALID_REQUEST',
          message: "Invalid --country value (expected 'DE', 'AT', or 'CH')"
        });
      }
      parsed.scope.country = value;
      i += 1;
      continue;
    }

    if (token === '--as-of') {
      const value = String(tokens[i + 1] || '').trim();
      if (!value) {
        throw new AppError({
          code: 'INVALID_REQUEST',
          message: 'Missing value for --as-of'
        });
      }
      if (!isIsoDate(value)) {
        throw new AppError({
          code: 'INVALID_REQUEST',
          message: 'Invalid --as-of value (expected YYYY-MM-DD)'
        });
      }
      parsed.scope.asOf = value;
      i += 1;
      continue;
    }

    if (token === '--geo-threshold-m') {
      const value = String(tokens[i + 1] || '').trim();
      if (!value) {
        throw new AppError({
          code: 'INVALID_REQUEST',
          message: 'Missing value for --geo-threshold-m'
        });
      }
      const parsedInt = Number.parseInt(value, 10);
      if (!Number.isFinite(parsedInt) || parsedInt < 0) {
        throw new AppError({
          code: 'INVALID_REQUEST',
          message: '--geo-threshold-m must be a non-negative integer'
        });
      }
      parsed.scope.geoThresholdMeters = parsedInt;
      i += 1;
      continue;
    }

    if (token === '--close-missing') {
      parsed.scope.closeMissing = true;
      continue;
    }

    if (token === '--no-close-missing') {
      parsed.scope.closeMissing = false;
      continue;
    }

    throw new AppError({
      code: 'INVALID_REQUEST',
      message: `Unknown argument: ${token}`
    });
  }

  return parsed;
}

function createCanonicalService(deps = {}) {
  const runScript = deps.runLegacyDataScript || runLegacyDataScript;
  const createClient = deps.createPostgisClient || createPostgisClient;
  const createJobsRepo = deps.createPipelineJobsRepo || createPipelineJobsRepo;
  const createOrchestrator = deps.createJobOrchestrator || createJobOrchestrator;
  const createQueueRepo = deps.createReviewQueueRepo || createReviewQueueRepo;
  const createCanonicalRepo = deps.createCanonicalStationsRepo || createCanonicalStationsRepo;
  const createRunsRepo = deps.createImportRunsRepo || createImportRunsRepo;

  async function runWithJobOrchestration(options, config) {
    const rootDir = options.rootDir || process.cwd();
    const args = Array.isArray(options.args) ? options.args : [];
    const runId = options.runId || '';
    const jobOrchestrationEnabled =
      options.jobOrchestrationEnabled !== undefined
        ? Boolean(options.jobOrchestrationEnabled)
        : String(process.env.PIPELINE_JOB_ORCHESTRATION_ENABLED || 'true').toLowerCase() !== 'false';
    const helpRequested = args.includes('--help') || args.includes('-h');

    const runCall =
      typeof config.execute === 'function'
        ? () =>
            config.execute({
              rootDir,
              runId,
              args,
              options
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
              loggerFactory: options.loggerFactory
            });

    if (!jobOrchestrationEnabled || helpRequested) {
      return runCall();
    }

    const client = createClient({ rootDir });
    await client.ensureReady();
    const jobsRepo = createJobsRepo(client);
    const logger = options.logger || createPipelineLogger(rootDir, config.jobType, runId || 'job');
    const jobOrchestrator = createOrchestrator({
      jobsRepo,
      logger
    });

    return jobOrchestrator.runJob({
      jobType: config.jobType,
      idempotencyKey: options.idempotencyKey || buildIdempotencyKey(config.jobType, args),
      runContext: {
        args
      },
      maxAttempts: Number.parseInt(process.env.PIPELINE_JOB_MAX_ATTEMPTS || '3', 10),
      maxConcurrent: Number.parseInt(process.env.PIPELINE_JOB_MAX_CONCURRENT || '1', 10),
      execute: async ({ updateCheckpoint }) => {
        const result = await runCall();
        await updateCheckpoint({
          completedAt: new Date().toISOString(),
          script: config.scriptFile
        });
        return result;
      }
    });
  }

  return {
    buildCanonicalStations(options = {}) {
      return runWithJobOrchestration(options, {
        service: 'canonical.build-stations',
        scriptFile: 'build-canonical-stations.js',
        errorCode: 'CANONICAL_BUILD_FAILED',
        jobType: 'canonical.build-stations',
        execute: async ({ rootDir, args }) => {
          const parsed = parseBuildCanonicalArgs(args);
          if (parsed.helpRequested) {
            printBuildCanonicalUsage();
            return {
              ok: true,
              help: true
            };
          }

          const runId = crypto.randomUUID();
          const client = createClient({ rootDir });
          await client.ensureReady();
          const canonicalRepo = createCanonicalRepo(client);
          const runsRepo = createRunsRepo(client);

          await runsRepo.createRun({
            runId,
            pipeline: 'canonical_build',
            status: 'running',
            sourceId: parsed.scope.sourceId || '',
            country: parsed.scope.country || '',
            snapshotDate: parsed.scope.asOf || ''
          });

          let summary;
          try {
            summary = await canonicalRepo.buildCanonicalStations({
              runId,
              country: parsed.scope.country,
              asOf: parsed.scope.asOf,
              sourceId: parsed.scope.sourceId
            });
          } catch (err) {
            await runsRepo
              .markFailed({
                runId,
                errorMessage: 'Canonical build failed'
              })
              .catch(() => {});
            throw err;
          }

          if (!summary || Number.parseInt(String(summary.canonicalRows || 0), 10) <= 0) {
            await runsRepo
              .markFailed({
                runId,
                errorMessage: 'Canonical build produced 0 rows',
                stats: summary || {}
              })
              .catch(() => {});
            throw new AppError({
              code: 'CANONICAL_BUILD_FAILED',
              message: 'Canonical build produced 0 rows'
            });
          }

          await runsRepo.markSucceeded({
            runId,
            stats: summary
          });

          process.stdout.write(`${JSON.stringify(summary)}\n`);
          return {
            ok: true,
            summary
          };
        }
      });
    },

    buildReviewQueue(options = {}) {
      return runWithJobOrchestration(options, {
        service: 'canonical.build-review-queue',
        scriptFile: 'build-review-queue.js',
        errorCode: 'REVIEW_QUEUE_BUILD_FAILED',
        jobType: 'canonical.build-review-queue',
        execute: async ({ rootDir, args }) => {
          const parsed = parseBuildReviewQueueArgs(args);
          if (parsed.helpRequested) {
            printBuildReviewQueueUsage();
            return {
              ok: true,
              help: true
            };
          }

          const client = createClient({ rootDir });
          await client.ensureReady();
          const queueRepo = createQueueRepo(client);
          const summary = await queueRepo.buildReviewQueue(parsed.scope);
          process.stdout.write(`${JSON.stringify(summary)}\n`);
          return {
            ok: true,
            summary
          };
        }
      });
    }
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
  createCanonicalService
};
