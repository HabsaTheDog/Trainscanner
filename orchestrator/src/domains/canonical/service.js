const { runLegacyDataScript, buildIdempotencyKey, createPipelineLogger } = require('../../core/pipeline-runner');
const { createPostgisClient } = require('../../data/postgis/client');
const { createPipelineJobsRepo } = require('../../data/postgis/repositories/pipeline-jobs-repo');
const { createJobOrchestrator } = require('../../core/job-orchestrator');

function createCanonicalService(deps = {}) {
  const runScript = deps.runLegacyDataScript || runLegacyDataScript;
  const createClient = deps.createPostgisClient || createPostgisClient;
  const createJobsRepo = deps.createPipelineJobsRepo || createPipelineJobsRepo;
  const createOrchestrator = deps.createJobOrchestrator || createJobOrchestrator;

  async function runWithJobOrchestration(options, config) {
    const rootDir = options.rootDir || process.cwd();
    const args = Array.isArray(options.args) ? options.args : [];
    const runId = options.runId || '';
    const jobOrchestrationEnabled =
      options.jobOrchestrationEnabled !== undefined
        ? Boolean(options.jobOrchestrationEnabled)
        : String(process.env.PIPELINE_JOB_ORCHESTRATION_ENABLED || 'true').toLowerCase() !== 'false';
    const helpRequested = args.includes('--help') || args.includes('-h');

    const runScriptCall = () =>
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
      return runScriptCall();
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
        const result = await runScriptCall();
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
        scriptFile: 'build-canonical-stations.legacy.sh',
        errorCode: 'CANONICAL_BUILD_FAILED',
        jobType: 'canonical.build-stations'
      });
    },

    buildReviewQueue(options = {}) {
      return runWithJobOrchestration(options, {
        service: 'canonical.build-review-queue',
        scriptFile: 'build-review-queue.legacy.sh',
        errorCode: 'REVIEW_QUEUE_BUILD_FAILED',
        jobType: 'canonical.build-review-queue'
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
