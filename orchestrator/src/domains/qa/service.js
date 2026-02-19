const { runLegacyDataScript, buildIdempotencyKey, createPipelineLogger } = require('../../core/pipeline-runner');
const { createPostgisClient } = require('../../data/postgis/client');
const { createPipelineJobsRepo } = require('../../data/postgis/repositories/pipeline-jobs-repo');
const { createJobOrchestrator } = require('../../core/job-orchestrator');

function createQaService(deps = {}) {
  const runScript = deps.runLegacyDataScript || runLegacyDataScript;
  const createClient = deps.createPostgisClient || createPostgisClient;
  const createJobsRepo = deps.createPipelineJobsRepo || createPipelineJobsRepo;
  const createOrchestrator = deps.createJobOrchestrator || createJobOrchestrator;

  return {
    async reportReviewQueue(options = {}) {
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
          service: 'qa.report-review-queue',
          scriptFile: 'report-review-queue.legacy.sh',
          errorCode: 'REVIEW_QUEUE_REPORT_FAILED',
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
      const logger =
        options.logger ||
        createPipelineLogger(rootDir, 'qa.report-review-queue', runId || 'job');
      const jobOrchestrator = createOrchestrator({
        jobsRepo,
        logger
      });

      return jobOrchestrator.runJob({
        jobType: 'qa.report-review-queue',
        idempotencyKey: options.idempotencyKey || buildIdempotencyKey('qa.report-review-queue', args),
        runContext: {
          args
        },
        maxAttempts: Number.parseInt(process.env.PIPELINE_JOB_MAX_ATTEMPTS || '3', 10),
        maxConcurrent: Number.parseInt(process.env.PIPELINE_JOB_MAX_CONCURRENT || '1', 10),
        execute: async ({ updateCheckpoint }) => {
          const result = await runScriptCall();
          await updateCheckpoint({
            completedAt: new Date().toISOString(),
            script: 'report-review-queue.legacy.sh'
          });
          return result;
        }
      });
    }
  };
}

const defaultService = createQaService();

function reportReviewQueue(options) {
  return defaultService.reportReviewQueue(options);
}

module.exports = {
  createQaService,
  reportReviewQueue
};
