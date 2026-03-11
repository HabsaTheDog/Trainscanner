const {
  runLegacyDataScript,
  buildIdempotencyKey,
  createPipelineLogger,
} = require("../../core/pipeline-runner");
const { readJobExecutionConfig } = require("../../core/runtime");
const { createPostgisClient } = require("../../data/postgis/client");
const {
  createPipelineJobsRepo,
} = require("../../data/postgis/repositories/pipeline-jobs-repo");
const { createJobOrchestrator } = require("../../core/job-orchestrator");

function createIngestService(deps = {}) {
  const runScript = deps.runLegacyDataScript || runLegacyDataScript;
  const createClient = deps.createPostgisClient || createPostgisClient;
  const createJobsRepo = deps.createPipelineJobsRepo || createPipelineJobsRepo;
  const createOrchestrator =
    deps.createJobOrchestrator || createJobOrchestrator;

  return {
    async ingestNetex(options = {}) {
      const rootDir = options.rootDir || process.cwd();
      const args = Array.isArray(options.args) ? options.args : [];
      const runId = options.runId || "";
      const jobExecutionConfig = readJobExecutionConfig(options.env);
      const jobOrchestrationEnabled =
        options.jobOrchestrationEnabled === undefined
          ? jobExecutionConfig.jobOrchestrationEnabled
          : Boolean(options.jobOrchestrationEnabled);
      const helpRequested = args.includes("--help") || args.includes("-h");

      const runScriptCall = () =>
        runScript({
          rootDir,
          runId,
          args,
          service: "ingest.netex",
          scriptFile: "ingest-netex.impl.sh",
          errorCode: "INGEST_FAILED",
          runCommand: options.runCommand,
          logger: options.logger,
          loggerFactory: options.loggerFactory,
        });

      if (!jobOrchestrationEnabled || helpRequested) {
        return runScriptCall();
      }

      const client = createClient({ rootDir });
      await client.ensureReady();
      const jobsRepo = createJobsRepo(client);
      const logger =
        options.logger ||
        createPipelineLogger(rootDir, "ingest.netex", runId || "job");
      const jobOrchestrator = createOrchestrator({
        jobsRepo,
        logger,
      });

      return jobOrchestrator.runJob({
        jobType: "ingest.netex",
        idempotencyKey:
          options.idempotencyKey || buildIdempotencyKey("ingest.netex", args),
        runContext: {
          args,
        },
        maxAttempts: jobExecutionConfig.maxAttempts,
        maxConcurrent: jobExecutionConfig.maxConcurrent,
        execute: async ({ updateCheckpoint }) => {
          const result = await runScriptCall();
          await updateCheckpoint({
            completedAt: new Date().toISOString(),
            script: "ingest-netex.impl.sh",
          });
          return result;
        },
      });
    },
  };
}

const defaultService = createIngestService();

function ingestNetex(options) {
  return defaultService.ingestNetex(options);
}

module.exports = {
  createIngestService,
  ingestNetex,
};
