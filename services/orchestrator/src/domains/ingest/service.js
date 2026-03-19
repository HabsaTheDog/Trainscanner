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
const {
  createPipelineStageRepo,
} = require("../../data/postgis/repositories/pipeline-stage-repo");
const { createJobOrchestrator } = require("../../core/job-orchestrator");
const {
  computeCodeFingerprint,
  fingerprintsMatch,
  normalizeStageScope,
  parseStageScopeArgs,
} = require("../pipeline/stage-runtime");
const {
  getStageInputFingerprint,
  getStageSummary,
  runTrackedStage,
} = require("../pipeline/stage-tracking");

const INGEST_STAGE_CODE_PATHS = [
  "services/orchestrator/src/domains/ingest/service.js",
  "services/orchestrator/src/cli/ingest-netex.js",
  "scripts/data/ingest-netex.sh",
  "scripts/data/ingest-netex.impl.sh",
  "scripts/data/netex_extract_stops.py",
  "scripts/data/netex_extract_timetable.py",
];

function resolveIngestStage(args = []) {
  const tokens = Array.isArray(args) ? args : [];
  for (let index = 0; index < tokens.length; index += 1) {
    if (tokens[index] === "--mode") {
      const mode = String(tokens[index + 1] || "").trim();
      if (mode === "export-schedule") {
        return "export-schedule";
      }
      return "stop-topology";
    }
  }
  return "stop-topology";
}

function isTruthy(value) {
  return ["1", "true", "yes", "on"].includes(
    String(value || "")
      .trim()
      .toLowerCase(),
  );
}

function resolveSkipUnchangedEnabled(options = {}) {
  if (options.skipUnchangedEnabled !== undefined) {
    return Boolean(options.skipUnchangedEnabled);
  }
  return !isTruthy(options.env?.INGEST_PIPELINE_DISABLE_SKIP_UNCHANGED);
}

function createIngestService(deps = {}) {
  const runScript = deps.runLegacyDataScript || runLegacyDataScript;
  const createClient = deps.createPostgisClient || createPostgisClient;
  const createJobsRepo = deps.createPipelineJobsRepo || createPipelineJobsRepo;
  const createStageRepo =
    deps.createPipelineStageRepo || createPipelineStageRepo;
  const createOrchestrator =
    deps.createJobOrchestrator || createJobOrchestrator;
  const computeFingerprint =
    deps.computeCodeFingerprint || computeCodeFingerprint;
  const getInputFingerprint =
    deps.getStageInputFingerprint || getStageInputFingerprint;
  const getSummary = deps.getStageSummary || getStageSummary;

  async function runIngestTrackedStage({
    client,
    rootDir,
    args,
    runScriptCall,
    options,
  }) {
    const stageId = resolveIngestStage(args);
    const scope = parseStageScopeArgs(args);
    const stageRepo = createStageRepo(client);
    const normalizedScope = normalizeStageScope(scope);
    const codeFingerprint = await computeFingerprint(
      rootDir,
      INGEST_STAGE_CODE_PATHS,
    );
    const inputFingerprint = await getInputFingerprint(
      client,
      stageId,
      normalizedScope,
    );
    const previousMaterialization = await stageRepo.getMaterialization(
      stageId,
      normalizedScope.scopeKey,
    );
    const canSkip =
      resolveSkipUnchangedEnabled(options) &&
      previousMaterialization &&
      previousMaterialization.status === "ready" &&
      previousMaterialization.code_fingerprint === codeFingerprint &&
      fingerprintsMatch(
        previousMaterialization.input_fingerprint,
        inputFingerprint,
      );

    return runTrackedStage({
      client,
      stageRepo,
      rootDir,
      stageId,
      scope,
      codePaths: INGEST_STAGE_CODE_PATHS,
      codeFingerprint,
      inputFingerprint,
      cacheHit: canSkip,
      skippedUnchanged: canSkip,
      execute: async () => {
        if (canSkip) {
          return {
            ok: true,
            summary: await getSummary(client, stageId, normalizedScope),
          };
        }
        return runScriptCall();
      },
    });
  }

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
        const client = createClient({ rootDir, env: options.env });
        try {
          await client.ensureReady();
          return await runIngestTrackedStage({
            client,
            rootDir,
            args,
            runScriptCall,
            options,
          });
        } finally {
          if (client && typeof client.end === "function") {
            await client.end();
          }
        }
      }

      const client = createClient({ rootDir });
      try {
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
            const result = await runIngestTrackedStage({
              client,
              rootDir,
              args,
              runScriptCall,
              options,
            });
            await updateCheckpoint({
              completedAt: new Date().toISOString(),
              script: "ingest-netex.impl.sh",
            });
            return result;
          },
        });
      } finally {
        if (client && typeof client.end === "function") {
          await client.end();
        }
      }
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
