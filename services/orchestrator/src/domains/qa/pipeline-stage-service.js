const { runLegacyDataScript } = require("../../core/pipeline-runner");
const { createPostgisClient } = require("../../data/postgis/client");
const {
  createPipelineStageRepo,
} = require("../../data/postgis/repositories/pipeline-stage-repo");
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

const QA_NETWORK_CONTEXT_CODE_PATHS = [
  "services/orchestrator/src/domains/qa/pipeline-stage-service.js",
  "services/orchestrator/src/cli/extract-qa-network-context.js",
  "scripts/data/extract-qa-network-context.sh",
  "scripts/data/netex_extract_qa_network.py",
];

const QA_NETWORK_PROJECTION_CODE_PATHS = [
  "services/orchestrator/src/domains/qa/pipeline-stage-service.js",
  "services/orchestrator/src/cli/project-qa-network-context.js",
  "scripts/data/project-qa-network-context.sh",
];

async function closeClient(client) {
  if (client && typeof client.end === "function") {
    await client.end();
  }
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
  return !isTruthy(options.env?.QA_PIPELINE_DISABLE_SKIP_UNCHANGED);
}

function createQaPipelineStageService(deps = {}) {
  const runScript = deps.runLegacyDataScript || runLegacyDataScript;
  const createClient = deps.createPostgisClient || createPostgisClient;
  const createStageRepo =
    deps.createPipelineStageRepo || createPipelineStageRepo;
  const computeFingerprint =
    deps.computeCodeFingerprint || computeCodeFingerprint;
  const getInputFingerprint =
    deps.getStageInputFingerprint || getStageInputFingerprint;
  const getSummary = deps.getStageSummary || getStageSummary;

  function runShellStage(stageId, scriptFile, codePaths, options = {}) {
    const rootDir = options.rootDir || process.cwd();
    const args = Array.isArray(options.args) ? options.args : [];
    const client = createClient({ rootDir, env: options.env });

    return (async () => {
      try {
        await client.ensureReady();
        const scope = parseStageScopeArgs(args);
        const stageRepo = createStageRepo(client);
        const normalizedScope = normalizeStageScope(scope);
        const codeFingerprint = await computeFingerprint(rootDir, codePaths);
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

        return await runTrackedStage({
          client,
          stageRepo,
          rootDir,
          stageId,
          scope,
          codePaths,
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
            return runScript({
              rootDir,
              runId: options.runId,
              args,
              env: options.env,
              service: `qa.${stageId}`,
              scriptFile,
              errorCode: "QA_STAGE_FAILED",
              runCommand: options.runCommand,
              logger: options.logger,
              loggerFactory: options.loggerFactory,
            });
          },
        });
      } finally {
        await closeClient(client);
      }
    })();
  }

  return {
    extractQaNetworkContext(options = {}) {
      return runShellStage(
        "qa-network-context",
        "extract-qa-network-context.sh",
        QA_NETWORK_CONTEXT_CODE_PATHS,
        options,
      );
    },

    projectQaNetworkContext(options = {}) {
      return runShellStage(
        "qa-network-projection",
        "project-qa-network-context.sh",
        QA_NETWORK_PROJECTION_CODE_PATHS,
        options,
      );
    },
  };
}

const defaultService = createQaPipelineStageService();

function extractQaNetworkContext(options) {
  return defaultService.extractQaNetworkContext(options);
}

function projectQaNetworkContext(options) {
  return defaultService.projectQaNetworkContext(options);
}

module.exports = {
  createQaPipelineStageService,
  extractQaNetworkContext,
  projectQaNetworkContext,
};
