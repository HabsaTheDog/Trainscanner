const { runLegacyDataScript } = require("../../core/pipeline-runner");
const { createCircuitBreaker } = require("../../core/circuit-breaker");
const { readCircuitBreakerConfig } = require("../../core/runtime");
const { createPostgisClient } = require("../../data/postgis/client");
const { parseStageScopeArgs } = require("../pipeline/stage-runtime");
const { runTrackedStage } = require("../pipeline/stage-tracking");

const FETCH_STAGE_CODE_PATHS = [
  "services/orchestrator/src/domains/source-discovery/service.js",
  "services/orchestrator/src/cli/fetch-sources.js",
  "scripts/data/fetch-sources.sh",
  "scripts/data/fetch-sources.impl.sh",
  "config/europe-data-sources.json",
];

function createSourceDiscoveryService(deps = {}) {
  const runScript = deps.runLegacyDataScript || runLegacyDataScript;
  const createClient = deps.createPostgisClient || createPostgisClient;
  const env = deps.env || process.env;
  const fetchBreakerConfig = readCircuitBreakerConfig(env, {
    thresholdKey: "SOURCE_FETCH_CIRCUIT_THRESHOLD",
    cooldownKey: "SOURCE_FETCH_CIRCUIT_COOLDOWN_MS",
  });
  const verifyBreakerConfig = readCircuitBreakerConfig(env, {
    thresholdKey: "SOURCE_VERIFY_CIRCUIT_THRESHOLD",
    cooldownKey: "SOURCE_VERIFY_CIRCUIT_COOLDOWN_MS",
  });
  const fetchBreaker =
    deps.fetchBreaker ||
    createCircuitBreaker({
      name: "source-discovery.fetch",
      failureThreshold: fetchBreakerConfig.failureThreshold,
      cooldownMs: fetchBreakerConfig.cooldownMs,
    });
  const verifyBreaker =
    deps.verifyBreaker ||
    createCircuitBreaker({
      name: "source-discovery.verify",
      failureThreshold: verifyBreakerConfig.failureThreshold,
      cooldownMs: verifyBreakerConfig.cooldownMs,
    });

  return {
    fetchSources(options = {}) {
      return fetchBreaker.execute(async () => {
        const rootDir = options.rootDir || process.cwd();
        const client = createClient({ rootDir, env: options.env });
        try {
          await client.ensureReady();
          return await runTrackedStage({
            client,
            rootDir,
            stageId: "fetch",
            scope: parseStageScopeArgs(options.args),
            codePaths: FETCH_STAGE_CODE_PATHS,
            execute: async () =>
              runScript({
                rootDir,
                runId: options.runId,
                args: options.args,
                env: options.env,
                service: "source-discovery.fetch",
                scriptFile: "fetch-sources.impl.sh",
                errorCode: "SOURCE_FETCH_FAILED",
                runCommand: options.runCommand,
                logger: options.logger,
                loggerFactory: options.loggerFactory,
              }),
          });
        } finally {
          if (client && typeof client.end === "function") {
            await client.end();
          }
        }
      });
    },

    verifySources(options = {}) {
      return verifyBreaker.execute(() =>
        runScript({
          rootDir: options.rootDir,
          runId: options.runId,
          args: options.args,
          env: options.env,
          service: "source-discovery.verify",
          scriptFile: "verify-sources.impl.sh",
          errorCode: "SOURCE_VERIFY_FAILED",
          runCommand: options.runCommand,
          logger: options.logger,
          loggerFactory: options.loggerFactory,
        }),
      );
    },
  };
}

const defaultService = createSourceDiscoveryService();

function fetchSources(options) {
  return defaultService.fetchSources(options);
}

function verifySources(options) {
  return defaultService.verifySources(options);
}

module.exports = {
  createSourceDiscoveryService,
  fetchSources,
  verifySources,
};
