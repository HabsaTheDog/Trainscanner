const { runLegacyDataScript } = require("../../core/pipeline-runner");
const { createCircuitBreaker } = require("../../core/circuit-breaker");
const { readCircuitBreakerConfig } = require("../../core/runtime");

function createSourceDiscoveryService(deps = {}) {
  const runScript = deps.runLegacyDataScript || runLegacyDataScript;
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
      return fetchBreaker.execute(() =>
        runScript({
          rootDir: options.rootDir,
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
      );
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
