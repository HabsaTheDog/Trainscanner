const { runLegacyDataScript } = require('../../core/pipeline-runner');
const { createCircuitBreaker } = require('../../core/circuit-breaker');

function createSourceDiscoveryService(deps = {}) {
  const runScript = deps.runLegacyDataScript || runLegacyDataScript;
  const fetchBreaker =
    deps.fetchBreaker ||
    createCircuitBreaker({
      name: 'source-discovery.fetch',
      failureThreshold: Number.parseInt(process.env.SOURCE_FETCH_CIRCUIT_THRESHOLD || '3', 10),
      cooldownMs: Number.parseInt(process.env.SOURCE_FETCH_CIRCUIT_COOLDOWN_MS || '15000', 10)
    });
  const verifyBreaker =
    deps.verifyBreaker ||
    createCircuitBreaker({
      name: 'source-discovery.verify',
      failureThreshold: Number.parseInt(process.env.SOURCE_VERIFY_CIRCUIT_THRESHOLD || '3', 10),
      cooldownMs: Number.parseInt(process.env.SOURCE_VERIFY_CIRCUIT_COOLDOWN_MS || '15000', 10)
    });

  return {
    fetchSources(options = {}) {
      return fetchBreaker.execute(() =>
        runScript({
          rootDir: options.rootDir,
          runId: options.runId,
          args: options.args,
          env: options.env,
          service: 'source-discovery.fetch',
          scriptFile: 'fetch-dach-sources.impl.sh',
          errorCode: 'SOURCE_FETCH_FAILED',
          runCommand: options.runCommand,
          logger: options.logger,
          loggerFactory: options.loggerFactory
        })
      );
    },

    verifySources(options = {}) {
      return verifyBreaker.execute(() =>
        runScript({
          rootDir: options.rootDir,
          runId: options.runId,
          args: options.args,
          env: options.env,
          service: 'source-discovery.verify',
          scriptFile: 'verify-dach-sources.impl.sh',
          errorCode: 'SOURCE_VERIFY_FAILED',
          runCommand: options.runCommand,
          logger: options.logger,
          loggerFactory: options.loggerFactory
        })
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
  verifySources
};
