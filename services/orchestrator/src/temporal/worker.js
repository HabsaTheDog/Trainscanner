const { Worker } = require("@temporalio/worker");
const _path = require("node:path");
const activities = require("./activities");
const { createPostgisClient } = require("../data/postgis/client");
const { loadConfig } = require("../config");
const { resolveTemporalAddress } = require("../core/runtime");
const { createLogger } = require("../logger");

function buildWorkerConnectionOptions(env = process.env) {
  return {
    address: resolveTemporalAddress(env),
  };
}

function buildWorkerOptions(connection, dbClient, config) {
  return {
    connection,
    namespace: "default",
    taskQueue: "review-pipeline",
    workflowsPath: require.resolve("./workflows"),
    activities: {
      ...activities.createAiEvaluationActivities(dbClient, config),
      ...activities.createIngestionActivities(dbClient, config),
      ...activities.createCompileActivities(dbClient, config),
    },
  };
}

async function run() {
  const config = loadConfig();
  const logger = createLogger(config.switchLogPath, {
    service: "temporal.worker",
  });

  // Reuse our Phase 1 pooled PostGIS client for the worker context
  const dbClient = createPostgisClient({ env: process.env });
  await dbClient.ensureReady();

  // Initialize Temporal worker
  // By default, Temporal uses `localhost:7233`. If running in docker,
  // we would override this via TEMPORAL_ADDRESS env var during client creation,
  // but for the worker side `Worker.create` resolves it through the default
  // NativeConnection under the hood, or we explicitly pass a connection.

  const { NativeConnection } = require("@temporalio/worker");
  const connection = await NativeConnection.connect(
    buildWorkerConnectionOptions(),
  );
  const worker = await Worker.create(
    buildWorkerOptions(connection, dbClient, config),
  );

  logger.info("Temporal worker started", { taskQueue: "review-pipeline" });
  await worker.run();
}

async function runCli() {
  try {
    await run();
    return 0;
  } catch (err) {
    const config = loadConfig();
    const logger = createLogger(config.switchLogPath, {
      service: "temporal.worker",
    });
    logger.error("Temporal worker failed", { err });
    return 1;
  }
}

function startCli() {
  void runCli().then((exitCode) => {
    process.exitCode = exitCode;
  });
}

if (require.main === module) {
  startCli();
}

module.exports = {
  buildWorkerConnectionOptions,
  buildWorkerOptions,
  run,
  runCli,
};
