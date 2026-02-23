const { Worker } = require("@temporalio/worker");
const _path = require("node:path");
const activities = require("./activities");
const { createPostgisClient } = require("../data/postgis/client");
const { loadConfig } = require("../config");

async function run() {
  const config = loadConfig();

  // Reuse our Phase 1 pooled PostGIS client for the worker context
  const dbClient = createPostgisClient({ env: process.env });
  await dbClient.ensureReady();

  // Initialize Temporal worker
  // By default, Temporal uses `localhost:7233`. If running in docker,
  // we would override this via TEMPORAL_ADDRESS env var during client creation,
  // but for the worker side `Worker.create` resolves it through the default
  // NativeConnection under the hood, or we explicitly pass a connection.

  // For production, create a NativeConnection using TEMPORAL_ADDRESS.
  const { NativeConnection } = require("@temporalio/worker");
  const connection = await NativeConnection.connect({
    address: process.env.TEMPORAL_ADDRESS || "localhost:7233",
  });

  const worker = await Worker.create({
    connection,
    namespace: "default",
    taskQueue: "review-pipeline",
    // In JS we point to the compiled/executable workflows file
    workflowsPath: require.resolve("./workflows"),
    activities: {
      ...activities.createIngestionActivities(dbClient, config),
    },
  });

  console.log("Temporal Worker started on taskQueue: review-pipeline");
  await worker.run();
}

if (require.main === module) {
  run().catch((err) => {
    console.error("Temporal worker failed:", err);
    process.exit(1);
  });
}

module.exports = { run };
