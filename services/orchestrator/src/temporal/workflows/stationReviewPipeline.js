const { proxyActivities } = require("@temporalio/workflow");

// Setup Node.js (Orchestrator) activities
const { runDbBootstrap, runFetchSources, buildGlobalModel, checkMotisReady } =
  proxyActivities({
    startToCloseTimeout: "1 hour",
    retry: {
      initialInterval: "10s",
      backoffCoefficient: 2,
      maximumInterval: "5m",
      maximumAttempts: 3,
    },
  });

// Setup Rust Ingestion Activities
const { extract_netex_stops } = proxyActivities({
  startToCloseTimeout: "4 hours", // The Rust worker can take hours for Europe-wide extraction
  retry: {
    initialInterval: "10s",
    backoffCoefficient: 2,
    maximumInterval: "5m",
    maximumAttempts: 3,
  },
});

/**
 * Main Orchestration Workflow
 * @param {Object} args
 * @param {boolean} args.skipDbBootstrap
 * @param {Array<string>} args.refreshArgs
 */
async function stationReviewPipeline(args = {}) {
  const { skipDbBootstrap = false, refreshArgs = [] } = args;

  if (!skipDbBootstrap) {
    await runDbBootstrap();
  }

  // 1. Fetch source data for the requested import scope.
  await runFetchSources(refreshArgs);

  // 2. Extract NeTEx (Rust Worker natively via Postgres COPY stream)
  // Hardcoding payload for the PoC. In reality this would be dynamic based on fetch output.
  await extract_netex_stops({
    zip_path: "/app/data/raw/DE/delfi/netex/2026-02-20/delfi-netex.zip",
    source_id: "delfi-de",
    snapshot_date: "2026-02-20",
    import_run_id: "test-run",
    provider_slug: "delfi",
    country: "DE",
    manifest_sha256: "",
  });

  // 3. Rebuild the global station model and merge queue over the full active dataset.
  await buildGlobalModel(refreshArgs);

  // 4. Post-pipeline health check
  await checkMotisReady();

  return {
    success: true,
    message: "Hybrid Node/Rust pipeline completed successfully",
  };
}

module.exports = {
  stationReviewPipeline,
};
