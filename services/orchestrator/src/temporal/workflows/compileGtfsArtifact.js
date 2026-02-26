const { proxyActivities } = require("@temporalio/workflow");

const { compileGtfsArtifact: runCompileGtfsArtifact } = proxyActivities({
  startToCloseTimeout: "2 hours",
  retry: {
    initialInterval: "10s",
    backoffCoefficient: 2,
    maximumInterval: "5m",
    maximumAttempts: 3,
  },
});

async function compileGtfsArtifact(args = {}) {
  const result = await runCompileGtfsArtifact(args);
  return {
    success: true,
    message: "GTFS artifact compilation completed",
    ...result,
  };
}

module.exports = {
  compileGtfsArtifact,
};
