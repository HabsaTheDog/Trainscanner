const { proxyActivities } = require("@temporalio/workflow");

const { executeAiEvaluationBenchmark } = proxyActivities({
  startToCloseTimeout: "2 hours",
  retry: {
    initialInterval: "10s",
    backoffCoefficient: 2,
    maximumInterval: "2m",
    maximumAttempts: 2,
  },
});

async function aiEvaluationBenchmark(input = {}) {
  return executeAiEvaluationBenchmark({
    runId: input.runId,
  });
}

module.exports = {
  aiEvaluationBenchmark,
};
