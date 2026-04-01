const {
  createAiEvaluationService,
} = require("../../domains/ai-evaluation/service");

function createAiEvaluationActivities(dbClient, config = {}) {
  const service = createAiEvaluationService({
    dbClient,
    rootDir: config.rootDir || process.cwd(),
    aiServiceUrl: process.env.AI_SCORING_URL,
    temporalAddress: process.env.TEMPORAL_ADDRESS,
  });

  return {
    async executeAiEvaluationBenchmark(input = {}) {
      return service.processBenchmarkRun(input.runId);
    },
  };
}

module.exports = {
  createAiEvaluationActivities,
};
