const {
  getReviewClustersV2,
  getReviewClusterDetailV2,
} = require("../domains/qa/api");
const _crypto = require("node:crypto");

// Normally we would use fetch to hit the python service directly or via Temporal
// We are mimicking the Temporal wrapper directly bridging the python FastAPI
async function requestAiScoreBridge(clusterId, candidates) {
  const aiServiceUrl = process.env.AI_SCORING_URL || "http://localhost:8000";
  try {
    const payload = {
      cluster_id: clusterId,
      candidates: candidates.map((c) => ({
        canonical_station_id: c.canonical_station_id,
        name: c.name || "Unknown",
      })),
    };

    const response = await fetch(`${aiServiceUrl}/score-cluster`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      console.error("AI Service Error:", await response.text());
      return {
        confidence_score: 0.0,
        suggested_action: "error",
        reasoning: "AI Service unreachable",
      };
    }

    return await response.json();
  } catch (e) {
    console.error("AI Bridge Exception:", e);
    return {
      confidence_score: 0.0,
      suggested_action: "error",
      reasoning: e.message,
    };
  }
}

const rootValue = {
  health: () => "GraphQL is running!",

  clusters: async ({ country }) => {
    // We map the REST query over args onto the GraphQL resolver
    const mockUrl = new URL("http://localhost/api");
    if (country) mockUrl.searchParams.set("country", country);

    // Leverage the existing API layer from Phase 1
    const result = await getReviewClustersV2(mockUrl);
    return result.items || [];
  },

  cluster: async ({ id }) => {
    const detail = await getReviewClusterDetailV2(id);
    return detail;
  },

  requestAiScore: async ({ clusterId }) => {
    // 1. Fetch cluster details to get candidates
    const detail = await getReviewClusterDetailV2(clusterId);
    if (!detail || !detail.candidates) {
      throw new Error("Cluster not found to score");
    }

    // 2. Proxy request to Python AI Microservice
    const aiResult = await requestAiScoreBridge(clusterId, detail.candidates);

    // 3. Return shaped response
    return {
      cluster_id: clusterId,
      confidence_score: aiResult.confidence_score,
      suggested_action: aiResult.suggested_action,
      reasoning: aiResult.reasoning,
    };
  },
};

module.exports = { rootValue };
