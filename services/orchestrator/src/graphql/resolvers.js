const {
  getReviewClusters,
  getReviewClusterDetail,
  postReviewClusterDecision,
} = require("../domains/qa/api");
const {
  getLowConfidenceQueue,
  recordAiMatchDecision,
  setMegaHubWalkTime,
} = require("../domains/qa/ai-queue");
const { createPostgisClient } = require("../data/postgis/client");
const _crypto = require("node:crypto");

// Lazily-initialised singleton DB client shared across resolver calls.
let _dbClient = null;
async function getDbClient() {
  if (!_dbClient) {
    _dbClient = createPostgisClient();
    await _dbClient.ensureReady();
  }
  return _dbClient;
}

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
        confidence_score: 0,
        suggested_action: "error",
        reasoning: "AI Service unreachable",
      };
    }

    return await response.json();
  } catch (e) {
    console.error("AI Bridge Exception:", e);
    return {
      confidence_score: 0,
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
    const result = await getReviewClusters(mockUrl);

    // getReviewClusters now returns { items: rows, ... }
    return (result.items || []).map((c) => ({
      ...c,
      member_nodes: (c.candidates || []).map((cand) => ({
        canonical_station_id: cand.canonical_station_id,
        name: cand.display_name,
        lat: cand.latitude,
        lon: cand.longitude,
      })),
      member_count: c.candidate_count,
      display_name: c.display_name,
      severity: c.severity,
      candidate_count: c.candidate_count,
      issue_count: c.issue_count,
      scope_tag: c.scope_tag,
    }));
  },

  cluster: async ({ id }) => {
    const detail = await getReviewClusterDetail(id);
    if (!detail) return null;

    return {
      ...detail,
      candidates: (detail.candidates || []).map((cand) => ({
        ...cand,
        display_name: cand.display_name,
        candidate_rank: cand.candidate_rank,
        aliases: cand.aliases,
        provider_labels: cand.provider_labels,
        lat: cand.latitude,
        lon: cand.longitude,
        segment_context: cand.segment_context,
      })),
    };
  },

  requestAiScore: async ({ clusterId }) => {
    // 1. Fetch cluster details to get candidates
    const detail = await getReviewClusterDetail(clusterId);
    if (!detail?.candidates) {
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

  // --- Task 5.1: Low-Confidence Queue -----------------------------------------

  lowConfidenceQueue: async ({ limit, offset }) => {
    const client = await getDbClient();
    return getLowConfidenceQueue(client, { limit, offset });
  },

  approveAiMatch: async ({ clusterId, evidenceId }) => {
    const client = await getDbClient();
    const result = await recordAiMatchDecision(client, {
      clusterId,
      evidenceId,
      operation: "approve",
      requestedBy: "operator",
    });
    return {
      ok: true,
      decision_id: result.decisionId,
      cluster_id: result.clusterId,
      operation: result.operation,
    };
  },

  rejectAiMatch: async ({ clusterId, evidenceId }) => {
    const client = await getDbClient();
    const result = await recordAiMatchDecision(client, {
      clusterId,
      evidenceId,
      operation: "reject",
      requestedBy: "operator",
    });
    return {
      ok: true,
      decision_id: result.decisionId,
      cluster_id: result.clusterId,
      operation: result.operation,
    };
  },

  overrideAiMatch: async ({ clusterId, evidenceId, targetClusterId }) => {
    const client = await getDbClient();
    const result = await recordAiMatchDecision(client, {
      clusterId,
      evidenceId,
      operation: "override",
      targetClusterId,
      requestedBy: "operator",
    });
    return {
      ok: true,
      decision_id: result.decisionId,
      cluster_id: result.clusterId,
      operation: result.operation,
    };
  },

  setMegaHubWalkTime: async ({ hubId, walkMinutes }) => {
    const client = await getDbClient();
    const result = await setMegaHubWalkTime(client, {
      hubId,
      walkMinutes: Number(walkMinutes),
      requestedBy: "operator",
    });
    return {
      ok: true,
      rule_id: result.ruleId,
      hub_id: result.hubId,
      walk_minutes: result.walkMinutes,
    };
  },

  submitClusterDecision: async ({ clusterId, input }) => {
    return postReviewClusterDecision(clusterId, input);
  },
};

module.exports = { rootValue };
