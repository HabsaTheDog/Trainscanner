const {
  getGlobalClusters,
  getGlobalClusterDetail,
  postGlobalClusterDecision,
  reopenGlobalCluster,
  resetGlobalClusterWorkspace,
  resolveGlobalCluster,
  saveGlobalClusterWorkspace,
  undoGlobalClusterWorkspace,
} = require("../domains/qa/api");

async function requestAiScoreBridge(clusterId, candidates) {
  const aiServiceUrl = process.env.AI_SCORING_URL || "http://localhost:8000";
  try {
    const payload = {
      cluster_id: clusterId,
      candidates: (Array.isArray(candidates) ? candidates : []).map((c) => ({
        global_station_id: c.global_station_id,
        name: c.display_name || "Unknown",
      })),
    };

    const response = await fetch(`${aiServiceUrl}/score-cluster`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      return {
        confidence_score: 0,
        suggested_action: "error",
        reasoning: "AI service unavailable",
      };
    }

    return await response.json();
  } catch (err) {
    return {
      confidence_score: 0,
      suggested_action: "error",
      reasoning: err.message,
    };
  }
}

function mapClusterCandidate(candidate) {
  const labels = Array.isArray(candidate.provider_labels)
    ? candidate.provider_labels
    : [];
  return {
    ...candidate,
    lat: candidate.latitude,
    lon: candidate.longitude,
    provider_labels: labels.map((item) => String(item)),
  };
}

const rootValue = {
  health: () => "GraphQL is running!",

  globalClusters: async ({ country, status }) => {
    const mockUrl = new URL("http://localhost/api");
    if (country) {
      mockUrl.searchParams.set("country", country);
    }
    if (status) {
      mockUrl.searchParams.set("status", status);
    }

    const result = await getGlobalClusters(mockUrl);
    return {
      items: (result.items || []).map((cluster) => ({
        ...cluster,
        country_tags: Array.isArray(cluster.country_tags)
          ? cluster.country_tags
          : [],
        candidates: (cluster.candidates || []).map(mapClusterCandidate),
      })),
      total_count: result.total_count || 0,
      limit: result.limit || 0,
    };
  },

  globalCluster: async ({ id }) => {
    const detail = await getGlobalClusterDetail(id);
    if (!detail) {
      return null;
    }
    return {
      ...detail,
      country_tags: Array.isArray(detail.country_tags)
        ? detail.country_tags
        : [],
      candidates: (detail.candidates || []).map(mapClusterCandidate),
      evidence: (detail.evidence || []).map((row) => ({
        ...row,
        evidence_type: row.evidence_type,
        source_global_station_id: row.source_global_station_id,
        target_global_station_id: row.target_global_station_id,
      })),
      decisions: (detail.decisions || []).map((row) => ({
        ...row,
        members: Array.isArray(row.members) ? row.members : [],
      })),
      edit_history: Array.isArray(detail.edit_history)
        ? detail.edit_history
        : [],
    };
  },

  requestAiScore: async ({ clusterId }) => {
    const detail = await getGlobalClusterDetail(clusterId);
    if (!detail) {
      throw new Error("Cluster not found");
    }
    const aiResult = await requestAiScoreBridge(clusterId, detail.candidates);
    return {
      cluster_id: clusterId,
      confidence_score: aiResult.confidence_score,
      suggested_action: aiResult.suggested_action,
      reasoning: aiResult.reasoning,
    };
  },

  submitGlobalMergeDecision: async ({ clusterId, input }) =>
    postGlobalClusterDecision(clusterId, input),

  saveGlobalClusterWorkspace: async ({ clusterId, input }) =>
    saveGlobalClusterWorkspace(clusterId, input),

  undoGlobalClusterWorkspace: async ({ clusterId, input }) =>
    undoGlobalClusterWorkspace(clusterId, input || {}),

  resetGlobalClusterWorkspace: async ({ clusterId, input }) =>
    resetGlobalClusterWorkspace(clusterId, input || {}),

  reopenGlobalCluster: async ({ clusterId, input }) =>
    reopenGlobalCluster(clusterId, input || {}),

  resolveGlobalCluster: async ({ clusterId, input }) =>
    resolveGlobalCluster(clusterId, input),
};

module.exports = { rootValue };
