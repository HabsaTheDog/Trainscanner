const {
  getExternalReferenceViewportPoints,
  getGlobalClusters,
  getGlobalClusterDetail,
  postGlobalClusterDecision,
  reopenGlobalCluster,
  resetGlobalClusterWorkspace,
  resolveGlobalCluster,
  saveGlobalClusterWorkspace,
  undoGlobalClusterWorkspace,
} = require("../domains/qa/api");
const {
  defaultService: aiEvaluationService,
} = require("../domains/ai-evaluation/service");
const { resolveSourceLabels } = require("../domains/source-discovery/catalog");

function resolveAiServiceUrl() {
  if (process.env.AI_SCORING_URL) {
    return process.env.AI_SCORING_URL;
  }
  if (fs.existsSync("/.dockerenv")) {
    return "http://ai-scoring:8000";
  }
  return "http://localhost:8000";
}

async function requestAiScoreBridge(clusterId, candidates) {
  const aiServiceUrl = resolveAiServiceUrl();
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
  const toInt = (value) => Number.parseInt(String(value ?? 0), 10) || 0;
  const providerLabels = Array.isArray(candidate.provider_labels)
    ? candidate.provider_labels
    : [];
  const provenance =
    candidate.provenance &&
    typeof candidate.provenance === "object" &&
    !Array.isArray(candidate.provenance)
      ? candidate.provenance
      : null;
  const activeSourceIds = Array.isArray(provenance?.active_source_ids)
    ? provenance.active_source_ids.map(String).filter(Boolean)
    : [];
  const historicalSourceIds = Array.isArray(provenance?.historical_source_ids)
    ? provenance.historical_source_ids.map(String).filter(Boolean)
    : [];
  const labels = resolveSourceLabels(providerLabels);
  const aliases = Array.isArray(candidate.aliases) ? candidate.aliases : [];
  const networkContext =
    candidate.network_context &&
    typeof candidate.network_context === "object" &&
    !Array.isArray(candidate.network_context)
      ? candidate.network_context
      : {};
  const networkSummary =
    candidate.network_summary &&
    typeof candidate.network_summary === "object" &&
    !Array.isArray(candidate.network_summary)
      ? candidate.network_summary
      : {};
  return {
    ...candidate,
    lat: candidate.latitude,
    lon: candidate.longitude,
    provider_labels: labels.map(String),
    aliases: aliases.map(String),
    coord_status: candidate.coord_status || "missing_coordinates",
    network_context: {
      routes: Array.isArray(networkContext.routes)
        ? networkContext.routes.map((row) => ({
            label: row?.label || "",
            transport_mode: row?.transport_mode || "",
            pattern_hits:
              row?.pattern_hits === null || row?.pattern_hits === undefined
                ? undefined
                : toInt(row?.pattern_hits),
          }))
        : [],
      incoming: Array.isArray(networkContext.incoming)
        ? networkContext.incoming.map((row) => ({
            station_name: row?.station_name || "",
            pattern_hits: toInt(row?.pattern_hits),
          }))
        : [],
      outgoing: Array.isArray(networkContext.outgoing)
        ? networkContext.outgoing.map((row) => ({
            station_name: row?.station_name || "",
            pattern_hits: toInt(row?.pattern_hits),
          }))
        : [],
      stop_points: Array.isArray(networkContext.stop_points)
        ? networkContext.stop_points
        : [],
    },
    network_summary: {
      route_count: toInt(
        networkSummary.route_count ?? networkSummary.route_pattern_count,
      ),
      route_pattern_count: toInt(
        networkSummary.route_pattern_count ?? networkSummary.route_count,
      ),
      incoming_neighbor_count: toInt(networkSummary.incoming_neighbor_count),
      outgoing_neighbor_count: toInt(networkSummary.outgoing_neighbor_count),
      stop_point_count: toInt(networkSummary.stop_point_count),
      provider_source_count: toInt(networkSummary.provider_source_count),
    },
    provenance: {
      has_active_source_mappings:
        provenance?.has_active_source_mappings === true ||
        activeSourceIds.length > 0,
      active_source_ids: activeSourceIds,
      active_source_labels: resolveSourceLabels(activeSourceIds).map(String),
      active_stop_place_refs: Array.isArray(provenance?.active_stop_place_refs)
        ? provenance.active_stop_place_refs.map(String).filter(Boolean)
        : [],
      historical_source_ids: historicalSourceIds,
      historical_source_labels:
        resolveSourceLabels(historicalSourceIds).map(String),
      historical_stop_place_refs: Array.isArray(
        provenance?.historical_stop_place_refs,
      )
        ? provenance.historical_stop_place_refs.map(String).filter(Boolean)
        : [],
      coord_input_stop_place_refs: Array.isArray(
        provenance?.coord_input_stop_place_refs,
      )
        ? provenance.coord_input_stop_place_refs.map(String).filter(Boolean)
        : [],
    },
    external_reference_summary: {
      source_counts:
        candidate.external_reference_summary &&
        typeof candidate.external_reference_summary.source_counts === "object"
          ? candidate.external_reference_summary.source_counts
          : {},
      primary_match_count:
        Number.parseInt(
          String(
            candidate.external_reference_summary?.primary_match_count ?? 0,
          ),
          10,
        ) || 0,
      strong_match_count:
        Number.parseInt(
          String(candidate.external_reference_summary?.strong_match_count ?? 0),
          10,
        ) || 0,
      probable_match_count:
        Number.parseInt(
          String(
            candidate.external_reference_summary?.probable_match_count ?? 0,
          ),
          10,
        ) || 0,
    },
    external_reference_matches: Array.isArray(
      candidate.external_reference_matches,
    )
      ? candidate.external_reference_matches.map((match) => ({
          source_id: match.source_id,
          external_id: match.external_id,
          display_name: match.display_name,
          category: match.category,
          lat: match.lat,
          lon: match.lon,
          distance_meters: match.distance_meters,
          match_status: match.match_status,
          match_confidence: match.match_confidence,
          source_url: match.source_url,
          is_primary: match.is_primary === true,
        }))
      : [],
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
      reference_overlay: Array.isArray(detail.reference_overlay)
        ? detail.reference_overlay.map((row) => ({
            source_id: row.source_id,
            external_id: row.external_id,
            display_name: row.display_name,
            category: row.category,
            lat: row.lat,
            lon: row.lon,
            source_url: row.source_url,
            matched_candidate_ids: Array.isArray(row.matched_candidate_ids)
              ? row.matched_candidate_ids.map(String).filter(Boolean)
              : [],
            match_count: Number.parseInt(String(row.match_count ?? 0), 10) || 0,
          }))
        : [],
      evidence: (detail.evidence || []).map((row) => ({
        ...row,
        evidence_type: row.evidence_type,
        source_global_station_id: row.source_global_station_id,
        target_global_station_id: row.target_global_station_id,
        category: row.category,
        is_seed_rule: row.is_seed_rule,
        seed_reasons: Array.isArray(row.seed_reasons) ? row.seed_reasons : [],
        status: row.status,
        raw_value: row.raw_value,
      })),
      evidence_summary:
        detail.evidence_summary && typeof detail.evidence_summary === "object"
          ? detail.evidence_summary
          : {},
      pair_summaries: Array.isArray(detail.pair_summaries)
        ? detail.pair_summaries.map((row) => ({
            ...row,
            categories: Array.isArray(row.categories) ? row.categories : [],
            seed_reasons: Array.isArray(row.seed_reasons)
              ? row.seed_reasons
              : [],
          }))
        : [],
      decisions: (detail.decisions || []).map((row) => ({
        ...row,
        members: Array.isArray(row.members) ? row.members : [],
      })),
      edit_history: Array.isArray(detail.edit_history)
        ? detail.edit_history
        : [],
    };
  },

  aiEvaluationConfigs: async () => aiEvaluationService.listConfigs(),

  aiEvaluationConfig: async ({ configKey, version }) =>
    aiEvaluationService.getConfig(configKey, version ?? null),

  aiEvaluationRuns: async ({ status, mode, limit }) => {
    const items = await aiEvaluationService.listRuns({
      status: status || "",
      mode: mode || "",
      limit: Number.isFinite(limit) ? limit : 20,
    });
    return {
      items,
      total_count: items.length,
      limit: Number.isFinite(limit) ? limit : 20,
    };
  },

  aiEvaluationRun: async ({ runId }) => aiEvaluationService.getRun(runId),

  aiEvaluationGoldSets: async () => aiEvaluationService.listGoldSets(),

  aiEvaluationGoldSet: async ({ goldSetId }) =>
    aiEvaluationService.getGoldSet(goldSetId),

  globalReferenceViewport: async ({
    minLat,
    minLon,
    maxLat,
    maxLon,
    sourceIds,
    limit,
  }) => {
    const rows = await getExternalReferenceViewportPoints({
      minLat,
      minLon,
      maxLat,
      maxLon,
      sourceIds,
      limit,
    });
    return Array.isArray(rows)
      ? rows.map((row) => ({
          source_id: row.source_id,
          external_id: row.external_id,
          display_name: row.display_name,
          category: row.category,
          lat: row.lat,
          lon: row.lon,
          source_url: row.source_url,
          matched_candidate_ids: Array.isArray(row.matched_candidate_ids)
            ? row.matched_candidate_ids.map(String).filter(Boolean)
            : [],
          match_count: Number.parseInt(String(row.match_count ?? 0), 10) || 0,
        }))
      : [];
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

  createAiEvaluationConfigVersion: async ({ input }) =>
    aiEvaluationService.createConfigVersion(input),

  runAiEvaluationPreview: async ({ clusterId, input }) =>
    aiEvaluationService.runPreview(clusterId, input),

  startAiEvaluationBenchmark: async ({ input }) =>
    aiEvaluationService.startBenchmark(input),

  createAiEvaluationGoldSet: async ({ input }) =>
    aiEvaluationService.createGoldSet(input),

  replaceAiEvaluationGoldSetItems: async ({ goldSetId, clusterIds }) =>
    aiEvaluationService.replaceGoldSetItems(goldSetId, clusterIds),

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

module.exports = {
  rootValue,
  _internal: {
    mapClusterCandidate,
  },
};
const fs = require("node:fs");
