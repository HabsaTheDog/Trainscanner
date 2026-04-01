function toSafeArray(value) {
  return Array.isArray(value) ? value : [];
}

function buildCandidateCore(candidate) {
  return {
    global_station_id: candidate.global_station_id || "",
    display_name: candidate.display_name || "",
    candidate_rank: candidate.candidate_rank ?? null,
    country: candidate.metadata?.country || candidate.country || "",
    coord_status: candidate.coord_status || "",
  };
}

function buildPromptContext(clusterDetail, configSnapshot = {}) {
  const sections = Array.isArray(configSnapshot.context_sections)
    ? configSnapshot.context_sections
    : [];
  const sectionSet = new Set(sections);
  const candidates = toSafeArray(clusterDetail?.candidates);
  const context = {};

  if (sectionSet.has("cluster_summary")) {
    context.cluster_summary =
      clusterDetail?.summary && typeof clusterDetail.summary === "object"
        ? clusterDetail.summary
        : {};
  }
  if (sectionSet.has("candidate_core")) {
    context.candidate_core = candidates.map(buildCandidateCore);
  }
  if (sectionSet.has("aliases")) {
    context.aliases = candidates.map((candidate) => ({
      global_station_id: candidate.global_station_id || "",
      aliases: toSafeArray(candidate.aliases),
    }));
  }
  if (sectionSet.has("provenance")) {
    context.provenance = candidates.map((candidate) => ({
      global_station_id: candidate.global_station_id || "",
      provenance:
        candidate.provenance && typeof candidate.provenance === "object"
          ? candidate.provenance
          : {},
    }));
  }
  if (sectionSet.has("network_context")) {
    context.network_context = candidates.map((candidate) => ({
      global_station_id: candidate.global_station_id || "",
      network_context:
        candidate.network_context && typeof candidate.network_context === "object"
          ? candidate.network_context
          : {},
    }));
  }
  if (sectionSet.has("network_summary")) {
    context.network_summary = candidates.map((candidate) => ({
      global_station_id: candidate.global_station_id || "",
      network_summary:
        candidate.network_summary && typeof candidate.network_summary === "object"
          ? candidate.network_summary
          : {},
    }));
  }
  if (sectionSet.has("external_reference_summary")) {
    context.external_reference_summary = candidates.map((candidate) => ({
      global_station_id: candidate.global_station_id || "",
      external_reference_summary:
        candidate.external_reference_summary &&
        typeof candidate.external_reference_summary === "object"
          ? candidate.external_reference_summary
          : {},
    }));
  }
  if (sectionSet.has("external_reference_matches")) {
    context.external_reference_matches = candidates.map((candidate) => ({
      global_station_id: candidate.global_station_id || "",
      external_reference_matches: toSafeArray(
        candidate.external_reference_matches,
      ),
    }));
  }
  if (sectionSet.has("evidence_summary")) {
    context.evidence_summary =
      clusterDetail?.evidence_summary &&
      typeof clusterDetail.evidence_summary === "object"
        ? clusterDetail.evidence_summary
        : {};
  }
  if (sectionSet.has("pair_summaries")) {
    context.pair_summaries = toSafeArray(clusterDetail?.pair_summaries);
  }
  if (sectionSet.has("cluster_metadata")) {
    context.cluster_metadata = {
      cluster_id: clusterDetail?.cluster_id || "",
      display_name: clusterDetail?.display_name || "",
      severity: clusterDetail?.severity || "",
      scope_tag: clusterDetail?.scope_tag || "",
      country_tags: toSafeArray(clusterDetail?.country_tags),
      candidate_count: clusterDetail?.candidate_count ?? candidates.length,
      issue_count: clusterDetail?.issue_count ?? 0,
    };
  }

  return context;
}

function buildPromptSnapshot(configSnapshot, inputContext) {
  return {
    provider: configSnapshot.provider || "litellm",
    model: configSnapshot.model || "",
    model_params:
      configSnapshot.model_params && typeof configSnapshot.model_params === "object"
        ? configSnapshot.model_params
        : {},
    system_prompt: configSnapshot.system_prompt || "",
    context_preamble: configSnapshot.context_preamble || "",
    context_sections: Array.isArray(configSnapshot.context_sections)
      ? configSnapshot.context_sections
      : [],
    input_context: inputContext,
  };
}

module.exports = {
  buildPromptContext,
  buildPromptSnapshot,
};
