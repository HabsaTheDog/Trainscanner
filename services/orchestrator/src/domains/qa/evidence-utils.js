function normalizeLooseStationName(value) {
  return String(value || "")
    .normalize("NFD")
    .replaceAll(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replaceAll(/\b(abzw|abzw\.)\b/g, " abzweig ")
    .replaceAll(/\b(bhf|bf)\b/g, " bahnhof ")
    .replaceAll(/\b(hbf)\b/g, " hauptbahnhof ")
    .replaceAll(/\b(str|str\.)\b/g, " strasse ")
    .replaceAll(/[^a-z0-9]+/g, " ")
    .replaceAll(/\s+/g, " ")
    .trim();
}

function tokenizeLooseStationName(value) {
  return normalizeLooseStationName(value)
    .split(/\s+/)
    .filter(Boolean)
    .filter(
      (token) =>
        ![
          "bahnhof",
          "hauptbahnhof",
          "station",
          "halt",
          "haltestelle",
          "busbahnhof",
          "bus",
          "tram",
        ].includes(token),
    );
}

function isGenericStationName(value, frequency = 0) {
  const normalized = normalizeLooseStationName(value);
  return (
    /^(steig|gleis|platform|plattform|quai|bussteig|bahnsteig)( [a-z0-9]+)?$/.test(
      normalized,
    ) || Number(frequency || 0) >= 12
  );
}

function classifyDistanceEvidence(distanceMeters) {
  if (
    distanceMeters === null ||
    distanceMeters === undefined ||
    distanceMeters === ""
  ) {
    return {
      status: "missing",
      distance_status: "missing_coordinates",
      score: 0,
    };
  }
  const value = Number(distanceMeters);
  if (!Number.isFinite(value)) {
    return {
      status: "missing",
      distance_status: "missing_coordinates",
      score: 0,
    };
  }
  if (value <= 50) {
    return { status: "supporting", distance_status: "same_location", score: 1 };
  }
  if (value <= 250) {
    return { status: "supporting", distance_status: "nearby", score: 0.85 };
  }
  if (value <= 1000) {
    return {
      status: "informational",
      distance_status: "far_apart",
      score: 0.6,
    };
  }
  if (value <= 5000) {
    return { status: "warning", distance_status: "far_apart", score: 0.25 };
  }
  return { status: "warning", distance_status: "too_far", score: 0.05 };
}

const EVIDENCE_CATEGORY_BY_TYPE = {
  name_exact: "core_match",
  name_loose_similarity: "core_match",
  token_overlap: "core_match",
  geographic_distance: "core_match",
  shared_provider_sources: "network_context",
  shared_route_context: "network_context",
  shared_adjacent_stations: "network_context",
  country_relation: "network_context",
  coordinate_quality: "risk_conflict",
  generic_name_penalty: "risk_conflict",
  external_reference_same_entity: "core_match",
  external_reference_nearby_alignment: "core_match",
  external_reference_coverage: "network_context",
  external_reference_conflict: "risk_conflict",
};

const SEED_EVIDENCE_TYPE_BY_REASON = {
  exact_name: "name_exact",
  loose_name_geo: "name_loose_similarity",
  loose_name_missing_coords: "name_loose_similarity",
  shared_route: "shared_route_context",
  shared_adjacent: "shared_adjacent_stations",
};

function normalizeSeedReasons(value) {
  return Array.from(
    new Set(
      (Array.isArray(value) ? value : [])
        .map((seedReason) => String(seedReason || "").trim())
        .filter(Boolean),
    ),
  );
}

function compareStationIds(left, right) {
  return String(left || "").localeCompare(String(right || ""));
}

function createEmptyPairEntry(sourceId, targetId) {
  const sortedIds = [sourceId, targetId].sort(compareStationIds);
  return {
    source_global_station_id: sortedIds[0] || sourceId,
    target_global_station_id: sortedIds[1] || targetId,
    supporting_count: 0,
    warning_count: 0,
    missing_count: 0,
    informational_count: 0,
    score_total: 0,
    score_count: 0,
    highlights: {
      evidence_types: [],
      distance_status: "",
      shared_signal_count: 0,
      seed_reasons: [],
    },
    categories: [],
    seed_reasons: [],
  };
}

function incrementStatusCount(statusCounts, status) {
  if (status in statusCounts) {
    statusCounts[status] += 1;
    return;
  }
  statusCounts.informational += 1;
}

function incrementCategoryCount(categoryCounts, category) {
  if (category in categoryCounts) {
    categoryCounts[category] += 1;
    return;
  }
  categoryCounts.risk_conflict += 1;
}

function incrementPairStatus(entry, status) {
  if (status === "supporting") {
    entry.supporting_count += 1;
    return;
  }
  if (status === "warning") {
    entry.warning_count += 1;
    return;
  }
  if (status === "missing") {
    entry.missing_count += 1;
    return;
  }
  entry.informational_count += 1;
}

function shouldHighlightEvidenceType(status, entry, evidenceType) {
  return (
    ["warning", "missing", "supporting"].includes(status) &&
    !entry.highlights.evidence_types.includes(evidenceType) &&
    entry.highlights.evidence_types.length < 4
  );
}

function appendUnique(target, value) {
  if (!target.includes(value)) {
    target.push(value);
  }
}

function updatePairHighlights(entry, row, evidenceType, category, seedReasons) {
  if (shouldHighlightEvidenceType(row.status, entry, evidenceType)) {
    entry.highlights.evidence_types.push(evidenceType);
  }
  if (
    row?.evidence_type === "geographic_distance" &&
    row?.details?.distance_status
  ) {
    entry.highlights.distance_status = String(row.details.distance_status);
  }
  if (
    [
      "shared_provider_sources",
      "shared_route_context",
      "shared_adjacent_stations",
    ].includes(evidenceType) &&
    Number.isFinite(Number(row?.raw_value))
  ) {
    entry.highlights.shared_signal_count += Number(row.raw_value);
  }
  appendUnique(entry.categories, category);
  for (const seedReason of seedReasons) {
    appendUnique(entry.highlights.seed_reasons, seedReason);
    appendUnique(entry.seed_reasons, seedReason);
  }
}

function buildPairSummaryMessage(entry) {
  if (entry.warning_count > 0) {
    return "Conflicting evidence needs review";
  }
  if (entry.missing_count > 0) {
    return "Key evidence is missing";
  }
  if (entry.supporting_count > 0) {
    return "Signals are mostly supportive";
  }
  return "Mostly contextual evidence";
}

function classifyEvidenceRow(row = {}) {
  const evidenceType =
    String(row?.evidence_type || "unknown").trim() || "unknown";
  const seedReasons = normalizeSeedReasons(
    row?.seed_reasons || row?.details?.seed_reasons,
  );
  return {
    category: EVIDENCE_CATEGORY_BY_TYPE[evidenceType] || "risk_conflict",
    is_seed_rule: seedReasons.some(
      (seedReason) => SEED_EVIDENCE_TYPE_BY_REASON[seedReason] === evidenceType,
    ),
    seed_reasons: seedReasons,
  };
}

function summarizeEvidenceRows(rows = []) {
  const statusCounts = {
    supporting: 0,
    warning: 0,
    missing: 0,
    informational: 0,
  };
  const typeCounts = {};
  const categoryCounts = {
    core_match: 0,
    network_context: 0,
    risk_conflict: 0,
  };
  const seedRuleCounts = {};
  const pairMap = new Map();

  for (const row of Array.isArray(rows) ? rows : []) {
    const status = String(row?.status || "informational").trim();
    incrementStatusCount(statusCounts, status);
    const evidenceType =
      String(row?.evidence_type || "unknown").trim() || "unknown";
    typeCounts[evidenceType] = (typeCounts[evidenceType] || 0) + 1;
    const classification = classifyEvidenceRow(row);
    const category =
      String(row?.category || classification.category).trim() ||
      "risk_conflict";
    incrementCategoryCount(categoryCounts, category);
    const seedReasons =
      Array.isArray(row?.seed_reasons) && row.seed_reasons.length > 0
        ? normalizeSeedReasons(row.seed_reasons)
        : classification.seed_reasons;
    for (const seedReason of seedReasons) {
      seedRuleCounts[seedReason] = (seedRuleCounts[seedReason] || 0) + 1;
    }

    const sourceId = String(row?.source_global_station_id || "").trim();
    const targetId = String(row?.target_global_station_id || "").trim();
    const pairKey = [sourceId, targetId].sort(compareStationIds).join("|");
    const entry =
      pairMap.get(pairKey) || createEmptyPairEntry(sourceId, targetId);

    incrementPairStatus(entry, status);

    const score = Number(row?.score);
    if (Number.isFinite(score)) {
      entry.score_total += score;
      entry.score_count += 1;
    }

    updatePairHighlights(entry, row, evidenceType, category, seedReasons);

    pairMap.set(pairKey, entry);
  }

  const evidenceSummary = {
    ...statusCounts,
    status_counts: statusCounts,
    type_counts: typeCounts,
    category_counts: categoryCounts,
    seed_rule_counts: seedRuleCounts,
  };

  const pairSummaries = Array.from(pairMap.values())
    .map((entry) => {
      const score =
        entry.score_count > 0 ? entry.score_total / entry.score_count : 0;
      return {
        source_global_station_id: entry.source_global_station_id,
        target_global_station_id: entry.target_global_station_id,
        supporting_count: entry.supporting_count,
        warning_count: entry.warning_count,
        missing_count: entry.missing_count,
        informational_count: entry.informational_count,
        score: Number(score.toFixed(3)),
        summary: buildPairSummaryMessage(entry),
        categories: entry.categories,
        seed_reasons: entry.seed_reasons,
        highlights: entry.highlights,
      };
    })
    .sort((a, b) => {
      if (b.warning_count !== a.warning_count) {
        return b.warning_count - a.warning_count;
      }
      if (a.score !== b.score) {
        return b.score - a.score;
      }
      return `${a.source_global_station_id}|${a.target_global_station_id}`.localeCompare(
        `${b.source_global_station_id}|${b.target_global_station_id}`,
      );
    });

  return { evidenceSummary, pairSummaries };
}

module.exports = {
  classifyDistanceEvidence,
  classifyEvidenceRow,
  isGenericStationName,
  normalizeLooseStationName,
  summarizeEvidenceRows,
  tokenizeLooseStationName,
};
