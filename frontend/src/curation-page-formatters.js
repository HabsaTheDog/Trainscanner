export function formatLabel(value) {
  return String(value || "")
    .replaceAll("_", " ")
    .replaceAll(/\b\w/g, (char) => char.toUpperCase());
}

const EVIDENCE_LABELS = {
  name_exact: "Exact Name",
  name_loose_similarity: "Loose Similarity",
  token_overlap: "Token Overlap",
  geographic_distance: "Distance",
  coordinate_quality: "Coord Quality",
  shared_provider_sources: "Shared Sources",
  shared_route_context: "Route Context",
  shared_adjacent_stations: "Adjacent Stations",
  country_relation: "Country",
  generic_name_penalty: "Generic Penalty",
};

const STATUS_LABELS = {
  supporting: "Supporting",
  warning: "Warning",
  missing: "Missing",
  informational: "Context",
  same_location: "Same Loc",
  nearby: "Nearby",
  far_apart: "Far",
  too_far: "Too Far",
  missing_coordinates: "No Coords",
  coordinates_present: "Coords",
};

const CATEGORY_LABELS = {
  core_match: "Core Match",
  network_context: "Network Context",
  risk_conflict: "Risk / Conflict",
};

const SEED_LABELS = {
  exact_name: "Exact Name",
  loose_name_geo: "Loose Name + Geo",
  loose_name_missing_coords: "Loose Name + Missing Coords",
  shared_route: "Shared Route",
  shared_adjacent: "Shared Adjacent",
};

const PERCENT_EVIDENCE_TYPES = new Set([
  "name_loose_similarity",
  "token_overlap",
]);

const COUNT_EVIDENCE_TYPES = new Set([
  "shared_provider_sources",
  "shared_route_context",
  "shared_adjacent_stations",
  "coordinate_quality",
  "generic_name_penalty",
]);

function toCount(value) {
  return Number.parseInt(String(value ?? 0), 10) || 0;
}

function trimStringArray(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.map((entry) => String(entry || "").trim()).filter(Boolean);
}

export function formatProviderFeedsTooltip(value) {
  const labels = Array.from(new Set(trimStringArray(value)));
  if (labels.length === 0) {
    return "No feeds available";
  }
  return `Feeds used: ${labels.join(", ")}`;
}

export function formatCandidateProvenanceTooltip(value) {
  const provenance =
    value && typeof value === "object" && !Array.isArray(value) ? value : {};
  const activeSources = trimStringArray(
    provenance.active_source_labels || provenance.active_source_ids,
  );
  const historicalSources = trimStringArray(
    provenance.historical_source_labels || provenance.historical_source_ids,
  );
  const activeRefs = trimStringArray(provenance.active_stop_place_refs);
  const historicalRefs = trimStringArray(provenance.historical_stop_place_refs);
  const coordRefs = trimStringArray(provenance.coord_input_stop_place_refs);
  const sections = [
    activeSources.length > 0
      ? `Active sources: ${activeSources.join(", ")}`
      : "No active source mappings",
  ];

  if (historicalSources.length > 0) {
    sections.push(`Historical sources: ${historicalSources.join(", ")}`);
  }
  if (activeRefs.length > 0) {
    sections.push(`Active refs: ${activeRefs.join(", ")}`);
  }
  if (historicalRefs.length > 0) {
    sections.push(`Historical refs: ${historicalRefs.join(", ")}`);
  }
  if (coordRefs.length > 0) {
    sections.push(`Coord refs: ${coordRefs.join(", ")}`);
  }

  return sections.join(" | ");
}

export function formatEvidenceTypeLabel(value) {
  return EVIDENCE_LABELS[value] || formatLabel(value || "unknown");
}

export function formatEvidenceStatusLabel(value) {
  return STATUS_LABELS[value] || formatLabel(value || "unknown");
}

export function formatEvidenceCategoryLabel(value) {
  return CATEGORY_LABELS[value] || formatLabel(value || "unknown");
}

export function formatSeedReasonLabel(value) {
  return SEED_LABELS[value] || formatLabel(value || "seed");
}

export function formatCoordinateStatusLabel(value) {
  return formatEvidenceStatusLabel(value || "missing_coordinates");
}

export function formatEvidenceValue(row) {
  if (!row) {
    return "—";
  }

  if (row.evidence_type === "geographic_distance") {
    const meters = Number(row.raw_value ?? row.details?.distance_meters);
    if (Number.isFinite(meters)) {
      return `${Math.round(meters)}m`;
    }
    return formatEvidenceStatusLabel(row.details?.distance_status);
  }

  if (
    PERCENT_EVIDENCE_TYPES.has(row.evidence_type) &&
    Number.isFinite(Number(row.raw_value))
  ) {
    return `${Math.round(Number(row.raw_value) * 100)}%`;
  }

  if (
    COUNT_EVIDENCE_TYPES.has(row.evidence_type) &&
    Number.isFinite(Number(row.raw_value))
  ) {
    return String(Number(row.raw_value));
  }

  if (row.evidence_type === "country_relation") {
    if (row.details?.same_country === true) {
      return "Same";
    }
    if (row.details?.same_country === false) {
      return "Cross";
    }
    return "?";
  }

  if (Number.isFinite(Number(row.score))) {
    return `${Math.round(Number(row.score) * 100)}%`;
  }

  return "—";
}

export function formatEvidenceDetails(details) {
  if (!details || typeof details !== "object") {
    return "";
  }
  if (details.explanation) {
    return String(details.explanation);
  }
  if (details.distance_status) {
    return formatEvidenceStatusLabel(details.distance_status);
  }
  if (details.reason) {
    return String(details.reason);
  }

  return Object.entries(details)
    .filter(
      ([key, value]) => key !== "seed_reasons" && value != null && value !== "",
    )
    .slice(0, 3)
    .map(([key, value]) => `${formatLabel(key)}: ${value}`)
    .join(" · ");
}

export function getSummaryCount(summary, key) {
  const counts =
    summary && typeof summary === "object"
      ? summary.status_counts || summary
      : {};
  return toCount(counts?.[key]);
}

export function getEvidenceTypeCounts(summary) {
  const counts =
    summary && typeof summary === "object" && summary.type_counts
      ? summary.type_counts
      : {};

  return Object.entries(counts)
    .map(([type, count]) => ({
      type,
      count: toCount(count),
    }))
    .filter((entry) => entry.count > 0)
    .sort((left, right) => {
      return right.count - left.count || left.type.localeCompare(right.type);
    });
}

export function getEvidenceCategoryCounts(summary) {
  const counts =
    summary && typeof summary === "object" && summary.category_counts
      ? summary.category_counts
      : {};

  return ["core_match", "network_context", "risk_conflict"]
    .map((category) => ({
      category,
      count: toCount(counts?.[category]),
    }))
    .filter((entry) => entry.count > 0);
}

export function getSeedRuleCounts(summary) {
  const counts =
    summary && typeof summary === "object" && summary.seed_rule_counts
      ? summary.seed_rule_counts
      : {};

  return Object.entries(counts)
    .map(([reason, count]) => ({
      reason,
      count: toCount(count),
    }))
    .filter((entry) => entry.count > 0)
    .sort((left, right) => {
      return (
        right.count - left.count || left.reason.localeCompare(right.reason)
      );
    });
}

export function getRowSeedReasons(row) {
  const inlineReasons = trimStringArray(row?.seed_reasons);
  if (inlineReasons.length > 0) {
    return inlineReasons;
  }
  return trimStringArray(row?.details?.seed_reasons);
}
