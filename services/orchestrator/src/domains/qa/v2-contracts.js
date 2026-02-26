const { AppError } = require("../../core/errors");

function normalizeIsoCountry(raw, options = {}) {
  const allowEmpty = options.allowEmpty !== false;
  const fieldName = options.fieldName || "country";
  const value = String(raw || "")
    .trim()
    .toUpperCase();

  if (!value) {
    if (allowEmpty) {
      return "";
    }
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: `${fieldName} is required`,
    });
  }

  if (!/^[A-Z]{2}$/.test(value)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: `${fieldName} must be an ISO-3166 alpha-2 code`,
    });
  }

  return value;
}

function normalizeStringArray(raw) {
  const list = Array.isArray(raw) ? raw : [];
  const out = [];
  const seen = new Set();

  for (const item of list) {
    const clean = String(item || "").trim();
    if (!clean || seen.has(clean)) {
      continue;
    }
    seen.add(clean);
    out.push(clean);
  }

  return out;
}

function normalizeNonNegativeInteger(raw, fallback = 0) {
  const parsed = Number.parseInt(String(raw ?? ""), 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return fallback;
  }
  return parsed;
}

function normalizeWalkLink(raw) {
  const input = raw && typeof raw === "object" ? raw : {};
  const fromSegmentId = String(
    input.from_segment_id || input.fromSegmentId || "",
  ).trim();
  const toSegmentId = String(
    input.to_segment_id || input.toSegmentId || "",
  ).trim();
  const bidirectional = Boolean(input.bidirectional);
  const minWalkMinutes = normalizeNonNegativeInteger(
    input.min_walk_minutes ?? input.minWalkMinutes,
    0,
  );
  const metadata =
    input.metadata && typeof input.metadata === "object" ? input.metadata : {};

  if (!fromSegmentId || !toSegmentId || fromSegmentId === toSegmentId) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message:
        "walking links require from_segment_id and to_segment_id with different values",
    });
  }

  return {
    fromSegmentId,
    toSegmentId,
    minWalkMinutes,
    bidirectional,
    metadata,
  };
}

function normalizeSectionType(raw, fallback = "other") {
  const sectionType =
    String(raw || fallback)
      .trim()
      .toLowerCase() || fallback;
  if (
    !["main", "secondary", "subway", "bus", "tram", "other"].includes(
      sectionType,
    )
  ) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: `invalid group section_type '${sectionType}'`,
    });
  }
  return sectionType;
}

function resolveCandidateDisplayName(candidate = {}) {
  const direct = String(candidate.display_name || "").trim();
  if (direct) {
    return direct;
  }

  const canonical = String(candidate.canonical_name || "").trim();
  if (canonical) {
    return canonical;
  }

  return "Unnamed station";
}

function normalizeDecisionGroup(raw, index) {
  const input = raw && typeof raw === "object" ? raw : {};
  const groupLabel =
    String(
      input.group_label || input.groupLabel || `group-${index + 1}`,
    ).trim() || `group-${index + 1}`;
  const targetCanonicalStationId = String(
    input.target_canonical_station_id || input.targetCanonicalStationId || "",
  ).trim();
  const memberStationIds = normalizeStringArray(
    input.member_station_ids || input.memberStationIds,
  );
  const renameTo = String(input.rename_to || input.renameTo || "").trim();
  const segmentAction =
    input.segment_action && typeof input.segment_action === "object"
      ? input.segment_action
      : {};
  const lineAction =
    input.line_action && typeof input.line_action === "object"
      ? input.line_action
      : {};
  const hasSectionType =
    Object.hasOwn(input, "section_type") || Object.hasOwn(input, "sectionType");
  const hasSectionName =
    Object.hasOwn(input, "section_name") || Object.hasOwn(input, "sectionName");
  const sectionType = hasSectionType
    ? normalizeSectionType(input.section_type || input.sectionType || "other")
    : "";
  const sectionName = hasSectionName
    ? String(input.section_name || input.sectionName || "").trim()
    : "";

  return {
    groupLabel,
    targetCanonicalStationId,
    memberStationIds,
    renameTo,
    segmentAction,
    lineAction,
    sectionType,
    sectionName,
    hasSectionMetadata: hasSectionType || hasSectionName,
  };
}

function normalizeRenameTarget(raw, index) {
  const input = raw && typeof raw === "object" ? raw : {};
  const canonicalStationId = String(
    input.canonical_station_id || input.canonicalStationId || "",
  ).trim();
  const renameTo = String(input.rename_to || input.renameTo || "").trim();

  if (!canonicalStationId) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: `rename_targets[${index}] requires canonical_station_id`,
    });
  }

  if (!renameTo) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: `rename_targets[${index}] requires rename_to`,
    });
  }

  return {
    canonicalStationId,
    renameTo,
  };
}

function normalizeClusterDecision(body) {
  const payload = body && typeof body === "object" ? body : {};
  const operation = String(payload.operation || "").trim();

  if (!operation || !["merge", "split"].includes(operation)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "operation must be one of 'merge', 'split'",
    });
  }

  const selectedStationIds = normalizeStringArray(
    payload.selected_station_ids || payload.selectedStationIds,
  );
  const groups = (Array.isArray(payload.groups) ? payload.groups : []).map(
    (group, index) => normalizeDecisionGroup(group, index),
  );
  const note = String(payload.note || "").trim();
  const requestedBy =
    String(
      payload.requested_by || payload.requestedBy || "curation_tool_v2",
    ).trim() || "curation_tool_v2";

  let lineDecisions = {};
  if (payload.line_decisions && typeof payload.line_decisions === "object") {
    lineDecisions = payload.line_decisions;
  } else if (
    payload.lineDecisions &&
    typeof payload.lineDecisions === "object"
  ) {
    lineDecisions = payload.lineDecisions;
  }
  const renameTargets = (
    Array.isArray(payload.rename_targets) ? payload.rename_targets : []
  ).map((row, index) => normalizeRenameTarget(row, index));

  if (
    operation === "merge" &&
    selectedStationIds.length < 2 &&
    groups.length === 0
  ) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message:
        "merge requires at least two selected_station_ids or explicit groups",
    });
  }

  if (operation === "split") {
    const hasValidSplit =
      groups.length >= 2 &&
      groups.every((group) => group.memberStationIds.length > 0);
    if (!hasValidSplit) {
      throw new AppError({
        code: "INVALID_REQUEST",
        statusCode: 400,
        message: "split requires at least two groups with member_station_ids",
      });
    }
  }

  return {
    operation,
    selectedStationIds,
    groups,
    note,
    requestedBy,
    lineDecisions,
    renameTo: String(payload.rename_to || payload.renameTo || "").trim(),
    renameTargets,
  };
}

module.exports = {
  normalizeClusterDecision,
  normalizeIsoCountry,
  normalizeStringArray,
  resolveCandidateDisplayName,
  normalizeWalkLink,
};
