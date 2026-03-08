const { AppError } = require("../../core/errors");

function invalid(message) {
  throw new AppError({
    code: "INVALID_REQUEST",
    statusCode: 400,
    message,
  });
}

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
    invalid(`${fieldName} is required`);
  }

  if (!/^[A-Z]{2}$/.test(value)) {
    invalid(`${fieldName} must be an ISO-3166 alpha-2 code`);
  }

  return value;
}

function normalizeStringArray(value) {
  const out = [];
  const seen = new Set();
  for (const item of Array.isArray(value) ? value : []) {
    const clean = String(item || "").trim();
    if (!clean || seen.has(clean)) {
      continue;
    }
    seen.add(clean);
    out.push(clean);
  }
  return out;
}

function normalizeDecisionGroup(raw, index) {
  const row = raw && typeof raw === "object" ? raw : {};
  return {
    groupLabel:
      String(
        row.group_label || row.groupLabel || `group-${index + 1}`,
      ).trim() || `group-${index + 1}`,
    memberGlobalStationIds: normalizeStringArray(
      row.member_global_station_ids || row.memberGlobalStationIds,
    ),
    renameTo: String(row.rename_to || row.renameTo || "").trim(),
  };
}

function normalizeRenameTarget(raw, index) {
  const row = raw && typeof raw === "object" ? raw : {};
  const globalStationId = String(
    row.global_station_id || row.globalStationId || "",
  ).trim();
  const renameTo = String(row.rename_to || row.renameTo || "").trim();

  if (!globalStationId) {
    invalid(`rename_targets[${index}] requires global_station_id`);
  }
  if (!renameTo) {
    invalid(`rename_targets[${index}] requires rename_to`);
  }

  return {
    globalStationId,
    renameTo,
  };
}

function normalizeGlobalMergeDecision(body) {
  const payload = body && typeof body === "object" ? body : {};
  const operation = String(payload.operation || "")
    .trim()
    .toLowerCase();

  if (!["merge", "split", "keep_separate", "rename"].includes(operation)) {
    invalid(
      "operation must be one of 'merge', 'split', 'keep_separate', 'rename'",
    );
  }

  const selectedGlobalStationIds = normalizeStringArray(
    payload.selected_global_station_ids || payload.selectedGlobalStationIds,
  );
  const groups = (Array.isArray(payload.groups) ? payload.groups : []).map(
    (row, index) => normalizeDecisionGroup(row, index),
  );
  const renameTargets = (
    Array.isArray(payload.rename_targets) ? payload.rename_targets : []
  ).map((row, index) => normalizeRenameTarget(row, index));
  const note = String(payload.note || "").trim();
  const requestedBy =
    String(
      payload.requested_by || payload.requestedBy || "qa_operator",
    ).trim() || "qa_operator";

  if (
    operation === "merge" &&
    selectedGlobalStationIds.length < 2 &&
    groups.length === 0
  ) {
    invalid(
      "merge decisions require at least two selected global stations or a non-empty groups list",
    );
  }

  return {
    operation,
    selectedGlobalStationIds,
    groups,
    renameTargets,
    note,
    requestedBy,
    rawPayload: payload,
  };
}

module.exports = {
  normalizeGlobalMergeDecision,
  normalizeIsoCountry,
};
