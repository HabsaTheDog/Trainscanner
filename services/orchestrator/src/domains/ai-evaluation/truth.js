const {
  createEmptyWorkspace,
  expandRefMembers,
  normalizeWorkspacePayload,
  parseWorkspaceRef,
} = require("../qa/workspace-contracts");

function uniqueSorted(values) {
  return Array.from(
    new Set(
      (Array.isArray(values) ? values : [])
        .map((value) => String(value || "").trim())
        .filter(Boolean),
    ),
  ).sort((a, b) => a.localeCompare(b));
}

function sortSetRows(rows) {
  return rows
    .map((row) => uniqueSorted(row))
    .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
}

function expandStationIds(refs, workspace) {
  return uniqueSorted(
    (Array.isArray(refs) ? refs : []).flatMap((ref) =>
      expandRefMembers(ref, workspace),
    ),
  );
}

function canonicalizeWorkspace(workspaceInput) {
  const workspace = normalizeWorkspacePayload(workspaceInput || createEmptyWorkspace());
  const merges = sortSetRows(
    (workspace.merges || []).map((merge) =>
      expandStationIds(merge.member_refs || [], workspace),
    ),
  );
  const groups = (workspace.groups || [])
    .map((group) => {
      const nodeLabelMap = new Map();
      const nodes = (group.internal_nodes || [])
        .map((node) => {
          const stationIds = uniqueSorted(
            (Array.isArray(node.member_global_station_ids)
              ? node.member_global_station_ids
              : []
            ).length > 0
              ? node.member_global_station_ids
              : expandRefMembers(node.source_ref, workspace),
          );
          const label = String(node.label || node.node_id || "").trim() || node.node_id;
          nodeLabelMap.set(node.node_id, label);
          return {
            label,
            station_ids: stationIds,
          };
        })
        .sort((a, b) =>
          JSON.stringify([a.label, a.station_ids]).localeCompare(
            JSON.stringify([b.label, b.station_ids]),
          ),
        );
      const transfer_matrix = (group.transfer_matrix || [])
        .map((row) => ({
          from_label: nodeLabelMap.get(row.from_node_id) || row.from_node_id,
          to_label: nodeLabelMap.get(row.to_node_id) || row.to_node_id,
          min_walk_seconds: Number.parseInt(
            String(row.min_walk_seconds ?? 0),
            10,
          ) || 0,
          bidirectional: row.bidirectional !== false,
        }))
        .sort((a, b) =>
          JSON.stringify(a).localeCompare(JSON.stringify(b)),
        );
      return { nodes, transfer_matrix };
    })
    .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
  const keep_separate_sets = sortSetRows(
    (workspace.keep_separate_sets || []).map((row) =>
      expandStationIds(row.refs || [], workspace),
    ),
  );
  const renames = (workspace.renames || [])
    .map((rename) => ({
      target_ref_type: parseWorkspaceRef(rename.ref).type,
      target_station_ids: uniqueSorted(expandRefMembers(rename.ref, workspace)),
      display_name: String(rename.display_name || "").trim(),
    }))
    .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));

  return {
    merges,
    groups,
    keep_separate_sets,
    renames,
  };
}

function resolveVerdict(status, canonicalWorkspace) {
  if (String(status || "").toLowerCase() === "dismissed") {
    return "dismiss";
  }
  const counts = [
    canonicalWorkspace.merges.length > 0,
    canonicalWorkspace.groups.length > 0,
    canonicalWorkspace.keep_separate_sets.length > 0,
    canonicalWorkspace.renames.length > 0,
  ].filter(Boolean).length;
  if (counts === 0) {
    return "needs_review";
  }
  if (
    canonicalWorkspace.merges.length > 0 &&
    canonicalWorkspace.groups.length === 0 &&
    canonicalWorkspace.keep_separate_sets.length === 0 &&
    canonicalWorkspace.renames.length === 0
  ) {
    return "merge_only";
  }
  if (
    canonicalWorkspace.groups.length > 0 &&
    canonicalWorkspace.merges.length === 0 &&
    canonicalWorkspace.keep_separate_sets.length === 0 &&
    canonicalWorkspace.renames.length === 0
  ) {
    return "group_only";
  }
  if (
    canonicalWorkspace.keep_separate_sets.length > 0 &&
    canonicalWorkspace.merges.length === 0 &&
    canonicalWorkspace.groups.length === 0 &&
    canonicalWorkspace.renames.length === 0
  ) {
    return "keep_separate_only";
  }
  if (
    canonicalWorkspace.renames.length > 0 &&
    canonicalWorkspace.merges.length === 0 &&
    canonicalWorkspace.groups.length === 0 &&
    canonicalWorkspace.keep_separate_sets.length === 0
  ) {
    return "rename_only";
  }
  return "mixed_resolution";
}

function buildTruthSnapshot(clusterDetail) {
  const status = String(clusterDetail?.status || "").toLowerCase();
  const canonicalWorkspace =
    status === "dismissed"
      ? canonicalizeWorkspace(createEmptyWorkspace())
      : canonicalizeWorkspace(clusterDetail?.workspace || createEmptyWorkspace());
  return {
    cluster_id: clusterDetail?.cluster_id || "",
    verdict: resolveVerdict(status, canonicalWorkspace),
    ...canonicalWorkspace,
  };
}

module.exports = {
  buildTruthSnapshot,
  canonicalizeWorkspace,
  resolveVerdict,
};
