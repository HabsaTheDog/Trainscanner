import { graphqlQuery } from "./graphql.js";

const CLUSTERS_QUERY = `
  query GetGlobalClusters($country: String, $status: String) {
    globalClusters(country: $country, status: $status) {
      total_count
      limit
      items {
        cluster_id
        country_tags
        status
        effective_status
        has_workspace
        workspace_version
        display_name
        severity
        candidate_count
        issue_count
        scope_tag
      }
    }
  }
`;

const CLUSTER_DETAIL_QUERY = `
  query GetGlobalClusterDetail($id: ID!) {
    globalCluster(id: $id) {
      cluster_id
      country_tags
      status
      effective_status
      has_workspace
      workspace_version
      workspace
      scope_tag
      severity
      display_name
      summary
      candidates {
        global_station_id
        display_name
        candidate_rank
        country
        provider_labels
        aliases
        coord_status
        service_context {
          lines
          incoming
          outgoing
          transport_modes
        }
        context_summary {
          route_count
          incoming_count
          outgoing_count
          stop_point_count
          provider_source_count
        }
        lat
        lon
      }
      evidence {
        evidence_type
        source_global_station_id
        target_global_station_id
        status
        score
        raw_value
        details
      }
      evidence_summary
      pair_summaries {
        source_global_station_id
        target_global_station_id
        supporting_count
        warning_count
        missing_count
        informational_count
        score
        summary
        highlights
      }
      decisions {
        decision_id
        operation
        note
        requested_by
        created_at
        members {
          global_station_id
          action
          group_label
          metadata
        }
      }
      edit_history {
        event_type
        requested_by
        created_at
      }
    }
  }
`;

const SAVE_WORKSPACE_MUTATION = `
  mutation SaveWorkspace($clusterId: ID!, $input: GlobalClusterWorkspaceInput!) {
    saveGlobalClusterWorkspace(clusterId: $clusterId, input: $input) {
      ok
      cluster_id
      workspace_version
      effective_status
      workspace
    }
  }
`;

const UNDO_WORKSPACE_MUTATION = `
  mutation UndoWorkspace($clusterId: ID!, $input: GlobalClusterWorkspaceActorInput) {
    undoGlobalClusterWorkspace(clusterId: $clusterId, input: $input) {
      ok
      cluster_id
      workspace_version
      effective_status
      workspace
    }
  }
`;

const RESET_WORKSPACE_MUTATION = `
  mutation ResetWorkspace($clusterId: ID!, $input: GlobalClusterWorkspaceActorInput) {
    resetGlobalClusterWorkspace(clusterId: $clusterId, input: $input) {
      ok
      cluster_id
      workspace_version
      effective_status
      workspace
    }
  }
`;

const REOPEN_CLUSTER_MUTATION = `
  mutation ReopenCluster($clusterId: ID!, $input: GlobalClusterWorkspaceActorInput) {
    reopenGlobalCluster(clusterId: $clusterId, input: $input) {
      ok
      cluster_id
      workspace_version
      effective_status
      workspace
    }
  }
`;

const RESOLVE_CLUSTER_MUTATION = `
  mutation ResolveCluster($clusterId: ID!, $input: ResolveGlobalClusterInput!) {
    resolveGlobalCluster(clusterId: $clusterId, input: $input) {
      ok
      cluster_id
      decision_id
      status
      next_cluster_id
    }
  }
`;

const AI_SCORE_MUTATION = `
  mutation ScoreCluster($id: ID!) {
    requestAiScore(clusterId: $id) {
      cluster_id
      confidence_score
      suggested_action
      reasoning
    }
  }
`;

export async function fetchClusters(filters = {}) {
  const data = await graphqlQuery(CLUSTERS_QUERY, {
    country: filters.country || null,
    status: filters.status || null,
  });
  const payload = data.globalClusters || {};
  const rows = Array.isArray(payload.items) ? payload.items : [];
  return {
    items: rows.map((row) => ({
      ...row,
      country:
        Array.isArray(row.country_tags) && row.country_tags.length > 0
          ? row.country_tags[0]
          : "EU",
    })),
    totalCount: Number.isFinite(payload.total_count) ? payload.total_count : 0,
    limit: Number.isFinite(payload.limit) ? payload.limit : rows.length,
  };
}

export function formatResultsLabel(totalCount, locale) {
  const safeCount = Number.isFinite(totalCount) ? totalCount : 0;
  return `${safeCount.toLocaleString(locale)} results`;
}

export async function fetchClusterDetail(clusterId) {
  const data = await graphqlQuery(CLUSTER_DETAIL_QUERY, { id: clusterId });
  if (!data.globalCluster) return null;
  const cluster = data.globalCluster;
  return {
    ...cluster,
    country:
      Array.isArray(cluster.country_tags) && cluster.country_tags.length > 0
        ? cluster.country_tags[0]
        : "EU",
    workspace: normalizeWorkspace(cluster.workspace),
    candidates: (cluster.candidates || []).map((candidate) => ({
      global_station_id: candidate.global_station_id,
      display_name: candidate.display_name,
      candidate_rank: candidate.candidate_rank,
      provider_labels: candidate.provider_labels || [],
      aliases: candidate.aliases || [],
      coord_status: candidate.coord_status || "missing_coordinates",
      lat: candidate.lat,
      lon: candidate.lon,
      service_context: {
        lines: candidate.service_context?.lines || [],
        incoming: candidate.service_context?.incoming || [],
        outgoing: candidate.service_context?.outgoing || [],
        transport_modes: candidate.service_context?.transport_modes || [],
      },
      context_summary: {
        route_count: candidate.context_summary?.route_count ?? 0,
        incoming_count: candidate.context_summary?.incoming_count ?? 0,
        outgoing_count: candidate.context_summary?.outgoing_count ?? 0,
        stop_point_count: candidate.context_summary?.stop_point_count ?? 0,
        provider_source_count:
          candidate.context_summary?.provider_source_count ?? 0,
      },
      segment_context: {},
      metadata: { country: candidate.country || "" },
    })),
    evidence: (cluster.evidence || []).map((row) => ({
      evidence_type: row.evidence_type,
      source_global_station_id: row.source_global_station_id,
      target_global_station_id: row.target_global_station_id,
      status: row.status || "informational",
      score: row.score,
      raw_value: row.raw_value ?? null,
      details: row.details || {},
    })),
    evidence_summary:
      cluster.evidence_summary && typeof cluster.evidence_summary === "object"
        ? cluster.evidence_summary
        : {},
    pair_summaries: Array.isArray(cluster.pair_summaries)
      ? cluster.pair_summaries.map((row) => ({
          source_global_station_id: row.source_global_station_id,
          target_global_station_id: row.target_global_station_id,
          supporting_count: row.supporting_count ?? 0,
          warning_count: row.warning_count ?? 0,
          missing_count: row.missing_count ?? 0,
          informational_count: row.informational_count ?? 0,
          score: row.score ?? null,
          summary: row.summary || "",
          highlights: row.highlights || {},
        }))
      : [],
    decisions: (cluster.decisions || []).map((row) => ({
      ...row,
      members: (row.members || []).map((member) => ({
        global_station_id: member.global_station_id,
        action: member.action,
        group_label: member.group_label,
        metadata: member.metadata || {},
      })),
    })),
    edit_history: Array.isArray(cluster.edit_history)
      ? cluster.edit_history
      : [],
  };
}

export async function saveClusterWorkspace(clusterId, workspace) {
  const data = await graphqlQuery(SAVE_WORKSPACE_MUTATION, {
    clusterId,
    input: {
      workspace: normalizeWorkspace(workspace),
      updated_by: "qa_operator",
    },
  });
  const result = data?.saveGlobalClusterWorkspace;
  if (!result?.ok) {
    throw new Error("Workspace save failed.");
  }
  return {
    ...result,
    workspace: normalizeWorkspace(result.workspace),
  };
}

export async function undoClusterWorkspace(clusterId) {
  const data = await graphqlQuery(UNDO_WORKSPACE_MUTATION, {
    clusterId,
    input: { updated_by: "qa_operator" },
  });
  const result = data?.undoGlobalClusterWorkspace;
  if (!result?.ok) {
    throw new Error("Workspace undo failed.");
  }
  return {
    ...result,
    workspace: normalizeWorkspace(result.workspace),
  };
}

export async function resetClusterWorkspace(clusterId) {
  const data = await graphqlQuery(RESET_WORKSPACE_MUTATION, {
    clusterId,
    input: { updated_by: "qa_operator" },
  });
  const result = data?.resetGlobalClusterWorkspace;
  if (!result?.ok) {
    throw new Error("Workspace reset failed.");
  }
  return {
    ...result,
    workspace: normalizeWorkspace(result.workspace),
  };
}

export async function reopenCluster(clusterId) {
  const data = await graphqlQuery(REOPEN_CLUSTER_MUTATION, {
    clusterId,
    input: { updated_by: "qa_operator" },
  });
  const result = data?.reopenGlobalCluster;
  if (!result?.ok) {
    throw new Error("Cluster reopen failed.");
  }
  return {
    ...result,
    workspace: normalizeWorkspace(result.workspace),
  };
}

export async function resolveCluster(clusterId, status, note) {
  const data = await graphqlQuery(RESOLVE_CLUSTER_MUTATION, {
    clusterId,
    input: {
      status,
      note: note || "",
      requested_by: "qa_operator",
    },
  });
  const result = data?.resolveGlobalCluster;
  if (!result?.ok) {
    throw new Error("Cluster resolve failed.");
  }
  return result;
}

export async function requestAiScore(clusterId) {
  const data = await graphqlQuery(AI_SCORE_MUTATION, { id: clusterId });
  if (!data?.requestAiScore) throw new Error("No response from AI.");
  return data.requestAiScore;
}

export function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

export function normalizeText(value) {
  return String(value || "")
    .trim()
    .toLowerCase();
}

export function uniqueStrings(values) {
  const out = [];
  const seen = new Set();
  for (const value of Array.isArray(values) ? values : []) {
    const clean = String(value || "").trim();
    if (!clean || seen.has(clean)) continue;
    seen.add(clean);
    out.push(clean);
  }
  return out;
}

export function createDraftId(prefix) {
  return `${prefix}_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`;
}

export function createEmptyWorkspace() {
  return {
    entities: [],
    merges: [],
    groups: [],
    renames: [],
    keep_separate_sets: [],
    note: "",
  };
}

export function normalizeWorkspace(workspace) {
  const base = workspace && typeof workspace === "object" ? workspace : {};
  return {
    entities: Array.isArray(base.entities) ? base.entities : [],
    merges: (Array.isArray(base.merges) ? base.merges : []).map((merge) => ({
      entity_id: String(merge?.entity_id || merge?.entityId || "").trim(),
      member_refs: uniqueStrings(merge?.member_refs || merge?.memberRefs || []),
      display_name: String(
        merge?.display_name || merge?.displayName || "",
      ).trim(),
    })),
    groups: (Array.isArray(base.groups) ? base.groups : []).map((group) => ({
      entity_id: String(group?.entity_id || group?.entityId || "").trim(),
      member_refs: uniqueStrings(group?.member_refs || group?.memberRefs || []),
      display_name: String(
        group?.display_name || group?.displayName || "",
      ).trim(),
      internal_nodes: (Array.isArray(group?.internal_nodes)
        ? group.internal_nodes
        : []
      ).map((node) => ({
        node_id: String(node?.node_id || node?.nodeId || "").trim(),
        source_ref: String(node?.source_ref || node?.sourceRef || "").trim(),
        member_global_station_ids: uniqueStrings(
          node?.member_global_station_ids || node?.memberGlobalStationIds || [],
        ),
        label: String(node?.label || "").trim(),
        lat:
          node?.lat === null || node?.lat === undefined || node?.lat === ""
            ? null
            : Number(node.lat),
        lon:
          node?.lon === null || node?.lon === undefined || node?.lon === ""
            ? null
            : Number(node.lon),
      })),
      transfer_matrix: (Array.isArray(group?.transfer_matrix)
        ? group.transfer_matrix
        : []
      ).map((row) => ({
        from_node_id: String(row?.from_node_id || row?.fromNodeId || "").trim(),
        to_node_id: String(row?.to_node_id || row?.toNodeId || "").trim(),
        min_walk_seconds: Number(
          row?.min_walk_seconds ?? row?.minWalkSeconds ?? 0,
        ),
        bidirectional: row?.bidirectional !== false,
      })),
    })),
    renames: (Array.isArray(base.renames) ? base.renames : []).map(
      (rename) => ({
        ref: String(rename?.ref || "").trim(),
        display_name: String(
          rename?.display_name || rename?.displayName || "",
        ).trim(),
      }),
    ),
    keep_separate_sets: (Array.isArray(base.keep_separate_sets)
      ? base.keep_separate_sets
      : []
    ).map((row) => ({
      refs: uniqueStrings(row?.refs || row || []),
    })),
    note: String(base.note || "").trim(),
  };
}

export function toRawRef(id) {
  return `raw:${String(id || "").trim()}`;
}

export function toMergeRef(id) {
  return `merge:${String(id || "").trim()}`;
}

export function toGroupRef(id) {
  return `group:${String(id || "").trim()}`;
}

export function parseRef(refKey) {
  const raw = String(refKey || "").trim();
  const idx = raw.indexOf(":");
  if (idx <= 0) return { type: "", id: "" };
  return { type: raw.slice(0, idx), id: raw.slice(idx + 1) };
}

export function compareCandidateRank(a, b) {
  const rankA = Number.parseInt(String(a?.candidate_rank ?? ""), 10);
  const rankB = Number.parseInt(String(b?.candidate_rank ?? ""), 10);
  const safeA =
    Number.isFinite(rankA) && rankA > 0 ? rankA : Number.MAX_SAFE_INTEGER;
  const safeB =
    Number.isFinite(rankB) && rankB > 0 ? rankB : Number.MAX_SAFE_INTEGER;
  if (safeA !== safeB) return safeA - safeB;
  return String(a?.global_station_id || "").localeCompare(
    String(b?.global_station_id || ""),
  );
}

export function buildCandidateMap(candidates = []) {
  return new Map(
    (candidates || []).map((candidate) => [
      candidate.global_station_id,
      candidate,
    ]),
  );
}

export function sortCandidateIds(stationIds, candidateMap) {
  return uniqueStrings(stationIds).sort((left, right) => {
    const candidateLeft = candidateMap.get(left) || {
      global_station_id: left,
      candidate_rank: Number.MAX_SAFE_INTEGER,
    };
    const candidateRight = candidateMap.get(right) || {
      global_station_id: right,
      candidate_rank: Number.MAX_SAFE_INTEGER,
    };
    return compareCandidateRank(candidateLeft, candidateRight);
  });
}

export function getRenameValue(workspace, ref) {
  return (
    (workspace.renames || []).find((row) => row.ref === ref)?.display_name || ""
  );
}

export function setRenameValue(workspace, ref, displayName) {
  const clean = String(displayName || "").trim();
  const next = normalizeWorkspace(workspace);
  next.renames = next.renames.filter((row) => row.ref !== ref);
  if (clean) {
    next.renames.push({ ref, display_name: clean });
  }
  return next;
}

export function resolveRefMemberStationIds(ref, workspace) {
  const parsed = parseRef(ref);
  if (parsed.type === "raw") return [parsed.id];
  if (parsed.type === "merge") {
    const merge = (workspace.merges || []).find(
      (row) => row.entity_id === parsed.id,
    );
    return uniqueStrings(
      (merge?.member_refs || []).map((memberRef) => parseRef(memberRef).id),
    );
  }
  if (parsed.type === "group") {
    const group = (workspace.groups || []).find(
      (row) => row.entity_id === parsed.id,
    );
    return uniqueStrings(
      (group?.internal_nodes || []).flatMap(
        (node) => node.member_global_station_ids || [],
      ),
    );
  }
  return [];
}

export function computeCompositeCoordinates(ref, workspace, candidateMap) {
  const stationIds = resolveRefMemberStationIds(ref, workspace);
  let latSum = 0;
  let lonSum = 0;
  let count = 0;
  for (const stationId of stationIds) {
    const candidate = candidateMap.get(stationId);
    if (Number.isFinite(candidate?.lat) && Number.isFinite(candidate?.lon)) {
      latSum += Number(candidate.lat);
      lonSum += Number(candidate.lon);
      count += 1;
    }
  }
  return {
    lat: count > 0 ? latSum / count : null,
    lon: count > 0 ? lonSum / count : null,
  };
}

export function resolveDisplayNameForRef(ref, workspace, candidateMap) {
  const renamed = getRenameValue(workspace, ref);
  if (renamed) return renamed;

  const parsed = parseRef(ref);
  if (parsed.type === "raw") {
    const candidate = candidateMap.get(parsed.id);
    return candidate?.display_name || parsed.id;
  }
  if (parsed.type === "merge") {
    return (
      (workspace.merges || []).find((row) => row.entity_id === parsed.id)
        ?.display_name || parsed.id
    );
  }
  if (parsed.type === "group") {
    return (
      (workspace.groups || []).find((row) => row.entity_id === parsed.id)
        ?.display_name || parsed.id
    );
  }
  return ref;
}

function computeDistanceMeters(a, b) {
  if (!Number.isFinite(a?.lat) || !Number.isFinite(a?.lon)) return 0;
  if (!Number.isFinite(b?.lat) || !Number.isFinite(b?.lon)) return 0;
  const earthRadius = 6371000;
  const lat1 = (a.lat * Math.PI) / 180;
  const lat2 = (b.lat * Math.PI) / 180;
  const dLat = ((b.lat - a.lat) * Math.PI) / 180;
  const dLon = ((b.lon - a.lon) * Math.PI) / 180;
  const hav =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
  return 2 * earthRadius * Math.atan2(Math.sqrt(hav), Math.sqrt(1 - hav));
}

export function estimateWalkSeconds(nodeA, nodeB) {
  const distance = computeDistanceMeters(nodeA, nodeB);
  if (distance <= 0) return 120;
  return Math.max(30, Math.min(900, Math.round(distance / 1.25)));
}

export function buildDefaultInternalNode(ref, workspace, candidateMap) {
  const parsed = parseRef(ref);
  const memberGlobalStationIds = resolveRefMemberStationIds(ref, workspace);
  const coordinates =
    parsed.type === "raw"
      ? {
          lat: candidateMap.get(parsed.id)?.lat ?? null,
          lon: candidateMap.get(parsed.id)?.lon ?? null,
        }
      : computeCompositeCoordinates(ref, workspace, candidateMap);
  return {
    node_id: createDraftId("node"),
    source_ref: ref,
    member_global_station_ids: memberGlobalStationIds,
    label: resolveDisplayNameForRef(ref, workspace, candidateMap),
    lat: coordinates.lat,
    lon: coordinates.lon,
  };
}

export function rebuildTransferMatrix(nodes = []) {
  const transferMatrix = [];
  for (let i = 0; i < nodes.length; i += 1) {
    for (let j = i + 1; j < nodes.length; j += 1) {
      transferMatrix.push({
        from_node_id: nodes[i].node_id,
        to_node_id: nodes[j].node_id,
        min_walk_seconds: estimateWalkSeconds(nodes[i], nodes[j]),
        bidirectional: true,
      });
    }
  }
  return transferMatrix;
}

export function buildRailItems(clusterDetail, workspace) {
  const candidates = clusterDetail?.candidates || [];
  const candidateMap = buildCandidateMap(candidates);
  const absorbedRefs = new Set();
  const items = [];

  for (const group of workspace.groups || []) {
    for (const ref of group.member_refs || []) absorbedRefs.add(ref);
  }

  for (const merge of workspace.merges || []) {
    for (const ref of merge.member_refs || []) absorbedRefs.add(ref);
    const mergeRef = toMergeRef(merge.entity_id);
    if (absorbedRefs.has(mergeRef)) continue;
    const coords = computeCompositeCoordinates(
      mergeRef,
      workspace,
      candidateMap,
    );
    items.push({
      ref: mergeRef,
      kind: "merge",
      display_name: merge.display_name || merge.entity_id,
      member_refs: merge.member_refs || [],
      station_ids: resolveRefMemberStationIds(mergeRef, workspace),
      lat: coords.lat,
      lon: coords.lon,
    });
  }

  for (const group of workspace.groups || []) {
    const groupRef = toGroupRef(group.entity_id);
    const coords = computeCompositeCoordinates(
      groupRef,
      workspace,
      candidateMap,
    );
    items.push({
      ref: groupRef,
      kind: "group",
      display_name: group.display_name || group.entity_id,
      member_refs: group.member_refs || [],
      station_ids: resolveRefMemberStationIds(groupRef, workspace),
      internal_nodes: group.internal_nodes || [],
      transfer_matrix: group.transfer_matrix || [],
      lat: coords.lat,
      lon: coords.lon,
    });
  }

  for (const candidate of candidates.slice().sort(compareCandidateRank)) {
    const ref = toRawRef(candidate.global_station_id);
    if (absorbedRefs.has(ref)) continue;
    items.push({
      ref,
      kind: "raw",
      display_name: getRenameValue(workspace, ref) || candidate.display_name,
      station_ids: [candidate.global_station_id],
      provider_labels: candidate.provider_labels || [],
      lat: candidate.lat,
      lon: candidate.lon,
      candidate,
    });
  }

  return items;
}

export function createMergeFromSelection(
  workspace,
  selectedRefs,
  candidates = [],
) {
  const next = normalizeWorkspace(workspace);
  const candidateMap = buildCandidateMap(candidates);
  const rawRefs = uniqueStrings(
    Array.from(selectedRefs).filter((ref) => parseRef(ref).type === "raw"),
  );
  if (rawRefs.length < 2) return next;

  const orderedRawRefs = rawRefs.sort((left, right) => {
    const candidateLeft = candidateMap.get(parseRef(left).id) || {};
    const candidateRight = candidateMap.get(parseRef(right).id) || {};
    return compareCandidateRank(candidateLeft, candidateRight);
  });
  const displayName = resolveDisplayNameForRef(
    orderedRawRefs[0],
    next,
    candidateMap,
  );
  next.merges.push({
    entity_id: createDraftId("merge"),
    member_refs: orderedRawRefs,
    display_name: displayName,
  });
  next.entities = [];
  return next;
}

export function createGroupFromSelection(
  workspace,
  selectedRefs,
  candidates = [],
) {
  const next = normalizeWorkspace(workspace);
  const candidateMap = buildCandidateMap(candidates);
  const eligibleRefs = uniqueStrings(
    Array.from(selectedRefs).filter((ref) => {
      const parsed = parseRef(ref);
      return parsed.type === "raw" || parsed.type === "merge";
    }),
  );
  if (eligibleRefs.length < 2) return next;

  const internalNodes = eligibleRefs.map((ref) =>
    buildDefaultInternalNode(ref, next, candidateMap),
  );
  next.groups.push({
    entity_id: createDraftId("group"),
    member_refs: eligibleRefs,
    display_name: resolveDisplayNameForRef(eligibleRefs[0], next, candidateMap),
    internal_nodes: internalNodes,
    transfer_matrix: rebuildTransferMatrix(internalNodes),
  });
  next.entities = [];
  return next;
}

export function splitComposite(workspace, compositeRef) {
  const parsed = parseRef(compositeRef);
  const next = normalizeWorkspace(workspace);
  if (parsed.type === "merge") {
    next.merges = next.merges.filter((row) => row.entity_id !== parsed.id);
  }
  if (parsed.type === "group") {
    next.groups = next.groups.filter((row) => row.entity_id !== parsed.id);
  }
  next.entities = [];
  return next;
}

export function updateCompositeName(workspace, compositeRef, displayName) {
  const parsed = parseRef(compositeRef);
  const next = normalizeWorkspace(workspace);
  if (parsed.type === "merge") {
    next.merges = next.merges.map((merge) =>
      merge.entity_id === parsed.id
        ? { ...merge, display_name: displayName }
        : merge,
    );
  }
  if (parsed.type === "group") {
    next.groups = next.groups.map((group) =>
      group.entity_id === parsed.id
        ? { ...group, display_name: displayName }
        : group,
    );
  }
  return next;
}

export function updateGroupTransferSeconds(
  workspace,
  groupId,
  fromNodeId,
  toNodeId,
  minWalkSeconds,
) {
  const next = normalizeWorkspace(workspace);
  next.groups = next.groups.map((group) => {
    if (group.entity_id !== groupId) return group;
    return {
      ...group,
      transfer_matrix: group.transfer_matrix.map((row) => {
        const matchesForward =
          row.from_node_id === fromNodeId && row.to_node_id === toNodeId;
        const matchesReverse =
          row.from_node_id === toNodeId && row.to_node_id === fromNodeId;
        return matchesForward || matchesReverse
          ? {
              ...row,
              min_walk_seconds: Number(minWalkSeconds) || 0,
            }
          : row;
      }),
    };
  });
  return next;
}

export function addSelectionToGroup(
  workspace,
  groupId,
  selectedRefs,
  candidates = [],
) {
  const next = normalizeWorkspace(workspace);
  const candidateMap = buildCandidateMap(candidates);
  next.groups = next.groups.map((group) => {
    if (group.entity_id !== groupId) return group;
    const newRefs = uniqueStrings([
      ...(group.member_refs || []),
      ...Array.from(selectedRefs).filter((ref) => {
        const parsed = parseRef(ref);
        return parsed.type === "raw" || parsed.type === "merge";
      }),
    ]);
    const internalNodes = [...(group.internal_nodes || [])];
    for (const ref of newRefs) {
      if (internalNodes.some((node) => node.source_ref === ref)) continue;
      internalNodes.push(buildDefaultInternalNode(ref, next, candidateMap));
    }
    return {
      ...group,
      member_refs: newRefs,
      internal_nodes: internalNodes,
      transfer_matrix: rebuildTransferMatrix(internalNodes),
    };
  });
  return next;
}

export function removeMemberFromGroup(workspace, groupId, memberRef) {
  const next = normalizeWorkspace(workspace);
  next.groups = next.groups
    .map((group) => {
      if (group.entity_id !== groupId) return group;
      const memberRefs = (group.member_refs || []).filter(
        (ref) => ref !== memberRef,
      );
      const internalNodes = (group.internal_nodes || []).filter(
        (node) => node.source_ref !== memberRef,
      );
      return {
        ...group,
        member_refs: memberRefs,
        internal_nodes: internalNodes,
        transfer_matrix: rebuildTransferMatrix(internalNodes),
      };
    })
    .filter((group) => group.member_refs.length >= 2);
  return next;
}

export function markKeepSeparate(workspace, selectedRefs) {
  const next = normalizeWorkspace(workspace);
  const refs = uniqueStrings(Array.from(selectedRefs));
  if (refs.length >= 2) {
    next.keep_separate_sets.push({ refs });
  }
  return next;
}

export function updateGroupNodeLabel(workspace, groupId, nodeId, label) {
  const next = normalizeWorkspace(workspace);
  next.groups = next.groups.map((group) =>
    group.entity_id === groupId
      ? {
          ...group,
          internal_nodes: group.internal_nodes.map((node) =>
            node.node_id === nodeId ? { ...node, label } : node,
          ),
        }
      : group,
  );
  return next;
}

export function updateWorkspaceNote(workspace, note) {
  return {
    ...normalizeWorkspace(workspace),
    note,
  };
}

export function serializeWorkspace(workspace) {
  return JSON.stringify(normalizeWorkspace(workspace));
}

export function resolveDefaultMapStyle() {
  if (globalThis.MAP_STYLE_URL) return globalThis.MAP_STYLE_URL;
  if (globalThis.PROTOMAPS_API_KEY) {
    return `https://api.protomaps.com/styles/v2/light.json?key=${globalThis.PROTOMAPS_API_KEY}`;
  }
  return "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json";
}

export function resolveSatelliteMapStyle() {
  if (globalThis.SATELLITE_MAP_STYLE_URL)
    return globalThis.SATELLITE_MAP_STYLE_URL;
  return {
    version: 8,
    sources: {
      satellite: {
        type: "raster",
        tiles: [
          "https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        ],
        tileSize: 256,
        attribution: "Tiles © Esri",
      },
    },
    layers: [{ id: "satellite", type: "raster", source: "satellite" }],
  };
}
