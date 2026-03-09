const { AppError } = require("../../core/errors");

function invalid(message) {
  throw new AppError({
    code: "INVALID_REQUEST",
    statusCode: 400,
    message,
  });
}

function normalizeString(value) {
  return String(value || "").trim();
}

function normalizeStringArray(values) {
  const out = [];
  const seen = new Set();
  for (const value of Array.isArray(values) ? values : []) {
    const clean = normalizeString(value);
    if (!clean || seen.has(clean)) {
      continue;
    }
    seen.add(clean);
    out.push(clean);
  }
  return out;
}

function parseWorkspaceRef(value, options = {}) {
  const fieldName = options.fieldName || "ref";
  const raw = normalizeString(value);
  const idx = raw.indexOf(":");
  if (idx <= 0 || idx === raw.length - 1) {
    invalid(`${fieldName} must use '<type>:<id>' format`);
  }

  const type = raw.slice(0, idx);
  const id = raw.slice(idx + 1);
  if (!["raw", "merge", "group"].includes(type)) {
    invalid(`${fieldName} type must be one of 'raw', 'merge', 'group'`);
  }

  return { ref: raw, type, id };
}

function createEmptyWorkspace() {
  return {
    entities: [],
    merges: [],
    groups: [],
    renames: [],
    keep_separate_sets: [],
    note: "",
  };
}

function normalizeRename(row, index) {
  const ref = parseWorkspaceRef(row?.ref, {
    fieldName: `renames[${index}].ref`,
  }).ref;
  const displayName = normalizeString(
    row?.display_name || row?.displayName || row?.rename_to || row?.renameTo,
  );

  if (!displayName) {
    invalid(`renames[${index}].display_name is required`);
  }

  return { ref, display_name: displayName };
}

function normalizeInternalNode(row, index, groupIndex) {
  const nodeId =
    normalizeString(row?.node_id || row?.nodeId) ||
    `node-${groupIndex + 1}-${index + 1}`;
  const sourceRef = parseWorkspaceRef(row?.source_ref || row?.sourceRef, {
    fieldName: `groups[${groupIndex}].internal_nodes[${index}].source_ref`,
  }).ref;
  const memberGlobalStationIds = normalizeStringArray(
    row?.member_global_station_ids || row?.memberGlobalStationIds,
  );

  return {
    node_id: nodeId,
    source_ref: sourceRef,
    member_global_station_ids: memberGlobalStationIds,
    label:
      normalizeString(row?.label || row?.display_name || row?.displayName) ||
      nodeId,
    lat:
      row?.lat === null || row?.lat === undefined || row?.lat === ""
        ? null
        : Number(row.lat),
    lon:
      row?.lon === null || row?.lon === undefined || row?.lon === ""
        ? null
        : Number(row.lon),
  };
}

function normalizeTransfer(row, index, groupIndex) {
  const fromNodeId = normalizeString(row?.from_node_id || row?.fromNodeId);
  const toNodeId = normalizeString(row?.to_node_id || row?.toNodeId);
  if (!fromNodeId || !toNodeId) {
    invalid(
      `groups[${groupIndex}].transfer_matrix[${index}] requires from_node_id and to_node_id`,
    );
  }
  if (fromNodeId === toNodeId) {
    invalid(
      `groups[${groupIndex}].transfer_matrix[${index}] must reference two different nodes`,
    );
  }

  const rawSeconds = row?.min_walk_seconds ?? row?.minWalkSeconds;
  const minWalkSeconds = Number.parseInt(String(rawSeconds), 10);
  if (!Number.isFinite(minWalkSeconds) || minWalkSeconds < 0) {
    invalid(
      `groups[${groupIndex}].transfer_matrix[${index}].min_walk_seconds must be a non-negative integer`,
    );
  }

  return {
    from_node_id: fromNodeId,
    to_node_id: toNodeId,
    min_walk_seconds: minWalkSeconds,
    bidirectional: row?.bidirectional !== false,
  };
}

function normalizeMerge(row, index) {
  const entityId =
    normalizeString(row?.entity_id || row?.entityId) || `merge-${index + 1}`;
  const memberRefs = normalizeStringArray(
    row?.member_refs || row?.memberRefs || row?.selected_refs,
  ).map(
    (ref) =>
      parseWorkspaceRef(ref, {
        fieldName: `merges[${index}].member_refs`,
      }).ref,
  );

  if (memberRefs.length < 2) {
    invalid(`merges[${index}] requires at least two member_refs`);
  }
  for (const ref of memberRefs) {
    const parsed = parseWorkspaceRef(ref, {
      fieldName: `merges[${index}].member_refs`,
    });
    if (parsed.type !== "raw") {
      invalid(`merges[${index}] member_refs must only contain raw refs`);
    }
  }

  return {
    entity_id: entityId,
    member_refs: memberRefs,
    display_name:
      normalizeString(
        row?.display_name ||
          row?.displayName ||
          row?.rename_to ||
          row?.renameTo,
      ) || entityId,
  };
}

function normalizeGroup(row, index) {
  const entityId =
    normalizeString(row?.entity_id || row?.entityId) || `group-${index + 1}`;
  const memberRefs = normalizeStringArray(
    row?.member_refs || row?.memberRefs || row?.selected_refs,
  ).map(
    (ref) =>
      parseWorkspaceRef(ref, {
        fieldName: `groups[${index}].member_refs`,
      }).ref,
  );

  if (memberRefs.length < 2) {
    invalid(`groups[${index}] requires at least two member_refs`);
  }

  const internalNodes = (
    Array.isArray(row?.internal_nodes)
      ? row.internal_nodes
      : Array.isArray(row?.internalNodes)
        ? row.internalNodes
        : []
  ).map((item, itemIndex) => normalizeInternalNode(item, itemIndex, index));
  const transferMatrix = (
    Array.isArray(row?.transfer_matrix)
      ? row.transfer_matrix
      : Array.isArray(row?.transferMatrix)
        ? row.transferMatrix
        : []
  ).map((item, itemIndex) => normalizeTransfer(item, itemIndex, index));

  if (internalNodes.length === 0) {
    invalid(`groups[${index}] requires at least one internal node`);
  }

  return {
    entity_id: entityId,
    member_refs: memberRefs,
    display_name:
      normalizeString(
        row?.display_name ||
          row?.displayName ||
          row?.rename_to ||
          row?.renameTo,
      ) || entityId,
    internal_nodes: internalNodes,
    transfer_matrix: transferMatrix,
  };
}

function normalizeWorkspacePayload(input, options = {}) {
  const requireCompleteGroups = options.requireCompleteGroups === true;
  const rawWorkspace =
    input && typeof input === "object" && input.workspace
      ? input.workspace
      : input;
  const payload =
    rawWorkspace && typeof rawWorkspace === "object" ? rawWorkspace : {};
  const normalized = createEmptyWorkspace();

  normalized.merges = (Array.isArray(payload.merges) ? payload.merges : []).map(
    (row, index) => normalizeMerge(row, index),
  );
  normalized.groups = (Array.isArray(payload.groups) ? payload.groups : []).map(
    (row, index) => normalizeGroup(row, index),
  );
  normalized.renames = (
    Array.isArray(payload.renames)
      ? payload.renames
      : Array.isArray(payload.rename_targets)
        ? payload.rename_targets.map((row) => ({
            ref:
              row?.ref ||
              `raw:${normalizeString(row?.global_station_id || row?.globalStationId)}`,
            display_name:
              row?.display_name ||
              row?.displayName ||
              row?.rename_to ||
              row?.renameTo,
          }))
        : []
  ).map((row, index) => normalizeRename(row, index));
  normalized.keep_separate_sets = (
    Array.isArray(payload.keep_separate_sets) ? payload.keep_separate_sets : []
  ).map((row, index) => {
    const refs = normalizeStringArray(
      Array.isArray(row) ? row : row?.refs || row?.member_refs,
    ).map(
      (ref) =>
        parseWorkspaceRef(ref, {
          fieldName: `keep_separate_sets[${index}]`,
        }).ref,
    );
    if (refs.length < 2) {
      invalid(`keep_separate_sets[${index}] requires at least two refs`);
    }
    return { refs };
  });
  normalized.note = normalizeString(payload.note);

  const mergeIds = new Set();
  for (const merge of normalized.merges) {
    if (mergeIds.has(merge.entity_id)) {
      invalid(`Duplicate merge entity_id '${merge.entity_id}'`);
    }
    mergeIds.add(merge.entity_id);
  }

  const groupIds = new Set();
  for (const group of normalized.groups) {
    if (groupIds.has(group.entity_id) || mergeIds.has(group.entity_id)) {
      invalid(`Duplicate composite entity_id '${group.entity_id}'`);
    }
    groupIds.add(group.entity_id);

    const memberRefSet = new Set(group.member_refs);
    for (const memberRef of group.member_refs) {
      const memberParsed = parseWorkspaceRef(memberRef);
      if (memberParsed.type === "group") {
        invalid(`groups '${group.entity_id}' cannot include nested group refs`);
      }
      if (memberParsed.type === "merge" && !mergeIds.has(memberParsed.id)) {
        invalid(
          `groups '${group.entity_id}' references unknown merge entity '${memberParsed.id}'`,
        );
      }
    }

    const nodeIds = new Set();
    const nodeRefs = new Set();
    for (const node of group.internal_nodes) {
      if (nodeIds.has(node.node_id)) {
        invalid(
          `groups '${group.entity_id}' contains duplicate node_id '${node.node_id}'`,
        );
      }
      nodeIds.add(node.node_id);
      if (nodeRefs.has(node.source_ref)) {
        invalid(
          `groups '${group.entity_id}' contains duplicate internal node source_ref '${node.source_ref}'`,
        );
      }
      nodeRefs.add(node.source_ref);
      if (!memberRefSet.has(node.source_ref)) {
        invalid(
          `groups '${group.entity_id}' internal node '${node.node_id}' must reference a member_ref`,
        );
      }
      const nodeSource = parseWorkspaceRef(node.source_ref);
      if (nodeSource.type === "merge" && !mergeIds.has(nodeSource.id)) {
        invalid(
          `groups '${group.entity_id}' internal node '${node.node_id}' references unknown merge entity '${nodeSource.id}'`,
        );
      }
      if (
        node.member_global_station_ids.length === 0 &&
        nodeSource.type === "raw"
      ) {
        node.member_global_station_ids = [nodeSource.id];
      }
    }

    const transferKeys = new Set();
    for (const transfer of group.transfer_matrix) {
      if (
        !nodeIds.has(transfer.from_node_id) ||
        !nodeIds.has(transfer.to_node_id)
      ) {
        invalid(
          `groups '${group.entity_id}' transfer_matrix references unknown node ids`,
        );
      }
      const orderedKey = [transfer.from_node_id, transfer.to_node_id]
        .sort((a, b) => a.localeCompare(b))
        .join("|");
      if (transferKeys.has(orderedKey)) {
        invalid(
          `groups '${group.entity_id}' transfer_matrix contains duplicate pair '${orderedKey}'`,
        );
      }
      transferKeys.add(orderedKey);
    }

    if (requireCompleteGroups && group.internal_nodes.length >= 2) {
      for (let i = 0; i < group.internal_nodes.length; i += 1) {
        for (let j = i + 1; j < group.internal_nodes.length; j += 1) {
          const key = [
            group.internal_nodes[i].node_id,
            group.internal_nodes[j].node_id,
          ]
            .sort((a, b) => a.localeCompare(b))
            .join("|");
          if (!transferKeys.has(key)) {
            invalid(
              `groups '${group.entity_id}' must define a walking time for every internal node pair`,
            );
          }
        }
      }
    }
  }

  const occupiedRawRefs = new Set();
  for (const merge of normalized.merges) {
    for (const ref of merge.member_refs) {
      if (occupiedRawRefs.has(ref)) {
        invalid(`Raw ref '${ref}' is already used by another merge`);
      }
      occupiedRawRefs.add(ref);
    }
  }

  normalized.entities = buildWorkspaceEntities(normalized);
  return normalized;
}

function buildWorkspaceEntities(workspace) {
  const entities = [];
  const compositeMemberRefs = new Set();

  for (const merge of workspace.merges) {
    for (const ref of merge.member_refs) {
      compositeMemberRefs.add(ref);
    }
    entities.push({
      entity_id: merge.entity_id,
      entity_type: "merge",
      member_refs: merge.member_refs,
      display_name: merge.display_name,
    });
  }

  for (const group of workspace.groups) {
    for (const ref of group.member_refs) {
      compositeMemberRefs.add(ref);
    }
    entities.push({
      entity_id: group.entity_id,
      entity_type: "group",
      member_refs: group.member_refs,
      display_name: group.display_name,
      internal_nodes: group.internal_nodes,
      transfer_matrix: group.transfer_matrix,
    });
  }

  const rawRefs = new Set();
  for (const rename of workspace.renames) {
    const parsed = parseWorkspaceRef(rename.ref);
    if (parsed.type === "raw" && !compositeMemberRefs.has(rename.ref)) {
      rawRefs.add(rename.ref);
    }
  }
  for (const setRow of workspace.keep_separate_sets) {
    for (const ref of setRow.refs) {
      const parsed = parseWorkspaceRef(ref);
      if (parsed.type === "raw" && !compositeMemberRefs.has(ref)) {
        rawRefs.add(ref);
      }
    }
  }

  for (const ref of Array.from(rawRefs).sort((a, b) => a.localeCompare(b))) {
    entities.push({
      entity_id: ref,
      entity_type: "raw",
      source_ref: ref,
    });
  }

  return entities;
}

function normalizeWorkspaceMutationInput(input, options = {}) {
  const payload = input && typeof input === "object" ? input : {};
  const workspace = normalizeWorkspacePayload(payload.workspace || payload, {
    requireCompleteGroups: options.requireCompleteGroups === true,
  });
  return {
    workspace,
    updatedBy:
      normalizeString(payload.updated_by || payload.updatedBy) || "qa_operator",
  };
}

function normalizeResolveRequest(input) {
  const payload = input && typeof input === "object" ? input : {};
  const status = normalizeString(payload.status).toLowerCase() || "resolved";
  if (!["resolved", "dismissed"].includes(status)) {
    invalid("resolve status must be either 'resolved' or 'dismissed'");
  }

  return {
    status,
    note: normalizeString(payload.note),
    requestedBy:
      normalizeString(payload.requested_by || payload.requestedBy) ||
      "qa_operator",
    clearWorkspaceOnDismiss: payload.clear_workspace_on_dismiss === true,
  };
}

function expandRefMembers(ref, workspace) {
  const parsed = parseWorkspaceRef(ref);
  if (parsed.type === "raw") {
    return [parsed.id];
  }

  if (parsed.type === "merge") {
    const merge = (workspace?.merges || []).find(
      (row) => row.entity_id === parsed.id,
    );
    if (!merge) {
      invalid(`Unknown merge ref '${ref}'`);
    }
    return merge.member_refs.map(
      (memberRef) => parseWorkspaceRef(memberRef).id,
    );
  }

  const group = (workspace?.groups || []).find(
    (row) => row.entity_id === parsed.id,
  );
  if (!group) {
    invalid(`Unknown group ref '${ref}'`);
  }
  return normalizeStringArray(
    group.internal_nodes.flatMap(
      (node) => node.member_global_station_ids || [],
    ),
  );
}

module.exports = {
  createEmptyWorkspace,
  expandRefMembers,
  normalizeResolveRequest,
  normalizeWorkspaceMutationInput,
  normalizeWorkspacePayload,
  parseWorkspaceRef,
};
