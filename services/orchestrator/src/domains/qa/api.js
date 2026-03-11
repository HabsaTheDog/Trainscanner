const crypto = require("node:crypto");

const { createPostgisClient } = require("../../data/postgis/client");
const {
  ensureMergeClusterEvidenceColumns,
} = require("../../data/postgis/repositories/merge-evidence-schema");
const {
  createPipelineJobsRepo,
} = require("../../data/postgis/repositories/pipeline-jobs-repo");
const { AppError } = require("../../core/errors");
const {
  normalizeGlobalMergeDecision,
  normalizeIsoCountry,
} = require("./cluster-decision-contracts");
const {
  classifyEvidenceRow,
  summarizeEvidenceRows,
} = require("./evidence-utils");
const {
  createEmptyWorkspace,
  expandRefMembers,
  normalizeResolveRequest,
  normalizeWorkspaceMutationInput,
  normalizeWorkspacePayload,
  parseWorkspaceRef,
} = require("./workspace-contracts");

let dbClient = null;

async function getDbClient() {
  if (!dbClient) {
    dbClient = createPostgisClient();
    await dbClient.ensureReady();
    await ensureMergeClusterEvidenceColumns(dbClient);
  }
  return dbClient;
}

function uniqueStrings(values) {
  const out = [];
  const seen = new Set();
  for (const value of Array.isArray(values) ? values : []) {
    const clean = String(value || "").trim();
    if (!clean || seen.has(clean)) {
      continue;
    }
    seen.add(clean);
    out.push(clean);
  }
  return out;
}

function stableHash(prefix, parts) {
  const hash = crypto
    .createHash("sha256")
    .update((Array.isArray(parts) ? parts : [parts]).join("|"))
    .digest("hex")
    .slice(0, 24);
  return `${prefix}${hash}`;
}

function parseListLimit(raw, fallback = 50, max = 250) {
  const parsed = Number.parseInt(String(raw || ""), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return Math.min(parsed, max);
}

function normalizeClusterStatusFilter(raw) {
  const value = String(raw || "")
    .trim()
    .toLowerCase();
  if (!value) {
    return "";
  }
  if (!["open", "in_review", "resolved", "dismissed"].includes(value)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message:
        "status must be one of 'open', 'in_review', 'resolved', 'dismissed'",
    });
  }
  return value;
}

function normalizeTextArray(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item) => String(item || "").trim())
    .filter((item) => item.length > 0);
}

function normalizeCandidateMetadata(candidate) {
  const metadata =
    candidate && typeof candidate.metadata === "object" && candidate.metadata
      ? candidate.metadata
      : {};
  const serviceContext =
    metadata.service_context &&
    typeof metadata.service_context === "object" &&
    !Array.isArray(metadata.service_context)
      ? metadata.service_context
      : {};
  const contextSummary =
    metadata.context_summary &&
    typeof metadata.context_summary === "object" &&
    !Array.isArray(metadata.context_summary)
      ? metadata.context_summary
      : {};

  return {
    ...candidate,
    aliases: normalizeTextArray(metadata.aliases),
    coord_status: String(
      metadata.coord_status ||
        (candidate.latitude != null && candidate.longitude != null
          ? "coordinates_present"
          : "missing_coordinates"),
    ).trim(),
    service_context: {
      lines: normalizeTextArray(serviceContext.lines),
      incoming: normalizeTextArray(serviceContext.incoming),
      outgoing: normalizeTextArray(serviceContext.outgoing),
      transport_modes: normalizeTextArray(serviceContext.transport_modes),
    },
    context_summary: {
      route_count:
        Number.parseInt(String(contextSummary.route_count ?? 0), 10) || 0,
      incoming_count:
        Number.parseInt(String(contextSummary.incoming_count ?? 0), 10) || 0,
      outgoing_count:
        Number.parseInt(String(contextSummary.outgoing_count ?? 0), 10) || 0,
      stop_point_count:
        Number.parseInt(String(contextSummary.stop_point_count ?? 0), 10) || 0,
      provider_source_count:
        Number.parseInt(
          String(contextSummary.provider_source_count ?? 0),
          10,
        ) || 0,
    },
  };
}

function normalizeEvidenceRow(row) {
  const normalized = {
    ...row,
    status: String(row?.status || "informational").trim() || "informational",
    raw_value:
      row?.raw_value === null || row?.raw_value === undefined
        ? null
        : Number(row.raw_value),
    score:
      row?.score === null || row?.score === undefined
        ? null
        : Number(row.score),
    details:
      row?.details &&
      typeof row.details === "object" &&
      !Array.isArray(row.details)
        ? row.details
        : {},
  };
  return {
    ...normalized,
    ...classifyEvidenceRow(normalized),
  };
}

function resolveUpdatedBy(input, fallback = "qa_operator") {
  return (
    String(input?.updated_by || input?.updatedBy || fallback).trim() || fallback
  );
}

function buildWorkspaceMutationResponse(
  clusterId,
  { workspaceVersion, effectiveStatus, workspace },
) {
  return {
    ok: true,
    cluster_id: clusterId,
    workspace_version: workspaceVersion,
    effective_status: effectiveStatus,
    workspace,
  };
}

async function requireCluster(client, clusterId) {
  const cleanClusterId = String(clusterId || "").trim();
  if (!cleanClusterId) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "cluster_id is required",
    });
  }

  const cluster = await client.queryOne(
    `
    SELECT
      c.merge_cluster_id AS cluster_id,
      c.status,
      c.display_name,
      c.resolved_at,
      c.resolved_by
    FROM qa_merge_clusters c
    WHERE c.merge_cluster_id = :'cluster_id'
    LIMIT 1;
    `,
    { cluster_id: cleanClusterId },
  );

  if (!cluster) {
    throw new AppError({
      code: "NOT_FOUND",
      statusCode: 404,
      message: "Global merge cluster not found",
    });
  }

  return cluster;
}

async function getCurrentWorkspace(client, clusterId) {
  return await client.queryOne(
    `
    SELECT
      merge_cluster_id,
      version,
      workspace_payload,
      updated_at,
      updated_by
    FROM qa_merge_cluster_workspaces
    WHERE merge_cluster_id = :'cluster_id'
    LIMIT 1;
    `,
    { cluster_id: clusterId },
  );
}

async function getLatestWorkspaceVersionNumber(client, clusterId) {
  const row = await client.queryOne(
    `
    SELECT GREATEST(
      COALESCE(
        (
          SELECT MAX(version)
          FROM qa_merge_cluster_workspaces
          WHERE merge_cluster_id = :'cluster_id'
        ),
        0
      ),
      COALESCE(
        (
          SELECT MAX(version)
          FROM qa_merge_cluster_workspace_versions
          WHERE merge_cluster_id = :'cluster_id'
        ),
        0
      )
    )::integer AS latest_version;
    `,
    { cluster_id: clusterId },
  );
  return Number.parseInt(String(row?.latest_version || 0), 10) || 0;
}

async function appendWorkspaceVersion(
  client,
  { clusterId, version, workspace, action, updatedBy },
) {
  await client.runSql(
    `
    INSERT INTO qa_merge_cluster_workspace_versions (
      merge_cluster_id,
      version,
      workspace_payload,
      action,
      updated_by
    )
    VALUES (
      :'cluster_id',
      :'version'::integer,
      COALESCE(NULLIF(:'workspace_payload', '')::jsonb, '{}'::jsonb),
      :'action',
      :'updated_by'
    );
    `,
    {
      cluster_id: clusterId,
      version: version,
      workspace_payload: JSON.stringify(workspace || createEmptyWorkspace()),
      action,
      updated_by: updatedBy,
    },
  );
}

async function persistWorkspaceSnapshot(
  client,
  { clusterId, workspace, updatedBy, action = "save" },
) {
  const nextVersion =
    (await getLatestWorkspaceVersionNumber(client, clusterId)) + 1;
  const payload = workspace || createEmptyWorkspace();

  await client.runSql(
    `
    INSERT INTO qa_merge_cluster_workspaces (
      merge_cluster_id,
      version,
      workspace_payload,
      updated_at,
      updated_by
    )
    VALUES (
      :'cluster_id',
      :'version'::integer,
      COALESCE(NULLIF(:'workspace_payload', '')::jsonb, '{}'::jsonb),
      now(),
      :'updated_by'
    )
    ON CONFLICT (merge_cluster_id)
    DO UPDATE SET
      version = EXCLUDED.version,
      workspace_payload = EXCLUDED.workspace_payload,
      updated_at = EXCLUDED.updated_at,
      updated_by = EXCLUDED.updated_by;
    `,
    {
      cluster_id: clusterId,
      version: nextVersion,
      workspace_payload: JSON.stringify(payload),
      updated_by: updatedBy,
    },
  );

  await appendWorkspaceVersion(client, {
    clusterId,
    version: nextVersion,
    workspace: payload,
    action,
    updatedBy,
  });

  return {
    version: nextVersion,
    workspace: payload,
  };
}

async function clearWorkspaceSnapshot(
  client,
  { clusterId, updatedBy, action },
) {
  const current = await getCurrentWorkspace(client, clusterId);
  if (!current) {
    return { version: 0, workspace: null };
  }

  const nextVersion =
    (await getLatestWorkspaceVersionNumber(client, clusterId)) + 1;
  await appendWorkspaceVersion(client, {
    clusterId,
    version: nextVersion,
    workspace: createEmptyWorkspace(),
    action,
    updatedBy,
  });
  await client.runSql(
    `
    DELETE FROM qa_merge_cluster_workspaces
    WHERE merge_cluster_id = :'cluster_id';
    `,
    { cluster_id: clusterId },
  );
  return { version: 0, workspace: null };
}

async function maybeMoveClusterToInReview(client, clusterId) {
  await client.runSql(
    `
    UPDATE qa_merge_clusters
    SET
      status = CASE WHEN status = 'open' THEN 'in_review' ELSE status END,
      updated_at = now()
    WHERE merge_cluster_id = :'cluster_id';
    `,
    { cluster_id: clusterId },
  );
}

async function setClusterFinalStatus(client, clusterId, status, requestedBy) {
  await client.runSql(
    `
    UPDATE qa_merge_clusters
    SET
      status = :'status',
      resolved_at = CASE
        WHEN :'status' IN ('resolved', 'dismissed') THEN now()
        ELSE NULL
      END,
      resolved_by = CASE
        WHEN :'status' IN ('resolved', 'dismissed') THEN :'resolved_by'
        ELSE NULL
      END,
      updated_at = now()
    WHERE merge_cluster_id = :'cluster_id';
    `,
    {
      cluster_id: clusterId,
      status,
      resolved_by: requestedBy,
    },
  );
}

async function getNextUnresolvedClusterId(client, currentClusterId) {
  const row = await client.queryOne(
    `
    SELECT c.merge_cluster_id AS cluster_id
    FROM qa_merge_clusters c
    WHERE c.merge_cluster_id <> :'cluster_id'
      AND c.status IN ('open', 'in_review')
    ORDER BY
      CASE c.severity WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
      c.updated_at DESC,
      c.merge_cluster_id ASC
    LIMIT 1;
    `,
    { cluster_id: currentClusterId },
  );
  return row?.cluster_id || null;
}

async function insertDecisionMembers(client, decisionId, normalizedDecision) {
  const rows = [];

  for (const stationId of normalizedDecision.selectedGlobalStationIds) {
    rows.push({
      globalStationId: stationId,
      action: "candidate",
      groupLabel: "",
      metadata: {},
    });
  }

  for (const group of normalizedDecision.groups) {
    for (const stationId of group.memberGlobalStationIds) {
      rows.push({
        globalStationId: stationId,
        action: "merge_member",
        groupLabel: group.groupLabel,
        metadata: {
          rename_to: group.renameTo || "",
        },
      });
    }
  }

  for (const target of normalizedDecision.renameTargets) {
    rows.push({
      globalStationId: target.globalStationId,
      action: "rename_target",
      groupLabel: "",
      metadata: {
        rename_to: target.renameTo,
      },
    });
  }

  for (const row of rows) {
    await client.runSql(
      `
      INSERT INTO qa_merge_decision_members (
        decision_id,
        global_station_id,
        action,
        group_label,
        metadata
      )
      VALUES (
        :'decision_id'::bigint,
        :'global_station_id',
        :'action',
        :'group_label',
        COALESCE(NULLIF(:'metadata', '')::jsonb, '{}'::jsonb)
      )
      ON CONFLICT (decision_id, global_station_id, action, group_label)
      DO UPDATE SET metadata = qa_merge_decision_members.metadata || EXCLUDED.metadata;
      `,
      {
        decision_id: String(decisionId),
        global_station_id: row.globalStationId,
        action: row.action,
        group_label: row.groupLabel,
        metadata: JSON.stringify(row.metadata || {}),
      },
    );
  }
}

async function insertWorkspaceDecisionMembers(client, decisionId, workspace) {
  const rows = [];

  for (const merge of workspace.merges || []) {
    for (const ref of merge.member_refs || []) {
      const parsed = parseWorkspaceRef(ref);
      rows.push({
        globalStationId: parsed.id,
        action: "merge_member",
        groupLabel: merge.entity_id,
        metadata: { display_name: merge.display_name || "" },
      });
    }
  }

  for (const group of workspace.groups || []) {
    const stationIds = uniqueStrings(
      (group.internal_nodes || []).flatMap(
        (node) => node.member_global_station_ids || [],
      ),
    );
    for (const stationId of stationIds) {
      rows.push({
        globalStationId: stationId,
        action: "group_member",
        groupLabel: group.entity_id,
        metadata: { display_name: group.display_name || "" },
      });
    }
  }

  for (const rename of workspace.renames || []) {
    const parsed = parseWorkspaceRef(rename.ref);
    if (parsed.type !== "raw") {
      continue;
    }
    rows.push({
      globalStationId: parsed.id,
      action: "rename_target",
      groupLabel: "",
      metadata: { rename_to: rename.display_name },
    });
  }

  for (const row of rows) {
    await client.runSql(
      `
      INSERT INTO qa_merge_decision_members (
        decision_id,
        global_station_id,
        action,
        group_label,
        metadata
      )
      VALUES (
        :'decision_id'::bigint,
        :'global_station_id',
        :'action',
        :'group_label',
        COALESCE(NULLIF(:'metadata', '')::jsonb, '{}'::jsonb)
      )
      ON CONFLICT (decision_id, global_station_id, action, group_label)
      DO UPDATE SET metadata = qa_merge_decision_members.metadata || EXCLUDED.metadata;
      `,
      {
        decision_id: String(decisionId),
        global_station_id: row.globalStationId,
        action: row.action,
        group_label: row.groupLabel,
        metadata: JSON.stringify(row.metadata || {}),
      },
    );
  }
}

async function applyRenameTargets(client, renameTargets = []) {
  for (const target of renameTargets) {
    await client.runSql(
      `
      UPDATE global_stations
      SET
        display_name = :'display_name',
        normalized_name = normalize_station_name(:'display_name'),
        updated_at = now()
      WHERE global_station_id = :'global_station_id';
      `,
      {
        display_name: target.renameTo,
        global_station_id: target.globalStationId,
      },
    );
  }
}

async function applyWorkspaceRenames(client, renames = []) {
  for (const rename of renames) {
    const parsed = parseWorkspaceRef(rename.ref);
    if (parsed.type !== "raw") {
      continue;
    }
    await client.runSql(
      `
      UPDATE global_stations
      SET
        display_name = :'display_name',
        normalized_name = normalize_station_name(:'display_name'),
        updated_at = now()
      WHERE global_station_id = :'global_station_id';
      `,
      {
        display_name: rename.display_name,
        global_station_id: parsed.id,
      },
    );
  }
}

function buildMergeGroups(normalizedDecision) {
  const explicitGroups = Array.isArray(normalizedDecision.groups)
    ? normalizedDecision.groups
    : [];
  const validGroups = explicitGroups
    .map((group, index) => ({
      groupLabel: group.groupLabel || `group-${index + 1}`,
      memberGlobalStationIds: Array.isArray(group.memberGlobalStationIds)
        ? group.memberGlobalStationIds.filter((item) =>
            String(item || "").trim(),
          )
        : [],
      renameTo: String(group.renameTo || "").trim(),
    }))
    .filter((group) => group.memberGlobalStationIds.length >= 2);

  if (validGroups.length > 0) {
    return validGroups;
  }

  const selected = Array.isArray(normalizedDecision.selectedGlobalStationIds)
    ? normalizedDecision.selectedGlobalStationIds.filter((item) =>
        String(item || "").trim(),
      )
    : [];
  if (selected.length < 2) {
    return [];
  }
  return [
    {
      groupLabel: "group-1",
      memberGlobalStationIds: selected,
      renameTo: "",
    },
  ];
}

async function loadStationsByIds(client, stationIds) {
  const ids = uniqueStrings(stationIds);
  if (ids.length === 0) {
    return new Map();
  }

  const rows = await client.queryRows(
    `
    SELECT
      global_station_id,
      display_name,
      country,
      latitude,
      longitude,
      metadata
    FROM global_stations
    WHERE global_station_id = ANY(:'station_ids'::text[]);
    `,
    { station_ids: ids },
  );

  return new Map(rows.map((row) => [row.global_station_id, row]));
}

function resolveStationAggregate(stationRows, displayName, metadata = {}) {
  const rows = Array.isArray(stationRows) ? stationRows : [];
  let latSum = 0;
  let lonSum = 0;
  let coordCount = 0;
  let country = "";

  for (const row of rows) {
    if (!country && row?.country) {
      country = row.country;
    }
    if (Number.isFinite(row?.latitude) && Number.isFinite(row?.longitude)) {
      latSum += Number(row.latitude);
      lonSum += Number(row.longitude);
      coordCount += 1;
    }
  }

  return {
    display_name: String(displayName || rows[0]?.display_name || "").trim(),
    country,
    lat: coordCount > 0 ? latSum / coordCount : null,
    lon: coordCount > 0 ? lonSum / coordCount : null,
    metadata,
  };
}

async function updateTargetStation(client, targetStationId, aggregate) {
  await client.runSql(
    `
    UPDATE global_stations
    SET
      display_name = COALESCE(NULLIF(:'display_name', ''), display_name),
      normalized_name = normalize_station_name(COALESCE(NULLIF(:'display_name', ''), display_name)),
      country = COALESCE(NULLIF(:'country', '')::char(2), country),
      latitude = COALESCE(:'lat'::double precision, latitude),
      longitude = COALESCE(:'lon'::double precision, longitude),
      geom = CASE
        WHEN :'lat'::double precision IS NOT NULL AND :'lon'::double precision IS NOT NULL
          THEN ST_SetSRID(ST_MakePoint(:'lon'::double precision, :'lat'::double precision), 4326)
        ELSE geom
      END,
      metadata = metadata || COALESCE(NULLIF(:'metadata', '')::jsonb, '{}'::jsonb),
      is_active = true,
      updated_at = now()
    WHERE global_station_id = :'global_station_id';
    `,
    {
      global_station_id: targetStationId,
      display_name: aggregate.display_name || "",
      country: aggregate.country || "",
      lat: aggregate.lat,
      lon: aggregate.lon,
      metadata: JSON.stringify(aggregate.metadata || {}),
    },
  );
}

async function moveProviderMappings(client, sourceStationIds, targetStationId) {
  const sourceIds = uniqueStrings(sourceStationIds).filter(
    (item) => item !== targetStationId,
  );
  if (sourceIds.length === 0) {
    return;
  }

  await client.runSql(
    `
    UPDATE provider_global_station_mappings
    SET
      global_station_id = :'target_station_id',
      updated_at = now()
    WHERE global_station_id = ANY(:'source_station_ids'::text[])
      AND is_active = true;
    `,
    {
      target_station_id: targetStationId,
      source_station_ids: sourceIds,
    },
  );
}

async function moveStopPoints(
  client,
  sourceStationIds,
  targetStationId,
  metadataPatch = {},
) {
  const sourceIds = uniqueStrings(sourceStationIds);
  if (sourceIds.length === 0) {
    return;
  }

  await client.runSql(
    `
    UPDATE global_stop_points
    SET
      global_station_id = :'target_station_id',
      metadata = metadata || COALESCE(NULLIF(:'metadata_patch', '')::jsonb, '{}'::jsonb),
      updated_at = now()
    WHERE global_station_id = ANY(:'source_station_ids'::text[])
      AND is_active = true;
    `,
    {
      target_station_id: targetStationId,
      source_station_ids: sourceIds,
      metadata_patch: JSON.stringify(metadataPatch || {}),
    },
  );
}

async function deactivateSourceStations(
  client,
  sourceStationIds,
  targetStationId,
  requestedBy,
  relationField,
) {
  const sourceIds = uniqueStrings(sourceStationIds).filter(
    (item) => item !== targetStationId,
  );
  if (sourceIds.length === 0) {
    return;
  }

  await client.runSql(
    `
    UPDATE global_stations
    SET
      is_active = false,
      metadata = metadata || jsonb_build_object(
        (:'relation_field')::text,
        (:'target_station_id')::text,
        'updated_by_qa',
        (:'requested_by')::text,
        'updated_at_qa',
        now()
      ),
      updated_at = now()
    WHERE global_station_id = ANY(:'source_station_ids'::text[]);
    `,
    {
      relation_field: relationField,
      target_station_id: targetStationId,
      requested_by: requestedBy,
      source_station_ids: sourceIds,
    },
  );
}

async function createOrUpdateSyntheticStopPoint(
  client,
  { stopPointId, targetStationId, displayName, country, lat, lon, metadata },
) {
  await client.runSql(
    `
    INSERT INTO global_stop_points (
      global_stop_point_id,
      global_station_id,
      display_name,
      normalized_name,
      country,
      latitude,
      longitude,
      geom,
      stop_point_kind,
      metadata,
      updated_at
    )
    VALUES (
      :'global_stop_point_id',
      :'global_station_id',
      :'display_name',
      normalize_station_name(:'display_name'),
      NULLIF(:'country', '')::char(2),
      :'lat'::double precision,
      :'lon'::double precision,
      CASE
        WHEN :'lat'::double precision IS NOT NULL AND :'lon'::double precision IS NOT NULL
          THEN ST_SetSRID(ST_MakePoint(:'lon'::double precision, :'lat'::double precision), 4326)
        ELSE NULL
      END,
      'platform',
      COALESCE(NULLIF(:'metadata', '')::jsonb, '{}'::jsonb),
      now()
    )
    ON CONFLICT (global_stop_point_id)
    DO UPDATE SET
      global_station_id = EXCLUDED.global_station_id,
      display_name = EXCLUDED.display_name,
      normalized_name = EXCLUDED.normalized_name,
      country = EXCLUDED.country,
      latitude = EXCLUDED.latitude,
      longitude = EXCLUDED.longitude,
      geom = EXCLUDED.geom,
      metadata = EXCLUDED.metadata,
      is_active = true,
      updated_at = now();
    `,
    {
      global_stop_point_id: stopPointId,
      global_station_id: targetStationId,
      display_name: displayName,
      country: country || "",
      lat,
      lon,
      metadata: JSON.stringify(metadata || {}),
    },
  );
}

async function getNodeStopPointIds(
  client,
  targetStationId,
  groupEntityId,
  nodeId,
) {
  const rows = await client.queryRows(
    `
    SELECT global_stop_point_id
    FROM global_stop_points
    WHERE global_station_id = :'global_station_id'
      AND is_active = true
      AND metadata ->> 'group_entity_id' = :'group_entity_id'
      AND metadata ->> 'internal_node_id' = :'node_id'
    ORDER BY global_stop_point_id;
    `,
    {
      global_station_id: targetStationId,
      group_entity_id: groupEntityId,
      node_id: nodeId,
    },
  );

  return rows.map((row) => row.global_stop_point_id);
}

async function deleteGeneratedTransferEdgesForStation(client, targetStationId) {
  await client.runSql(
    `
    DELETE FROM transfer_edges te
    USING global_stop_points from_sp, global_stop_points to_sp
    WHERE te.from_global_stop_point_id = from_sp.global_stop_point_id
      AND te.to_global_stop_point_id = to_sp.global_stop_point_id
      AND from_sp.global_station_id = :'global_station_id'
      AND to_sp.global_station_id = :'global_station_id'
      AND (
        te.metadata ->> 'generated_by' = 'global_station_build'
        OR te.metadata ->> 'generated_by' = 'qa_workspace_resolve'
      );
    `,
    { global_station_id: targetStationId },
  );
}

async function upsertTransferEdge(
  client,
  { fromStopPointId, toStopPointId, minWalkSeconds, metadata, bidirectional },
) {
  await client.runSql(
    `
    INSERT INTO transfer_edges (
      from_global_stop_point_id,
      to_global_stop_point_id,
      min_transfer_seconds,
      transfer_type,
      is_bidirectional,
      metadata,
      updated_at
    )
    VALUES (
      :'from_stop_point_id',
      :'to_stop_point_id',
      :'min_walk_seconds'::integer,
      2,
      :'is_bidirectional',
      COALESCE(NULLIF(:'metadata', '')::jsonb, '{}'::jsonb),
      now()
    )
    ON CONFLICT (from_global_stop_point_id, to_global_stop_point_id)
    DO UPDATE SET
      min_transfer_seconds = EXCLUDED.min_transfer_seconds,
      transfer_type = EXCLUDED.transfer_type,
      is_bidirectional = EXCLUDED.is_bidirectional,
      metadata = EXCLUDED.metadata,
      updated_at = now();
    `,
    {
      from_stop_point_id: fromStopPointId,
      to_stop_point_id: toStopPointId,
      min_walk_seconds: minWalkSeconds,
      is_bidirectional: bidirectional,
      metadata: JSON.stringify(metadata || {}),
    },
  );
}

async function applyMergeGroup(client, group, requestedBy) {
  const members = group.memberGlobalStationIds;
  const targetStationId = String(members[0] || "").trim();
  if (!targetStationId) {
    return;
  }

  const stationMap = await loadStationsByIds(client, members);
  const aggregate = resolveStationAggregate(
    members.map((id) => stationMap.get(id)).filter(Boolean),
    group.renameTo ||
      stationMap.get(targetStationId)?.display_name ||
      targetStationId,
    {
      qa_operation: "merge",
      merged_member_station_ids: members,
    },
  );

  await moveProviderMappings(client, members, targetStationId);
  await moveStopPoints(client, members, targetStationId, {
    generated_by: "qa_legacy_merge_decision",
  });
  await deactivateSourceStations(
    client,
    members,
    targetStationId,
    requestedBy,
    "merged_into",
  );
  await updateTargetStation(client, targetStationId, aggregate);
}

async function applyDecisionSideEffects(client, normalizedDecision) {
  if (normalizedDecision.operation === "rename") {
    await applyRenameTargets(client, normalizedDecision.renameTargets);
    return;
  }

  if (normalizedDecision.operation !== "merge") {
    return;
  }

  const mergeGroups = buildMergeGroups(normalizedDecision);
  for (const group of mergeGroups) {
    await applyMergeGroup(client, group, normalizedDecision.requestedBy);
  }
}

async function materializeMergeEntity(
  client,
  clusterId,
  merge,
  workspace,
  requestedBy,
) {
  const memberStationIds = uniqueStrings(
    (merge.member_refs || []).flatMap((ref) =>
      expandRefMembers(ref, workspace),
    ),
  );
  const targetStationId = memberStationIds[0];
  if (!targetStationId) {
    return null;
  }

  const stationMap = await loadStationsByIds(client, memberStationIds);
  const aggregate = resolveStationAggregate(
    memberStationIds.map((id) => stationMap.get(id)).filter(Boolean),
    merge.display_name ||
      stationMap.get(targetStationId)?.display_name ||
      targetStationId,
    {
      generated_by: "qa_workspace_resolve",
      cluster_id: clusterId,
      composite_entity_id: merge.entity_id,
      composite_type: "merge",
      member_station_ids: memberStationIds,
    },
  );

  await moveProviderMappings(client, memberStationIds, targetStationId);
  await moveStopPoints(client, memberStationIds, targetStationId, {
    generated_by: "qa_workspace_resolve",
    cluster_id: clusterId,
    composite_entity_id: merge.entity_id,
    composite_type: "merge",
  });
  await deactivateSourceStations(
    client,
    memberStationIds,
    targetStationId,
    requestedBy,
    "merged_into",
  );
  await updateTargetStation(client, targetStationId, aggregate);

  return targetStationId;
}

async function ensureGroupInternalNodeStopPoints(
  client,
  clusterId,
  group,
  workspace,
  targetStationId,
  aggregate,
) {
  for (const node of group.internal_nodes || []) {
    const nodeStationIds = uniqueStrings(
      node.member_global_station_ids?.length > 0
        ? node.member_global_station_ids
        : expandRefMembers(node.source_ref, workspace),
    );
    await moveStopPoints(client, nodeStationIds, targetStationId, {
      generated_by: "qa_workspace_resolve",
      cluster_id: clusterId,
      group_entity_id: group.entity_id,
      internal_node_id: node.node_id,
      internal_node_label: node.label,
    });

    const stopPointIds = await getNodeStopPointIds(
      client,
      targetStationId,
      group.entity_id,
      node.node_id,
    );
    if (stopPointIds.length > 0) {
      continue;
    }

    await createOrUpdateSyntheticStopPoint(client, {
      stopPointId: stableHash("gsp_grp_", [
        clusterId,
        group.entity_id,
        node.node_id,
      ]),
      targetStationId,
      displayName: node.label || group.display_name || targetStationId,
      country: aggregate.country,
      lat: Number.isFinite(node.lat) ? node.lat : aggregate.lat,
      lon: Number.isFinite(node.lon) ? node.lon : aggregate.lon,
      metadata: {
        generated_by: "qa_workspace_resolve",
        cluster_id: clusterId,
        group_entity_id: group.entity_id,
        internal_node_id: node.node_id,
        internal_node_label: node.label,
        synthetic: true,
        member_station_ids: nodeStationIds,
      },
    });
  }
}

async function upsertGroupTransferPair(
  client,
  clusterId,
  groupEntityId,
  transfer,
  fromStopPointId,
  toStopPointId,
) {
  const metadata = {
    generated_by: "qa_workspace_resolve",
    cluster_id: clusterId,
    composite_entity_id: groupEntityId,
    composite_type: "group",
    from_node_id: transfer.from_node_id,
    to_node_id: transfer.to_node_id,
  };
  await upsertTransferEdge(client, {
    fromStopPointId,
    toStopPointId,
    minWalkSeconds: transfer.min_walk_seconds,
    metadata,
    bidirectional: transfer.bidirectional,
  });
  if (!transfer.bidirectional) {
    return;
  }
  await upsertTransferEdge(client, {
    fromStopPointId: toStopPointId,
    toStopPointId: fromStopPointId,
    minWalkSeconds: transfer.min_walk_seconds,
    metadata: {
      ...metadata,
      from_node_id: transfer.to_node_id,
      to_node_id: transfer.from_node_id,
    },
    bidirectional: true,
  });
}

async function applyGroupTransferMatrix(
  client,
  clusterId,
  group,
  targetStationId,
) {
  for (const transfer of group.transfer_matrix || []) {
    const fromStopPointIds = await getNodeStopPointIds(
      client,
      targetStationId,
      group.entity_id,
      transfer.from_node_id,
    );
    const toStopPointIds = await getNodeStopPointIds(
      client,
      targetStationId,
      group.entity_id,
      transfer.to_node_id,
    );

    for (const fromStopPointId of fromStopPointIds) {
      for (const toStopPointId of toStopPointIds) {
        if (fromStopPointId === toStopPointId) {
          continue;
        }
        await upsertGroupTransferPair(
          client,
          clusterId,
          group.entity_id,
          transfer,
          fromStopPointId,
          toStopPointId,
        );
      }
    }
  }
}

async function materializeGroupEntity(
  client,
  clusterId,
  group,
  workspace,
  requestedBy,
) {
  const memberStationIds = uniqueStrings(
    (group.member_refs || []).flatMap((ref) =>
      expandRefMembers(ref, workspace),
    ),
  );
  const targetStationId = memberStationIds[0];
  if (!targetStationId) {
    return null;
  }

  const stationMap = await loadStationsByIds(client, memberStationIds);
  const aggregate = resolveStationAggregate(
    memberStationIds.map((id) => stationMap.get(id)).filter(Boolean),
    group.display_name ||
      stationMap.get(targetStationId)?.display_name ||
      targetStationId,
    {
      generated_by: "qa_workspace_resolve",
      cluster_id: clusterId,
      composite_entity_id: group.entity_id,
      composite_type: "group",
      member_station_ids: memberStationIds,
      internal_nodes: (group.internal_nodes || []).map((node) => ({
        node_id: node.node_id,
        label: node.label,
        source_ref: node.source_ref,
      })),
    },
  );

  await moveProviderMappings(client, memberStationIds, targetStationId);
  await ensureGroupInternalNodeStopPoints(
    client,
    clusterId,
    group,
    workspace,
    targetStationId,
    aggregate,
  );

  await deactivateSourceStations(
    client,
    memberStationIds,
    targetStationId,
    requestedBy,
    "grouped_into",
  );
  await updateTargetStation(client, targetStationId, aggregate);
  await deleteGeneratedTransferEdgesForStation(client, targetStationId);
  await applyGroupTransferMatrix(client, clusterId, group, targetStationId);

  return targetStationId;
}

async function materializeWorkspace(client, clusterId, workspace, requestedBy) {
  const groupedMergeIds = new Set();
  for (const group of workspace.groups || []) {
    for (const ref of group.member_refs || []) {
      const parsed = parseWorkspaceRef(ref);
      if (parsed.type === "merge") {
        groupedMergeIds.add(parsed.id);
      }
    }
  }

  for (const merge of workspace.merges || []) {
    if (groupedMergeIds.has(merge.entity_id)) {
      continue;
    }
    await materializeMergeEntity(
      client,
      clusterId,
      merge,
      workspace,
      requestedBy,
    );
  }

  for (const group of workspace.groups || []) {
    await materializeGroupEntity(
      client,
      clusterId,
      group,
      workspace,
      requestedBy,
    );
  }

  await applyWorkspaceRenames(client, workspace.renames || []);
}

async function createWorkspaceDecision(
  client,
  { clusterId, operation, workspace, note, requestedBy },
) {
  const decision = await client.queryOne(
    `
    INSERT INTO qa_merge_decisions (
      merge_cluster_id,
      operation,
      decision_payload,
      note,
      requested_by
    )
    VALUES (
      :'cluster_id',
      :'operation',
      COALESCE(NULLIF(:'decision_payload', '')::jsonb, '{}'::jsonb),
      NULLIF(:'note', ''),
      :'requested_by'
    )
    RETURNING decision_id;
    `,
    {
      cluster_id: clusterId,
      operation,
      decision_payload: JSON.stringify(workspace || createEmptyWorkspace()),
      note: note || "",
      requested_by: requestedBy,
    },
  );

  const decisionId = Number.parseInt(String(decision?.decision_id || 0), 10);
  if (!Number.isFinite(decisionId) || decisionId <= 0) {
    throw new AppError({
      code: "INTERNAL_ERROR",
      statusCode: 500,
      message: "Failed to persist workspace decision",
    });
  }

  await insertWorkspaceDecisionMembers(client, decisionId, workspace || {});
  return decisionId;
}

async function getGlobalClusters(url) {
  const client = await getDbClient();
  const country = normalizeIsoCountry(url.searchParams.get("country"), {
    allowEmpty: true,
  });
  const status = normalizeClusterStatusFilter(url.searchParams.get("status"));
  const scopeTag = String(url.searchParams.get("scope_tag") || "").trim();
  const limit = parseListLimit(url.searchParams.get("limit"), 50, 300);
  const filterParams = {
    country,
    status,
    scope_tag: scopeTag,
  };

  const totalRow = await client.queryOne(
    `
    SELECT COUNT(*)::integer AS total_count
    FROM qa_merge_clusters c
    WHERE (NULLIF(:'status', '') IS NULL OR c.status = NULLIF(:'status', ''))
      AND (NULLIF(:'scope_tag', '') IS NULL OR c.scope_tag = NULLIF(:'scope_tag', ''))
      AND (
        NULLIF(:'country', '') IS NULL
        OR NULLIF(:'country', '') = ANY (COALESCE(c.country_tags, ARRAY[]::text[]))
      )
    `,
    filterParams,
  );

  const rows = await client.queryRows(
    `
    SELECT
      c.merge_cluster_id AS cluster_id,
      c.status,
      CASE
        WHEN w.merge_cluster_id IS NOT NULL AND c.status = 'open' THEN 'in_review'
        ELSE c.status
      END AS effective_status,
      (w.merge_cluster_id IS NOT NULL) AS has_workspace,
      w.version AS workspace_version,
      c.severity,
      c.scope_tag,
      c.scope_as_of,
      c.display_name,
      c.summary,
      c.candidate_count,
      c.issue_count,
      c.country_tags,
      c.updated_at,
      (
        SELECT COALESCE(json_agg(json_build_object(
          'global_station_id', cc.global_station_id,
          'display_name', cc.display_name,
          'candidate_rank', cc.candidate_rank,
          'latitude', cc.latitude,
          'longitude', cc.longitude,
          'country', cc.country
        ) ORDER BY cc.candidate_rank, cc.global_station_id), '[]'::json)
        FROM qa_merge_cluster_candidates cc
        WHERE cc.merge_cluster_id = c.merge_cluster_id
      ) AS candidates
    FROM qa_merge_clusters c
    LEFT JOIN qa_merge_cluster_workspaces w
      ON w.merge_cluster_id = c.merge_cluster_id
    WHERE (NULLIF(:'status', '') IS NULL OR c.status = NULLIF(:'status', ''))
      AND (NULLIF(:'scope_tag', '') IS NULL OR c.scope_tag = NULLIF(:'scope_tag', ''))
      AND (
        NULLIF(:'country', '') IS NULL
        OR NULLIF(:'country', '') = ANY (COALESCE(c.country_tags, ARRAY[]::text[]))
      )
    ORDER BY
      CASE c.severity WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
      c.updated_at DESC,
      c.merge_cluster_id ASC
    LIMIT :'limit'::integer
    `,
    {
      ...filterParams,
      limit,
    },
  );

  return {
    items: rows,
    count: rows.length,
    total_count: totalRow?.total_count || 0,
    limit,
  };
}

async function getGlobalClusterDetail(clusterId) {
  const client = await getDbClient();
  const cleanClusterId = String(clusterId || "").trim();
  const cluster = await client.queryOne(
    `
    SELECT
      c.merge_cluster_id AS cluster_id,
      c.cluster_key,
      c.status,
      CASE
        WHEN w.merge_cluster_id IS NOT NULL AND c.status = 'open' THEN 'in_review'
        ELSE c.status
      END AS effective_status,
      (w.merge_cluster_id IS NOT NULL) AS has_workspace,
      w.version AS workspace_version,
      w.workspace_payload AS workspace,
      c.severity,
      c.scope_tag,
      c.scope_as_of,
      c.display_name,
      c.summary,
      c.candidate_count,
      c.issue_count,
      c.country_tags,
      c.resolved_at,
      c.resolved_by,
      c.created_at,
      c.updated_at
    FROM qa_merge_clusters c
    LEFT JOIN qa_merge_cluster_workspaces w
      ON w.merge_cluster_id = c.merge_cluster_id
    WHERE c.merge_cluster_id = :'cluster_id'
    `,
    { cluster_id: cleanClusterId },
  );

  if (!cluster) {
    throw new AppError({
      code: "NOT_FOUND",
      statusCode: 404,
      message: "Global merge cluster not found",
    });
  }

  const [candidates, evidence, decisions, editHistory] = await Promise.all([
    client.queryRows(
      `
      SELECT
        cc.global_station_id,
        cc.candidate_rank,
        cc.display_name,
        cc.latitude,
        cc.longitude,
        cc.country,
        cc.provider_labels,
        cc.metadata
      FROM qa_merge_cluster_candidates cc
      WHERE cc.merge_cluster_id = :'cluster_id'
      ORDER BY cc.candidate_rank, cc.global_station_id
      `,
      { cluster_id: cleanClusterId },
    ),
    client.queryRows(
      `
      SELECT
        e.evidence_id,
        e.source_global_station_id,
        e.target_global_station_id,
        e.evidence_type,
        e.status,
        e.score,
        e.raw_value,
        e.details,
        e.created_at
      FROM qa_merge_cluster_evidence e
      WHERE e.merge_cluster_id = :'cluster_id'
      ORDER BY e.evidence_type, e.source_global_station_id, e.target_global_station_id, e.evidence_id
      `,
      { cluster_id: cleanClusterId },
    ),
    client.queryRows(
      `
      SELECT
        d.decision_id,
        d.operation,
        d.decision_payload,
        d.note,
        d.requested_by,
        d.created_at,
        (
          SELECT COALESCE(json_agg(json_build_object(
            'global_station_id', m.global_station_id,
            'action', m.action,
            'group_label', m.group_label,
            'metadata', m.metadata
          ) ORDER BY m.global_station_id, m.action, m.group_label), '[]'::json)
          FROM qa_merge_decision_members m
          WHERE m.decision_id = d.decision_id
        ) AS members
      FROM qa_merge_decisions d
      WHERE d.merge_cluster_id = :'cluster_id'
      ORDER BY d.created_at DESC, d.decision_id DESC
      `,
      { cluster_id: cleanClusterId },
    ),
    client.queryRows(
      `
      SELECT
        event_type,
        requested_by,
        created_at
      FROM (
        SELECT
          v.action AS event_type,
          v.updated_by AS requested_by,
          v.updated_at AS created_at
        FROM qa_merge_cluster_workspace_versions v
        WHERE v.merge_cluster_id = :'cluster_id'
        UNION ALL
        SELECT
          d.operation AS event_type,
          d.requested_by,
          d.created_at
        FROM qa_merge_decisions d
        WHERE d.merge_cluster_id = :'cluster_id'
      ) events
      ORDER BY created_at DESC
      `,
      { cluster_id: cleanClusterId },
    ),
  ]);

  const normalizedCandidates = candidates.map(normalizeCandidateMetadata);
  const normalizedEvidence = evidence.map(normalizeEvidenceRow);
  const { evidenceSummary, pairSummaries } =
    summarizeEvidenceRows(normalizedEvidence);

  return {
    ...cluster,
    workspace: cluster.workspace || null,
    candidates: normalizedCandidates,
    evidence: normalizedEvidence,
    evidence_summary: evidenceSummary,
    pair_summaries: pairSummaries,
    decisions,
    edit_history: editHistory,
  };
}

async function saveGlobalClusterWorkspace(clusterId, input) {
  const client = await getDbClient();
  const cluster = await requireCluster(client, clusterId);
  const normalized = normalizeWorkspaceMutationInput(input);
  const saved = await persistWorkspaceSnapshot(client, {
    clusterId: cluster.cluster_id,
    workspace: normalized.workspace,
    updatedBy: normalized.updatedBy,
    action: "save",
  });

  if (cluster.status === "open") {
    await maybeMoveClusterToInReview(client, cluster.cluster_id);
  }

  return buildWorkspaceMutationResponse(cluster.cluster_id, {
    workspaceVersion: saved.version,
    effectiveStatus: cluster.status === "open" ? "in_review" : cluster.status,
    workspace: saved.workspace,
  });
}

async function undoGlobalClusterWorkspace(clusterId, input) {
  const client = await getDbClient();
  const cluster = await requireCluster(client, clusterId);
  const updatedBy = resolveUpdatedBy(input);
  const versions = await client.queryRows(
    `
    SELECT
      version,
      workspace_payload
    FROM qa_merge_cluster_workspace_versions
    WHERE merge_cluster_id = :'cluster_id'
    ORDER BY version DESC
    LIMIT 2;
    `,
    { cluster_id: cluster.cluster_id },
  );

  if (versions.length === 0) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "No workspace history exists for this cluster",
    });
  }

  if (versions.length === 1) {
    await clearWorkspaceSnapshot(client, {
      clusterId: cluster.cluster_id,
      updatedBy,
      action: "undo",
    });
    await setClusterFinalStatus(client, cluster.cluster_id, "open", updatedBy);
    return buildWorkspaceMutationResponse(cluster.cluster_id, {
      workspaceVersion: 0,
      effectiveStatus: "open",
      workspace: null,
    });
  }

  const previousPayload = normalizeWorkspacePayload(
    versions[1].workspace_payload,
  );
  const saved = await persistWorkspaceSnapshot(client, {
    clusterId: cluster.cluster_id,
    workspace: previousPayload,
    updatedBy,
    action: "undo",
  });
  await maybeMoveClusterToInReview(client, cluster.cluster_id);

  return buildWorkspaceMutationResponse(cluster.cluster_id, {
    workspaceVersion: saved.version,
    effectiveStatus: "in_review",
    workspace: saved.workspace,
  });
}

async function resetGlobalClusterWorkspace(clusterId, input) {
  const client = await getDbClient();
  const cluster = await requireCluster(client, clusterId);
  const updatedBy = resolveUpdatedBy(input);

  await clearWorkspaceSnapshot(client, {
    clusterId: cluster.cluster_id,
    updatedBy,
    action: "reset",
  });
  await setClusterFinalStatus(client, cluster.cluster_id, "open", updatedBy);

  return buildWorkspaceMutationResponse(cluster.cluster_id, {
    workspaceVersion: 0,
    effectiveStatus: "open",
    workspace: null,
  });
}

async function reopenGlobalCluster(clusterId, input) {
  const client = await getDbClient();
  const cluster = await requireCluster(client, clusterId);
  const updatedBy = resolveUpdatedBy(input);

  if (
    !["resolved", "dismissed"].includes(
      String(cluster.status || "").toLowerCase(),
    )
  ) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "Only resolved or dismissed clusters can be reopened",
    });
  }

  const currentWorkspace = await getCurrentWorkspace(
    client,
    cluster.cluster_id,
  );
  const workspace = currentWorkspace?.workspace_payload
    ? normalizeWorkspacePayload(currentWorkspace.workspace_payload)
    : null;
  const effectiveStatus = workspace ? "in_review" : "open";

  await createWorkspaceDecision(client, {
    clusterId: cluster.cluster_id,
    operation: "reopen_workspace",
    workspace: workspace || createEmptyWorkspace(),
    note: "",
    requestedBy: updatedBy,
  });

  await setClusterFinalStatus(
    client,
    cluster.cluster_id,
    effectiveStatus,
    updatedBy,
  );

  return buildWorkspaceMutationResponse(cluster.cluster_id, {
    workspaceVersion:
      Number.parseInt(String(currentWorkspace?.version || 0), 10) || 0,
    effectiveStatus,
    workspace,
  });
}

async function resolveGlobalCluster(clusterId, input) {
  const client = await getDbClient();
  const cluster = await requireCluster(client, clusterId);
  const request = normalizeResolveRequest(input);
  const currentWorkspace = await getCurrentWorkspace(
    client,
    cluster.cluster_id,
  );
  const workspace = currentWorkspace?.workspace_payload
    ? normalizeWorkspacePayload(currentWorkspace.workspace_payload, {
        requireCompleteGroups: request.status === "resolved",
      })
    : null;

  if (request.status === "resolved" && !workspace) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "A saved workspace is required before resolving a cluster",
    });
  }

  if (request.status === "resolved" && workspace) {
    await materializeWorkspace(
      client,
      cluster.cluster_id,
      workspace,
      request.requestedBy,
    );
  }

  const decisionId = await createWorkspaceDecision(client, {
    clusterId: cluster.cluster_id,
    operation:
      request.status === "dismissed"
        ? "dismiss_workspace"
        : "resolve_workspace",
    workspace: workspace || createEmptyWorkspace(),
    note: request.note,
    requestedBy: request.requestedBy,
  });

  if (workspace) {
    await persistWorkspaceSnapshot(client, {
      clusterId: cluster.cluster_id,
      workspace,
      updatedBy: request.requestedBy,
      action: request.status === "dismissed" ? "dismiss" : "resolve",
    });
  }

  if (request.status === "dismissed" && request.clearWorkspaceOnDismiss) {
    await clearWorkspaceSnapshot(client, {
      clusterId: cluster.cluster_id,
      updatedBy: request.requestedBy,
      action: "dismiss",
    });
  }

  await setClusterFinalStatus(
    client,
    cluster.cluster_id,
    request.status,
    request.requestedBy,
  );

  return {
    ok: true,
    cluster_id: cluster.cluster_id,
    decision_id: decisionId,
    status: request.status,
    next_cluster_id: await getNextUnresolvedClusterId(
      client,
      cluster.cluster_id,
    ),
  };
}

async function postGlobalClusterDecision(clusterId, body) {
  const client = await getDbClient();
  const cleanClusterId = String(clusterId || "").trim();
  const normalizedDecision = normalizeGlobalMergeDecision(body);
  await requireCluster(client, cleanClusterId);

  const decision = await client.queryOne(
    `
    INSERT INTO qa_merge_decisions (
      merge_cluster_id,
      operation,
      decision_payload,
      note,
      requested_by
    )
    VALUES (
      :'cluster_id',
      :'operation',
      COALESCE(NULLIF(:'decision_payload', '')::jsonb, '{}'::jsonb),
      NULLIF(:'note', ''),
      :'requested_by'
    )
    RETURNING decision_id;
    `,
    {
      cluster_id: cleanClusterId,
      operation: normalizedDecision.operation,
      decision_payload: JSON.stringify(normalizedDecision.rawPayload || {}),
      note: normalizedDecision.note || "",
      requested_by: normalizedDecision.requestedBy,
    },
  );

  const decisionId = Number.parseInt(String(decision?.decision_id || 0), 10);
  if (!Number.isFinite(decisionId) || decisionId <= 0) {
    throw new AppError({
      code: "INTERNAL_ERROR",
      statusCode: 500,
      message: "Failed to persist merge decision",
    });
  }

  await insertDecisionMembers(client, decisionId, normalizedDecision);
  await applyDecisionSideEffects(client, normalizedDecision);

  const nextStatus =
    normalizedDecision.operation === "keep_separate" ? "dismissed" : "resolved";
  await setClusterFinalStatus(
    client,
    cleanClusterId,
    nextStatus,
    normalizedDecision.requestedBy,
  );

  return {
    ok: true,
    cluster_id: cleanClusterId,
    decision_id: decisionId,
    operation: normalizedDecision.operation,
  };
}

async function getRefreshJob(jobId, options = {}) {
  const cleanJobId = String(jobId || "").trim();
  if (!cleanJobId) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "job_id is required",
    });
  }
  const client = createPostgisClient({
    rootDir: options.rootDir || process.cwd(),
  });
  await client.ensureReady();
  const jobsRepo = createPipelineJobsRepo(client);
  const job = await jobsRepo.getById(cleanJobId);
  if (!job) {
    throw new AppError({
      code: "NOT_FOUND",
      statusCode: 404,
      message: "Job not found",
    });
  }
  return job;
}

module.exports = {
  _internal: {
    buildWorkspaceMutationResponse,
    normalizeEvidenceRow,
    resolveUpdatedBy,
  },
  getGlobalClusters,
  getGlobalClusterDetail,
  getRefreshJob,
  postGlobalClusterDecision,
  reopenGlobalCluster,
  resetGlobalClusterWorkspace,
  resolveGlobalCluster,
  saveGlobalClusterWorkspace,
  undoGlobalClusterWorkspace,
};
