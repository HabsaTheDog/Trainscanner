const { createPostgisClient } = require("../../data/postgis/client");
const {
  normalizeGlobalMergeDecision,
  normalizeIsoCountry,
} = require("./cluster-decision-contracts");
const { AppError } = require("../../core/errors");
const {
  createPipelineJobsRepo,
} = require("../../data/postgis/repositories/pipeline-jobs-repo");

let dbClient = null;

async function getDbClient() {
  if (!dbClient) {
    dbClient = createPostgisClient();
    await dbClient.ensureReady();
  }
  return dbClient;
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

async function getGlobalClusters(url) {
  const client = await getDbClient();
  const country = normalizeIsoCountry(url.searchParams.get("country"), {
    allowEmpty: true,
  });
  const status = normalizeClusterStatusFilter(url.searchParams.get("status"));
  const scopeTag = String(url.searchParams.get("scope_tag") || "").trim();
  const limit = parseListLimit(url.searchParams.get("limit"), 50, 300);

  const rows = await client.queryRows(
    `
    SELECT
      c.merge_cluster_id AS cluster_id,
      c.status,
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
      country,
      status,
      scope_tag: scopeTag,
      limit,
    },
  );

  return {
    items: rows,
    count: rows.length,
    limit,
  };
}

async function getGlobalClusterDetail(clusterId) {
  const client = await getDbClient();
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
      c.cluster_key,
      c.status,
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

  const [candidates, evidence, decisions] = await Promise.all([
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
        e.score,
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
  ]);

  return {
    ...cluster,
    candidates,
    evidence,
    decisions,
    edit_history: [],
  };
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

async function applyMergeGroup(client, group, requestedBy) {
  const members = group.memberGlobalStationIds;
  const targetStationId = String(members[0] || "").trim();
  if (!targetStationId) {
    return;
  }

  const sourceStationIds = members
    .slice(1)
    .filter((item) => item !== targetStationId);
  for (const sourceStationId of sourceStationIds) {
    await client.runSql(
      `
      UPDATE provider_global_station_mappings
      SET
        global_station_id = :'target_station_id',
        updated_at = now()
      WHERE global_station_id = :'source_station_id'
        AND is_active = true;
      `,
      {
        target_station_id: targetStationId,
        source_station_id: sourceStationId,
      },
    );

    await client.runSql(
      `
      UPDATE global_stop_points
      SET
        global_station_id = :'target_station_id',
        updated_at = now()
      WHERE global_station_id = :'source_station_id'
        AND is_active = true;
      `,
      {
        target_station_id: targetStationId,
        source_station_id: sourceStationId,
      },
    );

    await client.runSql(
      `
      UPDATE global_stations
      SET
        is_active = false,
        metadata = metadata || jsonb_build_object(
          'merged_into', (:'target_station_id')::text,
          'merged_by', (:'requested_by')::text,
          'merged_at', now()
        ),
        updated_at = now()
      WHERE global_station_id = :'source_station_id';
      `,
      {
        target_station_id: targetStationId,
        source_station_id: sourceStationId,
        requested_by: requestedBy,
      },
    );
  }

  if (group.renameTo) {
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
        display_name: group.renameTo,
        global_station_id: targetStationId,
      },
    );
  }
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

async function postGlobalClusterDecision(clusterId, body) {
  const client = await getDbClient();
  const cleanClusterId = String(clusterId || "").trim();
  if (!cleanClusterId) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "cluster_id is required",
    });
  }

  const normalizedDecision = normalizeGlobalMergeDecision(body);
  const existing = await client.queryOne(
    `
    SELECT merge_cluster_id
    FROM qa_merge_clusters
    WHERE merge_cluster_id = :'cluster_id'
    LIMIT 1;
    `,
    { cluster_id: cleanClusterId },
  );

  if (!existing) {
    throw new AppError({
      code: "NOT_FOUND",
      statusCode: 404,
      message: "Global merge cluster not found",
    });
  }

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
  await client.runSql(
    `
    UPDATE qa_merge_clusters
    SET
      status = :'status',
      resolved_at = now(),
      resolved_by = :'resolved_by',
      updated_at = now()
    WHERE merge_cluster_id = :'cluster_id';
    `,
    {
      status: nextStatus,
      resolved_by: normalizedDecision.requestedBy,
      cluster_id: cleanClusterId,
    },
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
  getGlobalClusters,
  getGlobalClusterDetail,
  postGlobalClusterDecision,
  getRefreshJob,
};
