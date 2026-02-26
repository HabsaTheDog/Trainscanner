/**
 * ai-queue.js
 *
 * Domain layer for the AI Low-Confidence Match Queue (Task 5.1).
 * Exposes pure DB helpers consumed by GraphQL resolvers.
 */

const LOW_CONFIDENCE_THRESHOLD = 0.9;

/**
 * Fetch evidence rows where ai_confidence < 0.90.
 *
 * @param {object} client  - PostGIS client from createPostgisClient()
 * @param {object} options
 * @param {number} [options.limit=50]
 * @param {number} [options.offset=0]
 * @returns {Promise<{total: number, items: object[]}>}
 */
async function getLowConfidenceQueue(client, { limit = 50, offset = 0 } = {}) {
  const safeLimit = Math.min(
    Math.max(1, Number.parseInt(String(limit), 10) || 50),
    500,
  );
  const safeOffset = Math.max(0, Number.parseInt(String(offset), 10) || 0);

  const totalRow = await client.queryOne(
    `
    SELECT COUNT(*)::int AS total
    FROM qa_station_cluster_evidence_v2
    WHERE ai_confidence IS NOT NULL
      AND ai_confidence < :'threshold'
    `,
    { threshold: LOW_CONFIDENCE_THRESHOLD },
  );

  const items = await client.queryRows(
    `
    SELECT
      e.evidence_id,
      e.cluster_id,
      e.source_canonical_station_id,
      e.target_canonical_station_id,
      e.evidence_type,
      e.ai_confidence,
      e.ai_suggested_action,
      c.display_name AS cluster_display_name,
      src_cand.latitude  AS source_lat,
      src_cand.longitude AS source_lon,
      tgt_cand.latitude  AS target_lat,
      tgt_cand.longitude AS target_lon
    FROM qa_station_cluster_evidence_v2 e
    LEFT JOIN qa_station_clusters_v2 c USING (cluster_id)
    -- pull coordinates from the cluster candidate table (populated during cluster rebuild)
    LEFT JOIN qa_station_cluster_candidates_v2 src_cand
      ON src_cand.cluster_id = e.cluster_id
     AND src_cand.canonical_station_id = e.source_canonical_station_id
    LEFT JOIN qa_station_cluster_candidates_v2 tgt_cand
      ON tgt_cand.cluster_id = e.cluster_id
     AND tgt_cand.canonical_station_id = e.target_canonical_station_id
    WHERE e.ai_confidence IS NOT NULL
      AND e.ai_confidence < :'threshold'
    ORDER BY e.ai_confidence ASC, e.evidence_id ASC
    LIMIT :'lim' OFFSET :'off'
    `,
    {
      threshold: LOW_CONFIDENCE_THRESHOLD,
      lim: safeLimit,
      off: safeOffset,
    },
  );

  return {
    total: totalRow?.total || 0,
    items: items.map((row) => ({
      evidence_id: String(row.evidence_id),
      cluster_id: row.cluster_id,
      source_canonical_station_id: row.source_canonical_station_id,
      target_canonical_station_id: row.target_canonical_station_id,
      evidence_type: row.evidence_type,
      ai_confidence:
        row.ai_confidence != null ? Number(row.ai_confidence) : null,
      ai_suggested_action: row.ai_suggested_action || null,
      cluster_display_name: row.cluster_display_name || null,
      source_lat: row.source_lat != null ? Number(row.source_lat) : null,
      source_lon: row.source_lon != null ? Number(row.source_lon) : null,
      target_lat: row.target_lat != null ? Number(row.target_lat) : null,
      target_lon: row.target_lon != null ? Number(row.target_lon) : null,
    })),
  };
}

/**
 * Record an operator decision on an AI-suggested match.
 *
 * @param {object} client
 * @param {object} opts
 * @param {string} opts.clusterId
 * @param {string} opts.evidenceId
 * @param {string} opts.operation  - 'approve' | 'reject' | 'override'
 * @param {string} [opts.targetClusterId]  - required when operation === 'override'
 * @param {string} [opts.requestedBy]
 * @returns {Promise<{decisionId: string, clusterId: string, operation: string}>}
 */
async function recordAiMatchDecision(
  client,
  {
    clusterId,
    evidenceId,
    operation,
    targetClusterId = null,
    requestedBy = "operator",
  } = {},
) {
  if (!clusterId || !evidenceId) {
    throw Object.assign(new Error("clusterId and evidenceId are required"), {
      code: "INVALID_REQUEST",
      statusCode: 400,
    });
  }

  const allowed = new Set(["approve", "reject", "override"]);
  if (!allowed.has(operation)) {
    throw Object.assign(
      new Error(
        `Invalid operation '${operation}'. Must be one of: ${[...allowed].join(", ")}`,
      ),
      { code: "INVALID_REQUEST", statusCode: 400 },
    );
  }

  if (operation === "override" && !targetClusterId) {
    throw Object.assign(
      new Error("targetClusterId is required for 'override' operation"),
      { code: "INVALID_REQUEST", statusCode: 400 },
    );
  }

  // Fetch the ai_confidence at decision time for the audit trail
  const evidenceRow = await client.queryOne(
    `
    SELECT ai_confidence
    FROM qa_station_cluster_evidence_v2
    WHERE evidence_id = :'evidence_id'
      AND cluster_id = :'cluster_id'
    `,
    { evidence_id: evidenceId, cluster_id: clusterId },
  );

  if (!evidenceRow) {
    throw Object.assign(
      new Error(`Evidence id=${evidenceId} not found in cluster ${clusterId}`),
      { code: "NOT_FOUND", statusCode: 404 },
    );
  }

  // Map operator action to the canonical decision operation set
  const dbOperation =
    operation === "approve"
      ? "keep_separate"
      : operation === "reject"
        ? "keep_separate"
        : "merge"; // override → merge into targetCluster

  const decisionPayload = {
    ai_queue_action: operation,
    evidence_id: String(evidenceId),
    ...(targetClusterId ? { target_cluster_id: targetClusterId } : {}),
  };

  const row = await client.queryOne(
    `
    INSERT INTO qa_station_cluster_decisions_v2 (
      cluster_id,
      operation,
      decision_payload,
      requested_by,
      ai_confidence
    ) VALUES (
      :'cluster_id',
      :'operation',
      :'decision_payload',
      :'requested_by',
      :'ai_confidence'
    )
    RETURNING decision_id
    `,
    {
      cluster_id: clusterId,
      operation: dbOperation,
      decision_payload: JSON.stringify(decisionPayload),
      requested_by: requestedBy,
      ai_confidence: evidenceRow.ai_confidence ?? null,
    },
  );

  return {
    decisionId: String(row.decision_id),
    clusterId,
    operation,
  };
}

/**
 * Upsert a walk-time override for a named mega-hub into station_transfer_rules.
 *
 * Uses hub_name as the stable key so we don't need a canonical_station_id —
 * the hub frontend identifier (e.g. 'paris-cdg') is stored as hub_name and
 * the 'hub' rule_scope row is created / updated idempotently.
 *
 * @param {object} client
 * @param {object} opts
 * @param {string} opts.hubId      - stable frontend hub identifier (e.g. 'frankfurt-hbf')
 * @param {number} opts.walkMinutes - walk-time in whole minutes (>= 0)
 * @param {string} [opts.requestedBy]
 * @returns {Promise<{ruleId: string, hubId: string, walkMinutes: number}>}
 */
async function setMegaHubWalkTime(
  client,
  { hubId, walkMinutes, requestedBy = "operator" } = {},
) {
  if (!hubId || typeof walkMinutes !== "number" || walkMinutes < 0) {
    throw Object.assign(
      new Error(
        "hubId is required and walkMinutes must be a non-negative number",
      ),
      { code: "INVALID_REQUEST", statusCode: 400 },
    );
  }

  const safeMinutes = Math.round(walkMinutes);

  // Use the hub_id unique partial index (migration 014) for upsert.
  // hub_id = the stable frontend identifier (e.g. 'paris-cdg').
  // hub_name mirrors hub_id for backwards compatibility with legacy queries.
  // country = 'EU' sentinel for pan-European mega-hubs (migration 014 widened CHECK).
  const row = await client.queryOne(
    `
    INSERT INTO station_transfer_rules (
      rule_scope,
      country,
      hub_id,
      hub_name,
      min_transfer_minutes,
      long_wait_minutes,
      priority,
      source_reference,
      notes,
      created_by,
      updated_by
    ) VALUES (
      'hub',
      'EU',
      :'hub_id',
      :'hub_id',
      :'walk_minutes',
      45,
      500,
      'qa_operator_override',
      :'notes',
      :'requested_by',
      :'requested_by'
    )
    ON CONFLICT (hub_id)
    WHERE rule_scope = 'hub' AND hub_id IS NOT NULL AND is_active = true AND effective_to IS NULL
    DO UPDATE SET
      min_transfer_minutes = EXCLUDED.min_transfer_minutes,
      updated_by = EXCLUDED.updated_by,
      updated_at = now()
    RETURNING rule_id
    `,
    {
      hub_id: hubId,
      walk_minutes: safeMinutes,
      notes: `Operator walk-time override for mega-hub '${hubId}'`,
      requested_by: requestedBy,
    },
  );

  return {
    ruleId: String(row.rule_id),
    hubId,
    walkMinutes: safeMinutes,
  };
}

module.exports = {
  getLowConfidenceQueue,
  recordAiMatchDecision,
  setMegaHubWalkTime,
};
