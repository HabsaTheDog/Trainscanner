"use strict";

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
    const safeLimit = Math.min(Math.max(1, Number.parseInt(String(limit), 10) || 50), 500);
    const safeOffset = Math.max(0, Number.parseInt(String(offset), 10) || 0);

    const totalRow = await client.queryOne(
        `
    SELECT COUNT(*)::int AS total
    FROM qa_station_cluster_evidence_v2
    WHERE ai_confidence IS NOT NULL
      AND ai_confidence < :threshold
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
      c.display_name AS cluster_display_name
    FROM qa_station_cluster_evidence_v2 e
    LEFT JOIN qa_station_clusters_v2 c USING (cluster_id)
    WHERE e.ai_confidence IS NOT NULL
      AND e.ai_confidence < :threshold
    ORDER BY e.ai_confidence ASC, e.evidence_id ASC
    LIMIT :lim OFFSET :off
    `,
        {
            threshold: LOW_CONFIDENCE_THRESHOLD,
            lim: safeLimit,
            off: safeOffset,
        },
    );

    return {
        total: (totalRow && totalRow.total) || 0,
        items: items.map((row) => ({
            evidence_id: String(row.evidence_id),
            cluster_id: row.cluster_id,
            source_canonical_station_id: row.source_canonical_station_id,
            target_canonical_station_id: row.target_canonical_station_id,
            evidence_type: row.evidence_type,
            ai_confidence: row.ai_confidence != null ? Number(row.ai_confidence) : null,
            ai_suggested_action: row.ai_suggested_action || null,
            cluster_display_name: row.cluster_display_name || null,
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
async function recordAiMatchDecision(client, {
    clusterId,
    evidenceId,
    operation,
    targetClusterId = null,
    requestedBy = "operator",
} = {}) {
    if (!clusterId || !evidenceId) {
        throw Object.assign(new Error("clusterId and evidenceId are required"), { code: "INVALID_REQUEST", statusCode: 400 });
    }

    const allowed = new Set(["approve", "reject", "override"]);
    if (!allowed.has(operation)) {
        throw Object.assign(
            new Error(`Invalid operation '${operation}'. Must be one of: ${[...allowed].join(", ")}`),
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
    WHERE evidence_id = :evidence_id
      AND cluster_id = :cluster_id
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
    const dbOperation = operation === "approve" ? "keep_separate"
        : operation === "reject" ? "keep_separate"
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
      :cluster_id,
      :operation,
      :decision_payload,
      :requested_by,
      :ai_confidence
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

module.exports = { getLowConfidenceQueue, recordAiMatchDecision };
