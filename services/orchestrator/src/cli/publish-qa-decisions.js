#!/usr/bin/env node
const { AppError } = require("../core/errors");
const { createPostgisClient } = require("../data/postgis/client");
const {
  ensureQaPublishBatchDecisionColumns,
} = require("../data/postgis/repositories/publish-batch-schema");
const { parsePipelineCliArgs, printCliError } = require("./pipeline-common");
const { loadQaAudit } = require("./qa-audit");

const PUBLISH_READINESS_SQL = `
SELECT
  COUNT(*)::integer AS total_clusters,
  COUNT(*) FILTER (WHERE status = 'open')::integer AS open_clusters,
  COUNT(*) FILTER (WHERE status = 'in_review')::integer AS in_review_clusters,
  COUNT(*) FILTER (WHERE status = 'resolved')::integer AS resolved_clusters,
  COUNT(*) FILTER (WHERE status = 'dismissed')::integer AS dismissed_clusters,
  COUNT(*) FILTER (
    WHERE status IN ('open', 'in_review')
      AND severity = 'high'
  )::integer AS unresolved_high_clusters
FROM qa_merge_clusters cluster
WHERE (
  NULLIF(:'scope_country', '') IS NULL
  OR cluster.scope_country = NULLIF(:'scope_country', '')::char(2)
)
  AND (
    NULLIF(:'scope_as_of', '') IS NULL
    OR cluster.scope_as_of IS NOT DISTINCT FROM NULLIF(:'scope_as_of', '')::date
  );
`;

const PUBLISH_BATCH_SQL = `
WITH supersede AS (
  UPDATE qa_publish_batches
  SET status = 'superseded'
  WHERE status = 'published'
    AND (
      NULLIF(:'scope_country', '') IS NULL
      OR scope_country = NULLIF(:'scope_country', '')::char(2)
    )
    AND (
      NULLIF(:'scope_as_of', '') IS NULL
      OR scope_as_of IS NOT DISTINCT FROM NULLIF(:'scope_as_of', '')::date
    )
  RETURNING publish_batch_id
),
inserted_batch AS (
  INSERT INTO qa_publish_batches (
    scope_country,
    scope_as_of,
    created_by,
    note,
    status,
    summary
  )
  VALUES (
    NULLIF(:'scope_country', '')::iso_country_code,
    NULLIF(:'scope_as_of', '')::date,
    :'created_by',
    NULLIF(:'note', ''),
    'published',
    '{}'::jsonb
  )
  RETURNING publish_batch_id
),
latest_decisions AS (
  SELECT DISTINCT ON (decision.merge_cluster_id)
    decision.decision_id,
    decision.merge_cluster_id,
    decision.operation,
    decision.decision_payload,
    decision.note,
    decision.requested_by,
    decision.created_at
  FROM qa_merge_decisions decision
  JOIN qa_merge_clusters cluster
    ON cluster.merge_cluster_id = decision.merge_cluster_id
  WHERE (
    NULLIF(:'scope_country', '') IS NULL
    OR cluster.scope_country = NULLIF(:'scope_country', '')::char(2)
  )
    AND (
      NULLIF(:'scope_as_of', '') IS NULL
      OR cluster.scope_as_of IS NOT DISTINCT FROM NULLIF(:'scope_as_of', '')::date
    )
  ORDER BY
    decision.merge_cluster_id ASC,
    decision.created_at DESC,
    decision.decision_id DESC
),
inserted_decisions AS (
  INSERT INTO qa_publish_batch_decisions (
    publish_batch_id,
    decision_id,
    merge_cluster_id,
    cluster_key_snapshot,
    decision_payload_snapshot
  )
  SELECT
    (SELECT publish_batch_id FROM inserted_batch),
    decision.decision_id,
    decision.merge_cluster_id,
    COALESCE(cluster.cluster_key, ''),
    jsonb_build_object(
      'decision_id', decision.decision_id,
      'merge_cluster_id', decision.merge_cluster_id,
      'cluster_key', COALESCE(cluster.cluster_key, ''),
      'operation', decision.operation,
      'decision_payload', decision.decision_payload,
      'note', decision.note,
      'requested_by', decision.requested_by,
      'created_at', decision.created_at,
      'members', COALESCE(
        (
          SELECT jsonb_agg(
            jsonb_build_object(
              'global_station_id', member.global_station_id,
              'action', member.action,
              'group_label', member.group_label,
              'metadata', member.metadata
            )
            ORDER BY member.global_station_id ASC, member.action ASC, member.group_label ASC
          )
          FROM qa_merge_decision_members member
          WHERE member.decision_id = decision.decision_id
        ),
        '[]'::jsonb
      )
    )
  FROM latest_decisions decision
),
updated_batch AS (
  UPDATE qa_publish_batches batch
  SET summary = jsonb_build_object(
    'decisionCount',
    (
      SELECT COUNT(*)
      FROM qa_publish_batch_decisions published
      WHERE published.publish_batch_id = batch.publish_batch_id
    ),
    'clusterCount',
    (
      SELECT COUNT(DISTINCT published.merge_cluster_id)
      FROM qa_publish_batch_decisions published
      WHERE published.publish_batch_id = batch.publish_batch_id
    )
  )
  WHERE batch.publish_batch_id = (SELECT publish_batch_id FROM inserted_batch)
  RETURNING batch.publish_batch_id, batch.summary
)
SELECT
  batch.publish_batch_id,
  batch.summary
FROM updated_batch batch;
`;

function parseArgs(argv = []) {
  const parsed = parsePipelineCliArgs(argv);
  const args = Array.isArray(parsed.passthroughArgs)
    ? parsed.passthroughArgs
    : [];
  const options = {
    rootDir: parsed.rootDir,
    country: "",
    asOf: "",
    note: "",
    createdBy: process.env.USER || "codex",
    allowUnresolved: false,
    allowStructuralIssues: false,
  };

  for (let index = 0; index < args.length; index += 1) {
    const token = args[index];
    switch (token) {
      case "--country":
        options.country = String(args[index + 1] || "")
          .trim()
          .toUpperCase();
        index += 1;
        break;
      case "--as-of":
        options.asOf = String(args[index + 1] || "").trim();
        index += 1;
        break;
      case "--note":
        options.note = String(args[index + 1] || "").trim();
        index += 1;
        break;
      case "--created-by":
        options.createdBy =
          String(args[index + 1] || "").trim() || options.createdBy;
        index += 1;
        break;
      case "--allow-unresolved":
        options.allowUnresolved = true;
        break;
      case "--allow-structural-issues":
        options.allowStructuralIssues = true;
        break;
      case "-h":
      case "--help":
        process.stdout.write(
          "Usage: scripts/data/publish-qa-decisions.sh [--country ISO2] [--as-of YYYY-MM-DD] [--note TEXT] [--created-by USER] [--allow-unresolved] [--allow-structural-issues]\n",
        );
        return { ...options, help: true };
      default:
        throw new AppError({
          code: "INVALID_REQUEST",
          message: `Unknown argument: ${token}`,
        });
    }
  }

  return options;
}

function normalizeReadinessCounts(row) {
  return {
    totalClusters: Number.parseInt(String(row?.total_clusters || 0), 10) || 0,
    openClusters: Number.parseInt(String(row?.open_clusters || 0), 10) || 0,
    inReviewClusters:
      Number.parseInt(String(row?.in_review_clusters || 0), 10) || 0,
    resolvedClusters:
      Number.parseInt(String(row?.resolved_clusters || 0), 10) || 0,
    dismissedClusters:
      Number.parseInt(String(row?.dismissed_clusters || 0), 10) || 0,
    unresolvedHighClusters:
      Number.parseInt(String(row?.unresolved_high_clusters || 0), 10) || 0,
  };
}

function assertPublishReadiness(counts, options = {}) {
  const unresolvedClusters =
    Number(counts.openClusters || 0) + Number(counts.inReviewClusters || 0);
  if (!options.allowUnresolved && unresolvedClusters > 0) {
    throw new AppError({
      code: "QA_PUBLISH_BLOCKED_UNRESOLVED_CLUSTERS",
      message:
        "Cannot publish QA decisions while unresolved merge clusters remain. " +
        `open=${counts.openClusters} in_review=${counts.inReviewClusters} ` +
        `high_severity=${counts.unresolvedHighClusters}. ` +
        "Resolve or dismiss all clusters first, or rerun with --allow-unresolved.",
    });
  }
}

function assertPublishAuditHealthy(audit, options = {}) {
  if (options.allowStructuralIssues || audit?.status?.structurallyHealthy) {
    return;
  }

  const metrics = audit?.metrics || {};
  throw new AppError({
    code: "QA_PUBLISH_BLOCKED_AUDIT_FAILED",
    message:
      "Cannot publish QA decisions while the scope audit is structurally unhealthy. " +
      `required_stages_ready=${Boolean(metrics.allRequiredStagesReady)} ` +
      `candidate_mismatches=${Number(metrics.clustersWithCandidateCountMismatch || 0)} ` +
      `issue_mismatches=${Number(metrics.clustersWithIssueCountMismatch || 0)} ` +
      `too_few_candidates=${Number(metrics.clustersWithTooFewCandidates || 0)} ` +
      `clusters_without_pairs=${Number(metrics.clustersWithoutEligiblePairs || 0)} ` +
      `eligible_pairs_missing_cluster=${Number(metrics.eligiblePairsMissingClusterRow || 0)} ` +
      `eligible_pairs_missing_candidate_coverage=${Number(metrics.eligiblePairsMissingCandidateCoverage || 0)} ` +
      `final_without_decision=${Number(metrics.finalClustersWithoutDecision || 0)}. ` +
      "Repair the merge queue first, or rerun with --allow-structural-issues.",
  });
}

async function run() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    return;
  }

  const client = createPostgisClient({ rootDir: options.rootDir });
  try {
    await client.ensureReady();
    await ensureQaPublishBatchDecisionColumns(client);
    const audit = await loadQaAudit(client, options);
    assertPublishAuditHealthy(audit, options);
    const readiness = normalizeReadinessCounts(
      await client.queryOne(PUBLISH_READINESS_SQL, {
        scope_country: options.country,
        scope_as_of: options.asOf,
      }),
    );
    assertPublishReadiness(readiness, options);

    const row = await client.queryOne(PUBLISH_BATCH_SQL, {
      scope_country: options.country,
      scope_as_of: options.asOf,
      created_by: options.createdBy,
      note: options.note,
    });

    process.stdout.write(
      `${JSON.stringify({ ok: true, readiness, auditStatus: audit.status, ...row })}\n`,
    );
  } finally {
    await client.end();
  }
}

if (require.main === module) {
  void run().catch((error) => {
    printCliError("publish-qa-decisions", error, "Publish QA decisions failed");
    process.exit(1);
  });
}

module.exports = {
  run,
  _internal: {
    parseArgs,
    normalizeReadinessCounts,
    assertPublishReadiness,
    assertPublishAuditHealthy,
    PUBLISH_READINESS_SQL,
    PUBLISH_BATCH_SQL,
  },
};
