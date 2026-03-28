#!/usr/bin/env node
const { AppError } = require("../core/errors");
const { createPostgisClient } = require("../data/postgis/client");
const {
  createPipelineStageRepo,
} = require("../data/postgis/repositories/pipeline-stage-repo");
const {
  ensureQaPublishBatchDecisionColumns,
} = require("../data/postgis/repositories/publish-batch-schema");
const {
  ensureQaMergeEligiblePairsTable,
} = require("../data/postgis/repositories/merge-eligible-pairs-schema");
const { parsePipelineCliArgs, printCliError } = require("./pipeline-common");

const REQUIRED_STAGE_IDS = [
  "stop-topology",
  "qa-network-context",
  "global-stations",
  "qa-network-projection",
  "merge-queue",
];

const AUDIT_SQL = `
WITH scope AS (
  SELECT
    COALESCE(NULLIF(:'scope_as_of', ''), 'latest') AS scope_tag,
    NULLIF(:'scope_country', '')::char(2) AS scope_country
),
scoped_clusters AS (
  SELECT c.*
  FROM qa_merge_clusters c
  JOIN scope s
    ON c.scope_tag = s.scope_tag
   AND (
     s.scope_country IS NULL
     OR c.scope_country = s.scope_country
   )
),
scoped_eligible_pairs AS (
  SELECT ep.*
  FROM qa_merge_eligible_pairs ep
  JOIN scope s
    ON ep.scope_tag = s.scope_tag
   AND (
     s.scope_country IS NULL
     OR ep.scope_country = s.scope_country
   )
),
candidate_counts AS (
  SELECT
    c.merge_cluster_id,
    COUNT(*)::integer AS actual_candidate_count
  FROM qa_merge_cluster_candidates c
  JOIN scoped_clusters sc
    ON sc.merge_cluster_id = c.merge_cluster_id
  GROUP BY c.merge_cluster_id
),
issue_counts AS (
  SELECT
    e.merge_cluster_id,
    COUNT(
      DISTINCT (e.source_global_station_id || '|' || e.target_global_station_id)
    ) FILTER (
      WHERE e.status IN ('warning', 'missing')
    )::integer AS actual_issue_count
  FROM qa_merge_cluster_evidence e
  JOIN scoped_clusters sc
    ON sc.merge_cluster_id = e.merge_cluster_id
  GROUP BY e.merge_cluster_id
),
eligible_pair_cluster_counts AS (
  SELECT
    ep.merge_cluster_id,
    COUNT(*)::integer AS eligible_pair_count
  FROM scoped_eligible_pairs ep
  GROUP BY ep.merge_cluster_id
),
decision_counts AS (
  SELECT
    d.merge_cluster_id,
    COUNT(*)::integer AS decision_count
  FROM qa_merge_decisions d
  JOIN scoped_clusters sc
    ON sc.merge_cluster_id = d.merge_cluster_id
  GROUP BY d.merge_cluster_id
),
latest_published_batch AS (
  SELECT batch.publish_batch_id
  FROM qa_publish_batches batch
  JOIN scope s
    ON (
      s.scope_country IS NULL
      OR batch.scope_country = s.scope_country
    )
   AND batch.scope_as_of IS NOT DISTINCT FROM NULLIF(:'scope_as_of', '')::date
  WHERE batch.status = 'published'
  ORDER BY batch.created_at DESC, batch.publish_batch_id DESC
  LIMIT 1
),
published_cluster_refs AS (
  SELECT published.merge_cluster_id, published.cluster_key_snapshot
  FROM qa_publish_batch_decisions published
  WHERE published.publish_batch_id = (
    SELECT publish_batch_id FROM latest_published_batch
  )
),
stage_readiness AS (
  SELECT
    COUNT(*) FILTER (WHERE stage_id = ANY(:'required_stage_ids'))::integer AS stage_rows,
    COUNT(*) FILTER (
      WHERE stage_id = ANY(:'required_stage_ids')
        AND status = 'ready'
    )::integer AS ready_stage_rows
  FROM pipeline_stage_materializations stage
  WHERE (
    NULLIF(:'scope_country', '') IS NULL
    OR stage.scope_country = NULLIF(:'scope_country', '')::char(2)
  )
    AND (
      NULLIF(:'scope_as_of', '') IS NULL
      OR stage.scope_as_of IS NOT DISTINCT FROM NULLIF(:'scope_as_of', '')::date
    )
)
SELECT json_build_object(
  'scopeCountry', COALESCE(NULLIF(:'scope_country', ''), ''),
  'scopeAsOf', COALESCE(NULLIF(:'scope_as_of', ''), ''),
  'scopeTag', (SELECT scope_tag FROM scope),
  'requiredStageCount', array_length(:'required_stage_ids'::text[], 1),
  'readyStageCount', COALESCE((SELECT ready_stage_rows FROM stage_readiness), 0),
  'allRequiredStagesReady', COALESCE((SELECT ready_stage_rows FROM stage_readiness), 0) >= array_length(:'required_stage_ids'::text[], 1),
  'totalClusters', (SELECT COUNT(*) FROM scoped_clusters),
  'eligiblePairCount', (SELECT COUNT(*) FROM scoped_eligible_pairs),
  'openClusters', (SELECT COUNT(*) FROM scoped_clusters WHERE status = 'open'),
  'inReviewClusters', (SELECT COUNT(*) FROM scoped_clusters WHERE status = 'in_review'),
  'resolvedClusters', (SELECT COUNT(*) FROM scoped_clusters WHERE status = 'resolved'),
  'dismissedClusters', (SELECT COUNT(*) FROM scoped_clusters WHERE status = 'dismissed'),
  'unresolvedHighClusters', (
    SELECT COUNT(*)
    FROM scoped_clusters
    WHERE status IN ('open', 'in_review')
      AND severity = 'high'
  ),
  'clustersWithCandidateCountMismatch', (
    SELECT COUNT(*)
    FROM scoped_clusters sc
    LEFT JOIN candidate_counts cc
      ON cc.merge_cluster_id = sc.merge_cluster_id
    WHERE sc.candidate_count <> COALESCE(cc.actual_candidate_count, 0)
  ),
  'clustersWithIssueCountMismatch', (
    SELECT COUNT(*)
    FROM scoped_clusters sc
    LEFT JOIN issue_counts ic
      ON ic.merge_cluster_id = sc.merge_cluster_id
    WHERE sc.issue_count <> COALESCE(ic.actual_issue_count, 0)
  ),
  'clustersWithTooFewCandidates', (
    SELECT COUNT(*)
    FROM scoped_clusters sc
    WHERE sc.candidate_count < 2
  ),
  'clustersWithoutEligiblePairs', (
    SELECT COUNT(*)
    FROM scoped_clusters sc
    LEFT JOIN eligible_pair_cluster_counts epc
      ON epc.merge_cluster_id = sc.merge_cluster_id
    WHERE COALESCE(epc.eligible_pair_count, 0) = 0
  ),
  'eligiblePairsMissingClusterRow', (
    SELECT COUNT(*)
    FROM scoped_eligible_pairs ep
    LEFT JOIN scoped_clusters sc
      ON sc.merge_cluster_id = ep.merge_cluster_id
    WHERE sc.merge_cluster_id IS NULL
  ),
  'eligiblePairsMissingCandidateCoverage', (
    SELECT COUNT(*)
    FROM scoped_eligible_pairs ep
    LEFT JOIN qa_merge_cluster_candidates source_candidate
      ON source_candidate.merge_cluster_id = ep.merge_cluster_id
     AND source_candidate.global_station_id = ep.source_global_station_id
    LEFT JOIN qa_merge_cluster_candidates target_candidate
      ON target_candidate.merge_cluster_id = ep.merge_cluster_id
     AND target_candidate.global_station_id = ep.target_global_station_id
    WHERE source_candidate.global_station_id IS NULL
      OR target_candidate.global_station_id IS NULL
  ),
  'finalClustersWithoutDecision', (
    SELECT COUNT(*)
    FROM scoped_clusters sc
    LEFT JOIN decision_counts dc
      ON dc.merge_cluster_id = sc.merge_cluster_id
    WHERE sc.status IN ('resolved', 'dismissed')
      AND COALESCE(dc.decision_count, 0) = 0
  ),
  'latestPublishedBatchId', COALESCE(
    (SELECT publish_batch_id FROM latest_published_batch),
    0
  ),
  'latestPublishedClusterRefs', (
    SELECT COUNT(*)
    FROM published_cluster_refs
  ),
  'latestPublishedClustersMissingLiveRows', (
    SELECT COUNT(*)
    FROM published_cluster_refs published
    LEFT JOIN scoped_clusters sc
      ON sc.merge_cluster_id = published.merge_cluster_id
    WHERE published.merge_cluster_id IS NOT NULL
      AND sc.merge_cluster_id IS NULL
  )
)::text AS audit_json;
`;

function printUsage() {
  process.stdout.write(
    "Usage: scripts/data/qa-audit.sh [--country ISO2] [--as-of YYYY-MM-DD] [--json]\n",
  );
}

function parseArgs(argv = []) {
  const parsed = parsePipelineCliArgs(argv);
  const args = Array.isArray(parsed.passthroughArgs)
    ? parsed.passthroughArgs
    : [];
  const options = {
    rootDir: parsed.rootDir,
    country: "",
    asOf: "",
    json: false,
    help: false,
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
      case "--json":
        options.json = true;
        break;
      case "-h":
      case "--help":
        options.help = true;
        break;
      default:
        throw new AppError({
          code: "INVALID_REQUEST",
          message: `Unknown argument: ${token}`,
        });
    }
  }

  return options;
}

function toInt(value) {
  return Number.parseInt(String(value || 0), 10) || 0;
}

function normalizeAuditPayload(payload) {
  const normalized = payload || {};
  return {
    scopeCountry: String(normalized.scopeCountry || ""),
    scopeAsOf: String(normalized.scopeAsOf || ""),
    scopeTag: String(normalized.scopeTag || "latest"),
    requiredStageCount: toInt(normalized.requiredStageCount),
    readyStageCount: toInt(normalized.readyStageCount),
    allRequiredStagesReady: normalized.allRequiredStagesReady === true,
    totalClusters: toInt(normalized.totalClusters),
    eligiblePairCount: toInt(normalized.eligiblePairCount),
    openClusters: toInt(normalized.openClusters),
    inReviewClusters: toInt(normalized.inReviewClusters),
    resolvedClusters: toInt(normalized.resolvedClusters),
    dismissedClusters: toInt(normalized.dismissedClusters),
    unresolvedHighClusters: toInt(normalized.unresolvedHighClusters),
    clustersWithCandidateCountMismatch: toInt(
      normalized.clustersWithCandidateCountMismatch,
    ),
    clustersWithIssueCountMismatch: toInt(
      normalized.clustersWithIssueCountMismatch,
    ),
    clustersWithTooFewCandidates: toInt(
      normalized.clustersWithTooFewCandidates,
    ),
    clustersWithoutEligiblePairs: toInt(
      normalized.clustersWithoutEligiblePairs,
    ),
    eligiblePairsMissingClusterRow: toInt(
      normalized.eligiblePairsMissingClusterRow,
    ),
    eligiblePairsMissingCandidateCoverage: toInt(
      normalized.eligiblePairsMissingCandidateCoverage,
    ),
    finalClustersWithoutDecision: toInt(
      normalized.finalClustersWithoutDecision,
    ),
    latestPublishedBatchId: toInt(normalized.latestPublishedBatchId),
    latestPublishedClusterRefs: toInt(normalized.latestPublishedClusterRefs),
    latestPublishedClustersMissingLiveRows: toInt(
      normalized.latestPublishedClustersMissingLiveRows,
    ),
  };
}

function summarizeStageRows(rows = [], options = {}) {
  return REQUIRED_STAGE_IDS.map((stageId) => {
    const matching = rows.find(
      (row) =>
        row.stage_id === stageId &&
        (!options.country ||
          String(row.scope_country || "") === options.country) &&
        (!options.asOf || String(row.scope_as_of || "") === options.asOf),
    );
    return {
      stageId,
      status: matching?.status || "missing",
      lastFinishedAt: matching?.last_finished_at || null,
    };
  });
}

function deriveAuditStatus(payload) {
  const structurallyHealthy =
    payload.allRequiredStagesReady &&
    payload.clustersWithCandidateCountMismatch === 0 &&
    payload.clustersWithIssueCountMismatch === 0 &&
    payload.clustersWithTooFewCandidates === 0 &&
    payload.clustersWithoutEligiblePairs === 0 &&
    payload.eligiblePairsMissingClusterRow === 0 &&
    payload.eligiblePairsMissingCandidateCoverage === 0 &&
    payload.finalClustersWithoutDecision === 0;
  const publishReady =
    structurallyHealthy &&
    payload.openClusters === 0 &&
    payload.inReviewClusters === 0;

  return {
    structurallyHealthy,
    publishReady,
  };
}

async function loadQaAudit(client, options = {}) {
  await ensureQaPublishBatchDecisionColumns(client);
  await ensureQaMergeEligiblePairsTable(client);
  const stageRepo = createPipelineStageRepo(client);
  const stageRows = await stageRepo.listStageStatus(REQUIRED_STAGE_IDS);
  const auditRow = await client.queryOne(AUDIT_SQL, {
    scope_country: options.country || "",
    scope_as_of: options.asOf || "",
    required_stage_ids: REQUIRED_STAGE_IDS,
  });
  const payload = normalizeAuditPayload(
    JSON.parse(String(auditRow?.audit_json || "{}")),
  );
  const stages = summarizeStageRows(stageRows, options);
  const status = deriveAuditStatus(payload);

  return {
    scope: {
      country: payload.scopeCountry || "ALL",
      asOf: payload.scopeAsOf || "",
      scopeTag: payload.scopeTag,
    },
    stages,
    metrics: payload,
    status,
  };
}

async function run() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printUsage();
    return;
  }

  const client = createPostgisClient({ rootDir: options.rootDir });
  try {
    await client.ensureReady();
    const response = await loadQaAudit(client, options);
    const payload = response.metrics;
    const status = response.status;

    if (options.json) {
      process.stdout.write(`${JSON.stringify(response)}\n`);
      return;
    }

    process.stdout.write(
      [
        `scope=${response.scope.scopeTag}`,
        `country=${response.scope.country}`,
        `structurally_healthy=${status.structurallyHealthy}`,
        `publish_ready=${status.publishReady}`,
        `open=${payload.openClusters}`,
        `in_review=${payload.inReviewClusters}`,
        `resolved=${payload.resolvedClusters}`,
        `dismissed=${payload.dismissedClusters}`,
        `eligible_pairs=${payload.eligiblePairCount}`,
        `candidate_mismatches=${payload.clustersWithCandidateCountMismatch}`,
        `issue_mismatches=${payload.clustersWithIssueCountMismatch}`,
        `too_few_candidates=${payload.clustersWithTooFewCandidates}`,
        `clusters_without_pairs=${payload.clustersWithoutEligiblePairs}`,
        `eligible_pairs_missing_cluster=${payload.eligiblePairsMissingClusterRow}`,
        `eligible_pairs_missing_candidate_coverage=${payload.eligiblePairsMissingCandidateCoverage}`,
        `final_without_decision=${payload.finalClustersWithoutDecision}`,
        `published_missing_live=${payload.latestPublishedClustersMissingLiveRows}`,
      ].join(" "),
    );
    process.stdout.write("\n");
    for (const stage of stages) {
      process.stdout.write(
        [
          stage.stageId.padEnd(22, " "),
          `status=${stage.status}`,
          `last_finished_at=${stage.lastFinishedAt || "-"}`,
        ].join(" "),
      );
      process.stdout.write("\n");
    }
  } finally {
    await client.end();
  }
}

if (require.main === module) {
  void run().catch((error) => {
    printCliError("qa-audit", error, "QA audit failed");
    process.exit(1);
  });
}

module.exports = {
  run,
  loadQaAudit,
  _internal: {
    parseArgs,
    normalizeAuditPayload,
    summarizeStageRows,
    deriveAuditStatus,
    AUDIT_SQL,
    REQUIRED_STAGE_IDS,
  },
};
