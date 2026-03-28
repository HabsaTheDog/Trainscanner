const test = require("node:test");
const assert = require("node:assert/strict");

const {
  _internal: {
    parseArgs,
    normalizeAuditPayload,
    summarizeStageRows,
    deriveAuditStatus,
    REQUIRED_STAGE_IDS,
    AUDIT_SQL,
  },
} = require("../../src/cli/qa-audit");

test("parseArgs accepts QA audit scope flags", () => {
  const parsed = parseArgs([
    "--country",
    "DE",
    "--as-of",
    "2026-03-15",
    "--json",
  ]);

  assert.equal(parsed.country, "DE");
  assert.equal(parsed.asOf, "2026-03-15");
  assert.equal(parsed.json, true);
});

test("normalizeAuditPayload coerces integer fields", () => {
  assert.deepEqual(
    normalizeAuditPayload({
      scopeTag: "latest",
      requiredStageCount: "5",
      readyStageCount: "4",
      allRequiredStagesReady: false,
      totalClusters: "12",
      eligiblePairCount: "21",
      openClusters: "2",
      inReviewClusters: "1",
      resolvedClusters: "8",
      dismissedClusters: "1",
      unresolvedHighClusters: "1",
      clustersWithCandidateCountMismatch: "0",
      clustersWithIssueCountMismatch: "3",
      clustersWithTooFewCandidates: "1",
      clustersWithoutEligiblePairs: "2",
      eligiblePairsMissingClusterRow: "4",
      eligiblePairsMissingCandidateCoverage: "5",
      finalClustersWithoutDecision: "2",
      latestPublishedBatchId: "7",
      latestPublishedClusterRefs: "5",
      latestPublishedClustersMissingLiveRows: "4",
    }),
    {
      scopeCountry: "",
      scopeAsOf: "",
      scopeTag: "latest",
      requiredStageCount: 5,
      readyStageCount: 4,
      allRequiredStagesReady: false,
      totalClusters: 12,
      eligiblePairCount: 21,
      openClusters: 2,
      inReviewClusters: 1,
      resolvedClusters: 8,
      dismissedClusters: 1,
      unresolvedHighClusters: 1,
      clustersWithCandidateCountMismatch: 0,
      clustersWithIssueCountMismatch: 3,
      clustersWithTooFewCandidates: 1,
      clustersWithoutEligiblePairs: 2,
      eligiblePairsMissingClusterRow: 4,
      eligiblePairsMissingCandidateCoverage: 5,
      finalClustersWithoutDecision: 2,
      latestPublishedBatchId: 7,
      latestPublishedClusterRefs: 5,
      latestPublishedClustersMissingLiveRows: 4,
    },
  );
});

test("summarizeStageRows marks missing required stages", () => {
  const rows = [
    {
      stage_id: "stop-topology",
      scope_country: "DE",
      scope_as_of: "2026-03-15",
      status: "ready",
      last_finished_at: "2026-03-15T10:00:00Z",
    },
    {
      stage_id: "merge-queue",
      scope_country: "DE",
      scope_as_of: "2026-03-15",
      status: "running",
      last_finished_at: null,
    },
  ];

  const summary = summarizeStageRows(rows, {
    country: "DE",
    asOf: "2026-03-15",
  });

  assert.equal(summary.length, REQUIRED_STAGE_IDS.length);
  assert.deepEqual(summary[0], {
    stageId: "stop-topology",
    status: "ready",
    lastFinishedAt: "2026-03-15T10:00:00Z",
  });
  assert.deepEqual(summary.at(-1), {
    stageId: "merge-queue",
    status: "running",
    lastFinishedAt: null,
  });
  assert.equal(
    summary.find((row) => row.stageId === "global-stations")?.status,
    "missing",
  );
});

test("deriveAuditStatus separates structural health from publish readiness", () => {
  assert.deepEqual(
    deriveAuditStatus({
      allRequiredStagesReady: true,
      clustersWithCandidateCountMismatch: 0,
      clustersWithIssueCountMismatch: 0,
      clustersWithTooFewCandidates: 0,
      clustersWithoutEligiblePairs: 0,
      eligiblePairsMissingClusterRow: 0,
      eligiblePairsMissingCandidateCoverage: 0,
      finalClustersWithoutDecision: 0,
      openClusters: 0,
      inReviewClusters: 0,
    }),
    {
      structurallyHealthy: true,
      publishReady: true,
    },
  );

  assert.deepEqual(
    deriveAuditStatus({
      allRequiredStagesReady: true,
      clustersWithCandidateCountMismatch: 0,
      clustersWithIssueCountMismatch: 0,
      clustersWithTooFewCandidates: 0,
      clustersWithoutEligiblePairs: 0,
      eligiblePairsMissingClusterRow: 0,
      eligiblePairsMissingCandidateCoverage: 0,
      finalClustersWithoutDecision: 0,
      openClusters: 2,
      inReviewClusters: 1,
    }),
    {
      structurallyHealthy: true,
      publishReady: false,
    },
  );
});

test("audit SQL checks latest published clusters against current live rows", () => {
  assert.match(AUDIT_SQL, /latestPublishedClustersMissingLiveRows/);
  assert.match(AUDIT_SQL, /FROM published_cluster_refs published/);
  assert.match(AUDIT_SQL, /eligiblePairsMissingCandidateCoverage/);
  assert.match(AUDIT_SQL, /clustersWithoutEligiblePairs/);
});
