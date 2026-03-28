const test = require("node:test");
const assert = require("node:assert/strict");

const {
  _internal: {
    parseArgs,
    normalizeReadinessCounts,
    assertPublishReadiness,
    assertPublishAuditHealthy,
    PUBLISH_BATCH_SQL,
  },
} = require("../../src/cli/publish-qa-decisions");

test("parseArgs supports unresolved override and publish metadata", () => {
  const parsed = parseArgs([
    "--country",
    "AT",
    "--as-of",
    "2026-03-15",
    "--note",
    "nightly publish",
    "--created-by",
    "alice",
    "--allow-unresolved",
    "--allow-structural-issues",
  ]);

  assert.equal(parsed.country, "AT");
  assert.equal(parsed.asOf, "2026-03-15");
  assert.equal(parsed.note, "nightly publish");
  assert.equal(parsed.createdBy, "alice");
  assert.equal(parsed.allowUnresolved, true);
  assert.equal(parsed.allowStructuralIssues, true);
});

test("normalizeReadinessCounts coerces query results to integers", () => {
  assert.deepEqual(
    normalizeReadinessCounts({
      total_clusters: "10",
      open_clusters: "2",
      in_review_clusters: "1",
      resolved_clusters: "6",
      dismissed_clusters: "1",
      unresolved_high_clusters: "1",
    }),
    {
      totalClusters: 10,
      openClusters: 2,
      inReviewClusters: 1,
      resolvedClusters: 6,
      dismissedClusters: 1,
      unresolvedHighClusters: 1,
    },
  );
});

test("assertPublishReadiness blocks unresolved clusters by default", () => {
  assert.throws(
    () =>
      assertPublishReadiness({
        openClusters: 2,
        inReviewClusters: 1,
        unresolvedHighClusters: 1,
      }),
    /Cannot publish QA decisions while unresolved merge clusters remain/,
  );
});

test("assertPublishReadiness allows explicit unresolved override", () => {
  assert.doesNotThrow(() =>
    assertPublishReadiness(
      {
        openClusters: 2,
        inReviewClusters: 1,
        unresolvedHighClusters: 1,
      },
      { allowUnresolved: true },
    ),
  );
});

test("assertPublishAuditHealthy blocks structurally unhealthy scopes by default", () => {
  assert.throws(
    () =>
      assertPublishAuditHealthy({
        status: {
          structurallyHealthy: false,
        },
        metrics: {
          allRequiredStagesReady: false,
          clustersWithCandidateCountMismatch: 2,
          clustersWithIssueCountMismatch: 1,
          clustersWithTooFewCandidates: 0,
          clustersWithoutEligiblePairs: 1,
          eligiblePairsMissingClusterRow: 3,
          eligiblePairsMissingCandidateCoverage: 4,
          finalClustersWithoutDecision: 1,
        },
      }),
    /Cannot publish QA decisions while the scope audit is structurally unhealthy/,
  );
});

test("assertPublishAuditHealthy allows explicit structural override", () => {
  assert.doesNotThrow(() =>
    assertPublishAuditHealthy(
      {
        status: {
          structurallyHealthy: false,
        },
        metrics: {},
      },
      { allowStructuralIssues: true },
    ),
  );
});

test("publish batch SQL only snapshots the latest decision per cluster", () => {
  assert.match(
    PUBLISH_BATCH_SQL,
    /SELECT DISTINCT ON \(decision\.merge_cluster_id\)/,
  );
  assert.match(PUBLISH_BATCH_SQL, /ORDER BY\s+decision\.merge_cluster_id ASC,/);
});
