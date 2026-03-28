const test = require("node:test");
const assert = require("node:assert/strict");
const { graphql } = require("graphql");

const { schema } = require("../../src/graphql/schema");

test("schema builds without errors", () => {
  assert.ok(schema, "schema should be truthy");
});

test("globalClusters and globalCluster queries are present", () => {
  const queryType = schema.getQueryType();
  assert.ok(queryType, "Should have a Query type");

  const fields = queryType.getFields();
  assert.ok(fields.globalClusters, "globalClusters should exist on Query");
  assert.ok(fields.globalCluster, "globalCluster should exist on Query");
});

test("submitGlobalMergeDecision mutation is present", () => {
  const mutationType = schema.getMutationType();
  assert.ok(mutationType, "Should have a Mutation type");

  const field = mutationType.getFields().submitGlobalMergeDecision;
  assert.ok(field, "submitGlobalMergeDecision should exist on Mutation");
  const args = Object.fromEntries(field.args.map((a) => [a.name, a]));
  assert.ok(args.clusterId, "submitGlobalMergeDecision should have clusterId");
  assert.ok(args.input, "submitGlobalMergeDecision should have input");
});

test("workspace mutations are present", () => {
  const mutationType = schema.getMutationType();
  const fields = mutationType.getFields();

  for (const fieldName of [
    "saveGlobalClusterWorkspace",
    "undoGlobalClusterWorkspace",
    "resetGlobalClusterWorkspace",
    "reopenGlobalCluster",
    "resolveGlobalCluster",
  ]) {
    assert.ok(fields[fieldName], `${fieldName} should exist on Mutation`);
  }
});

test("global merge cluster types expose global station identifiers", () => {
  const candidateType = schema.getType("GlobalClusterCandidate");
  assert.ok(candidateType, "GlobalClusterCandidate should exist");
  const fields = candidateType.getFields();
  assert.ok(
    fields.global_station_id,
    "candidate should expose global_station_id",
  );
  assert.ok(fields.display_name, "candidate should expose display_name");
  assert.ok(fields.aliases, "candidate should expose aliases");
  assert.ok(fields.coord_status, "candidate should expose coord_status");
  assert.ok(fields.network_context, "candidate should expose network_context");
  assert.ok(fields.network_summary, "candidate should expose network_summary");
  assert.ok(fields.provenance, "candidate should expose provenance");
  assert.ok(
    fields.external_reference_summary,
    "candidate should expose external_reference_summary",
  );
  assert.ok(
    fields.external_reference_matches,
    "candidate should expose external_reference_matches",
  );
  const serviceContextType = schema.getType("GlobalCandidateNetworkContext");
  assert.ok(serviceContextType, "GlobalCandidateNetworkContext should exist");
  assert.ok(
    serviceContextType.getFields().stop_points,
    "network context should expose stop_points",
  );
});

test("global evidence type exposes status and raw values", () => {
  const evidenceType = schema.getType("GlobalEvidence");
  assert.ok(evidenceType, "GlobalEvidence should exist");
  const fields = evidenceType.getFields();
  assert.ok(fields.category, "evidence should expose category");
  assert.ok(fields.is_seed_rule, "evidence should expose is_seed_rule");
  assert.ok(fields.seed_reasons, "evidence should expose seed_reasons");
  assert.ok(fields.status, "evidence should expose status");
  assert.ok(fields.raw_value, "evidence should expose raw_value");
  assert.ok(fields.details, "evidence should expose details");
});

test("cluster detail exposes workspace metadata and additive evidence summary fields", () => {
  const detailType = schema.getType("GlobalMergeClusterDetail");
  assert.ok(detailType, "GlobalMergeClusterDetail should exist");
  const fields = detailType.getFields();
  assert.ok(fields.effective_status, "detail should expose effective_status");
  assert.ok(fields.workspace_version, "detail should expose workspace_version");
  assert.ok(fields.has_workspace, "detail should expose has_workspace");
  assert.ok(fields.workspace, "detail should expose workspace JSON");
  assert.ok(fields.evidence_summary, "detail should expose evidence_summary");
  assert.ok(fields.pair_summaries, "detail should expose pair_summaries");
  assert.ok(fields.reference_overlay, "detail should expose reference_overlay");
  const pairType = schema.getType("GlobalPairSummary");
  const pairFields = pairType.getFields();
  assert.ok(pairFields.categories, "pair summary should expose categories");
  assert.ok(pairFields.seed_reasons, "pair summary should expose seed_reasons");
});

test("globalClusters query returns connection metadata", () => {
  const connectionType = schema.getType("GlobalMergeClusterConnection");
  assert.ok(connectionType, "GlobalMergeClusterConnection should exist");
  const fields = connectionType.getFields();
  assert.ok(fields.items, "connection should expose items");
  assert.ok(fields.total_count, "connection should expose total_count");
  assert.ok(fields.limit, "connection should expose limit");
});

test("cluster detail exposes workspace metadata", () => {
  const detailType = schema.getType("GlobalMergeClusterDetail");
  assert.ok(detailType, "GlobalMergeClusterDetail should exist");
  const fields = detailType.getFields();
  assert.ok(fields.effective_status, "detail should expose effective_status");
  assert.ok(fields.workspace_version, "detail should expose workspace_version");
  assert.ok(fields.has_workspace, "detail should expose has_workspace");
  assert.ok(fields.workspace, "detail should expose workspace JSON");
});

test("external reference helper types are present", () => {
  const summaryType = schema.getType("GlobalCandidateExternalReferenceSummary");
  const matchType = schema.getType("GlobalCandidateExternalReferenceMatch");
  const overlayType = schema.getType("GlobalReferencePoint");

  assert.ok(
    summaryType,
    "GlobalCandidateExternalReferenceSummary should exist",
  );
  assert.ok(matchType, "GlobalCandidateExternalReferenceMatch should exist");
  assert.ok(overlayType, "GlobalReferencePoint should exist");
  assert.ok(
    summaryType.getFields().source_counts,
    "summary should expose source_counts",
  );
  assert.ok(matchType.getFields().source_id, "match should expose source_id");
  assert.ok(
    overlayType.getFields().matched_candidate_ids,
    "overlay should expose matched_candidate_ids",
  );
});

test("health query resolves without DB connection", async () => {
  const result = await graphql({
    schema,
    source: "{ health }",
    rootValue: { health: () => "ok" },
  });
  assert.ok(!result.errors, "Should have no GraphQL errors");
  assert.equal(result.data.health, "ok");
});
