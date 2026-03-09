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

test("health query resolves without DB connection", async () => {
  const result = await graphql({
    schema,
    source: "{ health }",
    rootValue: { health: () => "ok" },
  });
  assert.ok(!result.errors, "Should have no GraphQL errors");
  assert.equal(result.data.health, "ok");
});
