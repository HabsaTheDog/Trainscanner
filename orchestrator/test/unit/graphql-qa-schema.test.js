"use strict";

/**
 * test/unit/graphql-qa-schema.test.js
 *
 * Verifies that the GraphQL schema correctly defines:
 *  - lowConfidenceQueue query
 *  - approveAiMatch / rejectAiMatch / overrideAiMatch mutations
 *  - LowConfidenceQueueResult, LowConfidenceItem, AiMatchDecisionResult types
 *
 * Uses the Node.js built-in test runner (no external deps).
 */

const test = require("node:test");
const assert = require("node:assert/strict");
const { graphql } = require("graphql");

const { schema } = require("../../src/graphql/schema");

test("schema builds without errors", () => {
    assert.ok(schema, "schema should be truthy");
});

test("lowConfidenceQueue query is present in schema", () => {
    const queryType = schema.getQueryType();
    assert.ok(queryType, "Should have a Query type");
    const field = queryType.getFields().lowConfidenceQueue;
    assert.ok(field, "lowConfidenceQueue field should exist on Query");
    assert.equal(field.name, "lowConfidenceQueue");
});

test("lowConfidenceQueue returns LowConfidenceQueueResult type", () => {
    const queryType = schema.getQueryType();
    const field = queryType.getFields().lowConfidenceQueue;
    // Unwrap NonNull wrapper
    const returnType = field.type.ofType || field.type;
    assert.equal(returnType.name, "LowConfidenceQueueResult");
});

test("LowConfidenceQueueResult has expected fields", () => {
    const type = schema.getType("LowConfidenceQueueResult");
    assert.ok(type, "LowConfidenceQueueResult type should exist");
    const fields = type.getFields();
    assert.ok(fields.total, "should have 'total' field");
    assert.ok(fields.items, "should have 'items' field");
});

test("LowConfidenceItem has all required fields", () => {
    const type = schema.getType("LowConfidenceItem");
    assert.ok(type, "LowConfidenceItem type should exist");
    const fields = type.getFields();
    const required = [
        "evidence_id",
        "cluster_id",
        "source_canonical_station_id",
        "target_canonical_station_id",
        "evidence_type",
        "ai_confidence",
        "ai_suggested_action",
        "cluster_display_name",
    ];
    for (const fieldName of required) {
        assert.ok(fields[fieldName], `LowConfidenceItem should have '${fieldName}' field`);
    }
});

test("approveAiMatch mutation is present", () => {
    const mutationType = schema.getMutationType();
    assert.ok(mutationType, "Should have a Mutation type");
    const field = mutationType.getFields().approveAiMatch;
    assert.ok(field, "approveAiMatch should exist on Mutation");
    const args = Object.fromEntries(field.args.map((a) => [a.name, a]));
    assert.ok(args.clusterId, "approveAiMatch should have clusterId arg");
    assert.ok(args.evidenceId, "approveAiMatch should have evidenceId arg");
});

test("rejectAiMatch mutation is present", () => {
    const mutationType = schema.getMutationType();
    const field = mutationType.getFields().rejectAiMatch;
    assert.ok(field, "rejectAiMatch should exist on Mutation");
    const args = Object.fromEntries(field.args.map((a) => [a.name, a]));
    assert.ok(args.clusterId, "rejectAiMatch should have clusterId arg");
    assert.ok(args.evidenceId, "rejectAiMatch should have evidenceId arg");
});

test("overrideAiMatch mutation is present with targetClusterId", () => {
    const mutationType = schema.getMutationType();
    const field = mutationType.getFields().overrideAiMatch;
    assert.ok(field, "overrideAiMatch should exist on Mutation");
    const args = Object.fromEntries(field.args.map((a) => [a.name, a]));
    assert.ok(args.clusterId, "overrideAiMatch should have clusterId arg");
    assert.ok(args.evidenceId, "overrideAiMatch should have evidenceId arg");
    assert.ok(args.targetClusterId, "overrideAiMatch should have targetClusterId arg");
});

test("AiMatchDecisionResult has expected fields", () => {
    const type = schema.getType("AiMatchDecisionResult");
    assert.ok(type, "AiMatchDecisionResult type should exist");
    const fields = type.getFields();
    assert.ok(fields.ok, "should have 'ok' field");
    assert.ok(fields.decision_id, "should have 'decision_id' field");
    assert.ok(fields.cluster_id, "should have 'cluster_id' field");
    assert.ok(fields.operation, "should have 'operation' field");
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
