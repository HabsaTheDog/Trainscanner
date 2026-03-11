const test = require("node:test");
const assert = require("node:assert/strict");

const {
  _internal: {
    buildWorkspaceMutationResponse,
    normalizeEvidenceRow,
    resolveUpdatedBy,
  },
} = require("../../src/domains/qa/api");

test("normalizeEvidenceRow coerces metrics and preserves safe details", () => {
  const normalized = normalizeEvidenceRow({
    evidence_type: "geographic_distance",
    status: "",
    raw_value: "123.4",
    score: "0.75",
    details: {
      distance_meters: 123.4,
      distance_status: "nearby",
    },
  });

  assert.equal(normalized.status, "informational");
  assert.equal(normalized.raw_value, 123.4);
  assert.equal(normalized.score, 0.75);
  assert.deepEqual(normalized.details, {
    distance_meters: 123.4,
    distance_status: "nearby",
  });
  assert.equal(normalized.category, "core_match");
});

test("resolveUpdatedBy falls back to qa_operator and trims aliases", () => {
  assert.equal(resolveUpdatedBy(undefined), "qa_operator");
  assert.equal(resolveUpdatedBy({ updated_by: "  alice  " }), "alice");
  assert.equal(resolveUpdatedBy({ updatedBy: "bob" }), "bob");
});

test("buildWorkspaceMutationResponse shapes stable mutation payloads", () => {
  assert.deepEqual(
    buildWorkspaceMutationResponse("cluster-1", {
      workspaceVersion: 4,
      effectiveStatus: "in_review",
      workspace: { merges: [] },
    }),
    {
      ok: true,
      cluster_id: "cluster-1",
      workspace_version: 4,
      effective_status: "in_review",
      workspace: { merges: [] },
    },
  );
});
