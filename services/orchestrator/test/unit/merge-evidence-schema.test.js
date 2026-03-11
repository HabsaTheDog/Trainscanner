const test = require("node:test");
const assert = require("node:assert/strict");

const {
  ENSURE_MERGE_CLUSTER_EVIDENCE_COLUMNS_SQL,
  ensureMergeClusterEvidenceColumns,
} = require("../../src/data/postgis/repositories/merge-evidence-schema");

test("ensureMergeClusterEvidenceColumns adds compatibility columns", async () => {
  const statements = [];
  const client = {
    async runSql(sql) {
      statements.push(String(sql).trim().replaceAll(/\s+/g, " "));
      return { rows: [] };
    },
  };

  await ensureMergeClusterEvidenceColumns(client);

  assert.equal(
    statements.length,
    ENSURE_MERGE_CLUSTER_EVIDENCE_COLUMNS_SQL.length,
  );
  assert.match(
    statements[0],
    /ALTER TABLE IF EXISTS qa_merge_cluster_evidence/,
  );
  assert.match(statements[0], /ADD COLUMN IF NOT EXISTS status text/);
  assert.match(statements[1], /ADD COLUMN IF NOT EXISTS raw_value numeric/);
});
