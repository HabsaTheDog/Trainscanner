const ENSURE_MERGE_CLUSTER_EVIDENCE_COLUMNS_SQL = [
  `
  ALTER TABLE IF EXISTS qa_merge_cluster_evidence
    ADD COLUMN IF NOT EXISTS status text
  `,
  `
  ALTER TABLE IF EXISTS qa_merge_cluster_evidence
    ADD COLUMN IF NOT EXISTS raw_value numeric(12,4)
  `,
];

async function ensureMergeClusterEvidenceColumns(client) {
  for (const sql of ENSURE_MERGE_CLUSTER_EVIDENCE_COLUMNS_SQL) {
    await client.runSql(sql);
  }
}

module.exports = {
  ENSURE_MERGE_CLUSTER_EVIDENCE_COLUMNS_SQL,
  ensureMergeClusterEvidenceColumns,
};
