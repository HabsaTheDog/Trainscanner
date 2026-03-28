const ENSURE_QA_PUBLISH_BATCH_DECISION_COLUMNS_SQL = [
  `
  ALTER TABLE IF EXISTS qa_publish_batch_decisions
    ADD COLUMN IF NOT EXISTS cluster_key_snapshot text
  `,
  `
  ALTER TABLE IF EXISTS qa_publish_batch_decisions
    ALTER COLUMN cluster_key_snapshot SET DEFAULT ''
  `,
  `
  ALTER TABLE IF EXISTS qa_publish_batch_decisions
    DROP CONSTRAINT IF EXISTS qa_publish_batch_decisions_decision_id_fkey
  `,
  `
  ALTER TABLE IF EXISTS qa_publish_batch_decisions
    DROP CONSTRAINT IF EXISTS qa_publish_batch_decisions_merge_cluster_id_fkey
  `,
];

async function ensureQaPublishBatchDecisionColumns(client) {
  for (const sql of ENSURE_QA_PUBLISH_BATCH_DECISION_COLUMNS_SQL) {
    await client.runSql(sql);
  }
}

module.exports = {
  ENSURE_QA_PUBLISH_BATCH_DECISION_COLUMNS_SQL,
  ensureQaPublishBatchDecisionColumns,
};
