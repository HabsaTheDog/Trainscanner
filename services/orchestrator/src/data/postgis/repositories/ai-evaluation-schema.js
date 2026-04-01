const ENSURE_AI_EVALUATION_SCHEMA_SQL = [
  `
  DO $$
  BEGIN
    IF NOT EXISTS (
      SELECT 1 FROM pg_type WHERE typname = 'ai_eval_run_status'
    ) THEN
      CREATE TYPE ai_eval_run_status AS ENUM (
        'queued',
        'running',
        'succeeded',
        'failed',
        'cancelled'
      );
    END IF;
  END $$;
  `,
  `
  DO $$
  BEGIN
    IF NOT EXISTS (
      SELECT 1 FROM pg_type WHERE typname = 'ai_eval_run_mode'
    ) THEN
      CREATE TYPE ai_eval_run_mode AS ENUM (
        'preview',
        'benchmark'
      );
    END IF;
  END $$;
  `,
  `
  DO $$
  BEGIN
    IF NOT EXISTS (
      SELECT 1 FROM pg_type WHERE typname = 'ai_eval_dataset_source'
    ) THEN
      CREATE TYPE ai_eval_dataset_source AS ENUM (
        'resolved_history',
        'gold_set',
        'combined'
      );
    END IF;
  END $$;
  `,
  `
  CREATE TABLE IF NOT EXISTS ai_eval_configs (
    config_id bigserial PRIMARY KEY,
    config_key text NOT NULL,
    version integer NOT NULL,
    name text NOT NULL,
    description text NOT NULL DEFAULT '',
    provider text NOT NULL DEFAULT 'litellm',
    model text NOT NULL,
    model_params jsonb NOT NULL DEFAULT '{}'::jsonb,
    system_prompt text NOT NULL,
    context_sections jsonb NOT NULL DEFAULT '[]'::jsonb,
    context_preamble text NOT NULL DEFAULT '',
    created_by text NOT NULL DEFAULT 'qa_operator',
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (config_key, version)
  );
  `,
  `
  CREATE INDEX IF NOT EXISTS idx_ai_eval_configs_key_version
    ON ai_eval_configs (config_key, version DESC);
  `,
  `
  CREATE TABLE IF NOT EXISTS ai_eval_gold_sets (
    gold_set_id bigserial PRIMARY KEY,
    slug text NOT NULL UNIQUE,
    name text NOT NULL,
    description text NOT NULL DEFAULT '',
    is_frozen boolean NOT NULL DEFAULT false,
    created_by text NOT NULL DEFAULT 'qa_operator',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
  );
  `,
  `
  CREATE TABLE IF NOT EXISTS ai_eval_gold_set_items (
    gold_set_id bigint NOT NULL REFERENCES ai_eval_gold_sets(gold_set_id) ON DELETE CASCADE,
    merge_cluster_id text NOT NULL REFERENCES qa_merge_clusters(merge_cluster_id) ON DELETE CASCADE,
    note text NOT NULL DEFAULT '',
    truth_snapshot jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (gold_set_id, merge_cluster_id)
  );
  `,
  `
  CREATE INDEX IF NOT EXISTS idx_ai_eval_gold_set_items_cluster
    ON ai_eval_gold_set_items (merge_cluster_id);
  `,
  `
  CREATE TABLE IF NOT EXISTS ai_eval_runs (
    run_id uuid PRIMARY KEY,
    mode ai_eval_run_mode NOT NULL,
    status ai_eval_run_status NOT NULL,
    dataset_source ai_eval_dataset_source,
    gold_set_id bigint REFERENCES ai_eval_gold_sets(gold_set_id) ON DELETE SET NULL,
    config_id bigint REFERENCES ai_eval_configs(config_id) ON DELETE SET NULL,
    config_snapshot jsonb NOT NULL,
    filters jsonb NOT NULL DEFAULT '{}'::jsonb,
    summary_metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
    progress jsonb NOT NULL DEFAULT '{}'::jsonb,
    requested_by text NOT NULL DEFAULT 'qa_operator',
    error_message text,
    temporal_workflow_id text,
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    ended_at timestamptz
  );
  `,
  `
  CREATE INDEX IF NOT EXISTS idx_ai_eval_runs_created
    ON ai_eval_runs (created_at DESC);
  `,
  `
  CREATE INDEX IF NOT EXISTS idx_ai_eval_runs_status
    ON ai_eval_runs (status, created_at DESC);
  `,
  `
  CREATE TABLE IF NOT EXISTS ai_eval_run_items (
    run_item_id bigserial PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES ai_eval_runs(run_id) ON DELETE CASCADE,
    merge_cluster_id text NOT NULL REFERENCES qa_merge_clusters(merge_cluster_id) ON DELETE CASCADE,
    item_status text NOT NULL DEFAULT 'queued',
    truth_snapshot jsonb NOT NULL,
    input_context_snapshot jsonb NOT NULL,
    prompt_snapshot jsonb NOT NULL,
    raw_model_response jsonb,
    normalized_prediction jsonb,
    comparison jsonb NOT NULL DEFAULT '{}'::jsonb,
    token_usage jsonb NOT NULL DEFAULT '{}'::jsonb,
    estimated_cost_usd numeric(12,6),
    latency_ms integer,
    error_message text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
  );
  `,
  `
  CREATE INDEX IF NOT EXISTS idx_ai_eval_run_items_run
    ON ai_eval_run_items (run_id, run_item_id ASC);
  `,
  `
  CREATE INDEX IF NOT EXISTS idx_ai_eval_run_items_cluster
    ON ai_eval_run_items (merge_cluster_id);
  `,
];

async function ensureAiEvaluationSchema(client) {
  for (const sql of ENSURE_AI_EVALUATION_SCHEMA_SQL) {
    await client.runSql(sql);
  }
}

module.exports = {
  ENSURE_AI_EVALUATION_SCHEMA_SQL,
  ensureAiEvaluationSchema,
};
