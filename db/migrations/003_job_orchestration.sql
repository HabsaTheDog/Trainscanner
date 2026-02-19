CREATE TABLE IF NOT EXISTS pipeline_jobs (
  job_id uuid PRIMARY KEY,
  job_type text NOT NULL,
  idempotency_key text NOT NULL,
  status text NOT NULL CHECK (status IN ('queued', 'running', 'retry_wait', 'succeeded', 'failed')),
  attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
  started_at timestamptz,
  ended_at timestamptz,
  error_code text,
  error_message text,
  run_context jsonb NOT NULL DEFAULT '{}'::jsonb,
  checkpoint jsonb NOT NULL DEFAULT '{}'::jsonb,
  result_context jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (job_type, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_type_status_created
  ON pipeline_jobs (job_type, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_status_updated
  ON pipeline_jobs (status, updated_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_jobs_one_running_per_type
  ON pipeline_jobs (job_type)
  WHERE status = 'running';
