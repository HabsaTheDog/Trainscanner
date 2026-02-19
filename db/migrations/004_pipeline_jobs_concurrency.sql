DROP INDEX IF EXISTS idx_pipeline_jobs_one_running_per_type;

CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_running_by_type
  ON pipeline_jobs (job_type, started_at DESC)
  WHERE status = 'running';
