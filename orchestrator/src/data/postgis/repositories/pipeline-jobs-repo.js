const { validateOrThrow } = require('../../../core/schema');

const JOB_STATUS = ['queued', 'running', 'retry_wait', 'succeeded', 'failed'];

const JOB_SCHEMA = {
  type: 'object',
  required: ['jobId', 'jobType', 'idempotencyKey', 'status', 'attempt'],
  properties: {
    jobId: { type: 'string', minLength: 1 },
    jobType: { type: 'string', minLength: 1 },
    idempotencyKey: { type: 'string', minLength: 1 },
    status: { type: 'string', enum: JOB_STATUS },
    attempt: { type: 'integer', minimum: 0 },
    startedAt: { type: 'string' },
    endedAt: { type: 'string' },
    errorCode: { type: 'string' },
    errorMessage: { type: 'string' },
    runContext: { type: 'object' },
    checkpoint: { type: 'object' },
    resultContext: { type: 'object' }
  },
  additionalProperties: true
};

function normalizeJobRow(row) {
  if (!row) {
    return null;
  }

  const out = {
    jobId: row.job_id || row.jobid || row.jobId,
    jobType: row.job_type || row.jobtype || row.jobType,
    idempotencyKey: row.idempotency_key || row.idempotencykey || row.idempotencyKey,
    status: row.status,
    attempt: Number.parseInt(String(row.attempt || 0), 10) || 0,
    startedAt: row.started_at || row.startedat || row.startedAt || null,
    endedAt: row.ended_at || row.endedat || row.endedAt || null,
    errorCode: row.error_code || row.errorcode || row.errorCode || null,
    errorMessage: row.error_message || row.errormessage || row.errorMessage || null,
    runContext: row.run_context && typeof row.run_context === 'object' ? row.run_context : {},
    checkpoint: row.checkpoint && typeof row.checkpoint === 'object' ? row.checkpoint : {},
    resultContext: row.result_context && typeof row.result_context === 'object' ? row.result_context : {}
  };

  validateOrThrow(out, JOB_SCHEMA, {
    code: 'INVALID_CONFIG',
    message: 'Invalid pipeline job row returned from repository'
  });

  return out;
}

const RETURNING_SQL = `
  RETURNING
    job_id::text,
    job_type,
    idempotency_key,
    status,
    attempt,
    to_char(started_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS started_at,
    to_char(ended_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS ended_at,
    error_code,
    error_message,
    run_context,
    checkpoint,
    result_context
`;

function createPipelineJobsRepo(client) {
  return {
    async getByIdempotency(jobType, idempotencyKey) {
      const row = await client.queryOne(
        `
          SELECT
            job_id::text,
            job_type,
            idempotency_key,
            status,
            attempt,
            to_char(started_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS started_at,
            to_char(ended_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS ended_at,
            error_code,
            error_message,
            run_context,
            checkpoint,
            result_context
          FROM pipeline_jobs
          WHERE job_type = :'job_type'
            AND idempotency_key = :'idempotency_key'
          LIMIT 1;
        `,
        {
          job_type: jobType,
          idempotency_key: idempotencyKey
        }
      );
      return normalizeJobRow(row);
    },

    async getById(jobId) {
      const row = await client.queryOne(
        `
          SELECT
            job_id::text,
            job_type,
            idempotency_key,
            status,
            attempt,
            to_char(started_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS started_at,
            to_char(ended_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS ended_at,
            error_code,
            error_message,
            run_context,
            checkpoint,
            result_context
          FROM pipeline_jobs
          WHERE job_id = :'job_id'::uuid
          LIMIT 1;
        `,
        {
          job_id: jobId
        }
      );
      return normalizeJobRow(row);
    },

    async createQueuedJob(input) {
      const row = await client.queryOne(
        `
          INSERT INTO pipeline_jobs (
            job_id,
            job_type,
            idempotency_key,
            status,
            attempt,
            run_context,
            checkpoint,
            result_context
          )
          VALUES (
            :'job_id'::uuid,
            :'job_type',
            :'idempotency_key',
            'queued',
            0,
            COALESCE(NULLIF(:'run_context', '')::jsonb, '{}'::jsonb),
            COALESCE(NULLIF(:'checkpoint', '')::jsonb, '{}'::jsonb),
            '{}'::jsonb
          )
          ${RETURNING_SQL};
        `,
        {
          job_id: input.jobId,
          job_type: input.jobType,
          idempotency_key: input.idempotencyKey,
          run_context: JSON.stringify(input.runContext || {}),
          checkpoint: JSON.stringify(input.checkpoint || {})
        }
      );

      return normalizeJobRow(row);
    },

    async markRunning(input) {
      const row = await client.queryOne(
        `
          UPDATE pipeline_jobs
          SET
            status = 'running',
            attempt = :'attempt'::integer,
            started_at = now(),
            ended_at = NULL,
            error_code = NULL,
            error_message = NULL,
            updated_at = now()
          WHERE job_id = :'job_id'::uuid
          ${RETURNING_SQL};
        `,
        {
          job_id: input.jobId,
          attempt: String(input.attempt)
        }
      );

      return normalizeJobRow(row);
    },

    async claimRunning(input) {
      const maxConcurrent = Number.isFinite(input.maxConcurrent) ? Math.max(1, input.maxConcurrent) : 1;
      const row = await client.queryOne(
        `
          WITH lock_guard AS (
            SELECT pg_advisory_xact_lock(hashtext(:'job_type')) AS locked
          ),
          running AS (
            SELECT COUNT(*)::integer AS running_count
            FROM pipeline_jobs
            CROSS JOIN lock_guard
            WHERE job_type = :'job_type'
              AND status = 'running'
              AND job_id <> :'job_id'::uuid
          )
          UPDATE pipeline_jobs j
          SET
            status = 'running',
            attempt = :'attempt'::integer,
            started_at = now(),
            ended_at = NULL,
            error_code = NULL,
            error_message = NULL,
            updated_at = now()
          FROM running r
          WHERE j.job_id = :'job_id'::uuid
            AND j.status IN ('queued', 'retry_wait')
            AND r.running_count < :'max_concurrent'::integer
          ${RETURNING_SQL};
        `,
        {
          job_id: input.jobId,
          job_type: input.jobType,
          attempt: String(input.attempt),
          max_concurrent: String(maxConcurrent)
        }
      );

      return normalizeJobRow(row);
    },

    async markRetryWait(input) {
      const row = await client.queryOne(
        `
          UPDATE pipeline_jobs
          SET
            status = 'retry_wait',
            error_code = NULLIF(:'error_code', ''),
            error_message = NULLIF(:'error_message', ''),
            updated_at = now()
          WHERE job_id = :'job_id'::uuid
          ${RETURNING_SQL};
        `,
        {
          job_id: input.jobId,
          error_code: input.errorCode || '',
          error_message: input.errorMessage || ''
        }
      );

      return normalizeJobRow(row);
    },

    async markSucceeded(input) {
      const row = await client.queryOne(
        `
          UPDATE pipeline_jobs
          SET
            status = 'succeeded',
            ended_at = now(),
            error_code = NULL,
            error_message = NULL,
            result_context = COALESCE(NULLIF(:'result_context', '')::jsonb, '{}'::jsonb),
            updated_at = now()
          WHERE job_id = :'job_id'::uuid
          ${RETURNING_SQL};
        `,
        {
          job_id: input.jobId,
          result_context: JSON.stringify(input.resultContext || {})
        }
      );

      return normalizeJobRow(row);
    },

    async markFailed(input) {
      const row = await client.queryOne(
        `
          UPDATE pipeline_jobs
          SET
            status = 'failed',
            ended_at = now(),
            error_code = NULLIF(:'error_code', ''),
            error_message = NULLIF(:'error_message', ''),
            updated_at = now()
          WHERE job_id = :'job_id'::uuid
          ${RETURNING_SQL};
        `,
        {
          job_id: input.jobId,
          error_code: input.errorCode || '',
          error_message: input.errorMessage || ''
        }
      );

      return normalizeJobRow(row);
    },

    async updateCheckpoint(input) {
      const row = await client.queryOne(
        `
          UPDATE pipeline_jobs
          SET
            checkpoint = COALESCE(NULLIF(:'checkpoint', '')::jsonb, '{}'::jsonb),
            updated_at = now()
          WHERE job_id = :'job_id'::uuid
          ${RETURNING_SQL};
        `,
        {
          job_id: input.jobId,
          checkpoint: JSON.stringify(input.checkpoint || {})
        }
      );

      return normalizeJobRow(row);
    },

    async countRunningByType(jobType) {
      const row = await client.queryOne(
        `
          SELECT COUNT(*)::integer AS running_count
          FROM pipeline_jobs
          WHERE job_type = :'job_type'
            AND status = 'running';
        `,
        { job_type: jobType }
      );
      return Number.parseInt(String(row?.running_count || 0), 10) || 0;
    },

    async listRecentByType(jobType, limit = 20) {
      const rows = await client.queryRows(
        `
          SELECT
            job_id::text,
            job_type,
            idempotency_key,
            status,
            attempt,
            to_char(started_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS started_at,
            to_char(ended_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS ended_at,
            error_code,
            error_message,
            run_context,
            checkpoint,
            result_context
          FROM pipeline_jobs
          WHERE (NULLIF(:'job_type', '') IS NULL OR job_type = NULLIF(:'job_type', ''))
          ORDER BY created_at DESC
          LIMIT NULLIF(:'limit_rows', '')::integer;
        `,
        {
          job_type: jobType || '',
          limit_rows: String(limit)
        }
      );

      return rows.map((row) => normalizeJobRow(row));
    }
  };
}

module.exports = {
  JOB_STATUS,
  createPipelineJobsRepo,
  normalizeJobRow
};
