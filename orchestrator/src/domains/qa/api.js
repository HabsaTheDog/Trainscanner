const crypto = require('node:crypto');
const fs = require('node:fs/promises');
const path = require('node:path');

const { createPostgisClient } = require('../../data/postgis/client');
const { createPipelineJobsRepo } = require('../../data/postgis/repositories/pipeline-jobs-repo');
const { createJobOrchestrator } = require('../../core/job-orchestrator');
const { buildIdempotencyKey, createPipelineLogger } = require('../../core/pipeline-runner');
const { fetchSources } = require('../source-discovery/service');
const { ingestNetex } = require('../ingest/service');
const { buildCanonicalStations, buildReviewQueue } = require('../canonical/service');
const { AppError, toAppError } = require('../../core/errors');

let dbClient = null;
const refreshWorkers = new Map();

const REFRESH_JOB_TYPE = 'qa.refresh-pipeline';
const ACTIVE_REFRESH_JOB_STATUSES = new Set(['queued', 'running', 'retry_wait']);
const REFRESH_PIPELINE_STEP_COUNT = 4;
const REFRESH_PROGRESS_POLL_MS = 1500;

function isIsoDate(value) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return false;
  }
  const parsed = new Date(`${value}T00:00:00Z`);
  return Number.isFinite(parsed.getTime());
}

function normalizeRefreshScope(input) {
  const payload = input && typeof input === 'object' ? input : {};
  const countryInput = String(payload.country || '').trim().toUpperCase();
  const asOfInput = String(payload.asOf || payload.as_of || '').trim();
  const sourceIdInput = String(payload.sourceId || payload.source_id || '').trim();

  if (countryInput && !['DE', 'AT', 'CH'].includes(countryInput)) {
    throw new AppError({
      code: 'INVALID_REQUEST',
      statusCode: 400,
      message: "country must be one of 'DE', 'AT', 'CH'"
    });
  }

  if (asOfInput && !isIsoDate(asOfInput)) {
    throw new AppError({
      code: 'INVALID_REQUEST',
      statusCode: 400,
      message: 'asOf must be an ISO date in YYYY-MM-DD format'
    });
  }

  return {
    country: countryInput || '',
    asOf: asOfInput || '',
    sourceId: sourceIdInput || ''
  };
}

function appendArgIfSet(args, key, value) {
  if (!value) {
    return;
  }
  args.push(key, value);
}

function buildFetchArgs(scope) {
  const args = [];
  appendArgIfSet(args, '--as-of', scope.asOf);
  appendArgIfSet(args, '--country', scope.country);
  appendArgIfSet(args, '--source-id', scope.sourceId);
  return args;
}

function buildIngestArgs(scope) {
  const args = [];
  appendArgIfSet(args, '--as-of', scope.asOf);
  appendArgIfSet(args, '--country', scope.country);
  appendArgIfSet(args, '--source-id', scope.sourceId);
  return args;
}

function buildCanonicalArgs(scope) {
  const args = [];
  appendArgIfSet(args, '--as-of', scope.asOf);
  appendArgIfSet(args, '--country', scope.country);
  appendArgIfSet(args, '--source-id', scope.sourceId);
  return args;
}

function buildReviewQueueArgs(scope) {
  const args = [];
  appendArgIfSet(args, '--as-of', scope.asOf);
  appendArgIfSet(args, '--country', scope.country);
  return args;
}

function mapRefreshJobStatus(status) {
  if (status === 'succeeded') {
    return 'completed';
  }
  if (status === 'failed') {
    return 'failed';
  }
  return 'running';
}

function toNonNegativeInt(value) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return 0;
  }
  return parsed;
}

function normalizeDownloadProgress(input) {
  if (!input || typeof input !== 'object') {
    return null;
  }

  const stage = String(input.stage || '').trim();
  if (!stage) {
    return null;
  }

  return {
    stage,
    source_id: String(input.source_id || '').trim() || null,
    source_index: toNonNegativeInt(input.source_index),
    total_sources: toNonNegativeInt(input.total_sources),
    file_name: String(input.file_name || '').trim() || null,
    downloaded_bytes: toNonNegativeInt(input.downloaded_bytes),
    total_bytes: toNonNegativeInt(input.total_bytes),
    message: String(input.message || '').trim() || null,
    updated_at: String(input.updated_at || '').trim() || null
  };
}

async function readJsonObject(filePath) {
  try {
    const raw = await fs.readFile(filePath, 'utf8');
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') {
      return null;
    }
    return parsed;
  } catch (err) {
    if (err && err.code === 'ENOENT') {
      return null;
    }
    return null;
  }
}

function createProgressPoller(options = {}) {
  const readProgress = typeof options.readProgress === 'function' ? options.readProgress : async () => null;
  const onProgress = typeof options.onProgress === 'function' ? options.onProgress : async () => {};
  const pollMs = Number.isFinite(options.pollMs) ? Math.max(250, options.pollMs) : REFRESH_PROGRESS_POLL_MS;

  let intervalHandle = null;
  let stopped = false;
  let inFlight = false;
  let lastProgress = '';

  async function tick(force = false) {
    if ((!force && stopped) || inFlight) {
      return;
    }

    inFlight = true;
    try {
      const progress = await readProgress();
      if (!progress) {
        return;
      }

      const serialized = JSON.stringify(progress);
      if (!force && serialized === lastProgress) {
        return;
      }

      lastProgress = serialized;
      await onProgress(progress);
    } finally {
      inFlight = false;
    }
  }

  intervalHandle = setInterval(() => {
    tick(false).catch(() => {});
  }, pollMs);

  tick(false).catch(() => {});

  return async () => {
    stopped = true;
    if (intervalHandle) {
      clearInterval(intervalHandle);
      intervalHandle = null;
    }
    await tick(true).catch(() => {});
  };
}

function toRefreshJobPayload(job) {
  const checkpoint = job && job.checkpoint && typeof job.checkpoint === 'object' ? job.checkpoint : {};
  const completedSteps = Array.isArray(checkpoint.completedSteps) ? checkpoint.completedSteps : [];
  const downloadProgress = normalizeDownloadProgress(checkpoint.downloadProgress);
  const scope =
    checkpoint.scope && typeof checkpoint.scope === 'object'
      ? checkpoint.scope
      : (job?.runContext?.scope && typeof job.runContext.scope === 'object' ? job.runContext.scope : {});

  return {
    job_id: job.jobId,
    job_type: job.jobType,
    status: mapRefreshJobStatus(job.status),
    raw_status: job.status,
    step: typeof checkpoint.step === 'string' ? checkpoint.step : null,
    step_label: typeof checkpoint.stepLabel === 'string' ? checkpoint.stepLabel : null,
    progress: {
      completed_steps: completedSteps.length,
      total_steps: REFRESH_PIPELINE_STEP_COUNT
    },
    download_progress: downloadProgress,
    scope,
    checkpoint,
    attempt: job.attempt || 0,
    error_code: job.errorCode || null,
    error_message: job.errorMessage || null,
    started_at: job.startedAt || null,
    ended_at: job.endedAt || null
  };
}

async function updateRefreshCheckpoint(updateCheckpoint, state, patch) {
  const next = {
    ...state,
    ...patch,
    updatedAt: new Date().toISOString()
  };
  await updateCheckpoint(next);
  return next;
}

async function runRefreshPipelineSteps(input) {
  const rootDir = input.rootDir || process.cwd();
  const runId = String(input.runId || '').trim() || crypto.randomUUID();
  const scope = normalizeRefreshScope(input.scope || {});
  const updateCheckpoint =
    typeof input.updateCheckpoint === 'function' ? input.updateCheckpoint : async () => {};
  const fetchProgressFile = path.join(rootDir, 'state', `qa-refresh-fetch-${runId}.json`);

  await fs.rm(fetchProgressFile, { force: true }).catch(() => {});

  let checkpoint = {};
  let checkpointUpdateLock = Promise.resolve();
  async function patchCheckpoint(patch) {
    checkpointUpdateLock = checkpointUpdateLock.then(async () => {
      checkpoint = await updateRefreshCheckpoint(updateCheckpoint, checkpoint, patch);
    });
    await checkpointUpdateLock;
    return checkpoint;
  }

  await patchCheckpoint({
    status: 'running',
    step: 'starting',
    stepLabel: 'Starting pipeline',
    scope,
    completedSteps: []
  });

  const fetchArgs = buildFetchArgs(scope);
  const ingestArgs = buildIngestArgs(scope);
  const canonicalArgs = buildCanonicalArgs(scope);
  const queueArgs = buildReviewQueueArgs(scope);

  const steps = [
    {
      id: 'fetching_sources',
      label: 'Fetching DACH sources',
      run: () =>
        fetchSources({
          rootDir,
          runId: `${runId}-fetch`,
          args: fetchArgs,
          env: {
            FETCH_PROGRESS_FILE: fetchProgressFile
          }
        }),
      readProgress: async () => normalizeDownloadProgress(await readJsonObject(fetchProgressFile))
    },
    {
      id: 'ingesting_netex',
      label: 'Ingesting NeTEx snapshots',
      run: () =>
        ingestNetex({
          rootDir,
          runId: `${runId}-ingest`,
          args: ingestArgs,
          jobOrchestrationEnabled: false
        })
    },
    {
      id: 'building_canonical',
      label: 'Building canonical stations',
      run: () =>
        buildCanonicalStations({
          rootDir,
          runId: `${runId}-canonical`,
          args: canonicalArgs,
          jobOrchestrationEnabled: false
        })
    },
    {
      id: 'building_review_queue',
      label: 'Building review queue',
      run: () =>
        buildReviewQueue({
          rootDir,
          runId: `${runId}-queue`,
          args: queueArgs,
          jobOrchestrationEnabled: false
        })
    }
  ];

  for (const step of steps) {
    await patchCheckpoint({
      step: step.id,
      stepLabel: step.label,
      downloadProgress: null
    });

    let stopProgressPoller = null;
    if (typeof step.readProgress === 'function') {
      stopProgressPoller = createProgressPoller({
        pollMs: REFRESH_PROGRESS_POLL_MS,
        readProgress: step.readProgress,
        onProgress: async (progress) => {
          await patchCheckpoint({
            step: step.id,
            stepLabel: step.label,
            downloadProgress: progress
          });
        }
      });
    }

    try {
      await step.run();
    } catch (err) {
      if (stopProgressPoller) {
        await stopProgressPoller().catch(() => {});
      }
      const appErr = toAppError(err);
      await patchCheckpoint({
        status: 'failed',
        failedStep: step.id,
        errorCode: appErr.code,
        errorMessage: appErr.message
      }).catch(() => {});
      await fs.rm(fetchProgressFile, { force: true }).catch(() => {});
      throw err;
    }

    if (stopProgressPoller) {
      await stopProgressPoller().catch(() => {});
    }

    await patchCheckpoint({
      completedSteps: [...(Array.isArray(checkpoint.completedSteps) ? checkpoint.completedSteps : []), step.id]
    });
  }

  await patchCheckpoint({
    status: 'completed',
    step: 'completed',
    stepLabel: 'Pipeline completed',
    completedAt: new Date().toISOString(),
    downloadProgress: null
  });
  await fs.rm(fetchProgressFile, { force: true }).catch(() => {});

  return {
    ok: true,
    runId,
    scope
  };
}

async function getDbClient() {
  if (!dbClient) {
    dbClient = createPostgisClient();
    await dbClient.ensureReady();
  }
  return dbClient;
}

async function getReviewQueue(url) {
  const client = await getDbClient();
  const country = (url.searchParams.get('country') || '').trim().toUpperCase();

  // Validate country is strictly a 2-letter code to prevent injection
  const validCountry = /^[A-Z]{2}$/.test(country) ? country : '';

  const countryFilter = validCountry
    ? `AND q.country = '${validCountry}'`
    : '';

  const sql = `
    SELECT
      q.review_item_id,
      q.issue_key,
      q.country,
      q.canonical_station_id,
      q.issue_type,
      q.severity,
      q.status,
      q.details,
      q.created_at,
      CASE 
        WHEN q.canonical_station_id IS NOT NULL THEN (
          SELECT json_agg(json_build_object(
            'source_id', s.source_id,
            'source_stop_id', s.source_stop_id,
            'stop_name', s.stop_name,
            'latitude', s.latitude,
            'longitude', s.longitude
          ))
          FROM canonical_station_sources css
          JOIN netex_stops_staging s ON s.source_id = css.source_id AND s.source_stop_id = css.source_stop_id AND s.snapshot_date = css.snapshot_date
          WHERE css.canonical_station_id = q.canonical_station_id
        )
        ELSE NULL
      END as members,
      CASE
        WHEN q.details ? 'canonicalStationIds' THEN (
          SELECT json_agg(json_build_object(
            'canonical_station_id', cs.canonical_station_id,
            'canonical_name', cs.canonical_name,
            'latitude', cs.latitude,
            'longitude', cs.longitude
          ))
          FROM canonical_stations cs
          WHERE cs.canonical_station_id IN (
            SELECT value FROM jsonb_array_elements_text(q.details->'canonicalStationIds') AS value
          )
        )
        ELSE NULL
      END as related_stations
    FROM canonical_review_queue q
    WHERE q.status IN ('open', 'confirmed')
      ${countryFilter}
    ORDER BY
      CASE q.severity WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
      q.last_detected_at DESC,
      q.review_item_id DESC
    LIMIT 50
  `;

  const rows = await client.queryRows(sql);
  return rows;
}

async function findActiveRefreshJob(jobsRepo) {
  const jobs = await jobsRepo.listRecentByType(REFRESH_JOB_TYPE, 50);
  const active = jobs.filter((job) => ACTIVE_REFRESH_JOB_STATUSES.has(job.status));
  if (active.length === 0) {
    return null;
  }

  // Prefer exposing true in-flight work over queued placeholders.
  const priority = {
    running: 0,
    retry_wait: 1,
    queued: 2
  };

  active.sort((a, b) => {
    const pa = Number.isFinite(priority[a.status]) ? priority[a.status] : 99;
    const pb = Number.isFinite(priority[b.status]) ? priority[b.status] : 99;
    if (pa !== pb) {
      return pa - pb;
    }
    return String(b.createdAt || '').localeCompare(String(a.createdAt || ''));
  });

  return active[0];
}

function startRefreshWorker(options = {}) {
  const rootDir = options.rootDir || process.cwd();
  const job = options.job;
  const jobsRepo = options.jobsRepo;

  if (!job || !job.jobId || !jobsRepo) {
    return null;
  }

  const existing = refreshWorkers.get(job.jobId);
  if (existing) {
    return existing;
  }

  const logger = options.logger || createPipelineLogger(rootDir, REFRESH_JOB_TYPE, job.jobId);
  const jobOrchestrator = createJobOrchestrator({
    jobsRepo,
    logger
  });
  const scope = normalizeRefreshScope(job?.runContext?.scope || {});

  const worker = jobOrchestrator
    .runJob({
      jobId: job.jobId,
      jobType: REFRESH_JOB_TYPE,
      idempotencyKey: job.idempotencyKey,
      runContext: {
        ...(job.runContext && typeof job.runContext === 'object' ? job.runContext : {}),
        scope
      },
      maxAttempts: Number.parseInt(process.env.PIPELINE_JOB_MAX_ATTEMPTS || '3', 10),
      maxConcurrent: 1,
      execute: async ({ updateCheckpoint }) =>
        runRefreshPipelineSteps({
          rootDir,
          runId: job.jobId,
          scope,
          updateCheckpoint
        })
    })
    .catch((err) => {
      const appErr = toAppError(err);
      logger.error('refresh pipeline worker failed', {
        jobId: job.jobId,
        errorCode: appErr.code,
        error: appErr.message
      });
    })
    .finally(() => {
      refreshWorkers.delete(job.jobId);
    });

  refreshWorkers.set(job.jobId, worker);
  return worker;
}

async function postRefreshJob(body, options = {}) {
  const rootDir = options.rootDir || process.cwd();
  const scope = normalizeRefreshScope(body || {});
  const client = await getDbClient();
  const jobsRepo = createPipelineJobsRepo(client);

  const active = await findActiveRefreshJob(jobsRepo);
  if (active) {
    const payload = toRefreshJobPayload(active);
    if (active.status === 'queued' || active.status === 'retry_wait') {
      const runningCount = await jobsRepo.countRunningByType(REFRESH_JOB_TYPE).catch(() => 1);
      if (runningCount === 0) {
        startRefreshWorker({
          rootDir,
          job: active,
          jobsRepo
        });
      }
    }
    return {
      accepted: true,
      reused: true,
      in_flight: true,
      job_id: payload.job_id,
      job: payload
    };
  }

  const jobId = crypto.randomUUID();
  const idempotencyKey = buildIdempotencyKey(REFRESH_JOB_TYPE, [
    jobId,
    scope.country,
    scope.asOf,
    scope.sourceId
  ]);
  const queuedJob = await jobsRepo.createQueuedJob({
    jobId,
    jobType: REFRESH_JOB_TYPE,
    idempotencyKey,
    runContext: {
      scope,
      requestedAt: new Date().toISOString(),
      source: 'qa.refresh'
    },
    checkpoint: {
      status: 'queued',
      scope,
      step: 'queued',
      stepLabel: 'Queued',
      completedSteps: [],
      updatedAt: new Date().toISOString()
    }
  });

  startRefreshWorker({
    rootDir,
    job: queuedJob,
    jobsRepo
  });

  const payload = toRefreshJobPayload(queuedJob);
  return {
    accepted: true,
    reused: false,
    in_flight: true,
    job_id: payload.job_id,
    job: payload
  };
}

async function getRefreshJob(jobId, options = {}) {
  const rootDir = options.rootDir || process.cwd();
  const cleanJobId = String(jobId || '').trim();
  if (!cleanJobId) {
    throw new AppError({
      code: 'INVALID_REQUEST',
      statusCode: 400,
      message: 'job_id is required'
    });
  }

  const client = await getDbClient();
  const jobsRepo = createPipelineJobsRepo(client);

  let job;
  try {
    job = await jobsRepo.getById(cleanJobId);
  } catch (err) {
    if (String(err.message || '').toLowerCase().includes('invalid input syntax for type uuid')) {
      throw new AppError({
        code: 'INVALID_REQUEST',
        statusCode: 400,
        message: 'job_id must be a valid UUID'
      });
    }
    throw err;
  }

  if (!job || job.jobType !== REFRESH_JOB_TYPE) {
    throw new AppError({
      code: 'INVALID_REQUEST',
      statusCode: 404,
      message: 'Refresh pipeline job not found'
    });
  }

  if ((job.status === 'queued' || job.status === 'retry_wait') && !refreshWorkers.has(job.jobId)) {
    const runningCount = await jobsRepo.countRunningByType(REFRESH_JOB_TYPE).catch(() => 1);
    if (runningCount === 0) {
      startRefreshWorker({
        rootDir,
        job,
        jobsRepo
      });
    }
  }

  return toRefreshJobPayload(job);
}

async function postOverride(body) {
  const client = await getDbClient();
  const { review_item_id, operation, new_canonical_name, operation_payload } = body;

  if (!review_item_id || !operation) {
    throw new AppError({
      code: 'INVALID_REQUEST',
      statusCode: 400,
      message: 'review_item_id and operation are required'
    });
  }

  const queueItem = await client.queryOne(
    `SELECT * FROM canonical_review_queue WHERE review_item_id = :'review_item_id'::bigint`,
    { review_item_id }
  );

  if (!queueItem) {
    throw new AppError({
      code: 'NOT_FOUND',
      statusCode: 404,
      message: 'Review item not found'
    });
  }

  const country = queueItem.country;

  await client.withTransaction(async (tx) => {
    if (operation === 'keep_separate') {
      // Dismiss the issue
      tx.add(`
        UPDATE canonical_review_queue 
        SET status = 'dismissed', resolved_at = now(), resolved_by = :'user', resolution_note = 'Dismissed via curation tool'
        WHERE review_item_id = :'review_item_id'::bigint
      `, {
        review_item_id,
        user: 'curation_tool'
      });
    } else if (operation === 'merge') {
      const source_id = operation_payload?.source_canonical_station_id;
      const target_id = operation_payload?.target_canonical_station_id || queueItem.canonical_station_id;

      if (source_id && target_id && source_id !== target_id) {
        tx.add(`
          INSERT INTO canonical_station_overrides (
            operation, status, country, source_canonical_station_id, target_canonical_station_id, created_via, requested_by
          ) VALUES (
            'merge', 'approved', :'country', :'source_id', :'target_id', 'script', :'user'
          )
        `, {
          country,
          source_id,
          target_id,
          user: 'curation_tool'
        });
      }

      tx.add(`
        UPDATE canonical_review_queue 
        SET status = 'resolved', resolved_at = now(), resolved_by = :'user', resolution_note = 'Merged via curation tool'
        WHERE review_item_id = :'review_item_id'::bigint
      `, {
        review_item_id,
        user: 'curation_tool'
      });
    } else if (operation === 'rename') {
      const target_id = queueItem.canonical_station_id || operation_payload?.target_canonical_station_id;

      tx.add(`
        INSERT INTO canonical_station_overrides (
          operation, status, country, target_canonical_station_id, new_canonical_name, created_via, requested_by
        ) VALUES (
          'rename', 'approved', :'country', :'target_id', :'new_name', 'script', :'user'
        )
      `, {
        country,
        target_id,
        new_name: new_canonical_name,
        user: 'curation_tool'
      });

      tx.add(`
        UPDATE canonical_review_queue 
        SET status = 'resolved', resolved_at = now(), resolved_by = :'user', resolution_note = 'Renamed via curation tool'
        WHERE review_item_id = :'review_item_id'::bigint
      `, {
        review_item_id,
        user: 'curation_tool'
      });
    } else {
      throw new AppError({
        code: 'INVALID_REQUEST',
        statusCode: 400,
        message: 'Invalid operation'
      });
    }
  });

  return { ok: true };
}

module.exports = {
  getReviewQueue,
  postRefreshJob,
  getRefreshJob,
  postOverride
};
