const { AppError } = require('../../core/errors');
const { buildIdempotencyKey, createPipelineLogger } = require('../../core/pipeline-runner');
const { createPostgisClient } = require('../../data/postgis/client');
const { createPipelineJobsRepo } = require('../../data/postgis/repositories/pipeline-jobs-repo');
const { createReviewQueueRepo } = require('../../data/postgis/repositories/review-queue-repo');
const { createJobOrchestrator } = require('../../core/job-orchestrator');

function isIsoDate(value) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return false;
  }
  const parsed = new Date(`${value}T00:00:00Z`);
  return Number.isFinite(parsed.getTime());
}

function printReportReviewQueueUsage() {
  process.stdout.write('Usage: scripts/data/report-review-queue.sh [options]\n');
  process.stdout.write('\n');
  process.stdout.write('Report canonical review queue coverage and issue snapshots.\n');
  process.stdout.write('\n');
  process.stdout.write('Options:\n');
  process.stdout.write('  --country DE|AT|CH   Restrict report to one country\n');
  process.stdout.write('  --as-of YYYY-MM-DD   Report queue entries generated for this as-of scope tag\n');
  process.stdout.write('  --all-scopes         Report all scope tags (instead of latest/as-of tag)\n');
  process.stdout.write('  --limit N            Number of detailed rows to include (default: 20)\n');
  process.stdout.write('  -h, --help           Show this help\n');
}

function parseReportReviewQueueArgs(args = []) {
  const parsed = {
    helpRequested: false,
    country: '',
    asOf: '',
    allScopes: false,
    limitRows: 20
  };
  const tokens = Array.isArray(args) ? args : [];

  for (let i = 0; i < tokens.length; i += 1) {
    const token = String(tokens[i] || '');

    if (token === '-h' || token === '--help') {
      parsed.helpRequested = true;
      continue;
    }

    if (token === '--country') {
      const value = String(tokens[i + 1] || '').trim().toUpperCase();
      if (!value) {
        throw new AppError({
          code: 'INVALID_REQUEST',
          message: 'Missing value for --country'
        });
      }
      if (!['DE', 'AT', 'CH'].includes(value)) {
        throw new AppError({
          code: 'INVALID_REQUEST',
          message: "Invalid --country value (expected 'DE', 'AT', or 'CH')"
        });
      }
      parsed.country = value;
      i += 1;
      continue;
    }

    if (token === '--as-of') {
      const value = String(tokens[i + 1] || '').trim();
      if (!value) {
        throw new AppError({
          code: 'INVALID_REQUEST',
          message: 'Missing value for --as-of'
        });
      }
      if (!isIsoDate(value)) {
        throw new AppError({
          code: 'INVALID_REQUEST',
          message: 'Invalid --as-of value (expected YYYY-MM-DD)'
        });
      }
      parsed.asOf = value;
      i += 1;
      continue;
    }

    if (token === '--all-scopes') {
      parsed.allScopes = true;
      continue;
    }

    if (token === '--limit') {
      const value = String(tokens[i + 1] || '').trim();
      if (!value) {
        throw new AppError({
          code: 'INVALID_REQUEST',
          message: 'Missing value for --limit'
        });
      }
      const parsedLimit = Number.parseInt(value, 10);
      if (!Number.isFinite(parsedLimit) || parsedLimit <= 0) {
        throw new AppError({
          code: 'INVALID_REQUEST',
          message: '--limit must be a positive integer'
        });
      }
      parsed.limitRows = parsedLimit;
      i += 1;
      continue;
    }

    throw new AppError({
      code: 'INVALID_REQUEST',
      message: `Unknown argument: ${token}`
    });
  }

  return parsed;
}

function createQaService(deps = {}) {
  const createClient = deps.createPostgisClient || createPostgisClient;
  const createJobsRepo = deps.createPipelineJobsRepo || createPipelineJobsRepo;
  const createOrchestrator = deps.createJobOrchestrator || createJobOrchestrator;
  const createQueueRepo = deps.createReviewQueueRepo || createReviewQueueRepo;

  return {
    async reportReviewQueue(options = {}) {
      const rootDir = options.rootDir || process.cwd();
      const args = Array.isArray(options.args) ? options.args : [];
      const runId = options.runId || '';
      const jobOrchestrationEnabled =
        options.jobOrchestrationEnabled !== undefined
          ? Boolean(options.jobOrchestrationEnabled)
          : String(process.env.PIPELINE_JOB_ORCHESTRATION_ENABLED || 'true').toLowerCase() !== 'false';

      const parsed = parseReportReviewQueueArgs(args);

      const executeReport = async () => {
        if (parsed.helpRequested) {
          printReportReviewQueueUsage();
          return {
            ok: true,
            help: true
          };
        }

        const client = createClient({ rootDir });
        await client.ensureReady();
        const queueRepo = createQueueRepo(client);
        const scopeTag = parsed.asOf || 'latest';
        const scope = {
          country: parsed.country,
          scopeTag,
          allScopes: parsed.allScopes,
          limitRows: parsed.limitRows
        };

        const metrics = await queueRepo.fetchReportMetrics(scope);
        if (metrics.totalItems === 0) {
          throw new AppError({
            code: 'REVIEW_QUEUE_REPORT_FAILED',
            message: 'No review queue items found in selected scope'
          });
        }

        const issueTypeRows = await queueRepo.listCountsByIssueType(scope);
        const openOrConfirmedRows = await queueRepo.listOpenOrConfirmed(scope);
        const resolvedRows = await queueRepo.listResolved(scope);

        const payload = {
          scope: {
            country: parsed.country || 'ALL',
            scope_tag: scopeTag,
            all_scopes: parsed.allScopes
          },
          metrics: {
            total_items: metrics.totalItems,
            open_items: metrics.openItems,
            confirmed_items: metrics.confirmedItems,
            dismissed_items: metrics.dismissedItems,
            resolved_items: metrics.resolvedItems,
            auto_resolved_items: metrics.autoResolvedItems,
            review_coverage_percent: Number(metrics.reviewCoveragePercent.toFixed(2))
          },
          counts_by_issue_type: issueTypeRows,
          open_or_confirmed_items: openOrConfirmedRows,
          recently_resolved_items: resolvedRows
        };

        process.stdout.write(`${JSON.stringify(payload)}\n`);
        return {
          ok: true,
          payload
        };
      };

      if (!jobOrchestrationEnabled || parsed.helpRequested) {
        return executeReport();
      }

      const client = createClient({ rootDir });
      await client.ensureReady();
      const jobsRepo = createJobsRepo(client);
      const logger =
        options.logger ||
        createPipelineLogger(rootDir, 'qa.report-review-queue', runId || 'job');
      const jobOrchestrator = createOrchestrator({
        jobsRepo,
        logger
      });

      return jobOrchestrator.runJob({
        jobType: 'qa.report-review-queue',
        idempotencyKey: options.idempotencyKey || buildIdempotencyKey('qa.report-review-queue', args),
        runContext: {
          args
        },
        maxAttempts: Number.parseInt(process.env.PIPELINE_JOB_MAX_ATTEMPTS || '3', 10),
        maxConcurrent: Number.parseInt(process.env.PIPELINE_JOB_MAX_CONCURRENT || '1', 10),
        execute: async ({ updateCheckpoint }) => {
          const result = await executeReport();
          await updateCheckpoint({
            completedAt: new Date().toISOString(),
            script: 'report-review-queue.js'
          });
          return result;
        }
      });
    }
  };
}

const defaultService = createQaService();

function reportReviewQueue(options) {
  return defaultService.reportReviewQueue(options);
}

module.exports = {
  createQaService,
  reportReviewQueue
};
