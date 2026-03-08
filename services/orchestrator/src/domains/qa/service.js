const { AppError } = require("../../core/errors");
const {
  buildIdempotencyKey,
  createPipelineLogger,
} = require("../../core/pipeline-runner");
const { createPostgisClient } = require("../../data/postgis/client");
const {
  createPipelineJobsRepo,
} = require("../../data/postgis/repositories/pipeline-jobs-repo");
const { createJobOrchestrator } = require("../../core/job-orchestrator");
const { isStrictIsoDate } = require("../../core/date");

function printReportMergeQueueUsage() {
  process.stdout.write(
    "Usage: scripts/data/report-review-queue.sh [options]\n",
  );
  process.stdout.write("\n");
  process.stdout.write(
    "Report pan-European global merge queue coverage and cluster snapshots.\n",
  );
  process.stdout.write("\n");
  process.stdout.write("Options:\n");
  process.stdout.write(
    "  --country <ISO2>      Restrict report to one country\n",
  );
  process.stdout.write(
    "  --as-of YYYY-MM-DD    Report clusters generated for this scope tag\n",
  );
  process.stdout.write(
    "  --all-scopes          Report all scope tags (instead of latest/as-of tag)\n",
  );
  process.stdout.write(
    "  --limit N             Number of detailed rows to include (default: 20)\n",
  );
  process.stdout.write("  -h, --help            Show this help\n");
}

function readRequiredTokenValue(tokens, index, flagName) {
  const value = String(tokens[index + 1] || "").trim();
  if (!value) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: `Missing value for ${flagName}`,
    });
  }
  return value;
}

function parseCountry(value) {
  const normalized = String(value || "")
    .trim()
    .toUpperCase();
  if (!/^[A-Z]{2}$/.test(normalized)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: "Invalid --country value (expected ISO-3166 alpha-2 code)",
    });
  }
  return normalized;
}

function parseAsOf(value) {
  if (!isStrictIsoDate(value)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: "Invalid --as-of value (expected YYYY-MM-DD)",
    });
  }
  return value;
}

function parsePositiveInt(value, flagName) {
  const parsedValue = Number.parseInt(value, 10);
  if (!Number.isFinite(parsedValue) || parsedValue <= 0) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: `${flagName} must be a positive integer`,
    });
  }
  return parsedValue;
}

function parseReportArgs(args = []) {
  const parsed = {
    helpRequested: false,
    country: "",
    asOf: "",
    allScopes: false,
    limitRows: 20,
  };
  const tokens = Array.isArray(args) ? args : [];

  for (let i = 0; i < tokens.length; i += 1) {
    const token = String(tokens[i] || "");
    switch (token) {
      case "-h":
      case "--help":
        parsed.helpRequested = true;
        break;
      case "--country":
        parsed.country = parseCountry(
          readRequiredTokenValue(tokens, i, "--country"),
        );
        i += 1;
        break;
      case "--as-of":
        parsed.asOf = parseAsOf(readRequiredTokenValue(tokens, i, "--as-of"));
        i += 1;
        break;
      case "--all-scopes":
        parsed.allScopes = true;
        break;
      case "--limit":
        parsed.limitRows = parsePositiveInt(
          readRequiredTokenValue(tokens, i, "--limit"),
          "--limit",
        );
        i += 1;
        break;
      default:
        throw new AppError({
          code: "INVALID_REQUEST",
          message: `Unknown argument: ${token}`,
        });
    }
  }

  return parsed;
}

function createQaService(deps = {}) {
  const createClient = deps.createPostgisClient || createPostgisClient;
  const createJobsRepo = deps.createPipelineJobsRepo || createPipelineJobsRepo;
  const createOrchestrator =
    deps.createJobOrchestrator || createJobOrchestrator;

  return {
    async reportReviewQueue(options = {}) {
      const rootDir = options.rootDir || process.cwd();
      const args = Array.isArray(options.args) ? options.args : [];
      const runId = options.runId || "";
      const defaultJobOrchestrationEnabled =
        String(
          process.env.PIPELINE_JOB_ORCHESTRATION_ENABLED || "true",
        ).toLowerCase() !== "false";
      const jobOrchestrationEnabled =
        options.jobOrchestrationEnabled === undefined
          ? defaultJobOrchestrationEnabled
          : Boolean(options.jobOrchestrationEnabled);

      const parsed = parseReportArgs(args);

      const executeReport = async () => {
        if (parsed.helpRequested) {
          printReportMergeQueueUsage();
          return {
            ok: true,
            help: true,
          };
        }

        const client = createClient({ rootDir });
        await client.ensureReady();
        const scopeTag = parsed.asOf || "latest";

        const metrics = await client.queryOne(
          `
          SELECT
            COUNT(*)::integer AS total_clusters,
            COUNT(*) FILTER (WHERE status = 'open')::integer AS open_clusters,
            COUNT(*) FILTER (WHERE status = 'in_review')::integer AS in_review_clusters,
            COUNT(*) FILTER (WHERE status = 'resolved')::integer AS resolved_clusters,
            COUNT(*) FILTER (WHERE status = 'dismissed')::integer AS dismissed_clusters
          FROM qa_merge_clusters c
          WHERE (NULLIF(:'country', '') IS NULL OR NULLIF(:'country', '') = ANY (COALESCE(c.country_tags, ARRAY[]::text[])))
            AND (
              :'all_scopes' = 'true'
              OR c.scope_tag = :'scope_tag'
            );
          `,
          {
            country: parsed.country || "",
            all_scopes: parsed.allScopes ? "true" : "false",
            scope_tag: scopeTag,
          },
        );

        const totalClusters =
          Number.parseInt(String(metrics?.total_clusters || 0), 10) || 0;
        if (totalClusters <= 0) {
          throw new AppError({
            code: "REVIEW_QUEUE_REPORT_FAILED",
            message: "No global merge clusters found in selected scope",
          });
        }

        const countsBySeverity = await client.queryRows(
          `
          SELECT severity, status, COUNT(*)::integer AS clusters
          FROM qa_merge_clusters c
          WHERE (NULLIF(:'country', '') IS NULL OR NULLIF(:'country', '') = ANY (COALESCE(c.country_tags, ARRAY[]::text[])))
            AND (
              :'all_scopes' = 'true'
              OR c.scope_tag = :'scope_tag'
            )
          GROUP BY severity, status
          ORDER BY severity, status;
          `,
          {
            country: parsed.country || "",
            all_scopes: parsed.allScopes ? "true" : "false",
            scope_tag: scopeTag,
          },
        );

        const openRows = await client.queryRows(
          `
          SELECT
            merge_cluster_id AS cluster_id,
            severity,
            status,
            display_name,
            candidate_count,
            issue_count,
            scope_tag,
            to_char(updated_at, 'YYYY-MM-DD HH24:MI:SSOF') AS updated_at
          FROM qa_merge_clusters c
          WHERE (NULLIF(:'country', '') IS NULL OR NULLIF(:'country', '') = ANY (COALESCE(c.country_tags, ARRAY[]::text[])))
            AND (
              :'all_scopes' = 'true'
              OR c.scope_tag = :'scope_tag'
            )
            AND c.status IN ('open', 'in_review')
          ORDER BY
            CASE c.severity WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
            c.updated_at DESC
          LIMIT NULLIF(:'limit_rows', '')::integer;
          `,
          {
            country: parsed.country || "",
            all_scopes: parsed.allScopes ? "true" : "false",
            scope_tag: scopeTag,
            limit_rows: String(parsed.limitRows),
          },
        );

        const resolvedRows = await client.queryRows(
          `
          SELECT
            merge_cluster_id AS cluster_id,
            severity,
            status,
            display_name,
            scope_tag,
            COALESCE(to_char(resolved_at, 'YYYY-MM-DD HH24:MI:SSOF'), '-') AS resolved_at,
            COALESCE(resolved_by, '-') AS resolved_by
          FROM qa_merge_clusters c
          WHERE (NULLIF(:'country', '') IS NULL OR NULLIF(:'country', '') = ANY (COALESCE(c.country_tags, ARRAY[]::text[])))
            AND (
              :'all_scopes' = 'true'
              OR c.scope_tag = :'scope_tag'
            )
            AND c.status IN ('resolved', 'dismissed')
          ORDER BY c.resolved_at DESC NULLS LAST, c.updated_at DESC
          LIMIT NULLIF(:'limit_rows', '')::integer;
          `,
          {
            country: parsed.country || "",
            all_scopes: parsed.allScopes ? "true" : "false",
            scope_tag: scopeTag,
            limit_rows: String(parsed.limitRows),
          },
        );

        const payload = {
          scope: {
            country: parsed.country || "ALL",
            scope_tag: scopeTag,
            all_scopes: parsed.allScopes,
          },
          metrics: {
            total_clusters: totalClusters,
            open_clusters:
              Number.parseInt(String(metrics?.open_clusters || 0), 10) || 0,
            in_review_clusters:
              Number.parseInt(String(metrics?.in_review_clusters || 0), 10) ||
              0,
            resolved_clusters:
              Number.parseInt(String(metrics?.resolved_clusters || 0), 10) || 0,
            dismissed_clusters:
              Number.parseInt(String(metrics?.dismissed_clusters || 0), 10) ||
              0,
          },
          counts_by_severity: countsBySeverity,
          open_or_in_review_clusters: openRows,
          recently_resolved_clusters: resolvedRows,
        };

        process.stdout.write(`${JSON.stringify(payload)}\n`);
        return {
          ok: true,
          payload,
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
        createPipelineLogger(rootDir, "qa.report-review-queue", runId || "job");
      const jobOrchestrator = createOrchestrator({
        jobsRepo,
        logger,
      });

      return jobOrchestrator.runJob({
        jobType: "qa.report-review-queue",
        idempotencyKey:
          options.idempotencyKey ||
          buildIdempotencyKey("qa.report-review-queue", args),
        runContext: {
          args,
        },
        maxAttempts: Number.parseInt(
          process.env.PIPELINE_JOB_MAX_ATTEMPTS || "3",
          10,
        ),
        maxConcurrent: Number.parseInt(
          process.env.PIPELINE_JOB_MAX_CONCURRENT || "1",
          10,
        ),
        execute: async ({ updateCheckpoint }) => {
          const result = await executeReport();
          await updateCheckpoint({
            completedAt: new Date().toISOString(),
            script: "report-review-queue.js",
          });
          return result;
        },
      });
    },
  };
}

const defaultService = createQaService();

function reportReviewQueue(options) {
  return defaultService.reportReviewQueue(options);
}

module.exports = {
  createQaService,
  reportReviewQueue,
};
