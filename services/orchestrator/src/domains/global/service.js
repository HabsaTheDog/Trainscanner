const {
  runLegacyDataScript,
  buildIdempotencyKey,
  createPipelineLogger,
} = require("../../core/pipeline-runner");
const { AppError } = require("../../core/errors");
const { createPostgisClient } = require("../../data/postgis/client");
const {
  createPipelineJobsRepo,
} = require("../../data/postgis/repositories/pipeline-jobs-repo");
const { createJobOrchestrator } = require("../../core/job-orchestrator");
const {
  createGlobalStationsRepo,
} = require("../../data/postgis/repositories/global-stations-repo");
const {
  createMergeQueueRepo,
} = require("../../data/postgis/repositories/merge-queue-repo");
const { isStrictIsoDate } = require("../../core/date");

function parseCountryScope(value) {
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

function parseAsOfScope(value) {
  if (!isStrictIsoDate(value)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: "Invalid --as-of value (expected YYYY-MM-DD)",
    });
  }
  return value;
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

function parseBuildGlobalStationsArgs(args = []) {
  const parsed = {
    helpRequested: false,
    scope: {
      country: "",
      asOf: "",
      sourceId: "",
    },
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
        parsed.scope.country = parseCountryScope(
          readRequiredTokenValue(tokens, i, "--country"),
        );
        i += 1;
        break;
      case "--as-of":
        parsed.scope.asOf = parseAsOfScope(
          readRequiredTokenValue(tokens, i, "--as-of"),
        );
        i += 1;
        break;
      case "--source-id":
        parsed.scope.sourceId = readRequiredTokenValue(
          tokens,
          i,
          "--source-id",
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

function parseBuildMergeQueueArgs(args = []) {
  const parsed = {
    helpRequested: false,
    scope: {
      country: "",
      asOf: "",
    },
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
        parsed.scope.country = parseCountryScope(
          readRequiredTokenValue(tokens, i, "--country"),
        );
        i += 1;
        break;
      case "--as-of":
        parsed.scope.asOf = parseAsOfScope(
          readRequiredTokenValue(tokens, i, "--as-of"),
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

function printGlobalBuildUsage() {
  process.stdout.write(
    "Usage: scripts/data/build-global-stations.sh [options]\n",
  );
  process.stdout.write("\n");
  process.stdout.write(
    "Build pan-European global stations and stop point mappings.\n",
  );
  process.stdout.write("\n");
  process.stdout.write("Options:\n");
  process.stdout.write(
    "  --country <ISO2>      Restrict build scope to one country\n",
  );
  process.stdout.write("  --as-of YYYY-MM-DD    Use latest datasets <= date\n");
  process.stdout.write(
    "  --source-id ID        Restrict build scope to one source id\n",
  );
  process.stdout.write("  -h, --help            Show this help\n");
}

function printMergeQueueUsage() {
  process.stdout.write(
    "Usage: scripts/data/build-global-merge-queue.sh [options]\n",
  );
  process.stdout.write("\n");
  process.stdout.write(
    "Build pan-European QA merge clusters from global stations.\n",
  );
  process.stdout.write("\n");
  process.stdout.write("Options:\n");
  process.stdout.write(
    "  --country <ISO2>      Restrict build scope to one country\n",
  );
  process.stdout.write("  --as-of YYYY-MM-DD    Scope tag date\n");
  process.stdout.write("  -h, --help            Show this help\n");
}

async function closeClient(client) {
  if (client && typeof client.end === "function") {
    await client.end();
  }
}

function parsePositiveInt(value, fallback) {
  const parsed = Number.parseInt(String(value || ""), 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

async function runWithConcurrency(items, limit, worker) {
  const results = new Array(items.length);
  let cursor = 0;

  async function runNext() {
    while (cursor < items.length) {
      const index = cursor;
      cursor += 1;
      results[index] = await worker(items[index], index);
    }
  }

  const workers = Array.from(
    { length: Math.min(Math.max(limit, 1), items.length) },
    () => runNext(),
  );

  await Promise.all(workers);
  return results;
}

function createGlobalService(deps = {}) {
  const runScript = deps.runLegacyDataScript || runLegacyDataScript;
  const createClient = deps.createPostgisClient || createPostgisClient;
  const createJobsRepo = deps.createPipelineJobsRepo || createPipelineJobsRepo;
  const createOrchestrator =
    deps.createJobOrchestrator || createJobOrchestrator;
  const createStationsRepo =
    deps.createGlobalStationsRepo || createGlobalStationsRepo;
  const createQueueRepo = deps.createMergeQueueRepo || createMergeQueueRepo;

  async function runWithJobOrchestration(options, config) {
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
    const helpRequested = args.includes("--help") || args.includes("-h");

    const runCall =
      typeof config.execute === "function"
        ? () =>
            config.execute({
              rootDir,
              runId,
              args,
              options,
            })
        : () =>
            runScript({
              rootDir,
              runId,
              args,
              service: config.service,
              scriptFile: config.scriptFile,
              errorCode: config.errorCode,
              runCommand: options.runCommand,
              logger: options.logger,
              loggerFactory: options.loggerFactory,
            });

    if (!jobOrchestrationEnabled || helpRequested) {
      return runCall();
    }

    const client = createClient({ rootDir });

    try {
      await client.ensureReady();
      const jobsRepo = createJobsRepo(client);
      const logger =
        options.logger ||
        createPipelineLogger(rootDir, config.jobType, runId || "job");
      const jobOrchestrator = createOrchestrator({
        jobsRepo,
        logger,
      });

      return jobOrchestrator.runJob({
        jobType: config.jobType,
        idempotencyKey:
          options.idempotencyKey || buildIdempotencyKey(config.jobType, args),
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
          const result = await runCall();
          await updateCheckpoint({
            completedAt: new Date().toISOString(),
            script: config.scriptFile,
          });
          return result;
        },
      });
    } finally {
      await closeClient(client);
    }
  }

  return {
    buildGlobalStations(options = {}) {
      return runWithJobOrchestration(options, {
        service: "global.build-stations",
        scriptFile: "build-global-stations.js",
        errorCode: "GLOBAL_BUILD_FAILED",
        jobType: "global.build-stations",
        execute: async ({ rootDir, args }) => {
          const parsed = parseBuildGlobalStationsArgs(args);
          if (parsed.helpRequested) {
            printGlobalBuildUsage();
            return {
              ok: true,
              help: true,
            };
          }

          const client = createClient({ rootDir });
          try {
            await client.ensureReady();
            const stationsRepo = createStationsRepo(client);
            const summary = await stationsRepo.buildGlobalStations(
              parsed.scope,
            );
            process.stdout.write(`${JSON.stringify(summary)}\n`);
            return {
              ok: true,
              summary,
            };
          } finally {
            await closeClient(client);
          }
        },
      });
    },

    buildGlobalMergeQueue(options = {}) {
      return runWithJobOrchestration(options, {
        service: "global.build-merge-queue",
        scriptFile: "build-global-merge-queue.js",
        errorCode: "MERGE_QUEUE_BUILD_FAILED",
        jobType: "global.build-merge-queue",
        execute: async ({ rootDir, args }) => {
          const parsed = parseBuildMergeQueueArgs(args);
          if (parsed.helpRequested) {
            printMergeQueueUsage();
            return {
              ok: true,
              help: true,
            };
          }

          const writeMergeQueueNotice = (label, scopeCountry) => {
            process.stdout.write(
              `[merge-queue] ${label} country=${scopeCountry || "ALL"} scope=${parsed.scope.asOf || "latest"}\n`,
            );
          };

          if (parsed.scope.country) {
            const client = createClient({ rootDir });
            try {
              await client.ensureReady();
              const queueRepo = createQueueRepo(client);
              const summary = await queueRepo.rebuildMergeQueue(parsed.scope, {
                onPhase(phase) {
                  writeMergeQueueNotice(`phase=${phase}`, parsed.scope.country);
                },
                onInfo(info) {
                  writeMergeQueueNotice(
                    `${info.key}=${info.value}`,
                    parsed.scope.country,
                  );
                },
              });
              process.stdout.write(`${JSON.stringify(summary)}\n`);
              return {
                ok: true,
                summary,
              };
            } finally {
              await closeClient(client);
            }
          }

          const discoveryClient = createClient({ rootDir });
          let countries = [];
          try {
            await discoveryClient.ensureReady();
            const rows = await discoveryClient.queryRows(
              `
              SELECT DISTINCT country::text AS country
              FROM global_stations
              WHERE is_active = true
                AND country IS NOT NULL
              ORDER BY country
              `,
            );
            countries = rows
              .map((row) =>
                String(row.country || "")
                  .trim()
                  .toUpperCase(),
              )
              .filter(Boolean);
          } finally {
            await closeClient(discoveryClient);
          }

          if (countries.length === 0) {
            const summary = {
              scopeCountry: "",
              scopeAsOf: parsed.scope.asOf || "",
              scopeTag: parsed.scope.asOf || "latest",
              clusters: 0,
              candidates: 0,
              evidence: 0,
            };
            process.stdout.write(`${JSON.stringify(summary)}\n`);
            return {
              ok: true,
              summary,
            };
          }

          const concurrency = parsePositiveInt(
            process.env.GLOBAL_MERGE_QUEUE_COUNTRY_CONCURRENCY || "1",
            1,
          );
          process.stdout.write(
            `[merge-queue] batching countries=${countries.join(",")} concurrency=${concurrency} scope=${parsed.scope.asOf || "latest"}\n`,
          );

          const summaries = await runWithConcurrency(
            countries,
            concurrency,
            async (country) => {
              const client = createClient({ rootDir });
              try {
                await client.ensureReady();
                const queueRepo = createQueueRepo(client);
                return await queueRepo.rebuildMergeQueue(
                  {
                    country,
                    asOf: parsed.scope.asOf,
                  },
                  {
                    onPhase(phase) {
                      writeMergeQueueNotice(`phase=${phase}`, country);
                    },
                    onInfo(info) {
                      writeMergeQueueNotice(
                        `${info.key}=${info.value}`,
                        country,
                      );
                    },
                  },
                );
              } finally {
                await closeClient(client);
              }
            },
          );

          const summary = summaries.reduce(
            (acc, item) => ({
              scopeCountry: "",
              scopeAsOf: acc.scopeAsOf || item.scopeAsOf || "",
              scopeTag: acc.scopeTag || item.scopeTag || "latest",
              clusters: acc.clusters + item.clusters,
              candidates: acc.candidates + item.candidates,
              evidence: acc.evidence + item.evidence,
            }),
            {
              scopeCountry: "",
              scopeAsOf: "",
              scopeTag: "",
              clusters: 0,
              candidates: 0,
              evidence: 0,
            },
          );

          process.stdout.write(`${JSON.stringify(summary)}\n`);
          return {
            ok: true,
            summary,
          };
        },
      });
    },
  };
}

const defaultService = createGlobalService();

function buildGlobalStations(options) {
  return defaultService.buildGlobalStations(options);
}

function buildGlobalMergeQueue(options) {
  return defaultService.buildGlobalMergeQueue(options);
}

module.exports = {
  buildGlobalStations,
  buildGlobalMergeQueue,
  createGlobalService,
};
