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
const {
  createPipelineStageRepo,
} = require("../../data/postgis/repositories/pipeline-stage-repo");
const { isStrictIsoDate } = require("../../core/date");
const { readJobExecutionConfig } = require("../../core/runtime");
const {
  computeCodeFingerprint,
  fingerprintsMatch,
  normalizeStageScope,
} = require("../pipeline/stage-runtime");
const { runTrackedStage } = require("../pipeline/stage-tracking");

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
    reportLowConfidence: false,
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
      case "--report-low-confidence":
        parsed.reportLowConfidence = true;
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
  process.stdout.write(
    "  --report-low-confidence  Print low-confidence/conflicting coordinate rows after build\n",
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

function isTruthy(value) {
  return ["1", "true", "yes", "on"].includes(
    String(value || "")
      .trim()
      .toLowerCase(),
  );
}

function resolveSkipUnchangedEnabled(options = {}) {
  if (options.skipUnchangedEnabled !== undefined) {
    return Boolean(options.skipUnchangedEnabled);
  }
  return !isTruthy(options.env?.QA_PIPELINE_DISABLE_SKIP_UNCHANGED);
}

const GLOBAL_STATIONS_CODE_PATHS = [
  "services/orchestrator/src/domains/global/service.js",
  "services/orchestrator/src/data/postgis/repositories/global-stations-repo.js",
  "scripts/data/build-global-stations.sh",
];

const MERGE_QUEUE_CODE_PATHS = [
  "services/orchestrator/src/domains/global/service.js",
  "services/orchestrator/src/data/postgis/repositories/merge-queue-repo.js",
  "scripts/data/build-global-merge-queue.sh",
];

function toMetricValue(rawValue) {
  const value = String(rawValue ?? "").trim();
  if (value === "") {
    return "";
  }
  if (value === "true") {
    return true;
  }
  if (value === "false") {
    return false;
  }
  if (/^-?\d+$/.test(value)) {
    return Number.parseInt(value, 10);
  }
  if (/^-?\d+\.\d+$/.test(value)) {
    return Number.parseFloat(value);
  }
  return value;
}

function createPhaseTracker({ stageId, envTuning = {}, now = Date.now } = {}) {
  const startedAtMs = now();
  const phases = [];
  const phaseMap = new Map();
  const counters = {};
  let currentPhase = null;
  let currentPhaseStartedAtMs = startedAtMs;

  function ensurePhase(name) {
    const normalized = String(name || "").trim() || "unphased";
    if (!phaseMap.has(normalized)) {
      const phase = {
        name: normalized,
        startedAtMs: 0,
        durationMs: 0,
        info: {},
        rowCounts: {},
      };
      phaseMap.set(normalized, phase);
      phases.push(phase);
    }
    return phaseMap.get(normalized);
  }

  function closeCurrentPhase(closedAtMs) {
    if (!currentPhase) {
      return;
    }
    currentPhase.durationMs += Math.max(
      0,
      closedAtMs - currentPhaseStartedAtMs,
    );
  }

  return {
    onPhase(phaseName) {
      const switchedAtMs = now();
      closeCurrentPhase(switchedAtMs);
      currentPhase = ensurePhase(phaseName);
      if (currentPhase.startedAtMs === 0) {
        currentPhase.startedAtMs = Math.max(0, switchedAtMs - startedAtMs);
      }
      currentPhaseStartedAtMs = switchedAtMs;
    },
    onInfo(info) {
      if (!info || !info.key) {
        return;
      }
      const phase = currentPhase || ensurePhase("unphased");
      const value = toMetricValue(info.value);
      counters[info.key] = value;
      phase.info[info.key] = value;
      if (
        typeof value === "number" &&
        /(?:count|rows|total|mappings|edges|conflicts)$/i.test(info.key)
      ) {
        phase.rowCounts[info.key] = value;
      }
    },
    finalize(extra = {}) {
      const endedAtMs = now();
      closeCurrentPhase(endedAtMs);
      return {
        stageId,
        totalDurationMs: Math.max(0, endedAtMs - startedAtMs),
        phases: phases.map((phase) => ({
          name: phase.name,
          startedAtMs: phase.startedAtMs,
          durationMs: phase.durationMs,
          info: phase.info,
          rowCounts: phase.rowCounts,
        })),
        counters,
        envTuning,
        ...extra,
      };
    },
  };
}

function createMergeQueueCallbacks(
  writeMergeQueueNotice,
  scopeCountry,
  tracker = null,
  extraCallbacks = {},
) {
  return {
    onPhase(phase) {
      if (tracker) {
        tracker.onPhase(phase);
      }
      if (typeof extraCallbacks.onPhase === "function") {
        extraCallbacks.onPhase(phase);
      }
      writeMergeQueueNotice(`phase=${phase}`, scopeCountry);
    },
    onInfo(info) {
      if (tracker) {
        tracker.onInfo(info);
      }
      if (typeof extraCallbacks.onInfo === "function") {
        extraCallbacks.onInfo(info);
      }
      writeMergeQueueNotice(`${info.key}=${info.value}`, scopeCountry);
    },
  };
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
  const createStageRepo =
    deps.createPipelineStageRepo || createPipelineStageRepo;

  async function runWithJobOrchestration(options, config) {
    const rootDir = options.rootDir || process.cwd();
    const args = Array.isArray(options.args) ? options.args : [];
    const runId = options.runId || "";
    const jobExecutionConfig = readJobExecutionConfig(options.env);
    const jobOrchestrationEnabled =
      options.jobOrchestrationEnabled === undefined
        ? jobExecutionConfig.jobOrchestrationEnabled
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
        maxAttempts: jobExecutionConfig.maxAttempts,
        maxConcurrent: jobExecutionConfig.maxConcurrent,
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
            const envTuning =
              typeof stationsRepo.getTuningConfig === "function"
                ? stationsRepo.getTuningConfig()
                : {};
            const metricsTracker = createPhaseTracker({
              stageId: "global-stations",
              envTuning,
            });
            const writeGlobalNotice = (label) => {
              process.stdout.write(
                `[global-stations] ${label} country=${parsed.scope.country || "ALL"} scope=${parsed.scope.asOf || "latest"} source=${parsed.scope.sourceId || "ALL"}\n`,
              );
            };
            const skipUnchangedEnabled = resolveSkipUnchangedEnabled(options);
            const stageRepo = createStageRepo(client);
            const normalizedScope = normalizeStageScope(parsed.scope);
            const inputFingerprint =
              typeof stationsRepo.getBuildFingerprint === "function"
                ? await stationsRepo.getBuildFingerprint(parsed.scope)
                : null;
            const codeFingerprint = await computeCodeFingerprint(
              rootDir,
              GLOBAL_STATIONS_CODE_PATHS,
            );
            const previousMaterialization = await stageRepo.getMaterialization(
              "global-stations",
              normalizedScope.scopeKey,
            );
            const canSkip =
              skipUnchangedEnabled &&
              typeof stationsRepo.getCurrentSummary === "function" &&
              inputFingerprint &&
              previousMaterialization &&
              previousMaterialization.status === "ready" &&
              previousMaterialization.code_fingerprint === codeFingerprint &&
              fingerprintsMatch(
                previousMaterialization.input_fingerprint,
                inputFingerprint,
              );

            const trackedResult = await runTrackedStage({
              client,
              stageRepo,
              rootDir,
              stageId: "global-stations",
              scope: parsed.scope,
              codePaths: GLOBAL_STATIONS_CODE_PATHS,
              codeFingerprint,
              inputFingerprint,
              cacheHit: canSkip,
              skippedUnchanged: canSkip,
              execute: async () => {
                let summary;
                if (canSkip) {
                  metricsTracker.onInfo({ key: "cache_hit", value: "true" });
                  metricsTracker.onInfo({
                    key: "skipped_unchanged",
                    value: "true",
                  });
                  writeGlobalNotice("cache_hit=true");
                  writeGlobalNotice("skipped_unchanged=true");
                  summary = await stationsRepo.getCurrentSummary(parsed.scope);
                } else {
                  summary = await stationsRepo.buildGlobalStations(
                    parsed.scope,
                    {
                      onPhase(phase) {
                        metricsTracker.onPhase(phase);
                        if (typeof options.onPhase === "function") {
                          options.onPhase({
                            stageId: "global-stations",
                            phase,
                          });
                        }
                        writeGlobalNotice(`phase=${phase}`);
                      },
                      onInfo(info) {
                        metricsTracker.onInfo(info);
                        if (typeof options.onInfo === "function") {
                          options.onInfo({
                            stageId: "global-stations",
                            ...info,
                          });
                        }
                        writeGlobalNotice(`${info.key}=${info.value}`);
                      },
                    },
                  );
                }

                const metrics = metricsTracker.finalize({
                  cacheHit: canSkip,
                  skippedUnchanged: canSkip,
                });
                return {
                  ok: true,
                  summary,
                  metrics,
                  cacheHit: canSkip,
                  skippedUnchanged: canSkip,
                };
              },
            });

            process.stdout.write(
              `[global-stations] metrics=${JSON.stringify(trackedResult.metrics)}\n`,
            );
            process.stdout.write(`${JSON.stringify(trackedResult.summary)}\n`);
            if (parsed.reportLowConfidence) {
              const rows = await stationsRepo.listCoordinateAlerts(
                parsed.scope,
                { limit: 50 },
              );
              process.stdout.write(
                `${JSON.stringify({ lowConfidenceStations: rows })}\n`,
              );
            }
            return {
              ok: true,
              summary: trackedResult.summary,
              metrics: trackedResult.metrics,
              cacheHit: trackedResult.cacheHit,
              skippedUnchanged: trackedResult.skippedUnchanged,
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
          const client = createClient({ rootDir });
          try {
            await client.ensureReady();
            const queueRepo = createQueueRepo(client);
            const scope = parsed.scope.country
              ? parsed.scope
              : {
                  country: "",
                  asOf: parsed.scope.asOf,
                };
            const tracker = createPhaseTracker({
              stageId: "merge-queue",
              envTuning:
                typeof queueRepo.getTuningConfig === "function"
                  ? queueRepo.getTuningConfig()
                  : {},
            });
            const skipUnchangedEnabled = resolveSkipUnchangedEnabled(options);
            const stageRepo = createStageRepo(client);
            const normalizedScope = normalizeStageScope(scope);
            const inputFingerprint =
              typeof queueRepo.getRebuildFingerprint === "function"
                ? await queueRepo.getRebuildFingerprint(scope)
                : null;
            const codeFingerprint = await computeCodeFingerprint(
              rootDir,
              MERGE_QUEUE_CODE_PATHS,
            );
            const previousMaterialization = await stageRepo.getMaterialization(
              "merge-queue",
              normalizedScope.scopeKey,
            );
            const canSkip =
              skipUnchangedEnabled &&
              typeof queueRepo.getCurrentSummary === "function" &&
              inputFingerprint &&
              previousMaterialization &&
              previousMaterialization.status === "ready" &&
              previousMaterialization.code_fingerprint === codeFingerprint &&
              fingerprintsMatch(
                previousMaterialization.input_fingerprint,
                inputFingerprint,
              );

            const trackedResult = await runTrackedStage({
              client,
              stageRepo,
              rootDir,
              stageId: "merge-queue",
              scope,
              codePaths: MERGE_QUEUE_CODE_PATHS,
              codeFingerprint,
              inputFingerprint,
              cacheHit: canSkip,
              skippedUnchanged: canSkip,
              execute: async () => {
                let summary;
                if (canSkip) {
                  tracker.onInfo({ key: "cache_hit", value: "true" });
                  tracker.onInfo({
                    key: "skipped_unchanged",
                    value: "true",
                  });
                  writeMergeQueueNotice("cache_hit=true", scope.country);
                  writeMergeQueueNotice(
                    "skipped_unchanged=true",
                    scope.country,
                  );
                  summary = await queueRepo.getCurrentSummary(scope);
                } else {
                  summary = await queueRepo.rebuildMergeQueue(
                    scope,
                    createMergeQueueCallbacks(
                      writeMergeQueueNotice,
                      scope.country,
                      tracker,
                      {
                        onPhase(phase) {
                          if (typeof options.onPhase === "function") {
                            options.onPhase({
                              stageId: "merge-queue",
                              phase,
                            });
                          }
                        },
                        onInfo(info) {
                          if (typeof options.onInfo === "function") {
                            options.onInfo({
                              stageId: "merge-queue",
                              ...info,
                            });
                          }
                        },
                      },
                    ),
                  );
                }

                const metrics = tracker.finalize({
                  cacheHit: canSkip,
                  skippedUnchanged: canSkip,
                });
                return {
                  ok: true,
                  summary,
                  metrics,
                  cacheHit: canSkip,
                  skippedUnchanged: canSkip,
                };
              },
            });

            process.stdout.write(
              `[merge-queue] metrics=${JSON.stringify(trackedResult.metrics)}\n`,
            );
            process.stdout.write(`${JSON.stringify(trackedResult.summary)}\n`);
            return {
              ok: true,
              summary: trackedResult.summary,
              metrics: trackedResult.metrics,
              cacheHit: trackedResult.cacheHit,
              skippedUnchanged: trackedResult.skippedUnchanged,
            };
          } finally {
            await closeClient(client);
          }
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
