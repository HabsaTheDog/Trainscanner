const path = require("node:path");
const fs = require("node:fs");
const { execFile } = require("node:child_process");
const { promisify } = require("node:util");

const { AppError } = require("../../core/errors");
const {
  buildIdempotencyKey,
  createPipelineLogger,
} = require("../../core/pipeline-runner");
const { readJobExecutionConfig } = require("../../core/runtime");
const { isStrictIsoDate } = require("../../core/date");
const { createPostgisClient } = require("../../data/postgis/client");
const {
  createPipelineJobsRepo,
} = require("../../data/postgis/repositories/pipeline-jobs-repo");
const { createJobOrchestrator } = require("../../core/job-orchestrator");
const {
  createExternalReferenceRepo,
} = require("../../data/postgis/repositories/external-reference-repo");

const execFileAsync = promisify(execFile);

const SOURCE_IDS = ["overture", "wikidata", "geonames"];
const GEONAMES_DUMP_BASE_URL = "https://download.geonames.org/export/dump";
const DEFAULT_OVERTURE_RELEASE = "latest";
const EXTERNAL_REFERENCE_CACHE_ROOT = path.join(
  "data",
  "artifacts",
  "external-references",
);

function isTruthyEnv(value) {
  return ["1", "true", "yes", "on"].includes(
    String(value || "")
      .trim()
      .toLowerCase(),
  );
}

async function ensureDirectory(dirPath) {
  await fs.promises.mkdir(dirPath, { recursive: true });
}

async function downloadFile(url, outputPath) {
  await execFileAsync("curl", ["-fL", "--retry", "3", "-o", outputPath, url], {
    maxBuffer: 16 * 1024 * 1024,
  });
}

async function unzipFile(zipPath, outputDir) {
  await execFileAsync("unzip", ["-o", zipPath, "-d", outputDir], {
    maxBuffer: 16 * 1024 * 1024,
  });
}

async function readJsonFile(filePath) {
  return JSON.parse(await fs.promises.readFile(filePath, "utf8"));
}

async function writeJsonFile(filePath, value) {
  const tempPath = `${filePath}.${process.pid}.tmp`;
  await ensureDirectory(path.dirname(filePath));
  await fs.promises.writeFile(tempPath, JSON.stringify(value), "utf8");
  await fs.promises.rename(tempPath, filePath);
}

function resolveCachePolicy(env = process.env) {
  return {
    disableCache: isTruthyEnv(env.QA_EXTERNAL_REFERENCE_CACHE_DISABLED),
    forceRefresh: isTruthyEnv(env.QA_EXTERNAL_REFERENCE_FORCE_REFRESH),
  };
}

function resolveSourceCachePath(rootDir, sourceId, scope = {}, metadata = {}) {
  const country = String(scope.country || "global")
    .trim()
    .toLowerCase();
  const snapshotDate = normalizeSnapshotDate(
    metadata.snapshot_date,
    resolveSnapshotDate(scope),
  );

  if (sourceId === "wikidata") {
    return path.join(
      rootDir,
      EXTERNAL_REFERENCE_CACHE_ROOT,
      "wikidata",
      `${country}-${snapshotDate}.json`,
    );
  }

  if (sourceId === "overture") {
    const release = String(metadata.release || DEFAULT_OVERTURE_RELEASE)
      .trim()
      .replaceAll(/[^a-zA-Z0-9._-]+/g, "-");
    return path.join(
      rootDir,
      EXTERNAL_REFERENCE_CACHE_ROOT,
      "overture",
      `${country}-${release}.json`,
    );
  }

  return "";
}

async function resolveGeoNamesInputPath(rootDir, env, country) {
  const configuredPath = String(
    env?.QA_EXTERNAL_REFERENCE_GEONAMES_PATH || "",
  ).trim();
  if (configuredPath) {
    const stats = await fs.promises.stat(configuredPath).catch(() => null);
    if (!stats) {
      throw new Error(
        `QA_EXTERNAL_REFERENCE_GEONAMES_PATH does not exist: ${configuredPath}`,
      );
    }
    if (stats.isDirectory()) {
      if (!country) {
        throw new Error(
          "QA_EXTERNAL_REFERENCE_GEONAMES_PATH points to a directory and requires --country",
        );
      }
      const countryFilePath = path.join(configuredPath, `${country}.txt`);
      if (!fs.existsSync(countryFilePath)) {
        throw new Error(
          `GeoNames country dump not found in configured directory: ${countryFilePath}`,
        );
      }
      return countryFilePath;
    }
    return configuredPath;
  }

  if (!country) {
    throw new Error(
      "GeoNames imports require --country when QA_EXTERNAL_REFERENCE_GEONAMES_PATH is not set",
    );
  }

  const cacheDir = path.join(
    rootDir,
    "data",
    "artifacts",
    "external-references",
    `geonames-${country.toLowerCase()}`,
  );
  const textPath = path.join(cacheDir, `${country}.txt`);
  if (fs.existsSync(textPath)) {
    return textPath;
  }

  await ensureDirectory(cacheDir);
  const zipPath = path.join(cacheDir, `${country}.zip`);
  await downloadFile(`${GEONAMES_DUMP_BASE_URL}/${country}.zip`, zipPath);
  await unzipFile(zipPath, cacheDir);
  if (!fs.existsSync(textPath)) {
    throw new Error(
      `GeoNames archive did not contain expected file: ${textPath}`,
    );
  }
  return textPath;
}

function normalizeStationName(value) {
  return String(value || "")
    .toLowerCase()
    .replaceAll(/[^a-z0-9]+/g, " ")
    .replaceAll(/\s+/g, " ")
    .trim();
}

function parseCountry(value) {
  const normalized = String(value || "")
    .trim()
    .toUpperCase();
  if (!normalized) {
    return "";
  }
  if (!/^[A-Z]{2}$/.test(normalized)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: "Invalid --country value (expected ISO-3166 alpha-2 code)",
    });
  }
  return normalized;
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

function parseRefreshExternalReferenceArgs(args = []) {
  const parsed = {
    helpRequested: false,
    scope: {
      country: "",
      asOf: "",
      sourceId: "",
    },
  };
  const tokens = Array.isArray(args) ? args : [];

  for (let index = 0; index < tokens.length; index += 1) {
    const token = String(tokens[index] || "");
    switch (token) {
      case "-h":
      case "--help":
        parsed.helpRequested = true;
        break;
      case "--country":
        parsed.scope.country = parseCountry(
          readRequiredTokenValue(tokens, index, "--country"),
        );
        index += 1;
        break;
      case "--as-of":
        parsed.scope.asOf = readRequiredTokenValue(tokens, index, "--as-of");
        if (!isStrictIsoDate(parsed.scope.asOf)) {
          throw new AppError({
            code: "INVALID_REQUEST",
            message: "Invalid --as-of value (expected YYYY-MM-DD)",
          });
        }
        index += 1;
        break;
      case "--source-id":
        parsed.scope.sourceId = String(
          readRequiredTokenValue(tokens, index, "--source-id"),
        )
          .trim()
          .toLowerCase();
        if (!SOURCE_IDS.includes(parsed.scope.sourceId)) {
          throw new AppError({
            code: "INVALID_REQUEST",
            message: `Invalid --source-id value (expected one of ${SOURCE_IDS.join("|")})`,
          });
        }
        index += 1;
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

function printExternalReferenceUsage() {
  process.stdout.write(
    "Usage: scripts/data/refresh-external-references.sh [options]\n",
  );
  process.stdout.write("\n");
  process.stdout.write(
    "Import external station references and rebuild reviewer guidance matches.\n",
  );
  process.stdout.write("\n");
  process.stdout.write("Options:\n");
  process.stdout.write(
    "  --country <ISO2>      Restrict imports and matching to one country\n",
  );
  process.stdout.write(
    "  --as-of YYYY-MM-DD    Snapshot date label for imported rows\n",
  );
  process.stdout.write(
    "  --source-id <id>      Limit refresh to overture|wikidata|geonames\n",
  );
  process.stdout.write("  -h, --help            Show this help\n");
}

async function closeClient(client) {
  if (client && typeof client.end === "function") {
    await client.end();
  }
}

function resolveSnapshotDate(scope) {
  return scope.asOf || new Date().toISOString().slice(0, 10);
}

function normalizeSnapshotDate(value, fallback = "") {
  if (!value) {
    return fallback;
  }
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString().slice(0, 10);
  }
  const clean = String(value).trim();
  if (!clean) {
    return fallback;
  }
  if (isStrictIsoDate(clean)) {
    return clean;
  }
  const parsed = new Date(clean);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toISOString().slice(0, 10);
  }
  return fallback;
}

function resolveSnapshotLabel(sourceId, scope, metadata = {}) {
  const explicit = String(metadata.snapshot_label || "").trim();
  if (explicit) {
    return explicit;
  }
  const snapshotDate = normalizeSnapshotDate(metadata.snapshot_date, "");
  if (snapshotDate) {
    return `${sourceId}-${snapshotDate}`;
  }
  return `${sourceId}-${resolveSnapshotDate(scope)}`;
}

function normalizeImportedRows(rows = [], scope, sourceId) {
  return (Array.isArray(rows) ? rows : [])
    .map((row) => ({
      external_id: String(row?.external_id || "").trim(),
      display_name: String(row?.display_name || "").trim(),
      normalized_name: String(row?.normalized_name || "").trim(),
      country: String(row?.country || scope.country || "")
        .trim()
        .toUpperCase(),
      latitude:
        row?.latitude === null ||
        row?.latitude === undefined ||
        row?.latitude === ""
          ? null
          : Number(row.latitude),
      longitude:
        row?.longitude === null ||
        row?.longitude === undefined ||
        row?.longitude === ""
          ? null
          : Number(row.longitude),
      category: String(row?.category || "").trim(),
      subtype: String(row?.subtype || "").trim(),
      source_url: String(row?.source_url || "").trim(),
      metadata:
        row?.metadata &&
        typeof row.metadata === "object" &&
        !Array.isArray(row.metadata)
          ? row.metadata
          : {},
    }))
    .filter((row) => row.external_id && row.display_name)
    .map((row) => ({
      ...row,
      normalized_name:
        row.normalized_name || normalizeStationName(row.display_name),
      metadata: {
        ...row.metadata,
        imported_source: sourceId,
      },
    }));
}

async function runJsonImporter(command, args, options = {}) {
  const { stdout } = await execFileAsync(command, args, {
    cwd: options.cwd,
    env: options.env,
    maxBuffer: 64 * 1024 * 1024,
  });
  return JSON.parse(String(stdout || "[]"));
}

async function resolveOvertureImporterInvocation(rootDir, env = process.env) {
  const localScriptPath = path.join(
    rootDir,
    "scripts",
    "data",
    "import-overture-places.py",
  );
  try {
    await execFileAsync("python3", ["-c", "import duckdb"], {
      cwd: rootDir,
      env,
      maxBuffer: 1024 * 1024,
    });
    return {
      command: "python3",
      argsPrefix: [localScriptPath],
      cwd: rootDir,
      env,
    };
  } catch {}

  const containerName = String(
    env.QA_EXTERNAL_REFERENCE_IMPORTER_CONTAINER || "orchestrator",
  ).trim();
  const containerScriptPath = String(
    env.QA_EXTERNAL_REFERENCE_IMPORTER_CONTAINER_SCRIPT ||
      "/app/scripts/data/import-overture-places.py",
  ).trim();

  try {
    await execFileAsync(
      "docker",
      ["exec", containerName, "python3", "-c", "import duckdb"],
      {
        cwd: rootDir,
        env,
        maxBuffer: 1024 * 1024,
      },
    );
    return {
      command: "docker",
      argsPrefix: ["exec", containerName, "python3", containerScriptPath],
      cwd: rootDir,
      env,
    };
  } catch {}

  throw new Error(
    "Overture imports require duckdb in local python3 or in the importer container",
  );
}

function isJsonImporterInput(filePath) {
  const extension = path.extname(String(filePath || "").trim()).toLowerCase();
  return (
    extension === ".json" || extension === ".jsonl" || extension === ".ndjson"
  );
}

function resolveSourceDescriptors(rootDir, env = process.env) {
  const scriptDir = path.join(rootDir, "scripts", "data");
  const cachePolicy = resolveCachePolicy(env);
  return {
    overture: {
      sourceId: "overture",
      resolveMetadata(scope) {
        const inputPath = String(
          env.QA_EXTERNAL_REFERENCE_OVERTURE_PATH || "",
        ).trim();
        const release = String(
          env.QA_EXTERNAL_REFERENCE_OVERTURE_RELEASE ||
            DEFAULT_OVERTURE_RELEASE,
        ).trim();
        const snapshot_date = resolveSnapshotDate(scope);
        return {
          input_path: inputPath || "auto",
          importer: "overture_places",
          release,
          snapshot_date,
          cache_path: resolveSourceCachePath(rootDir, "overture", scope, {
            release,
            snapshot_date,
          }),
        };
      },
      async loadRows(scope, { metadata } = {}) {
        const inputPath = String(
          env.QA_EXTERNAL_REFERENCE_OVERTURE_PATH || "",
        ).trim();
        if (!inputPath && !scope.country) {
          throw new Error(
            "Overture imports require --country when QA_EXTERNAL_REFERENCE_OVERTURE_PATH is not set",
          );
        }
        const cachePath = String(metadata?.cache_path || "").trim();
        if (
          cachePath &&
          !cachePolicy.disableCache &&
          !cachePolicy.forceRefresh &&
          fs.existsSync(cachePath)
        ) {
          return {
            rows: await readJsonFile(cachePath),
            cacheHit: true,
          };
        }
        const release = String(
          env.QA_EXTERNAL_REFERENCE_OVERTURE_RELEASE ||
            DEFAULT_OVERTURE_RELEASE,
        ).trim();
        const importerArgs = [
          "--input",
          inputPath || "auto",
          "--country",
          scope.country || "",
          "--release",
          release,
        ];
        const rows =
          inputPath && isJsonImporterInput(inputPath)
            ? await runJsonImporter(
                "python3",
                [
                  path.join(scriptDir, "import-overture-places.py"),
                  ...importerArgs,
                ],
                { cwd: rootDir, env },
              )
            : await (async () => {
                const invocation = await resolveOvertureImporterInvocation(
                  rootDir,
                  env,
                );
                return runJsonImporter(
                  invocation.command,
                  [...invocation.argsPrefix, ...importerArgs],
                  invocation,
                );
              })();
        if (cachePath && !cachePolicy.disableCache) {
          await writeJsonFile(cachePath, rows);
        }
        return {
          rows,
          cacheHit: false,
        };
      },
    },
    wikidata: {
      sourceId: "wikidata",
      resolveMetadata(scope) {
        const snapshot_date = resolveSnapshotDate(scope);
        const fixture_path = String(
          env.QA_EXTERNAL_REFERENCE_WIKIDATA_FIXTURE || "",
        ).trim();
        return {
          importer: "wikidata_wdqs",
          fixture_path,
          snapshot_date,
          cache_path:
            fixture_path ||
            resolveSourceCachePath(rootDir, "wikidata", scope, {
              snapshot_date,
            }),
        };
      },
      async loadRows(scope, { metadata } = {}) {
        const args = [
          path.join(scriptDir, "import-wikidata.js"),
          "--country",
          scope.country || "",
        ];
        const fixturePath = String(
          env.QA_EXTERNAL_REFERENCE_WIKIDATA_FIXTURE || "",
        ).trim();
        if (!scope.country && !fixturePath) {
          throw new Error(
            "Country-scoped imports are required for live Wikidata refreshes unless QA_EXTERNAL_REFERENCE_WIKIDATA_FIXTURE is set",
          );
        }
        if (fixturePath) {
          args.push("--fixture", fixturePath);
          return {
            rows: await runJsonImporter(process.execPath, args, {
              cwd: rootDir,
              env,
            }),
            cacheHit: true,
          };
        }
        const cachePath = String(metadata?.cache_path || "").trim();
        if (
          cachePath &&
          !cachePolicy.disableCache &&
          !cachePolicy.forceRefresh &&
          fs.existsSync(cachePath)
        ) {
          return {
            rows: await readJsonFile(cachePath),
            cacheHit: true,
          };
        }
        const rows = await runJsonImporter(process.execPath, args, {
          cwd: rootDir,
          env,
        });
        if (cachePath && !cachePolicy.disableCache) {
          await writeJsonFile(cachePath, rows);
        }
        return {
          rows,
          cacheHit: false,
        };
      },
    },
    geonames: {
      sourceId: "geonames",
      resolveMetadata(scope) {
        return {
          input_path: String(
            env.QA_EXTERNAL_REFERENCE_GEONAMES_PATH || "",
          ).trim(),
          importer: "geonames_dump",
          snapshot_date: resolveSnapshotDate(scope),
        };
      },
      async loadRows(scope) {
        const inputPath = await resolveGeoNamesInputPath(
          rootDir,
          env,
          scope.country,
        );
        return runJsonImporter(
          process.execPath,
          [
            path.join(scriptDir, "import-geonames.js"),
            "--input",
            inputPath,
            "--country",
            scope.country || "",
          ],
          { cwd: rootDir, env },
        );
      },
    },
  };
}

function logImportSummary(summary) {
  for (const row of summary.imports || []) {
    const status = row.status === "succeeded" ? "ok" : "failed";
    process.stdout.write(
      `[reference-data] source=${row.source_id} status=${status} rows=${row.row_count || 0}\n`,
    );
  }
  process.stdout.write(
    `[reference-data] matched_stations=${summary.matches?.matched_stations || 0} rows=${summary.matches?.rows_inserted || 0}\n`,
  );
  const statusCounts = summary.matches?.status_counts || {};
  for (const key of ["strong", "probable", "weak"]) {
    process.stdout.write(
      `[reference-data] match_status=${key} count=${statusCounts[key] || 0}\n`,
    );
  }
}

function validateConfiguredInputPath(filePath, envName) {
  const resolved = String(filePath || "").trim();
  if (!resolved) {
    return {
      available: false,
      reason: `missing_${envName}`,
    };
  }

  if (!fs.existsSync(resolved)) {
    return {
      available: false,
      reason: `${envName}_not_found`,
    };
  }

  return {
    available: true,
    reason: "configured",
  };
}

function preflightSourceAvailability(_rootDir, env, scope, selectedSourceIds) {
  return selectedSourceIds.map((sourceId) => {
    if (sourceId === "overture") {
      const inputPath = String(
        env?.QA_EXTERNAL_REFERENCE_OVERTURE_PATH || "",
      ).trim();
      const inputStatus = validateConfiguredInputPath(
        inputPath,
        "QA_EXTERNAL_REFERENCE_OVERTURE_PATH",
      );
      return {
        sourceId,
        available: inputStatus.available,
        mode: inputStatus.available ? "local_file" : "unavailable",
        reason: inputStatus.reason,
      };
    }

    if (sourceId === "geonames") {
      const inputPath = String(
        env?.QA_EXTERNAL_REFERENCE_GEONAMES_PATH || "",
      ).trim();
      const inputStatus = validateConfiguredInputPath(
        inputPath,
        "QA_EXTERNAL_REFERENCE_GEONAMES_PATH",
      );
      return {
        sourceId,
        available: inputStatus.available,
        mode: inputStatus.available ? "local_file" : "unavailable",
        reason: inputStatus.reason,
      };
    }

    if (sourceId === "wikidata") {
      const fixturePath = String(
        env?.QA_EXTERNAL_REFERENCE_WIKIDATA_FIXTURE || "",
      ).trim();
      if (fixturePath) {
        const fixtureStatus = validateConfiguredInputPath(
          fixturePath,
          "QA_EXTERNAL_REFERENCE_WIKIDATA_FIXTURE",
        );
        return {
          sourceId,
          available: fixtureStatus.available,
          mode: fixtureStatus.available ? "fixture" : "unavailable",
          reason: fixtureStatus.reason,
        };
      }

      if (!scope.country) {
        return {
          sourceId,
          available: false,
          mode: "unavailable",
          reason: "country_required_for_live_wikidata",
        };
      }

      return {
        sourceId,
        available: true,
        mode: "live",
        reason: "country_scoped_live_refresh",
      };
    }

    return {
      sourceId,
      available: false,
      mode: "unavailable",
      reason: "unknown_source",
    };
  });
}

function logSourceAvailability(entries = []) {
  for (const entry of entries) {
    process.stdout.write(
      `[reference-data] preflight source=${entry.sourceId} available=${entry.available ? "true" : "false"} mode=${entry.mode} reason=${entry.reason}\n`,
    );
  }
}

async function runImportForSource(repo, descriptor, scope) {
  const metadata = descriptor.resolveMetadata(scope);
  const importRow = await repo.recordImportRun({
    sourceId: descriptor.sourceId,
    snapshotLabel: resolveSnapshotLabel(descriptor.sourceId, scope, metadata),
    snapshotDate: String(metadata.snapshot_date || resolveSnapshotDate(scope)),
    country: scope.country,
    status: "running",
    metadata,
  });

  try {
    const loaded = await descriptor.loadRows(scope, {
      metadata,
      rootDir: process.cwd(),
    });
    const rawRows =
      loaded && typeof loaded === "object" && !Array.isArray(loaded)
        ? loaded.rows
        : loaded;
    const cacheHit =
      loaded && typeof loaded === "object" && !Array.isArray(loaded)
        ? Boolean(loaded.cacheHit)
        : false;
    const rows = normalizeImportedRows(rawRows, scope, descriptor.sourceId);
    await repo.replaceImportRows({
      importId: importRow.import_id,
      sourceId: descriptor.sourceId,
      rows,
    });
    await repo.recordImportRun({
      importId: importRow.import_id,
      sourceId: descriptor.sourceId,
      snapshotLabel: importRow.snapshot_label,
      snapshotDate: normalizeSnapshotDate(
        importRow.snapshot_date,
        normalizeSnapshotDate(metadata.snapshot_date, ""),
      ),
      country: importRow.country || scope.country,
      status: "succeeded",
      metadata: {
        ...metadata,
        cache_hit: cacheHit,
        error_message: null,
        row_count: rows.length,
      },
    });
    return {
      source_id: descriptor.sourceId,
      status: "succeeded",
      row_count: rows.length,
    };
  } catch (error) {
    await repo.recordImportRun({
      importId: importRow.import_id,
      sourceId: descriptor.sourceId,
      snapshotLabel: importRow.snapshot_label,
      snapshotDate: normalizeSnapshotDate(
        importRow.snapshot_date,
        normalizeSnapshotDate(metadata.snapshot_date, ""),
      ),
      country: importRow.country || scope.country,
      status: "failed",
      metadata: {
        ...metadata,
        cache_hit: false,
        error_message: error.message,
        row_count: 0,
      },
    });
    return {
      source_id: descriptor.sourceId,
      status: "failed",
      row_count: 0,
      error_message: error.message,
    };
  }
}

function createReferenceService(deps = {}) {
  const createClient = deps.createPostgisClient || createPostgisClient;
  const createJobsRepo = deps.createPipelineJobsRepo || createPipelineJobsRepo;
  const createOrchestrator =
    deps.createJobOrchestrator || createJobOrchestrator;
  const createRepo =
    deps.createExternalReferenceRepo || createExternalReferenceRepo;

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

    const runCall = () =>
      config.execute({
        rootDir,
        runId,
        args,
        options,
      });

    if (!jobOrchestrationEnabled || helpRequested) {
      return runCall();
    }

    const client = createClient({ rootDir, env: options.env });
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
        runContext: { args },
        maxAttempts: jobExecutionConfig.maxAttempts,
        maxConcurrent: jobExecutionConfig.maxConcurrent,
        execute: async ({ updateCheckpoint }) => {
          const result = await runCall();
          await updateCheckpoint({
            completedAt: new Date().toISOString(),
            scope: parseRefreshExternalReferenceArgs(args).scope,
          });
          return result;
        },
      });
    } finally {
      await closeClient(client);
    }
  }

  return {
    refreshExternalReferences(options = {}) {
      return runWithJobOrchestration(options, {
        jobType: "reference.refresh",
        execute: async ({ rootDir, args, options: runOptions }) => {
          const parsed = parseRefreshExternalReferenceArgs(args);
          if (parsed.helpRequested) {
            printExternalReferenceUsage();
            return {
              ok: true,
              help: true,
            };
          }

          const selectedSourceIds = parsed.scope.sourceId
            ? [parsed.scope.sourceId]
            : SOURCE_IDS;
          const preflight = preflightSourceAvailability(
            rootDir,
            runOptions.env || process.env,
            parsed.scope,
            selectedSourceIds,
          );
          logSourceAvailability(preflight);
          const availableEntries = preflight.filter((entry) => entry.available);
          const unavailableImports = preflight
            .filter((entry) => !entry.available)
            .map((entry) => ({
              source_id: entry.sourceId,
              status: "failed",
              row_count: 0,
              error_message: entry.reason,
              preflight_only: true,
            }));

          if (availableEntries.length === 0) {
            throw new AppError({
              code: "EXTERNAL_REFERENCE_IMPORT_FAILED",
              message: "All external reference imports failed preflight",
              details: {
                imports: unavailableImports,
              },
            });
          }

          const client = createClient({ rootDir, env: runOptions.env });
          try {
            await client.ensureReady();
            const repo = createRepo(client);
            const descriptors = resolveSourceDescriptors(
              rootDir,
              runOptions.env || process.env,
            );

            const imports = [...unavailableImports];
            for (const sourceId of availableEntries.map(
              (entry) => entry.sourceId,
            )) {
              imports.push(
                await runImportForSource(
                  repo,
                  descriptors[sourceId],
                  parsed.scope,
                ),
              );
            }

            const successfulImports = imports.filter(
              (row) => row.status === "succeeded",
            );
            if (successfulImports.length === 0) {
              throw new AppError({
                code: "EXTERNAL_REFERENCE_IMPORT_FAILED",
                message: "All external reference imports failed",
                details: { imports },
              });
            }

            const matches = await repo.buildStationReferenceMatches(
              parsed.scope,
            );
            const summary = {
              imports,
              matches,
            };
            logImportSummary(summary);
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
  };
}

const defaultService = createReferenceService();

function refreshExternalReferences(options) {
  return defaultService.refreshExternalReferences(options);
}

module.exports = {
  createReferenceService,
  printExternalReferenceUsage,
  refreshExternalReferences,
  _internal: {
    preflightSourceAvailability,
    logSourceAvailability,
    resolveGeoNamesInputPath,
    normalizeImportedRows,
    normalizeSnapshotDate,
    parseRefreshExternalReferenceArgs,
    resolveOvertureImporterInvocation,
    resolveSnapshotLabel,
    resolveSourceDescriptors,
  },
};
