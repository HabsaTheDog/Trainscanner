#!/usr/bin/env node
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { execFile } = require("node:child_process");
const { promisify } = require("node:util");

const { AppError } = require("../core/errors");
const { fetchSources } = require("../domains/source-discovery/service");
const { ingestNetex } = require("../domains/ingest/service");
const {
  buildGlobalStations,
  buildGlobalMergeQueue,
} = require("../domains/global/service");
const { refreshExternalReferences } = require("../domains/reference/service");
const {
  extractQaNetworkContext,
  projectQaNetworkContext,
} = require("../domains/qa/pipeline-stage-service");
const { createPostgisClient } = require("../data/postgis/client");
const {
  createGlobalStationsRepo,
} = require("../data/postgis/repositories/global-stations-repo");
const {
  createMergeQueueRepo,
} = require("../data/postgis/repositories/merge-queue-repo");
const { parsePipelineCliArgs, printCliError } = require("./pipeline-common");
const { isStrictIsoDate } = require("../core/date");

const execFileAsync = promisify(execFile);

const STEP_IDS = [
  "fetch",
  "stop-topology",
  "qa-network-context",
  "global-stations",
  "reference-data",
  "qa-network-projection",
  "merge-queue",
  "export-schedule",
];

function parseStepToken(raw) {
  const token = String(raw || "")
    .trim()
    .toLowerCase();
  if (!token) {
    return "";
  }
  if (
    token === "queue" ||
    token === "merge" ||
    token === "merge_queue" ||
    token === "review"
  ) {
    return "merge-queue";
  }
  if (token === "ingest" || token === "stop_topology" || token === "topology") {
    return "stop-topology";
  }
  if (token === "qa-context" || token === "qa_network_context") {
    return "qa-network-context";
  }
  if (token === "global" || token === "stations") {
    return "global-stations";
  }
  if (
    token === "reference" ||
    token === "references" ||
    token === "external-references"
  ) {
    return "reference-data";
  }
  if (
    token === "projection" ||
    token === "project" ||
    token === "qa_network_projection"
  ) {
    return "qa-network-projection";
  }
  if (token === "export" || token === "schedule") {
    return "export-schedule";
  }
  return token;
}

function assertStepId(stepId, flagName) {
  if (STEP_IDS.includes(stepId)) {
    return;
  }
  throw new AppError({
    code: "INVALID_REQUEST",
    message: `${flagName} must be one of ${STEP_IDS.join("|")}`,
  });
}

function readRequiredValue(tokens, index, flagName) {
  const value = String(tokens[index + 1] || "").trim();
  if (!value) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: `Missing value for ${flagName}`,
    });
  }
  return value;
}

function printUsage() {
  process.stdout.write(
    "Usage: services/orchestrator/src/cli/benchmark-station-review.js [options]\n",
  );
  process.stdout.write("\n");
  process.stdout.write(
    "Benchmark selected station-review pipeline stages and write JSON artifacts.\n",
  );
  process.stdout.write("\n");
  process.stdout.write("Options:\n");
  process.stdout.write(
    "  --runs <n>               Measured runs (default: 3)\n",
  );
  process.stdout.write(
    "  --warmup-runs <n>        Warmup runs before measurement (default: 1)\n",
  );
  process.stdout.write(
    "  --benchmark-id <id>      Output directory id under reports/qa/pipeline-benchmarks\n",
  );
  process.stdout.write(
    "  --from-step <step>       Start from fetch|stop-topology|qa-network-context|global-stations|reference-data|qa-network-projection|merge-queue|export-schedule\n",
  );
  process.stdout.write(
    "  --to-step <step>         Stop after fetch|stop-topology|qa-network-context|global-stations|reference-data|qa-network-projection|merge-queue|export-schedule\n",
  );
  process.stdout.write(
    "  --skip-step <step>       Exclude a step inside the selected range (repeatable)\n",
  );
  process.stdout.write(
    "  --country <ISO2>         Restrict fetch/ingest/reference scope\n",
  );
  process.stdout.write(
    "  --as-of YYYY-MM-DD       Snapshot date override for scoped steps\n",
  );
  process.stdout.write(
    "  --source-id <id>         Restrict fetch/ingest/global-stations scope\n",
  );
  process.stdout.write(
    "  --sql-profile-sample     Capture targeted EXPLAIN ANALYZE JSON artifacts\n",
  );
  process.stdout.write(
    "  --emit-phase-metrics     Print per-step metrics summaries to stdout\n",
  );
  process.stdout.write("  -h, --help               Show this help\n");
}

function parseArgs(argv = []) {
  const parsed = parsePipelineCliArgs(argv);
  const passthrough = Array.isArray(parsed.passthroughArgs)
    ? parsed.passthroughArgs
    : [];

  const options = {
    rootDir: parsed.rootDir,
    help: false,
    runs: 3,
    warmupRuns: 1,
    benchmarkId: "",
    fromStep: "stop-topology",
    toStep: "merge-queue",
    skipSteps: [],
    country: "",
    asOf: "",
    sourceId: "",
    sqlProfileSample: false,
    emitPhaseMetrics: false,
  };

  for (let index = 0; index < passthrough.length; index += 1) {
    const token = passthrough[index];
    switch (token) {
      case "-h":
      case "--help":
        options.help = true;
        break;
      case "--runs":
        options.runs = Number.parseInt(
          readRequiredValue(passthrough, index, "--runs"),
          10,
        );
        index += 1;
        break;
      case "--warmup-runs":
        options.warmupRuns = Number.parseInt(
          readRequiredValue(passthrough, index, "--warmup-runs"),
          10,
        );
        index += 1;
        break;
      case "--benchmark-id":
        options.benchmarkId = readRequiredValue(
          passthrough,
          index,
          "--benchmark-id",
        );
        index += 1;
        break;
      case "--from-step":
        options.fromStep = parseStepToken(
          readRequiredValue(passthrough, index, "--from-step"),
        );
        index += 1;
        break;
      case "--to-step":
        options.toStep = parseStepToken(
          readRequiredValue(passthrough, index, "--to-step"),
        );
        index += 1;
        break;
      case "--country":
        options.country = readRequiredValue(passthrough, index, "--country")
          .trim()
          .toUpperCase();
        index += 1;
        break;
      case "--skip-step":
        options.skipSteps.push(
          parseStepToken(readRequiredValue(passthrough, index, "--skip-step")),
        );
        index += 1;
        break;
      case "--as-of":
        options.asOf = readRequiredValue(passthrough, index, "--as-of").trim();
        index += 1;
        break;
      case "--source-id":
        options.sourceId = readRequiredValue(
          passthrough,
          index,
          "--source-id",
        ).trim();
        index += 1;
        break;
      case "--sql-profile-sample":
        options.sqlProfileSample = true;
        break;
      case "--emit-phase-metrics":
        options.emitPhaseMetrics = true;
        break;
      default:
        throw new AppError({
          code: "INVALID_REQUEST",
          message: `Unknown argument: ${token}`,
        });
    }
  }

  if (options.help) {
    return options;
  }

  if (!Number.isInteger(options.runs) || options.runs <= 0) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: "--runs must be a positive integer",
    });
  }
  if (!Number.isInteger(options.warmupRuns) || options.warmupRuns < 0) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: "--warmup-runs must be a non-negative integer",
    });
  }
  assertStepId(options.fromStep, "--from-step");
  assertStepId(options.toStep, "--to-step");
  for (const stepId of options.skipSteps) {
    assertStepId(stepId, "--skip-step");
  }
  if (STEP_IDS.indexOf(options.fromStep) > STEP_IDS.indexOf(options.toStep)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: "--from-step must be before or equal to --to-step",
    });
  }
  if (options.country && !/^[A-Z]{2}$/.test(options.country)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: "--country must be an ISO-3166 alpha-2 code",
    });
  }
  if (options.asOf && !isStrictIsoDate(options.asOf)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: "--as-of must be YYYY-MM-DD",
    });
  }
  if (!options.benchmarkId) {
    options.benchmarkId = `station-review-${new Date()
      .toISOString()
      .replaceAll(/[:.]/g, "-")}`;
  }

  return options;
}

function selectSteps(options) {
  const skippedSteps = new Set(options.skipSteps || []);
  return STEP_IDS.filter((stepId) => {
    const index = STEP_IDS.indexOf(stepId);
    return (
      index >= STEP_IDS.indexOf(options.fromStep) &&
      index <= STEP_IDS.indexOf(options.toStep) &&
      !skippedSteps.has(stepId)
    );
  });
}

function appendArgIfSet(args, key, value) {
  if (!value) {
    return;
  }
  args.push(key, value);
}

function buildStepArgs(options, stepId) {
  const args = [];
  appendArgIfSet(args, "--as-of", options.asOf);
  if (
    stepId === "fetch" ||
    stepId === "stop-topology" ||
    stepId === "qa-network-context" ||
    stepId === "reference-data" ||
    stepId === "qa-network-projection" ||
    stepId === "merge-queue" ||
    stepId === "export-schedule"
  ) {
    appendArgIfSet(args, "--country", options.country);
  }
  if (
    stepId === "fetch" ||
    stepId === "stop-topology" ||
    stepId === "qa-network-context" ||
    stepId === "global-stations" ||
    stepId === "qa-network-projection" ||
    stepId === "export-schedule"
  ) {
    appendArgIfSet(args, "--source-id", options.sourceId);
  }
  if (stepId === "global-stations") {
    appendArgIfSet(args, "--country", options.country);
  }
  return args;
}

function createStepDefinitions(options) {
  return {
    fetch: {
      label: "Fetch source datasets",
      args: buildStepArgs(options, "fetch"),
      run(runOptions) {
        return fetchSources({
          rootDir: runOptions.rootDir,
          runId: runOptions.runId,
          args: runOptions.args,
        });
      },
    },
    "stop-topology": {
      label: "Ingest stop topology only",
      args: [
        "--mode",
        "stop-topology",
        ...buildStepArgs(options, "stop-topology"),
      ],
      run(runOptions) {
        return ingestNetex({
          rootDir: runOptions.rootDir,
          runId: runOptions.runId,
          args: runOptions.args,
          jobOrchestrationEnabled: false,
        });
      },
    },
    "qa-network-context": {
      label: "Extract provider QA network context",
      args: buildStepArgs(options, "qa-network-context"),
      run(runOptions) {
        return extractQaNetworkContext({
          rootDir: runOptions.rootDir,
          runId: runOptions.runId,
          args: runOptions.args,
        });
      },
    },
    "global-stations": {
      label: "Build global stations",
      args: buildStepArgs(options, "global-stations"),
      run(runOptions) {
        return buildGlobalStations({
          rootDir: runOptions.rootDir,
          runId: runOptions.runId,
          args: runOptions.args,
          jobOrchestrationEnabled: false,
          skipUnchangedEnabled: false,
          onPhase: runOptions.onPhase,
          onInfo: runOptions.onInfo,
        });
      },
    },
    "reference-data": {
      label: "Import external references and build matches",
      args: buildStepArgs(options, "reference-data"),
      run(runOptions) {
        return refreshExternalReferences({
          rootDir: runOptions.rootDir,
          runId: runOptions.runId,
          args: runOptions.args,
          jobOrchestrationEnabled: false,
        });
      },
    },
    "qa-network-projection": {
      label: "Project QA network context",
      args: buildStepArgs(options, "qa-network-projection"),
      run(runOptions) {
        return projectQaNetworkContext({
          rootDir: runOptions.rootDir,
          runId: runOptions.runId,
          args: runOptions.args,
        });
      },
    },
    "merge-queue": {
      label: "Build global merge queue",
      args: buildStepArgs(options, "merge-queue"),
      run(runOptions) {
        return buildGlobalMergeQueue({
          rootDir: runOptions.rootDir,
          runId: runOptions.runId,
          args: runOptions.args,
          jobOrchestrationEnabled: false,
          skipUnchangedEnabled: false,
          onPhase: runOptions.onPhase,
          onInfo: runOptions.onInfo,
        });
      },
    },
    "export-schedule": {
      label: "Ingest export schedule only",
      args: [
        "--mode",
        "export-schedule",
        ...buildStepArgs(options, "export-schedule"),
      ],
      run(runOptions) {
        return ingestNetex({
          rootDir: runOptions.rootDir,
          runId: runOptions.runId,
          args: runOptions.args,
          jobOrchestrationEnabled: false,
        });
      },
    },
  };
}

function collectMachineInfo() {
  return {
    hostname: os.hostname(),
    platform: os.platform(),
    release: os.release(),
    arch: os.arch(),
    cpuModel: os.cpus()[0]?.model || "",
    cpuCount: os.cpus().length,
    totalMemoryBytes: os.totalmem(),
    nodeVersion: process.version,
  };
}

function median(values = []) {
  if (!values.length) {
    return 0;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) {
    return sorted[middle];
  }
  return (sorted[middle - 1] + sorted[middle]) / 2;
}

function parsePsOutput(stdout) {
  return String(stdout || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const match = line.match(/^(\d+)\s+(\d+)\s+([\d.]+)\s+(\d+)\s+(.*)$/);
      if (!match) {
        return null;
      }
      return {
        pid: Number.parseInt(match[1], 10),
        ppid: Number.parseInt(match[2], 10),
        cpuPercent: Number.parseFloat(match[3]),
        rssKb: Number.parseInt(match[4], 10),
        command: match[5],
      };
    })
    .filter(Boolean);
}

function collectDescendantPids(processes, rootPid) {
  const byParent = new Map();
  for (const entry of processes) {
    if (!byParent.has(entry.ppid)) {
      byParent.set(entry.ppid, []);
    }
    byParent.get(entry.ppid).push(entry.pid);
  }

  const seen = new Set([rootPid]);
  const queue = [rootPid];
  while (queue.length > 0) {
    const pid = queue.shift();
    for (const childPid of byParent.get(pid) || []) {
      if (!seen.has(childPid)) {
        seen.add(childPid);
        queue.push(childPid);
      }
    }
  }
  return seen;
}

async function readProcIo(pid) {
  try {
    const content = await fs.promises.readFile(`/proc/${pid}/io`, "utf8");
    const result = {};
    for (const line of content.split(/\r?\n/)) {
      const [key, value] = line.split(":");
      if (key && value) {
        result[key.trim()] = Number.parseInt(value.trim(), 10) || 0;
      }
    }
    return result;
  } catch {
    return null;
  }
}

async function collectProcessTreeSample(rootPid = process.pid) {
  const { stdout } = await execFileAsync(
    "ps",
    ["-eo", "pid=,ppid=,%cpu=,rss=,comm="],
    {
      maxBuffer: 4 * 1024 * 1024,
    },
  );
  const processes = parsePsOutput(stdout);
  const pids = collectDescendantPids(processes, rootPid);
  const selected = processes.filter((entry) => pids.has(entry.pid));
  const ioValues = await Promise.all(
    selected.map(async (entry) => ({
      pid: entry.pid,
      io: await readProcIo(entry.pid),
    })),
  );
  return {
    capturedAt: new Date().toISOString(),
    pidCount: selected.length,
    totalCpuPercent: selected.reduce(
      (sum, entry) => sum + (entry.cpuPercent || 0),
      0,
    ),
    totalRssKb: selected.reduce((sum, entry) => sum + (entry.rssKb || 0), 0),
    readBytes: ioValues.reduce(
      (sum, entry) => sum + (entry.io?.read_bytes || 0),
      0,
    ),
    writeBytes: ioValues.reduce(
      (sum, entry) => sum + (entry.io?.write_bytes || 0),
      0,
    ),
    processes: selected,
  };
}

function parseDockerStats(rawJson) {
  if (!rawJson) {
    return null;
  }
  try {
    return JSON.parse(rawJson);
  } catch {
    return {
      raw: rawJson,
    };
  }
}

function parsePercentValue(value) {
  const normalized = String(value || "")
    .trim()
    .replace(/%$/, "");
  const parsed = Number.parseFloat(normalized);
  return Number.isFinite(parsed) ? parsed : 0;
}

async function readHostCpuSnapshot() {
  try {
    const content = await fs.promises.readFile("/proc/stat", "utf8");
    const cpuLine = content
      .split(/\r?\n/)
      .find((line) => line.startsWith("cpu "));
    if (!cpuLine) {
      return null;
    }
    const values = cpuLine
      .trim()
      .split(/\s+/)
      .slice(1)
      .map((value) => Number.parseInt(value, 10) || 0);
    const idle = (values[3] || 0) + (values[4] || 0);
    const total = values.reduce((sum, value) => sum + value, 0);
    return {
      total,
      idle,
    };
  } catch {
    return null;
  }
}

async function collectDockerSample(service = "postgis") {
  try {
    let target = service;
    try {
      const { stdout: serviceNames } = await execFileAsync(
        "docker",
        [
          "ps",
          "--filter",
          `label=com.docker.compose.service=${service}`,
          "--format",
          "{{.Names}}",
        ],
        {
          maxBuffer: 2 * 1024 * 1024,
        },
      );
      const resolved = String(serviceNames || "")
        .split(/\r?\n/)
        .map((line) => line.trim())
        .find(Boolean);
      if (resolved) {
        target = resolved;
      }
    } catch {}

    const { stdout } = await execFileAsync(
      "docker",
      ["stats", "--no-stream", "--format", "{{json .}}", target],
      {
        maxBuffer: 2 * 1024 * 1024,
      },
    );
    return {
      capturedAt: new Date().toISOString(),
      service,
      target,
      available: true,
      stats: parseDockerStats(String(stdout || "").trim()),
    };
  } catch (error) {
    return {
      capturedAt: new Date().toISOString(),
      service,
      available: false,
      error: error.message,
    };
  }
}

function calculateHostCpuPercents(samples = []) {
  const percentages = [];
  for (let index = 1; index < samples.length; index += 1) {
    const previous = samples[index - 1]?.hostCpu;
    const current = samples[index]?.hostCpu;
    if (!previous || !current) {
      continue;
    }
    const totalDelta = current.total - previous.total;
    const idleDelta = current.idle - previous.idle;
    if (totalDelta <= 0) {
      continue;
    }
    percentages.push(((totalDelta - idleDelta) / totalDelta) * 100);
  }
  return percentages;
}

function summarizeSamples(samples = [], dockerSamples = []) {
  if (!samples.length) {
    return {
      averageCpuPercent: 0,
      peakCpuPercent: 0,
      hostAverageCpuPercent: 0,
      hostPeakCpuPercent: 0,
      dockerAverageCpuPercent: 0,
      dockerPeakCpuPercent: 0,
      peakRssKb: 0,
      diskReadBytesDelta: 0,
      diskWriteBytesDelta: 0,
    };
  }
  const hostCpuPercents = calculateHostCpuPercents(samples);
  const dockerCpuPercents = dockerSamples
    .filter((sample) => sample.available)
    .map((sample) => parsePercentValue(sample.stats?.CPUPerc));
  return {
    averageCpuPercent:
      samples.reduce((sum, sample) => sum + (sample.totalCpuPercent || 0), 0) /
      samples.length,
    peakCpuPercent: Math.max(
      ...samples.map((sample) => sample.totalCpuPercent || 0),
    ),
    hostAverageCpuPercent: hostCpuPercents.length
      ? hostCpuPercents.reduce((sum, value) => sum + value, 0) /
        hostCpuPercents.length
      : 0,
    hostPeakCpuPercent: hostCpuPercents.length
      ? Math.max(...hostCpuPercents)
      : 0,
    dockerAverageCpuPercent: dockerCpuPercents.length
      ? dockerCpuPercents.reduce((sum, value) => sum + value, 0) /
        dockerCpuPercents.length
      : 0,
    dockerPeakCpuPercent: dockerCpuPercents.length
      ? Math.max(...dockerCpuPercents)
      : 0,
    peakRssKb: Math.max(...samples.map((sample) => sample.totalRssKb || 0)),
    diskReadBytesDelta:
      (samples.at(-1)?.readBytes || 0) - (samples[0]?.readBytes || 0),
    diskWriteBytesDelta:
      (samples.at(-1)?.writeBytes || 0) - (samples[0]?.writeBytes || 0),
  };
}

async function createSampler({
  dockerService = "postgis",
  intervalMs = 2000,
} = {}) {
  const resourceSamples = [];
  const dockerSamples = [];
  let timer = null;

  async function collectOnce() {
    const processSample = await collectProcessTreeSample(process.pid);
    processSample.hostCpu = await readHostCpuSnapshot();
    resourceSamples.push(processSample);
    dockerSamples.push(await collectDockerSample(dockerService));
  }

  return {
    async start() {
      await collectOnce();
      timer = setInterval(() => {
        void collectOnce();
      }, intervalMs);
    },
    async stop() {
      if (timer) {
        clearInterval(timer);
        timer = null;
      }
      await collectOnce();
      return {
        resourceSamples,
        dockerSamples,
        resourceSummary: summarizeSamples(resourceSamples, dockerSamples),
      };
    },
  };
}

async function queryPgActivitySnapshot(client, label, extra = {}) {
  if (!client) {
    return {
      label,
      capturedAt: new Date().toISOString(),
      unavailable: true,
      ...extra,
    };
  }

  try {
    const row = await client.queryOne(
      `
        WITH base AS (
          SELECT
            COALESCE(state, 'unknown') AS state,
            wait_event_type,
            EXTRACT(EPOCH FROM (now() - query_start)) AS query_age_seconds
          FROM pg_stat_activity
          WHERE datname = current_database()
        ),
        state_counts AS (
          SELECT state, COUNT(*)::integer AS count
          FROM base
          GROUP BY state
        )
        SELECT json_build_object(
          'total', (SELECT COUNT(*) FROM base),
          'active', (SELECT COUNT(*) FROM base WHERE state = 'active'),
          'idle', (SELECT COUNT(*) FROM base WHERE state = 'idle'),
          'waiting', (SELECT COUNT(*) FROM base WHERE wait_event_type IS NOT NULL),
          'maxActiveQueryAgeSeconds', COALESCE(
            (
              SELECT MAX(query_age_seconds)
              FROM base
              WHERE state = 'active'
            ),
            0
          ),
          'states', COALESCE(
            (SELECT jsonb_object_agg(state, count) FROM state_counts),
            '{}'::jsonb
          )
        )::text AS snapshot_json;
      `,
    );
    return {
      label,
      capturedAt: new Date().toISOString(),
      ...extra,
      snapshot: JSON.parse(String(row?.snapshot_json || "{}")),
    };
  } catch (error) {
    return {
      label,
      capturedAt: new Date().toISOString(),
      unavailable: true,
      error: error.message,
      ...extra,
    };
  }
}

async function writeJson(filePath, value) {
  await fs.promises.mkdir(path.dirname(filePath), { recursive: true });
  await fs.promises.writeFile(
    filePath,
    `${JSON.stringify(value, null, 2)}\n`,
    "utf8",
  );
}

async function runSqlProfileSample(rootDir, options, selectedSteps) {
  const client = createPostgisClient({ rootDir });
  try {
    await client.ensureReady();
    const output = {
      capturedAt: new Date().toISOString(),
      steps: {},
    };
    if (selectedSteps.includes("global-stations")) {
      output.steps["global-stations"] = await createGlobalStationsRepo(
        client,
      ).sampleBuildQueryPlans({
        country: options.country,
        asOf: options.asOf,
        sourceId: options.sourceId,
      });
    }
    if (selectedSteps.includes("merge-queue")) {
      output.steps["merge-queue"] = await createMergeQueueRepo(
        client,
      ).sampleRebuildQueryPlans({
        country: options.country,
        asOf: options.asOf,
      });
    }
    return output;
  } finally {
    await client.end();
  }
}

async function runBenchmarkOnce({
  runIndex,
  runType,
  options,
  selectedSteps,
  stepDefinitions,
  outputDir,
  benchmarkId,
}) {
  const runStartedAt = Date.now();
  const runId = `${benchmarkId}-${runType}-${String(runIndex).padStart(2, "0")}`;
  const sampler = await createSampler({
    dockerService: process.env.CANONICAL_DB_DOCKER_SERVICE || "postgis",
  });
  const pgClient = createPostgisClient({ rootDir: options.rootDir });
  let pgReady = false;
  try {
    await pgClient.ensureReady();
    pgReady = true;
  } catch {}

  const steps = [];
  let runError = null;
  let runArtifact = null;
  await sampler.start();

  try {
    for (const stepId of selectedSteps) {
      const def = stepDefinitions[stepId];
      const stepRunId = `${runId}-${stepId}`;
      const stepStartedAt = Date.now();
      const pgSnapshots = [];
      const phaseEvents = [];
      const phaseTasks = [];

      pgSnapshots.push(
        await queryPgActivitySnapshot(
          pgReady ? pgClient : null,
          "before_step",
          {
            stepId,
          },
        ),
      );

      const result = await def.run({
        rootDir: options.rootDir,
        runId: stepRunId,
        args: def.args,
        onPhase(event) {
          phaseEvents.push({
            type: "phase",
            stageId: event.stageId,
            phase: event.phase,
            capturedAt: new Date().toISOString(),
          });
          phaseTasks.push(
            queryPgActivitySnapshot(
              pgReady ? pgClient : null,
              "phase_transition",
              {
                stepId,
                stageId: event.stageId,
                phase: event.phase,
              },
            ).then((snapshot) => {
              pgSnapshots.push(snapshot);
            }),
          );
        },
        onInfo(event) {
          phaseEvents.push({
            type: "info",
            stageId: event.stageId,
            key: event.key,
            value: event.value,
            capturedAt: new Date().toISOString(),
          });
        },
      });

      await Promise.allSettled(phaseTasks);
      pgSnapshots.push(
        await queryPgActivitySnapshot(pgReady ? pgClient : null, "after_step", {
          stepId,
        }),
      );

      const stepArtifact = {
        stepId,
        label: def.label,
        args: def.args,
        durationMs: Date.now() - stepStartedAt,
        summary: result?.summary || null,
        metrics: result?.metrics || null,
        cacheHit: Boolean(result?.cacheHit),
        skippedUnchanged: Boolean(result?.skippedUnchanged),
        phaseEvents,
        pgSnapshots,
      };
      steps.push(stepArtifact);

      if (options.emitPhaseMetrics && stepArtifact.metrics) {
        process.stdout.write(
          `[benchmark-station-review] run=${runType}-${runIndex} step=${stepId} metrics=${JSON.stringify(stepArtifact.metrics)}\n`,
        );
      }
    }
  } catch (error) {
    runError = error;
  } finally {
    const samplerOutput = await sampler.stop();
    runArtifact = {
      benchmarkId,
      runIndex,
      runType,
      startedAt: new Date(runStartedAt).toISOString(),
      endedAt: new Date().toISOString(),
      totalDurationMs: Date.now() - runStartedAt,
      machineInfo: collectMachineInfo(),
      pipelineArgs: {
        fromStep: options.fromStep,
        toStep: options.toStep,
        skipSteps: options.skipSteps,
        country: options.country,
        asOf: options.asOf,
        sourceId: options.sourceId,
      },
      steps,
      resourceSamples: samplerOutput.resourceSamples,
      resourceSummary: samplerOutput.resourceSummary,
      dockerSamples: samplerOutput.dockerSamples,
      error: runError
        ? {
            message: runError.message,
            code: runError.code || "INTERNAL_ERROR",
          }
        : null,
    };
    await writeJson(
      path.join(
        outputDir,
        `run-${String(runIndex).padStart(2, "0")}-${runType}.json`,
      ),
      runArtifact,
    );
    await pgClient.end().catch(() => {});
  }

  if (runError) {
    throw runError;
  }

  return runArtifact;
}

function aggregateBenchmarks(artifacts = []) {
  const measured = artifacts.filter(
    (artifact) => artifact.runType === "measured",
  );
  const aggregate = {
    measuredRuns: measured.length,
    warmupRuns: artifacts.length - measured.length,
    medianTotalDurationMs: median(
      measured.map((artifact) => artifact.totalDurationMs),
    ),
    medianCpuPercent: median(
      measured.map(
        (artifact) => artifact.resourceSummary.averageCpuPercent || 0,
      ),
    ),
    medianHostCpuPercent: median(
      measured.map(
        (artifact) => artifact.resourceSummary.hostAverageCpuPercent || 0,
      ),
    ),
    medianDockerCpuPercent: median(
      measured.map(
        (artifact) => artifact.resourceSummary.dockerAverageCpuPercent || 0,
      ),
    ),
    peakRssKb: Math.max(
      0,
      ...measured.map((artifact) => artifact.resourceSummary.peakRssKb || 0),
    ),
    stepMedians: {},
    phaseMedians: {},
    outputCounts: {},
  };

  const stepIds = new Set();
  const phaseNames = new Set();
  for (const artifact of measured) {
    for (const step of artifact.steps || []) {
      stepIds.add(step.stepId);
      for (const phase of step.metrics?.phases || []) {
        phaseNames.add(`${step.stepId}:${phase.name}`);
      }
    }
  }

  for (const stepId of stepIds) {
    aggregate.stepMedians[stepId] = median(
      measured
        .map(
          (artifact) =>
            artifact.steps.find((step) => step.stepId === stepId)?.durationMs ||
            0,
        )
        .filter((value) => value > 0),
    );
  }

  for (const phaseKey of phaseNames) {
    const [stepId, phaseName] = phaseKey.split(":");
    aggregate.phaseMedians[phaseKey] = median(
      measured
        .map((artifact) => {
          const step = artifact.steps.find((entry) => entry.stepId === stepId);
          return (
            step?.metrics?.phases?.find((phase) => phase.name === phaseName)
              ?.durationMs || 0
          );
        })
        .filter((value) => value > 0),
    );
  }

  const lastMeasured = measured.at(-1);
  if (lastMeasured) {
    for (const step of lastMeasured.steps || []) {
      if (step.summary) {
        aggregate.outputCounts[step.stepId] = step.summary;
      }
    }
  }

  aggregate.topSlowPhases = Object.entries(aggregate.phaseMedians)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 3)
    .map(([phaseKey, durationMs]) => ({ phaseKey, durationMs }));

  return aggregate;
}

async function run() {
  try {
    const options = parseArgs(process.argv.slice(2));
    if (options.help) {
      printUsage();
      return;
    }

    const selectedSteps = selectSteps(options);
    if (selectedSteps.length === 0) {
      throw new AppError({
        code: "INVALID_REQUEST",
        message:
          "Selected benchmark step set is empty after applying --skip-step",
      });
    }
    const stepDefinitions = createStepDefinitions(options);
    const outputDir = path.join(
      options.rootDir,
      "reports",
      "qa",
      "pipeline-benchmarks",
      options.benchmarkId,
    );

    process.stdout.write(
      `[benchmark-station-review] benchmarkId=${options.benchmarkId} outputDir=${outputDir}\n`,
    );
    process.stdout.write(
      `[benchmark-station-review] steps=${selectedSteps.join(" -> ")} warmup=${options.warmupRuns} measured=${options.runs}\n`,
    );

    const artifacts = [];
    for (let i = 1; i <= options.warmupRuns; i += 1) {
      artifacts.push(
        await runBenchmarkOnce({
          runIndex: i,
          runType: "warmup",
          options,
          selectedSteps,
          stepDefinitions,
          outputDir,
          benchmarkId: options.benchmarkId,
        }),
      );
    }

    for (let i = 1; i <= options.runs; i += 1) {
      artifacts.push(
        await runBenchmarkOnce({
          runIndex: i,
          runType: "measured",
          options,
          selectedSteps,
          stepDefinitions,
          outputDir,
          benchmarkId: options.benchmarkId,
        }),
      );
    }

    const aggregate = {
      benchmarkId: options.benchmarkId,
      createdAt: new Date().toISOString(),
      machineInfo: collectMachineInfo(),
      pipelineArgs: {
        fromStep: options.fromStep,
        toStep: options.toStep,
        skipSteps: options.skipSteps,
        country: options.country,
        asOf: options.asOf,
        sourceId: options.sourceId,
        runs: options.runs,
        warmupRuns: options.warmupRuns,
      },
      summary: aggregateBenchmarks(artifacts),
      artifacts: artifacts.map((artifact) => ({
        runIndex: artifact.runIndex,
        runType: artifact.runType,
        totalDurationMs: artifact.totalDurationMs,
      })),
    };
    await writeJson(path.join(outputDir, "summary.json"), aggregate);

    if (options.sqlProfileSample) {
      const sqlProfiles = await runSqlProfileSample(
        options.rootDir,
        options,
        selectedSteps,
      );
      await writeJson(
        path.join(outputDir, "sql-profile-sample.json"),
        sqlProfiles,
      );
    }

    process.stdout.write(
      `[benchmark-station-review] median_total_ms=${aggregate.summary.medianTotalDurationMs}\n`,
    );
  } catch (error) {
    printCliError(
      "benchmark-station-review",
      error,
      "Station review benchmark failed",
    );
    process.exit(1);
  }
}

if (require.main === module) {
  void run();
}

module.exports = {
  run,
  _internal: {
    aggregateBenchmarks,
    buildStepArgs,
    parseArgs,
    selectSteps,
  },
};
