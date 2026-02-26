#!/usr/bin/env node
const crypto = require("node:crypto");

const { AppError } = require("../core/errors");
const { fetchSources } = require("../domains/source-discovery/service");
const { ingestNetex } = require("../domains/ingest/service");
const {
  buildCanonicalStations,
  buildReviewQueue,
} = require("../domains/canonical/service");
const { parsePipelineCliArgs, printCliError } = require("./pipeline-common");
const { isStrictIsoDate } = require("../core/date");

const STEP_IDS = ["fetch", "ingest", "canonical", "review-queue"];

function parseStepToken(raw) {
  const token = String(raw || "")
    .trim()
    .toLowerCase();
  if (!token) {
    return "";
  }
  if (token === "queue" || token === "review" || token === "review_queue") {
    return "review-queue";
  }
  return token;
}

function tokenizeStepList(rawValue) {
  return String(rawValue || "")
    .split(",")
    .map((value) => parseStepToken(value))
    .filter(Boolean);
}

function printUsage() {
  process.stdout.write(
    "Usage: scripts/data/refresh-station-review.sh [options]\n",
  );
  process.stdout.write("\n");
  process.stdout.write(
    "Run station review refresh stages in the terminal with live logs.\n",
  );
  process.stdout.write("\n");
  process.stdout.write("Options:\n");
  process.stdout.write(
    "  --country DE|AT|CH          Restrict refresh scope to one country (default: all DACH)\n",
  );
  process.stdout.write(
    "  --as-of YYYY-MM-DD          Snapshot date override for all stages\n",
  );
  process.stdout.write(
    "  --source-id <id>            Restrict fetch/ingest/canonical to one source id\n",
  );
  process.stdout.write(
    "  --only <list>               Comma-separated steps: fetch,ingest,canonical,review-queue\n",
  );
  process.stdout.write(
    "  --from-step <step>          Start from step: fetch|ingest|canonical|review-queue\n",
  );
  process.stdout.write(
    "  --to-step <step>            Stop after step: fetch|ingest|canonical|review-queue\n",
  );
  process.stdout.write("  --skip-fetch                Skip fetch step\n");
  process.stdout.write("  --skip-ingest               Skip ingest step\n");
  process.stdout.write(
    "  --skip-canonical            Skip canonical build step\n",
  );
  process.stdout.write(
    "  --skip-review-queue         Skip review queue build step\n",
  );
  process.stdout.write(
    "  --dry-run                   Print resolved plan without executing stages\n",
  );
  process.stdout.write(
    "  --run-id <id>               Optional run id prefix for stage logs\n",
  );
  process.stdout.write(
    "  --root <path>               Repo root (default: cwd)\n",
  );
  process.stdout.write("  -h, --help                  Show this help\n");
}

function assertStepId(stepId, flagName) {
  if (STEP_IDS.includes(stepId)) {
    return;
  }
  throw new AppError({
    code: "INVALID_REQUEST",
    message: `${flagName} must be one of fetch|ingest|canonical|review-queue`,
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

function applyOptionWithValue(options, tokens, index, flagName, applyValue) {
  const value = readRequiredValue(tokens, index, flagName);
  applyValue(value);
  return index + 1;
}

function parseArgsToken(options, tokens, index) {
  const arg = tokens[index];
  const skipFlags = {
    "--skip-fetch": "fetch",
    "--skip-ingest": "ingest",
    "--skip-canonical": "canonical",
    "--skip-review-queue": "review-queue",
  };

  switch (arg) {
    case "-h":
    case "--help":
      options.help = true;
      return index;
    case "--country":
      return applyOptionWithValue(
        options,
        tokens,
        index,
        "--country",
        (value) => {
          options.country = value.toUpperCase();
        },
      );
    case "--as-of":
      return applyOptionWithValue(options, tokens, index, "--as-of", (value) => {
        options.asOf = value;
      });
    case "--source-id":
      return applyOptionWithValue(
        options,
        tokens,
        index,
        "--source-id",
        (value) => {
          options.sourceId = value;
        },
      );
    case "--only":
      return applyOptionWithValue(options, tokens, index, "--only", (value) => {
        options.onlySteps.push(...tokenizeStepList(value));
      });
    case "--from-step":
      return applyOptionWithValue(
        options,
        tokens,
        index,
        "--from-step",
        (value) => {
          options.fromStep = parseStepToken(value);
        },
      );
    case "--to-step":
      return applyOptionWithValue(options, tokens, index, "--to-step", (value) => {
        options.toStep = parseStepToken(value);
      });
    case "--dry-run":
      options.dryRun = true;
      return index;
    default:
      if (skipFlags[arg]) {
        options.skipSteps.add(skipFlags[arg]);
        return index;
      }
      throw new AppError({
        code: "INVALID_REQUEST",
        message: `Unknown argument: ${arg}`,
      });
  }
}

function parseArgs(argv = []) {
  const parsed = parsePipelineCliArgs(argv);
  const passthrough = Array.isArray(parsed.passthroughArgs)
    ? parsed.passthroughArgs
    : [];

  const options = {
    rootDir: parsed.rootDir,
    runId: parsed.runId || "",
    country: "",
    asOf: "",
    sourceId: "",
    onlySteps: [],
    fromStep: "",
    toStep: "",
    skipSteps: new Set(),
    dryRun: false,
  };

  for (let i = 0; i < passthrough.length; i += 1) {
    i = parseArgsToken(options, passthrough, i);
  }

  if (options.help) {
    return options;
  }

  if (options.country && !["DE", "AT", "CH"].includes(options.country)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: "country must be one of 'DE', 'AT', 'CH'",
    });
  }

  if (options.asOf && !isStrictIsoDate(options.asOf)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: "as-of must be an ISO date in YYYY-MM-DD format",
    });
  }

  if (options.fromStep) {
    assertStepId(options.fromStep, "--from-step");
  }

  if (options.toStep) {
    assertStepId(options.toStep, "--to-step");
  }

  for (const step of options.onlySteps) {
    assertStepId(step, "--only");
  }

  return options;
}

function filterSelectedSteps(options) {
  let selected = STEP_IDS.slice();

  if (options.onlySteps.length > 0) {
    selected = STEP_IDS.filter((stepId) => options.onlySteps.includes(stepId));
  }

  selected = selected.filter((stepId) => !options.skipSteps.has(stepId));

  if (options.fromStep) {
    const fromIndex = STEP_IDS.indexOf(options.fromStep);
    selected = selected.filter(
      (stepId) => STEP_IDS.indexOf(stepId) >= fromIndex,
    );
  }

  if (options.toStep) {
    const toIndex = STEP_IDS.indexOf(options.toStep);
    selected = selected.filter((stepId) => STEP_IDS.indexOf(stepId) <= toIndex);
  }

  if (selected.length === 0) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: "No pipeline steps selected after applying filters",
    });
  }

  return selected;
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
  appendArgIfSet(args, "--country", options.country);

  if (stepId !== "review-queue") {
    appendArgIfSet(args, "--source-id", options.sourceId);
  }

  return args;
}

function toStepDefinitions(options) {
  return {
    fetch: {
      label: "Fetch DACH sources",
      args: buildStepArgs(options, "fetch"),
      run: (runOptions) =>
        fetchSources({
          rootDir: runOptions.rootDir,
          runId: runOptions.runId,
          args: runOptions.args,
        }),
    },
    ingest: {
      label: "Ingest NeTEx snapshots",
      args: buildStepArgs(options, "ingest"),
      run: (runOptions) =>
        ingestNetex({
          rootDir: runOptions.rootDir,
          runId: runOptions.runId,
          args: runOptions.args,
          jobOrchestrationEnabled: false,
        }),
    },
    canonical: {
      label: "Build canonical stations",
      args: buildStepArgs(options, "canonical"),
      run: (runOptions) =>
        buildCanonicalStations({
          rootDir: runOptions.rootDir,
          runId: runOptions.runId,
          args: runOptions.args,
          jobOrchestrationEnabled: false,
        }),
    },
    "review-queue": {
      label: "Build station review queue",
      args: buildStepArgs(options, "review-queue"),
      run: (runOptions) =>
        buildReviewQueue({
          rootDir: runOptions.rootDir,
          runId: runOptions.runId,
          args: runOptions.args,
          jobOrchestrationEnabled: false,
        }),
    },
  };
}

function formatArgs(args = []) {
  if (!Array.isArray(args) || args.length === 0) {
    return "(no extra args)";
  }
  return args.join(" ");
}

function run() {
  return (async () => {
    try {
      const options = parseArgs(process.argv.slice(2));
      if (options.help) {
        printUsage();
        return;
      }

      const selectedSteps = filterSelectedSteps(options);
      const stepDefs = toStepDefinitions(options);
      const runIdBase =
        String(options.runId || "").trim() || crypto.randomUUID();

      process.stdout.write(`[refresh-station-review] runId=${runIdBase}\n`);
      process.stdout.write(
        `[refresh-station-review] scope country=${options.country || "ALL"} as-of=${options.asOf || "latest"} source-id=${options.sourceId || "ALL"}\n`,
      );
      process.stdout.write(
        `[refresh-station-review] selected steps: ${selectedSteps.join(" -> ")}\n`,
      );

      if (options.dryRun) {
        for (const stepId of selectedSteps) {
          const def = stepDefs[stepId];
          process.stdout.write(
            `[refresh-station-review] dry-run ${stepId}: ${def.label} ${formatArgs(def.args)}\n`,
          );
        }
        return;
      }

      const startedAt = Date.now();
      for (let index = 0; index < selectedSteps.length; index += 1) {
        const stepId = selectedSteps[index];
        const def = stepDefs[stepId];
        const stepRunId = `${runIdBase}-${stepId.replaceAll(/[^a-z0-9]+/gi, "-")}`;
        const stepStartedAt = Date.now();

        process.stdout.write(
          `[refresh-station-review] (${index + 1}/${selectedSteps.length}) ${stepId}: ${def.label} ${formatArgs(def.args)}\n`,
        );

        await def.run({
          rootDir: options.rootDir,
          runId: stepRunId,
          args: def.args,
        });

        const elapsedMs = Date.now() - stepStartedAt;
        process.stdout.write(
          `[refresh-station-review] completed ${stepId} in ${(elapsedMs / 1000).toFixed(1)}s\n`,
        );
      }

      const totalMs = Date.now() - startedAt;
      process.stdout.write(
        `[refresh-station-review] done in ${(totalMs / 1000).toFixed(1)}s\n`,
      );
    } catch (err) {
      printCliError(
        "refresh-station-review",
        err,
        "Station review refresh pipeline failed",
      );
      process.exit(1);
    }
  })();
}

void run();
