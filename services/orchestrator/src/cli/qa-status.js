#!/usr/bin/env node
const { AppError } = require("../core/errors");
const { createPostgisClient } = require("../data/postgis/client");
const {
  createPipelineStageRepo,
} = require("../data/postgis/repositories/pipeline-stage-repo");
const { parsePipelineCliArgs, printCliError } = require("./pipeline-common");

const QA_STAGE_IDS = [
  "fetch",
  "stop-topology",
  "qa-network-context",
  "global-stations",
  "reference-data",
  "qa-network-projection",
  "merge-queue",
  "export-schedule",
];

function printUsage() {
  process.stdout.write(
    "Usage: scripts/data/qa-status.sh [--country ISO2] [--as-of YYYY-MM-DD] [--source-id ID] [--json]\n",
  );
}

function parseArgs(argv = []) {
  const parsed = parsePipelineCliArgs(argv);
  const args = Array.isArray(parsed.passthroughArgs)
    ? parsed.passthroughArgs
    : [];
  const options = {
    rootDir: parsed.rootDir,
    country: "",
    asOf: "",
    sourceId: "",
    json: false,
    help: false,
  };

  for (let index = 0; index < args.length; index += 1) {
    const token = args[index];
    switch (token) {
      case "--country":
        options.country = String(args[index + 1] || "")
          .trim()
          .toUpperCase();
        index += 1;
        break;
      case "--as-of":
        options.asOf = String(args[index + 1] || "").trim();
        index += 1;
        break;
      case "--source-id":
        options.sourceId = String(args[index + 1] || "").trim();
        index += 1;
        break;
      case "--json":
        options.json = true;
        break;
      case "-h":
      case "--help":
        options.help = true;
        break;
      default:
        throw new AppError({
          code: "INVALID_REQUEST",
          message: `Unknown argument: ${token}`,
        });
    }
  }

  return options;
}

function applyScopeFilter(rows, options) {
  return rows.filter((row) => {
    if (
      options.country &&
      String(row.scope_country || "") !== options.country
    ) {
      return false;
    }
    if (options.asOf && String(row.scope_as_of || "") !== options.asOf) {
      return false;
    }
    if (
      options.sourceId &&
      String(row.scope_source_id || "") !== options.sourceId
    ) {
      return false;
    }
    return true;
  });
}

function summarizeRow(row, referenceStaleAfterHours) {
  const timing = row.timing_summary || {};
  const output = row.output_summary || {};
  const durationMs = Number(timing.totalDurationMs || 0);
  const referenceAgeHours =
    row.stage_id === "reference-data" && row.last_finished_at
      ? Math.max(
          0,
          (Date.now() - new Date(row.last_finished_at).getTime()) /
            (1000 * 60 * 60),
        )
      : 0;
  const referenceStale =
    row.stage_id === "reference-data" &&
    (!row.last_finished_at || referenceAgeHours > referenceStaleAfterHours);

  return {
    stageId: row.stage_id,
    scopeKey: row.scope_key,
    scopeCountry: row.scope_country || "",
    scopeAsOf: row.scope_as_of || "",
    scopeSourceId: row.scope_source_id || "",
    status: referenceStale ? "stale" : row.status,
    lastFinishedAt: row.last_finished_at || null,
    durationMs,
    cacheHit: Boolean(timing.cacheHit),
    skippedUnchanged: Boolean(timing.skippedUnchanged),
    referenceStale,
    timetableTouched:
      Number(output.timetableTripsTouched || 0) > 0 ||
      Number(output.timetableTripStopTimesTouched || 0) > 0,
  };
}

async function run() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printUsage();
    return;
  }

  const referenceStaleAfterHours = Number.parseInt(
    process.env.QA_REFERENCE_STALE_AFTER_HOURS || "168",
    10,
  );

  const client = createPostgisClient({ rootDir: options.rootDir });
  try {
    await client.ensureReady();
    const repo = createPipelineStageRepo(client);
    const rows = await repo.listStageStatus(QA_STAGE_IDS);
    const filtered = applyScopeFilter(rows, options).map((row) =>
      summarizeRow(row, referenceStaleAfterHours),
    );

    if (options.json) {
      process.stdout.write(`${JSON.stringify({ stages: filtered })}\n`);
      return;
    }

    for (const row of filtered) {
      process.stdout.write(
        [
          row.stageId.padEnd(22, " "),
          `scope=${row.scopeKey}`,
          `status=${row.status}`,
          `duration_ms=${row.durationMs}`,
          `cache_hit=${row.cacheHit}`,
          `skipped_unchanged=${row.skippedUnchanged}`,
          `reference_stale=${row.referenceStale}`,
          `qa_touched_timetable=${row.timetableTouched}`,
          `last_finished_at=${row.lastFinishedAt || "-"}`,
        ].join(" "),
      );
      process.stdout.write("\n");
    }
  } finally {
    await client.end();
  }
}

void run().catch((error) => {
  printCliError("qa-status", error, "QA status failed");
  process.exit(1);
});
