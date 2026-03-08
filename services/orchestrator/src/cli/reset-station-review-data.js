#!/usr/bin/env node
const { AppError } = require("../core/errors");
const { createPostgisClient } = require("../data/postgis/client");
const { parsePipelineCliArgs, printCliError } = require("./pipeline-common");

function printUsage() {
  process.stdout.write(
    "Usage: scripts/data/reset-station-review-data.sh --yes\n",
  );
  process.stdout.write("\n");
  process.stdout.write(
    "Delete pan-European station build and QA merge data.\n",
  );
  process.stdout.write("\n");
  process.stdout.write("Options:\n");
  process.stdout.write("  --yes              Confirm destructive reset\n");
  process.stdout.write("  --root <path>      Repo root (default: cwd)\n");
  process.stdout.write("  -h, --help         Show this help\n");
}

function parseArgs(argv = []) {
  const parsed = parsePipelineCliArgs(argv);
  const args = Array.isArray(parsed.passthroughArgs)
    ? parsed.passthroughArgs
    : [];

  const options = {
    rootDir: parsed.rootDir,
    confirmed: false,
    help: false,
  };

  for (const arg of args) {
    if (arg === "--yes") {
      options.confirmed = true;
      continue;
    }
    if (arg === "--help" || arg === "-h") {
      options.help = true;
      continue;
    }
    throw new AppError({
      code: "INVALID_REQUEST",
      message: `Unknown argument: ${arg}`,
    });
  }

  return options;
}

function run() {
  return (async () => {
    try {
      const options = parseArgs(process.argv.slice(2));
      if (options.help) {
        printUsage();
        return;
      }
      if (!options.confirmed) {
        throw new AppError({
          code: "INVALID_REQUEST",
          message: "Refusing to reset station-review data without --yes",
        });
      }

      const client = createPostgisClient({ rootDir: options.rootDir });
      await client.ensureReady();

      const countRows = async () => {
        const row = await client.queryOne(
          `
          SELECT
            (SELECT COUNT(*)::integer FROM provider_datasets) AS provider_datasets,
            (SELECT COUNT(*)::integer FROM raw_provider_stop_places) AS raw_stop_places,
            (SELECT COUNT(*)::integer FROM raw_provider_stop_points) AS raw_stop_points,
            (SELECT COUNT(*)::integer FROM global_stations) AS global_stations,
            (SELECT COUNT(*)::integer FROM global_stop_points) AS global_stop_points,
            (SELECT COUNT(*)::integer FROM provider_global_station_mappings) AS station_mappings,
            (SELECT COUNT(*)::integer FROM provider_global_stop_point_mappings) AS stop_point_mappings,
            (SELECT COUNT(*)::integer FROM qa_merge_clusters) AS merge_clusters,
            (SELECT COUNT(*)::integer FROM qa_merge_decisions) AS merge_decisions
          `,
        );
        return row || {};
      };

      const before = await countRows();

      await client.exec(
        `
        BEGIN;
        TRUNCATE TABLE
          qa_merge_decision_members,
          qa_merge_decisions,
          qa_merge_cluster_evidence,
          qa_merge_cluster_candidates,
          qa_merge_clusters,
          transfer_edges,
          timetable_trip_stop_times,
          timetable_trips,
          provider_global_stop_point_mappings,
          provider_global_station_mappings,
          global_stop_points,
          global_stations,
          raw_provider_stop_points,
          raw_provider_stop_places,
          provider_datasets,
          import_runs
        RESTART IDENTITY CASCADE;
        COMMIT;
        `,
      );

      const after = await countRows();

      process.stdout.write(
        `${JSON.stringify({
          ok: true,
          before,
          after,
        })}\n`,
      );
    } catch (err) {
      printCliError(
        "reset-station-review-data",
        err,
        "Station-review data reset failed",
      );
      process.exit(1);
    }
  })();
}

void run();
