#!/usr/bin/env node
const { AppError } = require("../core/errors");
const { createPostgisClient } = require("../data/postgis/client");
const { parsePipelineCliArgs, printCliError } = require("./pipeline-common");

function printUsage() {
  process.stdout.write(
    "Usage: scripts/data/reset-station-review-data.sh --yes [--mode full|qa-derived|export-schedule]\n",
  );
  process.stdout.write("\n");
  process.stdout.write(
    "Delete pan-European station build, QA merge data, and/or export timetable data.\n",
  );
  process.stdout.write("\n");
  process.stdout.write("Options:\n");
  process.stdout.write("  --yes              Confirm destructive reset\n");
  process.stdout.write(
    "  --mode <mode>      Reset mode: full (default), qa-derived, export-schedule\n",
  );
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
    mode: "full",
  };

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === "--yes") {
      options.confirmed = true;
      continue;
    }
    if (arg === "--mode") {
      const value = String(args[index + 1] || "")
        .trim()
        .toLowerCase();
      if (!value) {
        throw new AppError({
          code: "INVALID_REQUEST",
          message: "Missing value for --mode",
        });
      }
      if (!["full", "qa-derived", "export-schedule"].includes(value)) {
        throw new AppError({
          code: "INVALID_REQUEST",
          message:
            "Invalid --mode value (expected full, qa-derived, or export-schedule)",
        });
      }
      options.mode = value;
      index += 1;
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
            (SELECT COUNT(*)::integer FROM qa_provider_stop_place_routes) AS qa_provider_routes,
            (SELECT COUNT(*)::integer FROM qa_provider_stop_place_adjacencies) AS qa_provider_adjacencies,
            (SELECT COUNT(*)::integer FROM qa_global_station_routes) AS qa_global_routes,
            (SELECT COUNT(*)::integer FROM qa_global_station_adjacencies) AS qa_global_adjacencies,
            (SELECT COUNT(*)::integer FROM qa_merge_clusters) AS merge_clusters,
            (SELECT COUNT(*)::integer FROM qa_merge_eligible_pairs) AS merge_eligible_pairs,
            (SELECT COUNT(*)::integer FROM qa_merge_decisions) AS merge_decisions,
            (SELECT COUNT(*)::integer FROM pipeline_stage_materializations) AS pipeline_stage_materializations,
            (SELECT COUNT(*)::integer FROM pipeline_stage_runs) AS pipeline_stage_runs,
            (SELECT COUNT(*)::integer FROM qa_publish_batches) AS qa_publish_batches,
            (SELECT COUNT(*)::integer FROM qa_publish_batch_decisions) AS qa_publish_batch_decisions,
            (SELECT COUNT(*)::integer FROM timetable_trips) AS timetable_trips,
            (SELECT COUNT(*)::integer FROM timetable_trip_stop_times) AS timetable_trip_stop_times
          `,
        );
        return row || {};
      };

      const before = await countRows();

      const truncateTargetsByMode = {
        full: [
          "qa_publish_batch_decisions",
          "qa_publish_batches",
          "qa_merge_cluster_workspace_versions",
          "qa_merge_cluster_workspaces",
          "qa_merge_decision_members",
          "qa_merge_decisions",
          "qa_merge_cluster_evidence",
          "qa_merge_cluster_candidates",
          "qa_merge_eligible_pairs",
          "qa_merge_clusters",
          "global_station_reference_matches",
          "qa_global_station_adjacencies",
          "qa_global_station_routes",
          "qa_provider_stop_place_adjacencies",
          "qa_provider_stop_place_routes",
          "transfer_edges",
          "timetable_trip_stop_times",
          "timetable_trips",
          "provider_global_stop_point_mappings",
          "provider_global_station_mappings",
          "global_stop_points",
          "global_stations",
          "raw_provider_stop_points",
          "raw_provider_stop_places",
          "provider_datasets",
          "pipeline_stage_runs",
          "pipeline_stage_materializations",
          "import_runs",
        ],
        "qa-derived": [
          "qa_merge_cluster_workspace_versions",
          "qa_merge_cluster_workspaces",
          "qa_merge_decision_members",
          "qa_merge_decisions",
          "qa_merge_cluster_evidence",
          "qa_merge_cluster_candidates",
          "qa_merge_eligible_pairs",
          "qa_merge_clusters",
          "global_station_reference_matches",
          "qa_global_station_adjacencies",
          "qa_global_station_routes",
          "qa_provider_stop_place_adjacencies",
          "qa_provider_stop_place_routes",
          "transfer_edges",
          "provider_global_stop_point_mappings",
          "provider_global_station_mappings",
          "global_stop_points",
          "global_stations",
        ],
        "export-schedule": ["timetable_trip_stop_times", "timetable_trips"],
      };

      const systemStateDeleteSqlByMode = {
        full: "",
        "qa-derived": `
          DELETE FROM pipeline_stage_materializations
          WHERE stage_id IN (
            'stop-topology',
            'qa-network-context',
            'global-stations',
            'reference-data',
            'qa-network-projection',
            'merge-queue'
          );
          DELETE FROM pipeline_stage_runs
          WHERE stage_id IN (
            'stop-topology',
            'qa-network-context',
            'global-stations',
            'reference-data',
            'qa-network-projection',
            'merge-queue'
          );
        `,
        "export-schedule": `
          DELETE FROM pipeline_stage_materializations
          WHERE stage_id IN ('export-schedule');
          DELETE FROM pipeline_stage_runs
          WHERE stage_id IN ('export-schedule');
        `,
      };

      const truncateTargets = truncateTargetsByMode[options.mode];
      const systemStateDeleteSql = systemStateDeleteSqlByMode[options.mode];
      await client.exec(
        `
        BEGIN;
        TRUNCATE TABLE
          ${truncateTargets.join(",\n          ")}
        RESTART IDENTITY CASCADE;
        ${systemStateDeleteSql}
        COMMIT;
        `,
      );

      const after = await countRows();

      process.stdout.write(
        `${JSON.stringify({
          ok: true,
          mode: options.mode,
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
