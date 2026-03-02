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
    "Delete all station-review curation data, including legacy override rows.\n",
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
            (SELECT COUNT(*)::integer FROM canonical_review_queue) AS review_queue_items,
            (SELECT COUNT(*)::integer FROM canonical_station_overrides) AS legacy_override_items,
            (SELECT COUNT(*)::integer FROM qa_station_clusters) AS clusters,
            (SELECT COUNT(*)::integer FROM qa_station_cluster_decisions) AS decisions,
            (SELECT COUNT(*)::integer FROM qa_station_groups) AS groups,
            (SELECT COUNT(*)::integer FROM qa_curated_stations) AS curated_stations
          `,
        );
        return row || {};
      };

      const before = await countRows();

      await client.exec(
        `
        BEGIN;
        TRUNCATE TABLE
          qa_curated_station_field_provenance,
          qa_curated_station_lineage,
          qa_curated_station_members,
          qa_curated_stations,
          qa_station_group_section_links,
          qa_station_group_section_members,
          qa_station_group_sections,
          qa_station_groups,
          qa_station_cluster_decision_members,
          qa_station_cluster_decisions,
          qa_station_cluster_queue_items,
          qa_station_cluster_evidence,
          qa_station_cluster_candidates,
          qa_station_clusters,
          qa_station_segment_links,
          qa_station_segments,
          qa_station_complexes,
          station_segment_line_links,
          canonical_line_identities,
          qa_station_naming_overrides,
          qa_station_display_names,
          canonical_station_overrides,
          canonical_review_queue
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
