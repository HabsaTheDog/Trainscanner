const { execFile } = require("node:child_process");
const { promisify } = require("node:util");
const path = require("node:path");

const execFileAsync = promisify(execFile);

function createIngestionActivities(_dbClient, config) {
  // Use the root directory where the scripts live
  const scriptsDir = path.resolve(
    config.rootDir || process.cwd(),
    "..",
    "scripts",
    "data",
  );

  return {
    async runDbBootstrap() {
      // Re-use the existing bash script for schema bootstrap
      const { stdout, stderr } = await execFileAsync(
        path.join(scriptsDir, "db-bootstrap.sh"),
        [],
        {
          cwd: scriptsDir,
          env: process.env,
        },
      );
      return { stdout, stderr };
    },

    async runFetchSources(args) {
      // Step 1: Just run the fetch logic from the bash script
      const safeArgs = args || [];
      const { stdout, stderr } = await execFileAsync(
        path.join(scriptsDir, "fetch-dach-sources.sh"),
        safeArgs,
        {
          cwd: scriptsDir,
          env: process.env,
        },
      );
      return { stdout, stderr };
    },

    // Step 2 (NeTEx ingest) is now handled natively by the Rust Worker (`extract_netex_stops`)!

    async buildCanonicalAndReviewQueue(args) {
      // Step 3 & 4: the rest of the pipeline
      const safeArgs = args || [];
      const { stdout, stderr } = await execFileAsync(
        path.join(scriptsDir, "check-canonical-pipeline.sh"),
        safeArgs,
        {
          cwd: scriptsDir,
          env: process.env,
        },
      );
      return { stdout, stderr };
    },
    async checkMotisReady() {
      const { stdout, stderr } = await execFileAsync(
        path.resolve(scriptsDir, "..", "check-motis-data.sh"),
        [],
        {
          cwd: path.resolve(scriptsDir, ".."),
          env: process.env,
        },
      );
      return { stdout, stderr };
    },
  };
}

module.exports = {
  createIngestionActivities,
};
