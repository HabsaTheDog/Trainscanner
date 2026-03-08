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
      const safeArgs = args || [];
      const { stdout, stderr } = await execFileAsync(
        path.join(scriptsDir, "fetch-sources.sh"),
        safeArgs,
        {
          cwd: scriptsDir,
          env: process.env,
        },
      );
      return { stdout, stderr };
    },

    async buildGlobalModel(args) {
      const safeArgs = args || [];
      const globalStationsResult = await execFileAsync(
        path.join(scriptsDir, "build-global-stations.sh"),
        safeArgs,
        {
          cwd: scriptsDir,
          env: process.env,
        },
      );
      const mergeQueueResult = await execFileAsync(
        path.join(scriptsDir, "build-global-merge-queue.sh"),
        safeArgs,
        {
          cwd: scriptsDir,
          env: process.env,
        },
      );
      return {
        stdout: `${globalStationsResult.stdout}\n${mergeQueueResult.stdout}`,
        stderr: `${globalStationsResult.stderr}\n${mergeQueueResult.stderr}`,
      };
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
