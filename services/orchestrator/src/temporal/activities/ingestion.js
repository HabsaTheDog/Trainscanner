const { execFile } = require("node:child_process");
const fs = require("node:fs");
const { promisify } = require("node:util");
const path = require("node:path");

const execFileAsync = promisify(execFile);

function resolveRepoRoot(config = {}) {
  const base = path.resolve(config.rootDir || process.cwd());
  const candidates = [
    base,
    path.resolve(base, ".."),
    path.resolve(__dirname, "..", "..", "..", "..", ".."),
  ];

  for (const candidate of candidates) {
    if (
      fs.existsSync(path.join(candidate, "scripts", "data", "db-bootstrap.sh"))
    ) {
      return candidate;
    }
  }

  throw new Error(
    "Could not resolve repository root for ingestion scripts (scripts/data/db-bootstrap.sh)",
  );
}

async function runShellScript(execRunner, scriptPath, args = [], options = {}) {
  return execRunner("bash", [scriptPath, ...(args || [])], options);
}

function selectGlobalBuildArgs(args = []) {
  const safeArgs = Array.isArray(args) ? args : [];
  const selected = [];

  for (let index = 0; index < safeArgs.length; index += 1) {
    const token = String(safeArgs[index] || "");
    if (token === "--as-of") {
      const value = safeArgs[index + 1];
      if (value !== undefined) {
        selected.push(token, value);
      }
      index += 1;
    }
  }

  return selected;
}

function createIngestionActivities(_dbClient, config = {}) {
  const execRunner = config.execFileAsync || execFileAsync;
  const repoRoot = resolveRepoRoot(config);
  const scriptsDir = path.join(repoRoot, "scripts", "data");

  return {
    async runDbBootstrap() {
      const { stdout, stderr } = await runShellScript(
        execRunner,
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
      const { stdout, stderr } = await runShellScript(
        execRunner,
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
      const safeArgs = selectGlobalBuildArgs(args);
      const globalStationsResult = await runShellScript(
        execRunner,
        path.join(scriptsDir, "build-global-stations.sh"),
        safeArgs,
        {
          cwd: scriptsDir,
          env: process.env,
        },
      );
      const mergeQueueResult = await runShellScript(
        execRunner,
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
      const { stdout, stderr } = await runShellScript(
        execRunner,
        path.join(repoRoot, "scripts", "check-motis-data.sh"),
        [],
        {
          cwd: path.join(repoRoot, "scripts"),
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
