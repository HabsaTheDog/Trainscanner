const { execFileSync, spawnSync } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");
const { Pool } = require("pg");

const BASH_PATH = fs.existsSync("/usr/bin/bash")
  ? "/usr/bin/bash"
  : "/bin/bash";
const DOCKER_PATH = ["/usr/bin/docker", "/bin/docker"].find((filePath) =>
  fs.existsSync(filePath),
);
const hasDocker =
  Boolean(DOCKER_PATH) && spawnSync(DOCKER_PATH, ["--version"]).status === 0;
const shouldRunPostgisTests =
  hasDocker && process.env.ENABLE_POSTGIS_TESTS === "1";

function createDbEnv(dbName, overrides = {}) {
  return {
    ...process.env,
    CANONICAL_DB_MODE: "docker-compose",
    CANONICAL_DB_DOCKER_PROFILE: "pan-europe-data",
    CANONICAL_DB_DOCKER_SERVICE: "postgis",
    CANONICAL_DB_NAME: dbName,
    ...overrides,
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function ensureBootstrapped(repoRoot, dbEnv) {
  const scriptPath = path.join(repoRoot, "scripts", "data", "db-bootstrap.sh");

  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      execFileSync(BASH_PATH, [scriptPath, "--quiet", "--if-ready"], {
        cwd: repoRoot,
        env: dbEnv,
        encoding: "utf8",
      });
      return;
    } catch (error) {
      const output = `${error.stdout || ""}\n${error.stderr || ""}`;
      if (attempt < 3 && output.includes("tuple concurrently updated")) {
        await sleep(500 * attempt);
        continue;
      }
      throw error;
    }
  }
}

function ensureDockerServiceRunning(repoRoot, dbEnv) {
  const scriptPath = path.join(repoRoot, "scripts", "data", "db-bootstrap.sh");
  const bootstrapEnv = {
    ...dbEnv,
    CANONICAL_DB_NAME: process.env.CANONICAL_DB_NAME || "trainscanner",
  };

  execFileSync(BASH_PATH, [scriptPath, "--quiet"], {
    cwd: repoRoot,
    env: bootstrapEnv,
    encoding: "utf8",
  });
}

function createAdminPool(dbEnv) {
  return new Pool({
    host: dbEnv.CANONICAL_DB_HOST || "localhost",
    port: Number.parseInt(dbEnv.CANONICAL_DB_PORT || "55432", 10),
    user: dbEnv.CANONICAL_DB_USER || "trainscanner",
    password: dbEnv.CANONICAL_DB_PASSWORD || "trainscanner",
    database: "postgres",
  });
}

async function createDatabase(dbEnv) {
  const pool = createAdminPool(dbEnv);

  try {
    await pool.query(`CREATE DATABASE "${dbEnv.CANONICAL_DB_NAME}"`);
  } catch (error) {
    if (error?.code !== "42P04") {
      throw error;
    }
  } finally {
    await pool.end();
  }
}

async function dropDatabase(dbEnv) {
  const pool = createAdminPool(dbEnv);

  try {
    await pool.query(
      `DROP DATABASE IF EXISTS "${dbEnv.CANONICAL_DB_NAME}" WITH (FORCE)`,
    );
  } finally {
    await pool.end();
  }
}

module.exports = {
  BASH_PATH,
  createDatabase,
  createDbEnv,
  dropDatabase,
  ensureDockerServiceRunning,
  ensureBootstrapped,
  shouldRunPostgisTests,
};
