const fs = require("node:fs");
const path = require("node:path");
const { spawn } = require("node:child_process");
const crypto = require("node:crypto");

const { AppError } = require("./errors");
const { generateId } = require("./ids");
const { createLogger } = require("../logger");

function resolveRepoRoot(startPath = __dirname) {
  return path.resolve(startPath, "../../../..");
}

function toSafeArgs(args) {
  if (!Array.isArray(args)) {
    return [];
  }
  return args.map((arg) => String(arg));
}

function createPipelineLogger(
  rootDir,
  service,
  runId,
  loggerFactory = createLogger,
) {
  const stateDir = path.join(rootDir, "services", "orchestrator", "state");
  const logPath = path.join(stateDir, "pipeline.log");
  return loggerFactory(logPath, {
    service,
    runId,
  });
}

function buildIdempotencyKey(service, args = []) {
  const payload = JSON.stringify({
    service: String(service || ""),
    args: Array.isArray(args) ? args.map((arg) => String(arg)) : [],
  });
  return crypto.createHash("sha256").update(payload).digest("hex");
}

function spawnInherit(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd,
      env: options.env,
      stdio: "inherit",
    });

    child.on("error", (err) => reject(err));
    child.on("close", (exitCode, signal) => {
      resolve({
        exitCode: Number.isInteger(exitCode) ? exitCode : 1,
        signal: signal || null,
      });
    });
  });
}

async function runLegacyDataScript(options = {}) {
  const rootDir = path.resolve(options.rootDir || resolveRepoRoot());
  const scriptFile = String(options.scriptFile || "").trim();
  const service = String(options.service || "pipeline").trim();
  const errorCode = String(options.errorCode || "INTERNAL_ERROR").trim();
  const args = toSafeArgs(options.args);
  const runId =
    String(options.runId || "").trim() ||
    generateId(service.replaceAll(/[^A-Za-z0-9]+/g, "-"));
  const runCommand = options.runCommand || spawnInherit;

  if (!scriptFile) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: `Missing scriptFile for service '${service}'`,
    });
  }

  const scriptPath = path.join(rootDir, "scripts", "data", scriptFile);
  if (!fs.existsSync(scriptPath)) {
    throw new AppError({
      code: "INVALID_CONFIG",
      message: `Pipeline script not found: ${scriptPath}`,
      details: { runId, scriptFile },
    });
  }

  const logger =
    options.logger ||
    createPipelineLogger(rootDir, service, runId, options.loggerFactory);

  logger.info("pipeline command started", {
    scriptFile,
    args,
  });

  const envOverrides =
    options.env && typeof options.env === "object" ? options.env : {};

  let result;
  try {
    result = await runCommand("bash", [scriptPath, ...args], {
      cwd: rootDir,
      env: {
        ...process.env,
        ...envOverrides,
      },
    });
  } catch (err) {
    logger.error("pipeline command execution failed", {
      scriptFile,
      error: err,
    });

    throw new AppError({
      code: errorCode,
      message: `Command execution failed: ${scriptFile}`,
      details: {
        runId,
        scriptFile,
      },
      cause: err,
    });
  }

  if (result.exitCode !== 0) {
    logger.error("pipeline command failed", {
      scriptFile,
      exitCode: result.exitCode,
      signal: result.signal,
    });

    throw new AppError({
      code: errorCode,
      message: `Command failed: ${scriptFile}`,
      details: {
        runId,
        scriptFile,
        exitCode: result.exitCode,
        signal: result.signal,
      },
    });
  }

  logger.info("pipeline command completed", {
    scriptFile,
  });

  return {
    ok: true,
    runId,
    scriptFile,
  };
}

module.exports = {
  buildIdempotencyKey,
  createPipelineLogger,
  resolveRepoRoot,
  runLegacyDataScript,
  spawnInherit,
};
