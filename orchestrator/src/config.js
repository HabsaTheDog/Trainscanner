const path = require("node:path");
const { validateOrThrow } = require("./core/schema");

function toInt(value, fallback) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }
  const n = Number.parseInt(value, 10);
  return Number.isFinite(n) ? n : fallback;
}

function toBool(value, fallback) {
  if (value === undefined) {
    return fallback;
  }
  return String(value).toLowerCase() === "true";
}

function resolvePath(envValue, fallback) {
  if (envValue && String(envValue).trim()) {
    return path.resolve(String(envValue).trim());
  }
  return path.resolve(fallback);
}

function validateRuntimeConfig(config) {
  validateOrThrow(
    config,
    {
      type: "object",
      required: [
        "port",
        "configDir",
        "stateDir",
        "dataDir",
        "frontendDir",
        "profilesPath",
        "activeProfilePath",
        "switchStatusPath",
        "switchLockPath",
        "switchLogPath",
        "motisActiveGtfsPath",
        "motisBaseUrl",
        "motisRoutePath",
      ],
      properties: {
        port: { type: "integer", minimum: 1, maximum: 65535 },
        configDir: { type: "string", minLength: 1 },
        stateDir: { type: "string", minLength: 1 },
        dataDir: { type: "string", minLength: 1 },
        frontendDir: { type: "string", minLength: 1 },
        profilesPath: { type: "string", minLength: 1 },
        activeProfilePath: { type: "string", minLength: 1 },
        legacyActiveProfilePath: { type: "string", minLength: 1 },
        switchStatusPath: { type: "string", minLength: 1 },
        switchLockPath: { type: "string", minLength: 1 },
        switchLockStaleMs: { type: "integer", minimum: 1000 },
        switchLogPath: { type: "string", minLength: 1 },
        motisActiveGtfsPath: { type: "string", minLength: 1 },
        motisBaseUrl: { type: "string", minLength: 1, pattern: /^https?:\/\// },
        motisHealthPath: { type: "string", minLength: 1 },
        motisHealthAccept404: { type: "boolean" },
        motisRoutePath: { type: "string", minLength: 1 },
        motisDatasetTag: { type: "string", minLength: 1 },
        motisReadyTimeoutMs: { type: "integer", minimum: 1000 },
        motisHealthPollIntervalMs: { type: "integer", minimum: 200 },
        motisRequestTimeoutMs: { type: "integer", minimum: 200 },
        motisRestartMode: { type: "string", enum: ["docker", "none"] },
        motisSkipHealthcheck: { type: "boolean" },
        motisDockerSocketPath: { type: "string", minLength: 1 },
        motisDockerApiVersion: { type: "string", minLength: 1 },
        motisContainerName: { type: "string", minLength: 1 },
        motisRestartTimeoutSec: { type: "integer", minimum: 1 },
        stationIndexCacheMaxEntries: { type: "integer", minimum: 1 },
        stationIndexCacheTtlMs: { type: "integer", minimum: 1000 },
        metricsEnabled: { type: "boolean" },
        pipelineJobMaxConcurrent: { type: "integer", minimum: 1 },
      },
      additionalProperties: false,
    },
    {
      message: "Invalid orchestrator runtime configuration",
      code: "INVALID_CONFIG",
    },
  );

  return config;
}

function loadConfig() {
  const cwd = process.cwd();
  const configDir = resolvePath(
    process.env.CONFIG_DIR,
    path.join(cwd, "config"),
  );
  const stateDir = resolvePath(process.env.STATE_DIR, path.join(cwd, "state"));
  const dataDir = resolvePath(process.env.DATA_DIR, path.join(cwd, "data"));
  const frontendDir = resolvePath(
    process.env.FRONTEND_DIR,
    path.join(cwd, "frontend", "dist"),
  );

  const config = {
    port: toInt(process.env.PORT, 3000),
    configDir,
    stateDir,
    dataDir,
    frontendDir,
    profilesPath: path.join(configDir, "gtfs-profiles.json"),
    activeProfilePath: path.join(stateDir, "active-gtfs.json"),
    legacyActiveProfilePath: path.join(configDir, "active-gtfs.json"),
    switchStatusPath: path.join(stateDir, "gtfs-switch-status.json"),
    switchLockPath: path.join(stateDir, "gtfs-switch.lock"),
    switchLockStaleMs: toInt(process.env.GTFS_SWITCH_LOCK_STALE_MS, 1800000),
    switchLogPath: path.join(stateDir, "gtfs-switch.log"),
    motisActiveGtfsPath: resolvePath(
      process.env.MOTIS_ACTIVE_GTFS_PATH,
      path.join(dataDir, "motis", "active-gtfs.zip"),
    ),
    motisBaseUrl: (process.env.MOTIS_BASE_URL || "http://motis:8080").replace(
      /\/$/,
      "",
    ),
    motisHealthPath: process.env.MOTIS_HEALTH_PATH || "/health",
    motisHealthAccept404: toBool(process.env.MOTIS_HEALTH_ACCEPT_404, true),
    motisRoutePath: process.env.MOTIS_ROUTE_PATH || "/api/v5/plan",
    motisDatasetTag: process.env.MOTIS_DATASET_TAG || "active-gtfs",
    motisReadyTimeoutMs: toInt(process.env.MOTIS_READY_TIMEOUT_MS, 180000),
    motisHealthPollIntervalMs: toInt(
      process.env.MOTIS_HEALTH_POLL_INTERVAL_MS,
      2000,
    ),
    motisRequestTimeoutMs: toInt(process.env.MOTIS_REQUEST_TIMEOUT_MS, 10000),
    motisRestartMode: process.env.MOTIS_RESTART_MODE || "docker",
    motisSkipHealthcheck: toBool(process.env.MOTIS_SKIP_HEALTHCHECK, false),
    motisDockerSocketPath:
      process.env.MOTIS_DOCKER_SOCKET_PATH || "/var/run/docker.sock",
    motisDockerApiVersion: process.env.MOTIS_DOCKER_API_VERSION || "auto",
    motisContainerName: process.env.MOTIS_CONTAINER_NAME || "motis",
    motisRestartTimeoutSec: toInt(process.env.MOTIS_RESTART_TIMEOUT_SEC, 10),
    stationIndexCacheMaxEntries: toInt(
      process.env.STATION_INDEX_CACHE_MAX_ENTRIES,
      8,
    ),
    stationIndexCacheTtlMs: toInt(
      process.env.STATION_INDEX_CACHE_TTL_MS,
      300000,
    ),
    metricsEnabled: toBool(process.env.METRICS_ENABLED, true),
    pipelineJobMaxConcurrent: toInt(process.env.PIPELINE_JOB_MAX_CONCURRENT, 1),
  };

  return validateRuntimeConfig(config);
}

module.exports = {
  loadConfig,
  validateRuntimeConfig,
};
