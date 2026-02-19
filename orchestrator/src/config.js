const path = require('node:path');

function toInt(value, fallback) {
  const n = Number.parseInt(value, 10);
  return Number.isFinite(n) ? n : fallback;
}

function resolvePath(envValue, fallback) {
  if (envValue && envValue.trim().length > 0) {
    return path.resolve(envValue);
  }
  return path.resolve(fallback);
}

function toBool(value, fallback) {
  if (value === undefined) {
    return fallback;
  }
  return String(value).toLowerCase() === 'true';
}

function loadConfig() {
  const cwd = process.cwd();

  const configDir = resolvePath(process.env.CONFIG_DIR, path.join(cwd, 'config'));
  const stateDir = resolvePath(process.env.STATE_DIR, path.join(cwd, 'state'));
  const dataDir = resolvePath(process.env.DATA_DIR, path.join(cwd, 'data'));
  const frontendDir = resolvePath(process.env.FRONTEND_DIR, path.join(cwd, 'frontend'));

  return {
    port: toInt(process.env.PORT, 3000),
    configDir,
    stateDir,
    dataDir,
    frontendDir,
    profilesPath: path.join(configDir, 'gtfs-profiles.json'),
    activeProfilePath: path.join(stateDir, 'active-gtfs.json'),
    legacyActiveProfilePath: path.join(configDir, 'active-gtfs.json'),
    switchStatusPath: path.join(stateDir, 'gtfs-switch-status.json'),
    switchLockPath: path.join(stateDir, 'gtfs-switch.lock'),
    switchLockStaleMs: toInt(process.env.GTFS_SWITCH_LOCK_STALE_MS, 1800000),
    switchLogPath: path.join(stateDir, 'gtfs-switch.log'),
    motisActiveGtfsPath: resolvePath(
      process.env.MOTIS_ACTIVE_GTFS_PATH,
      path.join(dataDir, 'motis', 'active-gtfs.zip')
    ),
    motisBaseUrl: (process.env.MOTIS_BASE_URL || 'http://motis:8080').replace(/\/$/, ''),
    motisHealthPath: process.env.MOTIS_HEALTH_PATH || '/health',
    motisHealthAccept404: toBool(process.env.MOTIS_HEALTH_ACCEPT_404, true),
    motisRoutePath: process.env.MOTIS_ROUTE_PATH || '/api/v5/plan',
    motisDatasetTag: process.env.MOTIS_DATASET_TAG || 'active-gtfs',
    motisReadyTimeoutMs: toInt(process.env.MOTIS_READY_TIMEOUT_MS, 180000),
    motisHealthPollIntervalMs: toInt(process.env.MOTIS_HEALTH_POLL_INTERVAL_MS, 2000),
    motisRequestTimeoutMs: toInt(process.env.MOTIS_REQUEST_TIMEOUT_MS, 10000),
    motisRestartMode: process.env.MOTIS_RESTART_MODE || 'docker',
    motisSkipHealthcheck: toBool(process.env.MOTIS_SKIP_HEALTHCHECK, false),
    motisDockerSocketPath: process.env.MOTIS_DOCKER_SOCKET_PATH || '/var/run/docker.sock',
    motisDockerApiVersion: process.env.MOTIS_DOCKER_API_VERSION || 'auto',
    motisContainerName: process.env.MOTIS_CONTAINER_NAME || 'motis',
    motisRestartTimeoutSec: toInt(process.env.MOTIS_RESTART_TIMEOUT_SEC, 10)
  };
}

module.exports = { loadConfig };
