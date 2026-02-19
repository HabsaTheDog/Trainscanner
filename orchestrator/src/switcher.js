const fs = require('node:fs/promises');
const path = require('node:path');

const { acquireLock } = require('./lock');
const { restartMotisContainer, waitForMotisReady } = require('./motis');
const { normalizeProfiles, resolveProfileArtifact } = require('./profile-resolver');
const { AppError, toAppError } = require('./core/errors');
const { generateId } = require('./core/ids');
const { VALID_SWITCH_STATES, isInFlightState } = require('./domains/switch-runtime/status');

async function readJson(filePath, fallback) {
  try {
    const content = await fs.readFile(filePath, 'utf8');
    return JSON.parse(content);
  } catch (err) {
    if (err.code === 'ENOENT') {
      return fallback;
    }
    throw err;
  }
}

async function writeJsonAtomic(filePath, payload) {
  const dir = path.dirname(filePath);
  const tempPath = `${filePath}.tmp`;
  await fs.mkdir(dir, { recursive: true });
  await fs.writeFile(tempPath, JSON.stringify(payload, null, 2) + '\n', 'utf8');
  await fs.rename(tempPath, filePath);
}

function nowIso() {
  return new Date().toISOString();
}

class GtfsSwitcher {
  constructor(config, logger, options = {}) {
    this.config = config;
    this.logger = logger;
    this.currentJob = null;
    this.motisAdapter = options.motisAdapter || {
      restartMotisContainer,
      waitForMotisReady
    };
  }

  async getProfilesWithMeta() {
    const profilesRaw = await readJson(this.config.profilesPath, { profiles: {} });
    const activeRaw = await this.readActiveProfile();
    const profiles = normalizeProfiles(profilesRaw);
    const items = [];

    for (const [name, value] of Object.entries(profiles)) {
      try {
        const resolved = await resolveProfileArtifact(name, value, {
          dataDir: this.config.dataDir,
          allowMissing: true
        });

        items.push({
          name,
          zipPath: resolved.zipPath,
          description: value.description || '',
          sourceType: resolved.sourceType,
          runtime: resolved.runtime || null,
          exists: resolved.exists,
          absolutePath: resolved.absolutePath,
          resolutionError: null
        });
      } catch (err) {
        items.push({
          name,
          zipPath: '',
          description: value.description || '',
          sourceType: value.sourceType || 'static',
          runtime: value.runtime || null,
          exists: false,
          absolutePath: null,
          resolutionError: err.message
        });
      }
    }

    return {
      activeProfile: activeRaw.activeProfile || null,
      profiles: items
    };
  }

  async getStatus() {
    return readJson(this.config.switchStatusPath, {
      state: 'idle',
      activeProfile: null,
      requestedProfile: null,
      runId: null,
      message: 'No switch executed yet',
      updatedAt: nowIso(),
      error: null
    });
  }

  async readActiveProfile() {
    const next = await readJson(this.config.activeProfilePath, null);
    if (next && typeof next === 'object') {
      return next;
    }

    const legacyPath = this.config.legacyActiveProfilePath;
    if (!legacyPath) {
      return { activeProfile: null };
    }

    const legacy = await readJson(legacyPath, { activeProfile: null });
    if (legacy && typeof legacy === 'object' && legacy.activeProfile) {
      await writeJsonAtomic(this.config.activeProfilePath, legacy);
      this.logger.info('Migrated legacy active profile state into state directory', {
        step: 'active_profile_migration',
        from: legacyPath,
        to: this.config.activeProfilePath
      });
    }
    return legacy;
  }

  async setStatus(next) {
    if (!VALID_SWITCH_STATES.has(next.state)) {
      throw new AppError({
        code: 'INVALID_CONFIG',
        message: `Invalid switch state: ${next.state}`
      });
    }

    const existing = await this.getStatus();
    const payload = {
      ...existing,
      ...next,
      updatedAt: nowIso()
    };
    await writeJsonAtomic(this.config.switchStatusPath, payload);
    this.logger.info('GTFS switch status updated', {
      step: 'status',
      state: payload.state,
      runId: payload.runId || null,
      activeProfile: payload.activeProfile,
      requestedProfile: payload.requestedProfile || null,
      message: payload.message
    });
    return payload;
  }

  async start(profileName, options = {}) {
    const requestedProfile = String(profileName || '').trim();
    if (!requestedProfile) {
      throw new AppError({
        code: 'INVALID_REQUEST',
        statusCode: 400,
        message: 'Missing required profile name'
      });
    }

    if (this.currentJob) {
      if (this.currentJob.profileName === requestedProfile) {
        this.logger.info('Idempotent switch request reused in-flight run', {
          step: 'switch_start_reused',
          profile: requestedProfile,
          runId: this.currentJob.runId
        });
        return {
          accepted: true,
          reused: true,
          runId: this.currentJob.runId,
          profile: requestedProfile,
          state: 'switching'
        };
      }

      throw new AppError({
        code: 'SWITCH_CONFLICT',
        statusCode: 409,
        message: `Another profile switch is already running for '${this.currentJob.profileName}'`
      });
    }

    const [status, active] = await Promise.all([this.getStatus(), this.readActiveProfile().catch(() => ({ activeProfile: null }))]);
    if (status.state === 'ready' && active.activeProfile === requestedProfile) {
      return {
        accepted: false,
        noop: true,
        profile: requestedProfile,
        runId: status.runId || null,
        message: `Profile '${requestedProfile}' is already active`
      };
    }

    await fs.mkdir(path.dirname(this.config.switchLockPath), { recursive: true });
    const lock = await acquireLock(this.config.switchLockPath, {
      staleMs: this.config.switchLockStaleMs,
      logger: this.logger
    });
    if (!lock) {
      if (isInFlightState(status.state) && status.requestedProfile === requestedProfile) {
        return {
          accepted: true,
          reused: true,
          profile: requestedProfile,
          runId: status.runId || null,
          state: status.state,
          message: `Switch already in progress for '${requestedProfile}'`
        };
      }

      throw new AppError({
        code: 'SWITCH_LOCK_HELD',
        statusCode: 409,
        message: 'Switch lock already held. Another switch is in progress.'
      });
    }

    const runId = options.runId || generateId('switch');
    this.currentJob = {
      profileName: requestedProfile,
      runId
    };

    this.logger.info('GTFS profile switch accepted', {
      step: 'switch_start',
      profile: requestedProfile,
      runId
    });

    this.run(requestedProfile, runId)
      .catch((err) => {
        const appErr = toAppError(err);
        this.logger.error('GTFS switch failed unexpectedly', {
          step: 'switch_exception',
          profile: requestedProfile,
          runId,
          error: appErr.message,
          errorCode: appErr.code
        });
      })
      .finally(async () => {
        this.currentJob = null;
        await lock.release().catch((err) => {
          this.logger.error('Failed to release switch lock', {
            step: 'unlock',
            runId,
            error: err.message
          });
        });
      });

    return {
      accepted: true,
      reused: false,
      runId,
      profile: requestedProfile,
      message: `Profile switch to '${requestedProfile}' started`
    };
  }

  async run(profileName, runId) {
    try {
      await this.setStatus({
        state: 'switching',
        runId,
        requestedProfile: profileName,
        message: `Validating profile '${profileName}'`,
        error: null
      });

      const profilesRaw = await readJson(this.config.profilesPath, { profiles: {} });
      const profiles = normalizeProfiles(profilesRaw);
      const selected = profiles[profileName];

      if (!selected) {
        throw new AppError({
          code: 'UNKNOWN_PROFILE',
          statusCode: 404,
          message: `Profile '${profileName}' not found in ${this.config.profilesPath}`
        });
      }

      const resolved = await resolveProfileArtifact(profileName, selected, {
        dataDir: this.config.dataDir,
        allowMissing: false
      });
      const sourceZipPath = resolved.absolutePath;

      await this.setStatus({
        state: 'importing',
        runId,
        requestedProfile: profileName,
        message: `Preparing MOTIS input from ${resolved.zipPath}`,
        error: null
      });

      await fs.mkdir(path.dirname(this.config.motisActiveGtfsPath), { recursive: true });
      await fs.copyFile(sourceZipPath, this.config.motisActiveGtfsPath);

      await writeJsonAtomic(this.config.activeProfilePath, {
        activeProfile: profileName,
        zipPath: resolved.zipPath,
        sourceType: resolved.sourceType,
        runtime: resolved.runtime || null,
        activatedAt: nowIso(),
        runId
      });

      await this.setStatus({
        state: 'restarting',
        runId,
        requestedProfile: profileName,
        activeProfile: profileName,
        message: 'Restarting MOTIS service',
        error: null
      });

      await this.motisAdapter.restartMotisContainer(this.config);

      const ready = await this.motisAdapter.waitForMotisReady(this.config, this.logger);
      if (!ready.ok) {
        throw new AppError({
          code: 'MOTIS_UNAVAILABLE',
          statusCode: 502,
          message: `MOTIS did not become ready in ${this.config.motisReadyTimeoutMs}ms`
        });
      }

      await this.setStatus({
        state: 'ready',
        runId,
        requestedProfile: profileName,
        activeProfile: profileName,
        message: `Profile '${profileName}' activated successfully`,
        lastHealth: ready.health,
        error: null
      });
    } catch (err) {
      const appErr = toAppError(err);
      await this.setStatus({
        state: 'failed',
        runId,
        requestedProfile: profileName,
        message: appErr.message,
        error: appErr.message,
        errorCode: appErr.code
      });
      this.logger.error('GTFS profile switch failed', {
        step: 'switch_failed',
        runId,
        error: appErr.message,
        errorCode: appErr.code,
        profile: profileName
      });
    }
  }
}

module.exports = {
  GtfsSwitcher,
  normalizeProfiles
};
