const fs = require('node:fs/promises');
const path = require('node:path');

const { acquireLock } = require('./lock');
const { restartMotisContainer, waitForMotisReady } = require('./motis');
const { normalizeProfiles, resolveProfileArtifact } = require('./profile-resolver');

const VALID_STATES = new Set(['idle', 'switching', 'importing', 'restarting', 'ready', 'failed']);

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
  constructor(config, logger) {
    this.config = config;
    this.logger = logger;
    this.running = false;
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
    if (!VALID_STATES.has(next.state)) {
      throw new Error(`Invalid switch state: ${next.state}`);
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
      activeProfile: payload.activeProfile,
      message: payload.message
    });
    return payload;
  }

  async start(profileName) {
    if (this.running) {
      throw Object.assign(new Error('Another profile switch is already running'), { statusCode: 409 });
    }

    const lock = await acquireLock(this.config.switchLockPath, {
      staleMs: this.config.switchLockStaleMs,
      logger: this.logger
    });
    if (!lock) {
      throw Object.assign(new Error('Switch lock already held. Another switch is in progress.'), { statusCode: 409 });
    }

    this.running = true;
    this.logger.info('GTFS profile switch accepted', { step: 'switch_start', profile: profileName });

    this.run(profileName)
      .catch((err) => {
        this.logger.error('GTFS switch failed unexpectedly', {
          step: 'switch_exception',
          profile: profileName,
          error: err.message
        });
      })
      .finally(async () => {
        this.running = false;
        await lock.release().catch((err) => {
          this.logger.error('Failed to release switch lock', {
            step: 'unlock',
            error: err.message
          });
        });
      });
  }

  async run(profileName) {
    try {
      await this.setStatus({
        state: 'switching',
        requestedProfile: profileName,
        message: `Validating profile '${profileName}'`,
        error: null
      });

      const profilesRaw = await readJson(this.config.profilesPath, { profiles: {} });
      const profiles = normalizeProfiles(profilesRaw);
      const selected = profiles[profileName];

      if (!selected) {
        throw new Error(`Profile '${profileName}' not found in ${this.config.profilesPath}`);
      }

      const resolved = await resolveProfileArtifact(profileName, selected, {
        dataDir: this.config.dataDir,
        allowMissing: false
      });
      const sourceZipPath = resolved.absolutePath;

      await this.setStatus({
        state: 'importing',
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
        activatedAt: nowIso()
      });

      await this.setStatus({
        state: 'restarting',
        requestedProfile: profileName,
        activeProfile: profileName,
        message: 'Restarting MOTIS service',
        error: null
      });

      await restartMotisContainer(this.config);

      const ready = await waitForMotisReady(this.config, this.logger);
      if (!ready.ok) {
        throw new Error(`MOTIS did not become ready in ${this.config.motisReadyTimeoutMs}ms`);
      }

      await this.setStatus({
        state: 'ready',
        requestedProfile: profileName,
        activeProfile: profileName,
        message: `Profile '${profileName}' activated successfully`,
        lastHealth: ready.health,
        error: null
      });
    } catch (err) {
      await this.setStatus({
        state: 'failed',
        message: err.message,
        error: err.message
      });
      this.logger.error('GTFS profile switch failed', {
        step: 'switch_failed',
        error: err.message,
        profile: profileName
      });
    }
  }
}

module.exports = {
  GtfsSwitcher,
  normalizeProfiles
};
