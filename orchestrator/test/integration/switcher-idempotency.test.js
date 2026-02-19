const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const path = require('node:path');

const { GtfsSwitcher } = require('../../src/switcher');
const { createLogger } = require('../../src/logger');
const { mkTempDir, writeJson, waitFor } = require('../helpers/test-utils');

test('GtfsSwitcher supports idempotent start semantics', async () => {
  const root = await mkTempDir('switcher-idempotency-');
  const configDir = path.join(root, 'config');
  const stateDir = path.join(root, 'state');
  const dataDir = path.join(root, 'data');

  await fs.mkdir(path.join(dataDir, 'gtfs'), { recursive: true });
  await fs.mkdir(path.join(dataDir, 'motis'), { recursive: true });

  const profileZipA = path.join(dataDir, 'gtfs', 'a.zip');
  const profileZipB = path.join(dataDir, 'gtfs', 'b.zip');
  await fs.writeFile(profileZipA, 'zip-a');
  await fs.writeFile(profileZipB, 'zip-b');

  await writeJson(path.join(configDir, 'gtfs-profiles.json'), {
    profiles: {
      profile_a: { zipPath: 'data/gtfs/a.zip' },
      profile_b: { zipPath: 'data/gtfs/b.zip' }
    }
  });

  const config = {
    configDir,
    stateDir,
    dataDir,
    profilesPath: path.join(configDir, 'gtfs-profiles.json'),
    activeProfilePath: path.join(stateDir, 'active-gtfs.json'),
    legacyActiveProfilePath: path.join(configDir, 'active-gtfs.json'),
    switchStatusPath: path.join(stateDir, 'gtfs-switch-status.json'),
    switchLockPath: path.join(stateDir, 'gtfs-switch.lock'),
    switchLockStaleMs: 60000,
    switchLogPath: path.join(stateDir, 'gtfs-switch.log'),
    motisActiveGtfsPath: path.join(dataDir, 'motis', 'active-gtfs.zip'),
    motisReadyTimeoutMs: 2000
  };

  const logger = createLogger(config.switchLogPath, { service: 'test' });
  const switcher = new GtfsSwitcher(config, logger, {
    motisAdapter: {
      async restartMotisContainer() {
        return { skipped: true };
      },
      async waitForMotisReady() {
        await new Promise((resolve) => setTimeout(resolve, 200));
        return { ok: true, health: { ok: true, status: 200, body: {} } };
      }
    }
  });

  const first = await switcher.start('profile_a');
  assert.equal(first.accepted, true);
  assert.equal(first.reused, false);
  assert.ok(first.runId);

  const second = await switcher.start('profile_a');
  assert.equal(second.accepted, true);
  assert.equal(second.reused, true);
  assert.equal(second.runId, first.runId);

  await assert.rejects(() => switcher.start('profile_b'), /already running/);

  await waitFor(async () => {
    const status = await switcher.getStatus();
    return status.state === 'ready' ? status : null;
  }, { timeoutMs: 5000, intervalMs: 100 });

  const noop = await switcher.start('profile_a');
  assert.equal(noop.noop, true);
  assert.equal(noop.accepted, false);
});
