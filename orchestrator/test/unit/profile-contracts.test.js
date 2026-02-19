const test = require('node:test');
const assert = require('node:assert/strict');

const { validateGtfsProfilesConfig } = require('../../src/domains/switch-runtime/contracts');

test('accepts mixed static/runtime GTFS profile config', () => {
  const payload = {
    profiles: {
      static_profile: { zipPath: 'data/gtfs/test.zip' },
      runtime_profile: {
        runtime: {
          mode: 'canonical-export',
          profile: 'runtime_profile',
          asOf: 'latest',
          country: 'DE'
        }
      }
    }
  };

  const result = validateGtfsProfilesConfig(payload);
  assert.equal(typeof result, 'object');
  assert.ok(result.static_profile);
  assert.ok(result.runtime_profile);
});

test('rejects runtime profile with invalid asOf', () => {
  const payload = {
    profiles: {
      broken: {
        runtime: {
          mode: 'canonical-export',
          asOf: '2026/01/01'
        }
      }
    }
  };

  assert.throws(() => validateGtfsProfilesConfig(payload), /runtime\.asOf/);
});
