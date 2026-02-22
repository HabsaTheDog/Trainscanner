const test = require('node:test');
const assert = require('node:assert/strict');

const { createSourceDiscoveryService } = require('../../src/domains/source-discovery/service');

test('fetchSources delegates to fetch script with explicit error code', async () => {
  const calls = [];
  const service = createSourceDiscoveryService({
    runLegacyDataScript: async (options) => {
      calls.push(options);
      return { ok: true, runId: options.runId || 'run-1' };
    }
  });

  await service.fetchSources({
    rootDir: '/tmp/repo',
    runId: 'run-fetch-1',
    args: ['--as-of', '2026-02-19']
  });

  assert.equal(calls.length, 1);
  assert.equal(calls[0].scriptFile, 'fetch-dach-sources.impl.sh');
  assert.equal(calls[0].errorCode, 'SOURCE_FETCH_FAILED');
  assert.equal(calls[0].service, 'source-discovery.fetch');
  assert.deepEqual(calls[0].args, ['--as-of', '2026-02-19']);
});

test('verifySources delegates to verify script with explicit error code', async () => {
  const calls = [];
  const service = createSourceDiscoveryService({
    runLegacyDataScript: async (options) => {
      calls.push(options);
      return { ok: true, runId: options.runId || 'run-1' };
    }
  });

  await service.verifySources({
    rootDir: '/tmp/repo',
    runId: 'run-verify-1',
    args: ['--country', 'DE']
  });

  assert.equal(calls.length, 1);
  assert.equal(calls[0].scriptFile, 'verify-dach-sources.impl.sh');
  assert.equal(calls[0].errorCode, 'SOURCE_VERIFY_FAILED');
  assert.equal(calls[0].service, 'source-discovery.verify');
  assert.deepEqual(calls[0].args, ['--country', 'DE']);
});

test('fetchSources uses circuit breaker execution wrapper', async () => {
  const calls = [];
  let breakerExecutions = 0;

  const service = createSourceDiscoveryService({
    runLegacyDataScript: async (options) => {
      calls.push(options);
      return { ok: true, runId: options.runId || 'run-1' };
    },
    fetchBreaker: {
      async execute(fn) {
        breakerExecutions += 1;
        return fn();
      }
    },
    verifyBreaker: {
      async execute(fn) {
        return fn();
      }
    }
  });

  await service.fetchSources({
    rootDir: '/tmp/repo',
    runId: 'run-fetch-2',
    args: ['--country', 'CH']
  });

  assert.equal(breakerExecutions, 1);
  assert.equal(calls.length, 1);
});
