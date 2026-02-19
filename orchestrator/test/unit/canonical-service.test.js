const test = require('node:test');
const assert = require('node:assert/strict');

const { createCanonicalService } = require('../../src/domains/canonical/service');

test('buildCanonicalStations delegates to canonical build legacy script', async () => {
  const calls = [];
  const service = createCanonicalService({
    runLegacyDataScript: async (options) => {
      calls.push(options);
      return { ok: true, runId: options.runId || 'run-1' };
    }
  });

  await service.buildCanonicalStations({
    rootDir: '/tmp/repo',
    runId: 'run-canonical-1',
    args: ['--country', 'AT'],
    jobOrchestrationEnabled: false
  });

  assert.equal(calls.length, 1);
  assert.equal(calls[0].scriptFile, 'build-canonical-stations.legacy.sh');
  assert.equal(calls[0].errorCode, 'CANONICAL_BUILD_FAILED');
  assert.equal(calls[0].service, 'canonical.build-stations');
  assert.deepEqual(calls[0].args, ['--country', 'AT']);
});

test('buildReviewQueue delegates to review-queue build legacy script', async () => {
  const calls = [];
  const service = createCanonicalService({
    runLegacyDataScript: async (options) => {
      calls.push(options);
      return { ok: true, runId: options.runId || 'run-1' };
    }
  });

  await service.buildReviewQueue({
    rootDir: '/tmp/repo',
    runId: 'run-review-build-1',
    args: ['--country', 'CH', '--geo-threshold-m', '4000'],
    jobOrchestrationEnabled: false
  });

  assert.equal(calls.length, 1);
  assert.equal(calls[0].scriptFile, 'build-review-queue.legacy.sh');
  assert.equal(calls[0].errorCode, 'REVIEW_QUEUE_BUILD_FAILED');
  assert.equal(calls[0].service, 'canonical.build-review-queue');
  assert.deepEqual(calls[0].args, ['--country', 'CH', '--geo-threshold-m', '4000']);
});
