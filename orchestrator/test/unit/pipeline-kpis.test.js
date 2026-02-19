const test = require('node:test');
const assert = require('node:assert/strict');

const { buildKpiPayload } = require('../../src/domains/qa/pipeline-kpis');

test('buildKpiPayload computes throughput/failure/duration metrics', () => {
  const now = Date.now();
  const iso = (offsetMs) => new Date(now + offsetMs).toISOString();

  const payload = buildKpiPayload(
    [
      {
        status: 'succeeded',
        startedAt: iso(-5000),
        endedAt: iso(-3000)
      },
      {
        status: 'failed',
        startedAt: iso(-2000),
        endedAt: iso(-1000)
      },
      {
        status: 'succeeded',
        startedAt: iso(-1000),
        endedAt: iso(-500)
      }
    ],
    { windowHours: 1 }
  );

  assert.equal(payload.totalJobs, 3);
  assert.equal(payload.succeededJobs, 2);
  assert.equal(payload.failedJobs, 1);
  assert.ok(payload.throughputPerHour > 0);
  assert.ok(payload.failureRatePercent > 0);
  assert.ok(payload.durationAvgMs > 0);
});
