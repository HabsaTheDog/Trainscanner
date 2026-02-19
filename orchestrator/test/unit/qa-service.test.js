const test = require('node:test');
const assert = require('node:assert/strict');

const { createQaService } = require('../../src/domains/qa/service');

test('reportReviewQueue delegates to review-queue report legacy script', async () => {
  const calls = [];
  const service = createQaService({
    runLegacyDataScript: async (options) => {
      calls.push(options);
      return { ok: true, runId: options.runId || 'run-1' };
    }
  });

  await service.reportReviewQueue({
    rootDir: '/tmp/repo',
    runId: 'run-review-report-1',
    args: ['--country', 'DE', '--limit', '10'],
    jobOrchestrationEnabled: false
  });

  assert.equal(calls.length, 1);
  assert.equal(calls[0].scriptFile, 'report-review-queue.legacy.sh');
  assert.equal(calls[0].errorCode, 'REVIEW_QUEUE_REPORT_FAILED');
  assert.equal(calls[0].service, 'qa.report-review-queue');
  assert.deepEqual(calls[0].args, ['--country', 'DE', '--limit', '10']);
});
