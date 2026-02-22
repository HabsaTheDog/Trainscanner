const test = require('node:test');
const assert = require('node:assert/strict');

const { createQaService } = require('../../src/domains/qa/service');

test('reportReviewQueue runs repository-backed report generator', async () => {
  const ensureReadyCalls = [];
  const fetchCalls = [];
  const stdoutWrites = [];
  const originalStdoutWrite = process.stdout.write;
  process.stdout.write = (chunk, encoding, callback) => {
    stdoutWrites.push(String(chunk));
    if (typeof encoding === 'function') {
      encoding();
    } else if (typeof callback === 'function') {
      callback();
    }
    return true;
  };

  try {
    const service = createQaService({
      createPostgisClient: () => ({
        ensureReady: async () => {
          ensureReadyCalls.push('ready');
        }
      }),
      createReviewQueueRepo: () => ({
        fetchReportMetrics: async (scope) => {
          fetchCalls.push(scope);
          return {
            totalItems: 5,
            openItems: 2,
            confirmedItems: 1,
            dismissedItems: 1,
            resolvedItems: 1,
            autoResolvedItems: 0,
            reviewCoveragePercent: 40
          };
        },
        listCountsByIssueType: async () => [{ issue_type: 'duplicate_hard_id', status: 'open', items: 2 }],
        listOpenOrConfirmed: async () => [{ review_item_id: 1 }],
        listResolved: async () => [{ review_item_id: 2 }]
      })
    });

    await service.reportReviewQueue({
      rootDir: '/tmp/repo',
      runId: 'run-review-report-1',
      args: ['--country', 'DE', '--limit', '10'],
      jobOrchestrationEnabled: false
    });
  } finally {
    process.stdout.write = originalStdoutWrite;
  }

  assert.equal(ensureReadyCalls.length, 1);
  assert.equal(fetchCalls.length, 1);
  assert.deepEqual(fetchCalls[0], {
    country: 'DE',
    scopeTag: 'latest',
    allScopes: false,
    limitRows: 10
  });
  assert.match(stdoutWrites.join(''), /"total_items":5/);
});
