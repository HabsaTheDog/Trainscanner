const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');

const execFileAsync = promisify(execFile);

const cliCases = [
  {
    file: 'fetch-dach-sources.js',
    expectedUsage: /Usage: scripts\/data\/fetch-dach-sources\.sh/
  },
  {
    file: 'verify-dach-sources.js',
    expectedUsage: /Usage: scripts\/data\/verify-dach-sources\.sh/
  },
  {
    file: 'ingest-netex.js',
    expectedUsage: /Usage: scripts\/data\/ingest-netex\.sh/
  },
  {
    file: 'build-canonical-stations.js',
    expectedUsage: /Usage: scripts\/data\/build-canonical-stations\.sh/
  },
  {
    file: 'build-review-queue.js',
    expectedUsage: /Usage: scripts\/data\/build-review-queue\.sh/
  },
  {
    file: 'report-review-queue.js',
    expectedUsage: /Usage: scripts\/data\/report-review-queue\.sh/
  }
];

for (const cliCase of cliCases) {
  test(`${cliCase.file} forwards --help to legacy command contract`, async () => {
    const repoRoot = path.resolve(__dirname, '../../..');
    const cliPath = path.join(repoRoot, 'orchestrator', 'src', 'cli', cliCase.file);

    const result = await execFileAsync(process.execPath, [cliPath, '--root', repoRoot, '--help'], {
      cwd: repoRoot
    });

    assert.match(result.stdout, cliCase.expectedUsage);
  });
}

test('pipeline CLI returns machine-readable error payload for invalid wrapper args', async () => {
  const repoRoot = path.resolve(__dirname, '../../..');
  const cliPath = path.join(repoRoot, 'orchestrator', 'src', 'cli', 'fetch-dach-sources.js');

  await assert.rejects(
    execFileAsync(process.execPath, [cliPath, '--root'], {
      cwd: repoRoot
    }),
    (err) => {
      assert.match(err.stderr, /errorCode=INVALID_REQUEST/);
      assert.match(err.stderr, /"errorCode":"INVALID_REQUEST"/);
      return true;
    }
  );
});
