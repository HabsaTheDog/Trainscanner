const test = require('node:test');
const assert = require('node:assert/strict');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const path = require('node:path');

const execFileAsync = promisify(execFile);

test('validate-configs CLI validates all core config files', async () => {
  const repoRoot = path.resolve(__dirname, '../../..');
  const script = path.join(repoRoot, 'orchestrator', 'src', 'cli', 'validate-configs.js');

  const result = await execFileAsync(process.execPath, [script, '--root', repoRoot], {
    cwd: repoRoot
  });

  assert.match(result.stdout, /ok profiles/);
  assert.match(result.stdout, /ok dach/);
  assert.match(result.stdout, /ok ojp/);
  assert.match(result.stdout, /ok ojp-mock/);
});
