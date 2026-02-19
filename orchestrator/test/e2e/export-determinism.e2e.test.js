const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const fsSync = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');

const { mkTempDir } = require('../helpers/test-utils');

const execFileAsync = promisify(execFile);

function sha256(filePath) {
  return crypto.createHash('sha256').update(fsSync.readFileSync(filePath)).digest('hex');
}

test('deterministic export produces stable artifact hash', async () => {
  const repoRoot = path.resolve(__dirname, '../../..');
  const temp = await mkTempDir('export-determinism-');
  const csvPath = path.join(temp, 'stops.csv');
  const summaryA = path.join(temp, 'summary-a.json');
  const summaryB = path.join(temp, 'summary-b.json');
  const zipA = path.join(temp, 'a.zip');
  const zipB = path.join(temp, 'b.zip');

  const csv = [
    'stop_id,stop_name,country,stop_lat,stop_lon',
    'de_a,Alpha Station,DE,48.100000,11.500000',
    'de_b,Beta Station,DE,48.200000,11.600000'
  ].join('\n');
  await fs.writeFile(csvPath, csv + '\n', 'utf8');

  const exportScript = path.join(repoRoot, 'scripts', 'qa', 'export-canonical-gtfs.py');
  const validateScript = path.join(repoRoot, 'scripts', 'qa', 'validate-export.sh');

  await execFileAsync('python3', [
    exportScript,
    '--stops-csv',
    csvPath,
    '--profile',
    'fixture_profile',
    '--as-of',
    '2026-01-15',
    '--output-zip',
    zipA,
    '--summary-json',
    summaryA
  ]);

  await execFileAsync('python3', [
    exportScript,
    '--stops-csv',
    csvPath,
    '--profile',
    'fixture_profile',
    '--as-of',
    '2026-01-15',
    '--output-zip',
    zipB,
    '--summary-json',
    summaryB
  ]);

  assert.equal(sha256(zipA), sha256(zipB));

  await execFileAsync('bash', [validateScript, '--zip', zipA], {
    cwd: repoRoot
  });
});
