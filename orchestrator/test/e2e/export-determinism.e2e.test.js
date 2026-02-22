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

test('group-aware export keeps user-facing parents and emits transfer links deterministically', async () => {
  const repoRoot = path.resolve(__dirname, '../../..');
  const temp = await mkTempDir('export-groups-');
  const csvPath = path.join(temp, 'stops-groups.csv');
  const summaryA = path.join(temp, 'summary-groups-a.json');
  const summaryB = path.join(temp, 'summary-groups-b.json');
  const zipA = path.join(temp, 'groups-a.zip');
  const zipB = path.join(temp, 'groups-b.zip');

  const csv = [
    'stop_id,stop_name,country,stop_lat,stop_lon,location_type,parent_station,is_user_facing,walk_links_json,section_type',
    'grp_alpha,Alpha Hub,DE,48.100000,11.500000,,,true,[],',
    'sec_alpha_main,Alpha Main Hall,DE,48.100100,11.500100,0,grp_alpha,false,\"[{\"\"to_stop_id\"\":\"\"sec_alpha_bus\"\",\"\"min_walk_minutes\"\":4}]\",main',
    'sec_alpha_bus,Alpha Bus Terminal,DE,48.100900,11.500900,0,grp_alpha,false,\"[{\"\"to_stop_id\"\":\"\"sec_alpha_main\"\",\"\"min_walk_minutes\"\":4}]\",bus',
    'de_gamma,Gamma Station,DE,48.200000,11.600000,,,true,[],'
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

  const summary = JSON.parse(await fs.readFile(summaryA, 'utf8'));
  assert.equal(summary.bridgeMode, 'group-aware-synthetic-journeys-from-canonical-stops');
  assert.equal(summary.counts.userFacingStops, 2);
  assert.equal(summary.counts.sectionStops, 2);
  assert.equal(summary.counts.transfers, 2);

  await execFileAsync('bash', [validateScript, '--zip', zipA], {
    cwd: repoRoot
  });
});
