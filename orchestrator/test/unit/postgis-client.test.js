const test = require('node:test');
const assert = require('node:assert/strict');

const { parseRowsFromJsonOutput, resolveConnectionConfig } = require('../../src/data/postgis/client');

test('parseRowsFromJsonOutput parses last json line', () => {
  const rows = parseRowsFromJsonOutput('NOTICE something\n[{"a":1},{"a":2}]\n');
  assert.equal(rows.length, 2);
  assert.equal(rows[0].a, 1);
});

test('resolveConnectionConfig resolves defaults and explicit fields', () => {
  const cfg = resolveConnectionConfig({
    rootDir: '/tmp/repo',
    env: {
      CANONICAL_DB_MODE: 'direct',
      CANONICAL_DB_HOST: 'db.local',
      CANONICAL_DB_PORT: '5433',
      CANONICAL_DB_USER: 'u',
      CANONICAL_DB_NAME: 'n',
      CANONICAL_DB_PASSWORD: 'p'
    }
  });

  assert.equal(cfg.rootDir, '/tmp/repo');
  assert.equal(cfg.mode, 'direct');
  assert.equal(cfg.host, 'db.local');
  assert.equal(cfg.port, '5433');
  assert.equal(cfg.user, 'u');
  assert.equal(cfg.database, 'n');
  assert.equal(cfg.password, 'p');
});
