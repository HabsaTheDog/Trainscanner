const test = require('node:test');
const assert = require('node:assert/strict');

const { resolveStationInput } = require('../../src/domains/routing/normalization');

function createIndex() {
  const station = {
    id: '1001',
    name: 'Alpha Station',
    value: 'Alpha Station [1001]',
    token: 'active-gtfs_1001',
    coordinateToken: '48.1,11.5',
    locationType: '1'
  };

  const byId = new Map([['1001', station]]);
  const byValueFold = new Map([['alpha station [1001]', station]]);
  const byNameFold = new Map([['alpha station', station]]);

  return {
    byId,
    byValueFold,
    byNameFold
  };
}

test('resolveStationInput handles tagged stop ids', () => {
  const index = createIndex();
  const result = resolveStationInput('active-gtfs_1001', index, 'active-gtfs');
  assert.equal(result.strategy, 'tagged_stop_id');
  assert.equal(result.resolved, 'active-gtfs_1001');
});

test('resolveStationInput resolves station lookup by bracket id', () => {
  const index = createIndex();
  const result = resolveStationInput('Alpha Station [1001]', index, 'active-gtfs');
  assert.equal(result.strategy, 'station_lookup');
  assert.equal(result.resolved, 'active-gtfs_1001');
});

test('resolveStationInput accepts coordinate tokens', () => {
  const index = createIndex();
  const result = resolveStationInput('48.1000,11.5000', index, 'active-gtfs');
  assert.equal(result.strategy, 'coordinates');
  assert.equal(result.resolved, '48.1,11.5');
});
