const test = require('node:test');
const assert = require('node:assert/strict');

const { validateSourceDiscoveryConfig } = require('../../src/domains/source-discovery/contracts');

test('accepts NeTEx sources without fallback reason', () => {
  const payload = {
    schemaVersion: '1.0.0',
    sources: [
      {
        id: 'de_netex',
        country: 'DE',
        provider: 'provider',
        datasetName: 'dataset',
        format: 'netex',
        accessType: 'public',
        downloadMethod: 'manual_redirect',
        downloadUrlOrEndpoint: 'https://example.invalid/dataset'
      }
    ]
  };

  const result = validateSourceDiscoveryConfig(payload);
  assert.equal(result.sources.length, 1);
});

test('rejects GTFS source without fallbackReason', () => {
  const payload = {
    schemaVersion: '1.0.0',
    sources: [
      {
        id: 'de_gtfs',
        country: 'DE',
        provider: 'provider',
        datasetName: 'dataset',
        format: 'gtfs',
        accessType: 'public',
        downloadMethod: 'manual_redirect',
        downloadUrlOrEndpoint: 'https://example.invalid/dataset'
      }
    ]
  };

  assert.throws(() => validateSourceDiscoveryConfig(payload), /fallbackReason/);
});
