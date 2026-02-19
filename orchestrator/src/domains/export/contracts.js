const fs = require('node:fs');
const crypto = require('node:crypto');
const { validateOrThrow } = require('../../core/schema');

const EXPORT_OPTIONS_SCHEMA = {
  type: 'object',
  required: ['profile', 'asOf'],
  properties: {
    profile: { type: 'string', minLength: 1 },
    asOf: { type: 'string', pattern: /^\d{4}-\d{2}-\d{2}$/ },
    country: { type: 'string', enum: ['DE', 'AT', 'CH'] },
    outputZip: { type: 'string', minLength: 1 }
  },
  additionalProperties: false
};

const EXPORT_MANIFEST_SCHEMA = {
  type: 'object',
  required: ['profile', 'asOf', 'artifactPath', 'sha256', 'generationTimestamp'],
  properties: {
    profile: { type: 'string', minLength: 1 },
    asOf: { type: 'string', pattern: /^\d{4}-\d{2}-\d{2}$/ },
    countryScope: { type: 'string', enum: ['DE', 'AT', 'CH'] },
    bridgeMode: { type: 'string' },
    artifactPath: { type: 'string', minLength: 1 },
    manifestPath: { type: 'string' },
    dbSnapshotBounds: { type: 'object' },
    rowCounts: { type: 'object' },
    sha256: { type: 'string', pattern: /^[a-f0-9]{64}$/ },
    generationTimestamp: { type: 'string', minLength: 1 }
  },
  additionalProperties: true
};

function sortObject(value) {
  if (Array.isArray(value)) {
    return value.map(sortObject);
  }
  if (value && typeof value === 'object') {
    const out = {};
    for (const key of Object.keys(value).sort()) {
      out[key] = sortObject(value[key]);
    }
    return out;
  }
  return value;
}

function deterministicJson(value) {
  return JSON.stringify(sortObject(value));
}

function deterministicObjectHash(value) {
  return crypto.createHash('sha256').update(deterministicJson(value)).digest('hex');
}

function hashFileSha256(filePath) {
  const hash = crypto.createHash('sha256');
  const content = fs.readFileSync(filePath);
  hash.update(content);
  return hash.digest('hex');
}

function validateExportOptions(options) {
  return validateOrThrow(options || {}, EXPORT_OPTIONS_SCHEMA, {
    message: 'Invalid export options',
    code: 'INVALID_CONFIG'
  });
}

function validateExportManifest(manifest) {
  return validateOrThrow(manifest || {}, EXPORT_MANIFEST_SCHEMA, {
    message: 'Invalid export manifest',
    code: 'INVALID_CONFIG'
  });
}

module.exports = {
  deterministicJson,
  deterministicObjectHash,
  hashFileSha256,
  validateExportManifest,
  validateExportOptions
};
