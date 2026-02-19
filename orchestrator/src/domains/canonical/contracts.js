const { validateOrThrow } = require('../../core/schema');

const CANONICAL_SCOPE_SCHEMA = {
  type: 'object',
  properties: {
    country: { type: 'string', enum: ['DE', 'AT', 'CH'] },
    asOf: { type: 'string', pattern: /^\d{4}-\d{2}-\d{2}$/ },
    sourceId: { type: 'string', minLength: 1 },
    geoThresholdMeters: { type: 'integer', minimum: 1 }
  },
  additionalProperties: false
};

function validateCanonicalScope(scope) {
  return validateOrThrow(scope || {}, CANONICAL_SCOPE_SCHEMA, {
    message: 'Invalid canonical scope options',
    code: 'INVALID_CONFIG'
  });
}

module.exports = {
  validateCanonicalScope
};
