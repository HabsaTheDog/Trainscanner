const test = require('node:test');
const assert = require('node:assert/strict');

const { validateSchema, validateOrThrow } = require('../../src/core/schema');

test('validateSchema returns no errors for valid payload', () => {
  const errors = validateSchema(
    { name: 'alpha', count: 2 },
    {
      type: 'object',
      required: ['name', 'count'],
      properties: {
        name: { type: 'string', minLength: 1 },
        count: { type: 'integer', minimum: 1 }
      },
      additionalProperties: false
    }
  );

  assert.equal(errors.length, 0);
});

test('validateOrThrow throws AppError for invalid payload', () => {
  assert.throws(
    () =>
      validateOrThrow(
        { name: '', extra: true },
        {
          type: 'object',
          required: ['name'],
          properties: {
            name: { type: 'string', minLength: 1 }
          },
          additionalProperties: false
        },
        {
          code: 'INVALID_CONFIG',
          message: 'invalid config'
        }
      ),
    (err) => {
      assert.equal(err.code, 'INVALID_CONFIG');
      assert.equal(err.statusCode, 500);
      assert.equal(err.details.errors.length, 2);
      return true;
    }
  );
});
