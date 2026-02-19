const fs = require('node:fs/promises');
const path = require('node:path');
const { validateOrThrow } = require('../../core/schema');

const ROUTE_CASE_SCHEMA = {
  type: 'object',
  required: ['id', 'origin', 'destination', 'datetime'],
  properties: {
    id: { type: 'string', minLength: 1 },
    label: { type: 'string' },
    origin: { type: 'string', minLength: 1 },
    destination: { type: 'string', minLength: 1 },
    datetime: { type: 'string', minLength: 1 },
    expectedStatus: { type: 'integer', minimum: 100, maximum: 599 }
  },
  additionalProperties: true
};

const ROUTE_CASE_FILE_SCHEMA = {
  type: 'object',
  required: ['version', 'cases'],
  properties: {
    version: { type: 'string', minLength: 1 },
    cases: { type: 'array', minItems: 1, items: ROUTE_CASE_SCHEMA }
  },
  additionalProperties: false
};

async function loadRouteCaseFile(filePath) {
  const raw = JSON.parse(await fs.readFile(filePath, 'utf8'));
  return validateOrThrow(raw, ROUTE_CASE_FILE_SCHEMA, {
    message: `Invalid route case file: ${filePath}`,
    code: 'INVALID_CONFIG'
  });
}

async function loadBaseline(filePath) {
  const raw = JSON.parse(await fs.readFile(filePath, 'utf8'));
  return validateOrThrow(
    raw,
    {
      type: 'object',
      required: ['caseId', 'expected'],
      properties: {
        caseId: { type: 'string', minLength: 1 },
        expected: { type: 'object' }
      },
      additionalProperties: true
    },
    {
      message: `Invalid route baseline file: ${filePath}`,
      code: 'INVALID_CONFIG'
    }
  );
}

async function writeQaReport(reportDir, fileName, payload) {
  const absoluteDir = path.resolve(reportDir);
  await fs.mkdir(absoluteDir, { recursive: true });
  const reportPath = path.join(absoluteDir, fileName);
  await fs.writeFile(reportPath, JSON.stringify(payload, null, 2) + '\n', 'utf8');
  return reportPath;
}

module.exports = {
  loadBaseline,
  loadRouteCaseFile,
  writeQaReport
};
