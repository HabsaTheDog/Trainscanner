const { AppError } = require("./errors");

function pushError(errors, path, message) {
  errors.push({ path, message });
}

function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function validateType(value, type) {
  if (type === "array") {
    return Array.isArray(value);
  }
  if (type === "integer") {
    return Number.isInteger(value);
  }
  if (type === "number") {
    return typeof value === "number" && Number.isFinite(value);
  }
  if (type === "object") {
    return isObject(value);
  }
  return typeof value === type;
}

function validateEnum(value, schema, path, errors) {
  if (schema?.enum && !schema.enum.includes(value)) {
    pushError(errors, path, `Expected one of: ${schema.enum.join(", ")}`);
  }
}

function validateStringConstraints(value, schema, path, errors) {
  if (typeof value !== "string") {
    return;
  }
  if (Number.isInteger(schema?.minLength) && value.length < schema.minLength) {
    pushError(errors, path, `Expected minimum length ${schema.minLength}`);
  }
  if (Number.isInteger(schema?.maxLength) && value.length > schema.maxLength) {
    pushError(errors, path, `Expected maximum length ${schema.maxLength}`);
  }
  if (!schema?.pattern) {
    return;
  }

  const rx =
    schema.pattern instanceof RegExp
      ? schema.pattern
      : new RegExp(schema.pattern);
  if (!rx.test(value)) {
    pushError(errors, path, `Expected to match pattern ${rx}`);
  }
}

function validateNumberConstraints(value, schema, path, errors) {
  if (typeof value !== "number") {
    return;
  }
  if (Number.isFinite(schema?.minimum) && value < schema.minimum) {
    pushError(errors, path, `Expected minimum ${schema.minimum}`);
  }
  if (Number.isFinite(schema?.maximum) && value > schema.maximum) {
    pushError(errors, path, `Expected maximum ${schema.maximum}`);
  }
}

function validateArrayConstraints(value, schema, path, errors) {
  if (!Array.isArray(value)) {
    return;
  }
  if (Number.isInteger(schema?.minItems) && value.length < schema.minItems) {
    pushError(errors, path, `Expected at least ${schema.minItems} items`);
  }
  if (Number.isInteger(schema?.maxItems) && value.length > schema.maxItems) {
    pushError(errors, path, `Expected at most ${schema.maxItems} items`);
  }
  if (!schema?.items) {
    return;
  }
  for (let i = 0; i < value.length; i += 1) {
    errors.push(...validateSchema(value[i], schema.items, `${path}[${i}]`));
  }
}

function validateObjectConstraints(value, schema, path, errors) {
  if (!isObject(value)) {
    return;
  }
  const required = schema?.required || [];
  for (const key of required) {
    if (!(key in value)) {
      pushError(errors, `${path}.${key}`, "Missing required property");
    }
  }

  const properties = schema?.properties || {};
  for (const [key, def] of Object.entries(properties)) {
    if (key in value) {
      errors.push(...validateSchema(value[key], def, `${path}.${key}`));
    }
  }

  if (schema?.additionalProperties !== false) {
    return;
  }
  const allowed = new Set(Object.keys(properties));
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) {
      pushError(errors, `${path}.${key}`, "Unknown property");
    }
  }
}

function validateCustomChecks(value, schema, path, errors) {
  if (!schema || !Array.isArray(schema.customChecks)) {
    return;
  }
  for (const check of schema.customChecks) {
    const result = check(value);
    if (typeof result === "string" && result.length > 0) {
      pushError(errors, path, result);
    }
  }
}

function validateSchema(value, schema, path = "$") {
  const errors = [];

  const type = schema?.type;
  if (type && !validateType(value, type)) {
    pushError(errors, path, `Expected ${type}`);
    return errors;
  }

  validateEnum(value, schema, path, errors);
  validateStringConstraints(value, schema, path, errors);
  validateNumberConstraints(value, schema, path, errors);
  validateArrayConstraints(value, schema, path, errors);
  validateObjectConstraints(value, schema, path, errors);
  validateCustomChecks(value, schema, path, errors);

  return errors;
}

function validateOrThrow(value, schema, options = {}) {
  const errors = validateSchema(value, schema, options.path || "$");
  if (errors.length === 0) {
    return value;
  }

  throw new AppError({
    code: options.code || "INVALID_CONFIG",
    statusCode: options.statusCode || 500,
    message: options.message || "Schema validation failed",
    details: {
      errors,
    },
  });
}

module.exports = {
  validateSchema,
  validateOrThrow,
};
