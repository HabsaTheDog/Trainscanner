const ERROR_DEFINITIONS = {
  INVALID_CONFIG: { statusCode: 500 },
  INVALID_JSON: { statusCode: 400 },
  REQUEST_TOO_LARGE: { statusCode: 413 },
  INVALID_REQUEST: { statusCode: 400 },
  UNKNOWN_PROFILE: { statusCode: 404 },
  PROFILE_ARTIFACT_MISSING: { statusCode: 404 },
  SWITCH_CONFLICT: { statusCode: 409 },
  SWITCH_LOCK_HELD: { statusCode: 409 },
  ROUTE_NOT_READY: { statusCode: 409 },
  MOTIS_UNAVAILABLE: { statusCode: 502 },
  STATION_INDEX_FAILED: { statusCode: 500 },
  SOURCE_FETCH_FAILED: { statusCode: 500 },
  SOURCE_VERIFY_FAILED: { statusCode: 500 },
  INGEST_FAILED: { statusCode: 500 },
  CANONICAL_BUILD_FAILED: { statusCode: 500 },
  REVIEW_QUEUE_BUILD_FAILED: { statusCode: 500 },
  REVIEW_QUEUE_REPORT_FAILED: { statusCode: 500 },
  JOB_CONFLICT: { statusCode: 409 },
  JOB_BACKPRESSURE: { statusCode: 429 },
  CIRCUIT_OPEN: { statusCode: 503 },
  INTERNAL_ERROR: { statusCode: 500 },
};

class AppError extends Error {
  constructor({ code, message, statusCode, details, cause } = {}) {
    super(message || "Unknown error");
    this.name = "AppError";
    this.code = code || "INTERNAL_ERROR";
    this.statusCode =
      Number.isInteger(statusCode) && statusCode > 0
        ? statusCode
        : ERROR_DEFINITIONS[this.code]?.statusCode || 500;
    this.details = details || null;
    if (cause) {
      this.cause = cause;
    }
    Error.captureStackTrace?.(this, AppError);
  }
}

function isAppError(err) {
  return Boolean(
    err &&
      typeof err === "object" &&
      err.name === "AppError" &&
      typeof err.code === "string",
  );
}

function toAppError(
  err,
  fallbackCode = "INTERNAL_ERROR",
  fallbackMessage = "Internal server error",
) {
  if (isAppError(err)) {
    return err;
  }

  const message = err?.message ? err.message : fallbackMessage;
  const statusCode =
    err && Number.isInteger(err.statusCode) ? err.statusCode : undefined;
  return new AppError({
    code: fallbackCode,
    message,
    statusCode,
    cause: err,
  });
}

function errorToPayload(err, options = {}) {
  const includeDetails = Boolean(options.includeDetails);
  const appErr = toAppError(err);
  const payload = {
    error: appErr.message,
    errorCode: appErr.code,
  };
  if (
    includeDetails &&
    appErr.details !== null &&
    appErr.details !== undefined
  ) {
    payload.details = appErr.details;
  }
  return {
    statusCode: appErr.statusCode,
    payload,
  };
}

function assert(condition, errorLike) {
  if (condition) {
    return;
  }
  if (errorLike instanceof Error) {
    throw errorLike;
  }
  throw new AppError(
    errorLike || { code: "INTERNAL_ERROR", message: "Assertion failed" },
  );
}

module.exports = {
  AppError,
  ERROR_DEFINITIONS,
  assert,
  errorToPayload,
  isAppError,
  toAppError,
};
