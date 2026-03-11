function parseBooleanEnv(value, fallback) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }

  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }
  return fallback;
}

function parseIntegerEnv(value, fallback, options = {}) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isInteger(parsed)) {
    return fallback;
  }

  const min =
    Number.isInteger(options.min) || Number.isFinite(options.min)
      ? Number(options.min)
      : Number.NEGATIVE_INFINITY;
  const max =
    Number.isInteger(options.max) || Number.isFinite(options.max)
      ? Number(options.max)
      : Number.POSITIVE_INFINITY;

  if (parsed < min || parsed > max) {
    return fallback;
  }
  return parsed;
}

function resolveTemporalAddress(env = process.env) {
  const value = env && typeof env === "object" ? env.TEMPORAL_ADDRESS : "";
  return String(value || "").trim() || "localhost:7233";
}

function readJobExecutionConfig(env = process.env) {
  return {
    jobOrchestrationEnabled: parseBooleanEnv(
      env?.PIPELINE_JOB_ORCHESTRATION_ENABLED,
      true,
    ),
    maxAttempts: parseIntegerEnv(env?.PIPELINE_JOB_MAX_ATTEMPTS, 3, {
      min: 1,
    }),
    maxConcurrent: parseIntegerEnv(env?.PIPELINE_JOB_MAX_CONCURRENT, 1, {
      min: 1,
    }),
  };
}

function readCircuitBreakerConfig(env = process.env, options = {}) {
  return {
    failureThreshold: parseIntegerEnv(
      env?.[options.thresholdKey],
      options.defaultFailureThreshold ?? 3,
      { min: 1 },
    ),
    cooldownMs: parseIntegerEnv(
      env?.[options.cooldownKey],
      options.defaultCooldownMs ?? 15000,
      { min: 1 },
    ),
  };
}

module.exports = {
  parseBooleanEnv,
  parseIntegerEnv,
  readCircuitBreakerConfig,
  readJobExecutionConfig,
  resolveTemporalAddress,
};
