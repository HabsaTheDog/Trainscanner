const { AppError } = require("../../core/errors");
const { validateOrThrow } = require("../../core/schema");
const { ISO_DATE_RE, isStrictIsoDate } = require("../../core/date");

const runtimeSchema = {
  type: "object",
  properties: {
    mode: { type: "string", minLength: 1 },
    source: { type: "string", minLength: 1 },
    profile: { type: "string" },
    asOf: { type: "string" },
    country: { type: "string" },
    artifactPath: { type: "string" },
  },
  additionalProperties: false,
};

const profileObjectSchema = {
  type: "object",
  properties: {
    sourceType: { type: "string", enum: ["static", "runtime"] },
    zipPath: { type: "string" },
    zip: { type: "string" },
    runtime: runtimeSchema,
    description: { type: "string" },
  },
  additionalProperties: false,
};

function validateRuntimeDescriptor(runtime, contextPath) {
  validateOrThrow(runtime, runtimeSchema, {
    message: `Invalid runtime descriptor at ${contextPath}`,
    code: "INVALID_CONFIG",
  });

  const mode = String(
    runtime.mode || runtime.source || "canonical-export",
  ).trim();
  if (mode !== "canonical-export") {
    throw new AppError({
      code: "INVALID_CONFIG",
      message: `Unsupported runtime mode '${mode}' at ${contextPath}.runtime.mode`,
      details: {
        expectedMode: "canonical-export",
        found: mode,
      },
    });
  }

  if (
    runtime.asOf &&
    runtime.asOf !== "latest" &&
    !isStrictIsoDate(runtime.asOf)
  ) {
    throw new AppError({
      code: "INVALID_CONFIG",
      message: `runtime.asOf must be 'latest' or YYYY-MM-DD at ${contextPath}`,
    });
  }

  if (runtime.country && !["DE", "AT", "CH"].includes(runtime.country)) {
    throw new AppError({
      code: "INVALID_CONFIG",
      message: `runtime.country must be DE, AT, or CH at ${contextPath}`,
    });
  }
}

function validateGtfsProfilesConfig(raw) {
  const source = raw && typeof raw === "object" ? raw.profiles || raw : null;
  if (!source || typeof source !== "object" || Array.isArray(source)) {
    throw new AppError({
      code: "INVALID_CONFIG",
      message:
        "GTFS profiles config must be an object or { profiles: { ... } }",
    });
  }

  for (const [name, entry] of Object.entries(source)) {
    if (!name.trim()) {
      throw new AppError({
        code: "INVALID_CONFIG",
        message: "GTFS profile names must be non-empty strings",
      });
    }

    if (typeof entry === "string") {
      if (!entry.trim()) {
        throw new AppError({
          code: "INVALID_CONFIG",
          message: `Profile '${name}' static path cannot be empty`,
        });
      }
      continue;
    }

    if (!entry || typeof entry !== "object") {
      throw new AppError({
        code: "INVALID_CONFIG",
        message: `Profile '${name}' must be a string path or object`,
      });
    }

    validateOrThrow(entry, profileObjectSchema, {
      message: `Invalid GTFS profile object '${name}'`,
      code: "INVALID_CONFIG",
    });

    if (entry.runtime) {
      validateRuntimeDescriptor(entry.runtime, `profiles.${name}`);
      continue;
    }

    const zipPath =
      (typeof entry.zipPath === "string" && entry.zipPath.trim()) ||
      (typeof entry.zip === "string" && entry.zip.trim()) ||
      "";

    if (!zipPath) {
      throw new AppError({
        code: "INVALID_CONFIG",
        message: `Profile '${name}' must define zipPath/zip or runtime descriptor`,
      });
    }
  }

  return source;
}

module.exports = {
  ISO_DATE_RE,
  validateGtfsProfilesConfig,
  validateRuntimeDescriptor,
};
