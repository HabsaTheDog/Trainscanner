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
    runtime.mode || runtime.source || "pan-europe-export",
  ).trim();
  if (mode !== "pan-europe-export") {
    throw new AppError({
      code: "INVALID_CONFIG",
      message: `Unsupported runtime mode '${mode}' at ${contextPath}.runtime.mode`,
      details: {
        expectedMode: "pan-europe-export",
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

  if (runtime.country && !/^[A-Z]{2}$/.test(runtime.country)) {
    throw new AppError({
      code: "INVALID_CONFIG",
      message: `runtime.country must be an ISO-3166 alpha-2 code at ${contextPath}`,
    });
  }
}

function validateStaticProfilePath(name, entry) {
  if (!entry.trim()) {
    throw new AppError({
      code: "INVALID_CONFIG",
      message: `Profile '${name}' static path cannot be empty`,
    });
  }
}

function validateProfileObject(name, entry) {
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
    return;
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

function validateProfileEntry(name, entry) {
  if (!name.trim()) {
    throw new AppError({
      code: "INVALID_CONFIG",
      message: "GTFS profile names must be non-empty strings",
    });
  }

  if (typeof entry === "string") {
    validateStaticProfilePath(name, entry);
    return;
  }

  validateProfileObject(name, entry);
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
    validateProfileEntry(name, entry);
  }

  return source;
}

module.exports = {
  ISO_DATE_RE,
  validateGtfsProfilesConfig,
  validateRuntimeDescriptor,
};
