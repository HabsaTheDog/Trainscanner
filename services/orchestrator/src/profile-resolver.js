const fs = require("node:fs/promises");
const path = require("node:path");
const { AppError } = require("./core/errors");
const { isStrictIsoDate } = require("./core/date");
const {
  validateGtfsProfilesConfig,
} = require("./domains/switch-runtime/contracts");

function normalizeProfiles(raw) {
  const source = validateGtfsProfilesConfig(raw);
  const normalized = {};

  for (const [name, entry] of Object.entries(source)) {
    const normalizedEntry = normalizeProfileEntry(entry);
    if (normalizedEntry) {
      normalized[name] = normalizedEntry;
    }
  }

  return normalized;
}

function normalizeRuntimeDescriptor(runtime) {
  return {
    mode: String(runtime.mode || runtime.source || "canonical-export").trim(),
    profile: typeof runtime.profile === "string" ? runtime.profile.trim() : "",
    asOf: typeof runtime.asOf === "string" ? runtime.asOf.trim() : "latest",
    country: typeof runtime.country === "string" ? runtime.country.trim() : "",
    artifactPath:
      typeof runtime.artifactPath === "string"
        ? runtime.artifactPath.trim()
        : "",
  };
}

function normalizeProfileEntry(entry) {
  if (typeof entry === "string") {
    return {
      sourceType: "static",
      zipPath: entry,
      description: "",
    };
  }

  if (!entry || typeof entry !== "object") {
    return null;
  }

  const description =
    typeof entry.description === "string" ? entry.description : "";
  if (entry.runtime && typeof entry.runtime === "object") {
    return {
      sourceType: "runtime",
      description,
      runtime: normalizeRuntimeDescriptor(entry.runtime),
    };
  }

  const zipPath =
    (typeof entry.zipPath === "string" && entry.zipPath) ||
    (typeof entry.zip === "string" && entry.zip) ||
    "";
  if (!zipPath) {
    return null;
  }

  return {
    sourceType: "static",
    zipPath,
    description,
  };
}

function normalizeRelPath(value) {
  return value.split(path.sep).join("/");
}

function projectRootFromDataDir(dataDir) {
  return path.resolve(dataDir, "..");
}

function resolveAgainstProject(projectRoot, maybeRelative) {
  if (path.isAbsolute(maybeRelative)) {
    return maybeRelative;
  }
  return path.resolve(projectRoot, maybeRelative);
}

function toProjectRelativeOrAbsolute(projectRoot, absolutePath) {
  const rel = path.relative(projectRoot, absolutePath);
  if (rel.startsWith("..") || path.isAbsolute(rel)) {
    return absolutePath;
  }
  return normalizeRelPath(rel);
}

async function fileExists(filePath) {
  try {
    const stat = await fs.stat(filePath);
    return stat.isFile();
  } catch {
    return false;
  }
}

async function pickLatestRuntimeDate(runtimeRootAbs) {
  let entries;
  try {
    entries = await fs.readdir(runtimeRootAbs, { withFileTypes: true });
  } catch {
    return null;
  }

  const dateDirs = entries
    .filter((entry) => entry.isDirectory() && isStrictIsoDate(entry.name))
    .map((entry) => entry.name)
    .sort();

  for (let i = dateDirs.length - 1; i >= 0; i -= 1) {
    const dateDir = dateDirs[i];
    const zipPath = path.join(runtimeRootAbs, dateDir, "active-gtfs.zip");
    if (await fileExists(zipPath)) {
      return dateDir;
    }
  }

  return null;
}

function assertDataDir(dataDir) {
  if (!dataDir || typeof dataDir !== "string") {
    throw new AppError({
      code: "INVALID_CONFIG",
      message: "resolveProfileArtifact requires option dataDir",
    });
  }
}

function assertProfileDefinition(profileName, profile) {
  if (!profile || typeof profile !== "object") {
    throw new AppError({
      code: "INVALID_CONFIG",
      message: `Invalid profile '${profileName}' definition`,
    });
  }
}

async function resolveStaticProfileArtifact(
  profileName,
  profile,
  projectRoot,
  allowMissing,
) {
  if (!profile.zipPath || typeof profile.zipPath !== "string") {
    throw new AppError({
      code: "INVALID_CONFIG",
      message: `Static profile '${profileName}' is missing zipPath`,
    });
  }

  const absolutePath = resolveAgainstProject(projectRoot, profile.zipPath);
  const exists = await fileExists(absolutePath);
  if (!exists && !allowMissing) {
    throw new AppError({
      code: "PROFILE_ARTIFACT_MISSING",
      statusCode: 404,
      message: `GTFS zip not found for profile '${profileName}': ${absolutePath}`,
    });
  }

  return {
    sourceType: "static",
    zipPath: profile.zipPath,
    absolutePath,
    exists,
    runtime: null,
  };
}

function resolveRuntimeMode(profileName, runtime) {
  const mode = runtime.mode || "canonical-export";
  if (mode !== "canonical-export") {
    throw new AppError({
      code: "INVALID_CONFIG",
      message: `Profile '${profileName}' runtime mode '${mode}' is unsupported (expected canonical-export)`,
    });
  }
  return mode;
}

function buildMissingRuntimeArtifactResult(
  projectRoot,
  unresolvedPath,
  runtime,
  runtimeProfile,
  mode,
) {
  return {
    sourceType: "runtime",
    zipPath: toProjectRelativeOrAbsolute(projectRoot, unresolvedPath),
    absolutePath: unresolvedPath,
    exists: false,
    runtime: {
      mode,
      profile: runtimeProfile,
      requestedAsOf: runtime.asOf || "latest",
      resolvedAsOf: null,
      country: runtime.country || "",
    },
  };
}

async function resolveRuntimeDate(options) {
  const {
    profileName,
    runtimeProfile,
    runtimeRootAbs,
    requestedAsOf,
    allowMissing,
    mode,
    projectRoot,
    runtime,
  } = options;
  if (requestedAsOf !== "latest") {
    if (!isStrictIsoDate(requestedAsOf)) {
      throw new AppError({
        code: "INVALID_CONFIG",
        message: `Profile '${profileName}' runtime.asOf must be 'latest' or YYYY-MM-DD`,
      });
    }
    return requestedAsOf;
  }

  const latest = await pickLatestRuntimeDate(runtimeRootAbs);
  if (latest) {
    return latest;
  }

  if (allowMissing) {
    const unresolved = path.join(runtimeRootAbs, "<latest>", "active-gtfs.zip");
    return buildMissingRuntimeArtifactResult(
      projectRoot,
      unresolved,
      runtime,
      runtimeProfile,
      mode,
    );
  }

  throw new AppError({
    code: "PROFILE_ARTIFACT_MISSING",
    statusCode: 404,
    message: `No runtime GTFS artifact found for profile '${profileName}' in ${runtimeRootAbs}. Run scripts/qa/build-profile.sh --profile ${runtimeProfile} --as-of <YYYY-MM-DD>.`,
  });
}

async function resolveRuntimeProfileArtifact(
  profileName,
  profile,
  dataDir,
  projectRoot,
  allowMissing,
) {
  const runtime = profile.runtime || {};
  const mode = resolveRuntimeMode(profileName, runtime);

  let absolutePath = "";
  let zipPath = "";
  let resolvedAsOf = runtime.asOf || "latest";

  if (runtime.artifactPath) {
    absolutePath = resolveAgainstProject(projectRoot, runtime.artifactPath);
    zipPath = runtime.artifactPath;
  } else {
    const runtimeProfile = runtime.profile || profileName;
    const runtimeRootAbs = path.join(
      dataDir,
      "gtfs",
      "runtime",
      runtimeProfile,
    );
    const requestedAsOf = runtime.asOf || "latest";
    const resolved = await resolveRuntimeDate({
      profileName,
      runtimeProfile,
      runtimeRootAbs,
      requestedAsOf,
      allowMissing,
      mode,
      projectRoot,
      runtime,
    });

    if (typeof resolved === "object") {
      return resolved;
    }

    resolvedAsOf = resolved;
    absolutePath = path.join(runtimeRootAbs, resolvedAsOf, "active-gtfs.zip");
    zipPath = toProjectRelativeOrAbsolute(projectRoot, absolutePath);
  }

  const exists = await fileExists(absolutePath);
  if (!exists && !allowMissing) {
    throw new AppError({
      code: "PROFILE_ARTIFACT_MISSING",
      statusCode: 404,
      message: `Runtime GTFS artifact not found for profile '${profileName}': ${absolutePath}`,
    });
  }

  return {
    sourceType: "runtime",
    zipPath,
    absolutePath,
    exists,
    runtime: {
      mode,
      profile: runtime.profile || profileName,
      requestedAsOf: runtime.asOf || "latest",
      resolvedAsOf,
      country: runtime.country || "",
    },
  };
}

async function resolveProfileArtifact(profileName, profile, options = {}) {
  const allowMissing = Boolean(options.allowMissing);
  const dataDir = options.dataDir;

  assertDataDir(dataDir);

  const projectRoot = projectRootFromDataDir(dataDir);
  assertProfileDefinition(profileName, profile);

  if (profile.sourceType === "static") {
    return resolveStaticProfileArtifact(
      profileName,
      profile,
      projectRoot,
      allowMissing,
    );
  }

  if (profile.sourceType !== "runtime") {
    throw new AppError({
      code: "INVALID_CONFIG",
      message: `Profile '${profileName}' has unsupported source type`,
    });
  }

  return resolveRuntimeProfileArtifact(
    profileName,
    profile,
    dataDir,
    projectRoot,
    allowMissing,
  );
}

module.exports = {
  normalizeProfiles,
  resolveProfileArtifact,
};
