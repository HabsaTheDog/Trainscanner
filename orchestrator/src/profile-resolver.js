const fs = require('node:fs/promises');
const path = require('node:path');

const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

function normalizeProfiles(raw) {
  const source = raw && typeof raw === 'object' ? raw.profiles || raw : {};
  const normalized = {};

  for (const [name, entry] of Object.entries(source)) {
    if (typeof entry === 'string') {
      normalized[name] = {
        sourceType: 'static',
        zipPath: entry,
        description: ''
      };
      continue;
    }

    if (!entry || typeof entry !== 'object') {
      continue;
    }

    const description = typeof entry.description === 'string' ? entry.description : '';

    if (entry.runtime && typeof entry.runtime === 'object') {
      normalized[name] = {
        sourceType: 'runtime',
        description,
        runtime: {
          mode: String(entry.runtime.mode || entry.runtime.source || 'canonical-export').trim(),
          profile: typeof entry.runtime.profile === 'string' ? entry.runtime.profile.trim() : '',
          asOf: typeof entry.runtime.asOf === 'string' ? entry.runtime.asOf.trim() : 'latest',
          country: typeof entry.runtime.country === 'string' ? entry.runtime.country.trim() : '',
          artifactPath: typeof entry.runtime.artifactPath === 'string' ? entry.runtime.artifactPath.trim() : ''
        }
      };
      continue;
    }

    const zipPath =
      (typeof entry.zipPath === 'string' && entry.zipPath) ||
      (typeof entry.zip === 'string' && entry.zip) ||
      '';

    if (zipPath) {
      normalized[name] = {
        sourceType: 'static',
        zipPath,
        description
      };
    }
  }

  return normalized;
}

function normalizeRelPath(value) {
  return value.split(path.sep).join('/');
}

function projectRootFromDataDir(dataDir) {
  return path.resolve(dataDir, '..');
}

function resolveAgainstProject(projectRoot, maybeRelative) {
  if (path.isAbsolute(maybeRelative)) {
    return maybeRelative;
  }
  return path.resolve(projectRoot, maybeRelative);
}

function toProjectRelativeOrAbsolute(projectRoot, absolutePath) {
  const rel = path.relative(projectRoot, absolutePath);
  if (rel.startsWith('..') || path.isAbsolute(rel)) {
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
    .filter((entry) => entry.isDirectory() && ISO_DATE_RE.test(entry.name))
    .map((entry) => entry.name)
    .sort();

  for (let i = dateDirs.length - 1; i >= 0; i -= 1) {
    const dateDir = dateDirs[i];
    const zipPath = path.join(runtimeRootAbs, dateDir, 'active-gtfs.zip');
    if (await fileExists(zipPath)) {
      return dateDir;
    }
  }

  return null;
}

async function resolveProfileArtifact(profileName, profile, options = {}) {
  const allowMissing = Boolean(options.allowMissing);
  const dataDir = options.dataDir;

  if (!dataDir || typeof dataDir !== 'string') {
    throw new Error('resolveProfileArtifact requires option dataDir');
  }

  const projectRoot = projectRootFromDataDir(dataDir);

  if (!profile || typeof profile !== 'object') {
    throw new Error(`Invalid profile '${profileName}' definition`);
  }

  if (profile.sourceType === 'static') {
    if (!profile.zipPath || typeof profile.zipPath !== 'string') {
      throw new Error(`Static profile '${profileName}' is missing zipPath`);
    }

    const absolutePath = resolveAgainstProject(projectRoot, profile.zipPath);
    const exists = await fileExists(absolutePath);
    if (!exists && !allowMissing) {
      throw new Error(`GTFS zip not found for profile '${profileName}': ${absolutePath}`);
    }

    return {
      sourceType: 'static',
      zipPath: profile.zipPath,
      absolutePath,
      exists,
      runtime: null
    };
  }

  if (profile.sourceType !== 'runtime') {
    throw new Error(`Profile '${profileName}' has unsupported source type`);
  }

  const runtime = profile.runtime || {};
  const mode = runtime.mode || 'canonical-export';
  if (mode !== 'canonical-export') {
    throw new Error(`Profile '${profileName}' runtime mode '${mode}' is unsupported (expected canonical-export)`);
  }

  let absolutePath = '';
  let zipPath = '';
  let resolvedAsOf = runtime.asOf || 'latest';

  if (runtime.artifactPath) {
    absolutePath = resolveAgainstProject(projectRoot, runtime.artifactPath);
    zipPath = runtime.artifactPath;
  } else {
    const runtimeProfile = runtime.profile || profileName;
    const runtimeRootAbs = path.join(dataDir, 'gtfs', 'runtime', runtimeProfile);
    const requestedAsOf = runtime.asOf || 'latest';

    if (requestedAsOf === 'latest') {
      const latest = await pickLatestRuntimeDate(runtimeRootAbs);
      if (!latest) {
        if (allowMissing) {
          const unresolved = path.join(runtimeRootAbs, '<latest>', 'active-gtfs.zip');
          return {
            sourceType: 'runtime',
            zipPath: toProjectRelativeOrAbsolute(projectRoot, unresolved),
            absolutePath: unresolved,
            exists: false,
            runtime: {
              mode,
              profile: runtimeProfile,
              requestedAsOf,
              resolvedAsOf: null,
              country: runtime.country || ''
            }
          };
        }
        throw new Error(
          `No runtime GTFS artifact found for profile '${profileName}' in ${runtimeRootAbs}. Run scripts/qa/build-profile.sh --profile ${runtimeProfile} --as-of <YYYY-MM-DD>.`
        );
      }
      resolvedAsOf = latest;
    } else {
      if (!ISO_DATE_RE.test(requestedAsOf)) {
        throw new Error(`Profile '${profileName}' runtime.asOf must be 'latest' or YYYY-MM-DD`);
      }
      resolvedAsOf = requestedAsOf;
    }

    absolutePath = path.join(runtimeRootAbs, resolvedAsOf, 'active-gtfs.zip');
    zipPath = toProjectRelativeOrAbsolute(projectRoot, absolutePath);
  }

  const exists = await fileExists(absolutePath);
  if (!exists && !allowMissing) {
    throw new Error(`Runtime GTFS artifact not found for profile '${profileName}': ${absolutePath}`);
  }

  return {
    sourceType: 'runtime',
    zipPath,
    absolutePath,
    exists,
    runtime: {
      mode,
      profile: runtime.profile || profileName,
      requestedAsOf: runtime.asOf || 'latest',
      resolvedAsOf,
      country: runtime.country || ''
    }
  };
}

module.exports = {
  normalizeProfiles,
  resolveProfileArtifact
};
