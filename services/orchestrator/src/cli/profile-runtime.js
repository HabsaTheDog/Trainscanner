#!/usr/bin/env node
const fs = require("node:fs");
const path = require("node:path");
const {
  normalizeProfiles,
  resolveProfileArtifact,
} = require("../profile-resolver");

function parseArgs(argv) {
  const args = {
    command: "",
    root: process.cwd(),
    profile: "",
  };

  const [command, ...rest] = argv;
  args.command = command || "";

  for (let i = 0; i < rest.length; i += 1) {
    const arg = rest[i];
    if (arg === "--root") {
      args.root = rest[i + 1] || args.root;
      i += 1;
      continue;
    }
    if (arg === "--profile") {
      args.profile = rest[i + 1] || "";
      i += 1;
      continue;
    }
    if (arg === "-h" || arg === "--help") {
      printHelp();
      process.exit(0);
    }
    throw new Error(`Unknown argument: ${arg}`);
  }

  return args;
}

function printHelp() {
  process.stdout.write("Usage:\n");
  process.stdout.write(
    "  node services/orchestrator/src/cli/profile-runtime.js resolve-artifact --profile <name> [--root <path>]\n",
  );
  process.stdout.write(
    "  node services/orchestrator/src/cli/profile-runtime.js resolve-default-profile [--root <path>]\n",
  );
}

function readJson(filePath, fallback = null) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return fallback;
  }
}

async function resolveArtifact(rootDir, profileName) {
  const profilesPath = path.join(rootDir, "config", "gtfs-profiles.json");
  const profilesRaw = readJson(profilesPath, null);
  if (!profilesRaw) {
    throw new Error(`Failed to read profiles: ${profilesPath}`);
  }

  const profiles = normalizeProfiles(profilesRaw);
  const selected = profiles[profileName];
  if (!selected) {
    throw new Error(`Profile '${profileName}' not found in ${profilesPath}`);
  }

  const resolved = await resolveProfileArtifact(profileName, selected, {
    dataDir: path.join(rootDir, "data"),
    allowMissing: false,
  });

  process.stdout.write(
    JSON.stringify({
      profile: profileName,
      sourceType: resolved.sourceType,
      zipPath: resolved.zipPath,
      absolutePath: resolved.absolutePath,
      runtime: resolved.runtime || null,
    }),
  );
}

function resolveDefaultProfile(rootDir) {
  const activePath = path.join(
    rootDir,
    "services",
    "orchestrator",
    "state",
    "active-gtfs.json",
  );
  const legacyActivePath = path.join(rootDir, "config", "active-gtfs.json");
  const profilesPath = path.join(rootDir, "config", "gtfs-profiles.json");

  const activeState =
    readJson(activePath, null) || readJson(legacyActivePath, null) || {};
  const profilesRaw = readJson(profilesPath, null);
  if (!profilesRaw) {
    throw new Error(`Failed to read profiles: ${profilesPath}`);
  }

  const profiles = normalizeProfiles(profilesRaw);
  const names = Object.keys(profiles);
  const activeName =
    typeof activeState.activeProfile === "string"
      ? activeState.activeProfile
      : "";

  if (activeName && names.includes(activeName)) {
    process.stdout.write(activeName);
    return;
  }

  if (names.length > 0) {
    process.stdout.write(names[0]);
    return;
  }

  throw new Error("No profiles configured");
}

(async () => {
  const args = parseArgs(process.argv.slice(2));
  const rootDir = path.resolve(args.root);

  if (args.command === "resolve-artifact") {
    if (!args.profile) {
      throw new Error("resolve-artifact requires --profile");
    }
    await resolveArtifact(rootDir, args.profile);
    return;
  }

  if (args.command === "resolve-default-profile") {
    resolveDefaultProfile(rootDir);
    return;
  }

  throw new Error(`Unknown command '${args.command}'`);
})().catch((err) => {
  process.stderr.write(`[profile-runtime] ERROR: ${err.message}\n`);
  process.exit(1);
});
