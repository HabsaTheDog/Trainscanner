const { execFile } = require("node:child_process");
const fs = require("node:fs/promises");
const path = require("node:path");
const { promisify } = require("node:util");

const execFileAsync = promisify(execFile);
const VALID_TIERS = new Set(["high-speed", "regional", "local", "all"]);

function isIsoDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(value || "");
}

function normalizeTier(value) {
  const clean = String(value || "all")
    .trim()
    .toLowerCase();
  if (!VALID_TIERS.has(clean)) {
    throw new Error(
      `Invalid tier '${value}'. Expected one of: high-speed, regional, local, all`,
    );
  }
  return clean;
}

function resolveRepoRoot(config) {
  const base = path.resolve(config.rootDir || process.cwd());
  const candidates = [
    base,
    path.resolve(base, ".."),
    path.resolve(base, "..", ".."),
  ];

  for (const candidate of candidates) {
    const exportScript = path.join(
      candidate,
      "scripts",
      "qa",
      "export-canonical-gtfs.py",
    );
    if (require("node:fs").existsSync(exportScript)) {
      return candidate;
    }
  }

  throw new Error(
    "Could not resolve repository root for GTFS export script (scripts/qa/export-canonical-gtfs.py)",
  );
}

function toAbsolutePath(rootDir, value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }
  if (path.isAbsolute(raw)) {
    return raw;
  }
  return path.resolve(rootDir, raw);
}

function slugifyProfile(profile) {
  return String(profile || "pan_europe_runtime")
    .trim()
    .replaceAll(/[^A-Za-z0-9._-]+/g, "-")
    .replaceAll(/-+/g, "-")
    .replaceAll(/^-|-$/g, "");
}

function createCompileActivities(_dbClient, config) {
  const repoRoot = resolveRepoRoot(config || {});
  const exportScript = path.join(
    repoRoot,
    "scripts",
    "qa",
    "export-canonical-gtfs.py",
  );

  return {
    async compileGtfsArtifact(input = {}) {
      const tier = normalizeTier(input.tier);
      const profile =
        String(input.profile || "pan_europe_runtime").trim() ||
        "pan_europe_runtime";
      const asOf = String(input.asOf || "").trim();
      const country = String(input.country || "")
        .trim()
        .toUpperCase();

      if (!isIsoDate(asOf)) {
        throw new Error(
          `Invalid asOf '${input.asOf}'. Expected ISO date (YYYY-MM-DD).`,
        );
      }

      if (country && !/^[A-Z]{2}$/.test(country)) {
        throw new Error(
          `Invalid country '${input.country}'. Expected ISO-3166 alpha-2 code.`,
        );
      }

      const outputDir = toAbsolutePath(
        repoRoot,
        input.outputDir || path.join("data", "artifacts"),
      );
      await fs.mkdir(outputDir, { recursive: true });

      const defaultStem = `${slugifyProfile(profile)}-${tier}-${asOf}`;
      const outputZip = toAbsolutePath(
        repoRoot,
        input.outputZip || path.join(outputDir, `${defaultStem}.zip`),
      );
      const summaryJson = toAbsolutePath(
        repoRoot,
        input.summaryJson ||
          path.join(outputDir, `${defaultStem}.summary.json`),
      );

      await fs.mkdir(path.dirname(outputZip), { recursive: true });
      await fs.mkdir(path.dirname(summaryJson), { recursive: true });

      const args = [
        exportScript,
        "--from-db",
        "--profile",
        profile,
        "--as-of",
        asOf,
        "--tier",
        tier,
        "--output-zip",
        outputZip,
        "--summary-json",
        summaryJson,
      ];

      if (country) {
        args.push("--country", country);
      }

      const { stdout, stderr } = await execFileAsync("python3", args, {
        cwd: repoRoot,
        env: process.env,
        maxBuffer: 64 * 1024 * 1024,
      });

      const result = {
        success: true,
        profile,
        tier,
        asOf,
        country: country || null,
        outputZip,
        summaryJson,
        stdout,
        stderr,
      };

      console.log("compileGtfsArtifact completed", {
        profile,
        tier,
        asOf,
        country: country || null,
        outputZip,
        summaryJson,
      });

      return result;
    },
  };
}

module.exports = {
  createCompileActivities,
};
