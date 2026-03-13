#!/usr/bin/env node

const fs = require("node:fs");
const readline = require("node:readline");

const RELEVANT_FEATURE_CODES = new Set([
  "RSTN",
  "RSTP",
  "RSTNQ",
  "STNF",
  "BUSTN",
  "MTRST",
  "MTRO",
  "RSTNB",
  "HSTS",
  "AIRP",
  "FY",
]);

function normalizeStationName(value) {
  return String(value || "")
    .toLowerCase()
    .replaceAll(/[^a-z0-9]+/g, " ")
    .replaceAll(/\s+/g, " ")
    .trim();
}

function parseArgs(argv = []) {
  const parsed = {
    input: "",
    country: "",
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = String(argv[index] || "");
    switch (token) {
      case "--input":
        parsed.input = String(argv[index + 1] || "").trim();
        index += 1;
        break;
      case "--country":
        parsed.country = String(argv[index + 1] || "")
          .trim()
          .toUpperCase();
        index += 1;
        break;
      default:
        throw new Error(`Unknown argument: ${token}`);
    }
  }

  if (!parsed.input) {
    throw new Error("--input is required for GeoNames imports");
  }

  return parsed;
}

function isRelevantFeatureCode(featureClass, featureCode) {
  const cleanCode = String(featureCode || "")
    .trim()
    .toUpperCase();
  if (RELEVANT_FEATURE_CODES.has(cleanCode)) {
    return true;
  }
  return (
    String(featureClass || "")
      .trim()
      .toUpperCase() === "S" && cleanCode.includes("STN")
  );
}

async function loadRows(parsed) {
  const rows = [];
  const stream = fs.createReadStream(parsed.input, "utf8");
  const rl = readline.createInterface({
    input: stream,
    crlfDelay: Number.POSITIVE_INFINITY,
  });

  for await (const line of rl) {
    if (!line) {
      continue;
    }
    const columns = line.split("\t");
    if (columns.length < 9) {
      continue;
    }

    const featureClass = columns[6];
    const featureCode = columns[7];
    const country = String(columns[8] || "")
      .trim()
      .toUpperCase();
    if (parsed.country && country !== parsed.country) {
      continue;
    }
    if (!isRelevantFeatureCode(featureClass, featureCode)) {
      continue;
    }

    const displayName = String(columns[1] || columns[2] || "").trim();
    const externalId = String(columns[0] || "").trim();
    if (!displayName || !externalId) {
      continue;
    }

    rows.push({
      external_id: externalId,
      display_name: displayName,
      normalized_name: normalizeStationName(displayName),
      country,
      latitude: Number(columns[4]),
      longitude: Number(columns[5]),
      category: "station",
      subtype: String(featureCode || "")
        .trim()
        .toLowerCase(),
      source_url: `https://www.geonames.org/${externalId}`,
      metadata: {
        geonames_id: externalId,
        feature_class: String(featureClass || "").trim(),
        feature_code: String(featureCode || "").trim(),
      },
    });
  }

  return rows;
}

async function run() {
  const parsed = parseArgs(process.argv.slice(2));
  const rows = await loadRows(parsed);
  process.stdout.write(JSON.stringify(rows));
}

run().catch((error) => {
  process.stderr.write(`${error.message}\n`);
  process.exit(1);
});
