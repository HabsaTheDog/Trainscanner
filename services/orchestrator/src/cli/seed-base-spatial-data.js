#!/usr/bin/env node
const crypto = require("node:crypto");
const fs = require("node:fs/promises");
const path = require("node:path");

const { AppError } = require("../core/errors");
const { createPostgisClient } = require("../data/postgis/client");
const { parsePipelineCliArgs, printCliError } = require("./pipeline-common");

const DEFAULT_COUNTRIES = ["DE", "AT", "CH"];
const SUPPORTED_COUNTRIES = new Set(DEFAULT_COUNTRIES);
const DEFAULT_OVERPASS_ENDPOINT = "https://overpass-api.de/api/interpreter";
const DEFAULT_OUTPUT_DIR = path.join("data", "raw", "base-spatial-seed");
const OVERPASS_STATION_FILTER = "station|halt";

const OVERPASS_NAME_PRIORITY = {
  DE: ["name:de", "name", "name:en"],
  AT: ["name:de", "name", "name:en"],
  CH: ["name", "name:de", "name:fr", "name:it", "name:rm", "name:en"],
};

const UIC_TAG_KEYS = [
  "uic_ref",
  "ref:uic",
  "uic",
  "uic_code",
  "uic:ref",
  "ref",
];

const LEGAL_ISOLATION_NOTE =
  "ODbL-derived topology is only written into canonical_stations; schedule staging/proprietary payload tables are never read or mutated by this seeder.";

const UPSERT_BASE_STATIONS_SQL = `
WITH input_rows AS (
  SELECT
    btrim(r.canonical_station_id) AS canonical_station_id,
    btrim(r.canonical_name) AS canonical_name,
    upper(btrim(r.country)) AS country,
    r.latitude::double precision AS latitude,
    r.longitude::double precision AS longitude,
    CASE
      WHEN r.match_method IN ('hard_id', 'name_geo', 'name_only') THEN r.match_method
      ELSE 'name_geo'
    END AS match_method,
    GREATEST(COALESCE(r.member_count, 1), 1)::integer AS member_count,
    COALESCE(r.first_seen_snapshot_date, :'as_of'::date) AS first_seen_snapshot_date,
    COALESCE(r.last_seen_snapshot_date, :'as_of'::date) AS last_seen_snapshot_date
  FROM jsonb_to_recordset(:'rows_json'::jsonb) AS r(
    canonical_station_id text,
    canonical_name text,
    country text,
    latitude double precision,
    longitude double precision,
    match_method text,
    member_count integer,
    first_seen_snapshot_date date,
    last_seen_snapshot_date date
  )
  WHERE COALESCE(btrim(r.canonical_station_id), '') <> ''
    AND COALESCE(btrim(r.canonical_name), '') <> ''
    AND upper(btrim(r.country)) IN ('DE', 'AT', 'CH')
),
prepared AS (
  SELECT
    canonical_station_id,
    canonical_name,
    normalize_station_name(canonical_name) AS normalized_name,
    country::char(2) AS country,
    latitude,
    longitude,
    CASE
      WHEN longitude BETWEEN -180 AND 180 AND latitude BETWEEN -90 AND 90
        THEN ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
      ELSE NULL::geometry(Point, 4326)
    END AS geom,
    match_method,
    member_count,
    first_seen_snapshot_date,
    last_seen_snapshot_date
  FROM input_rows
),
dedup AS (
  SELECT DISTINCT ON (canonical_station_id)
    canonical_station_id,
    canonical_name,
    normalized_name,
    country,
    latitude,
    longitude,
    geom,
    match_method,
    member_count,
    first_seen_snapshot_date,
    last_seen_snapshot_date
  FROM prepared
  ORDER BY
    canonical_station_id,
    CASE
      WHEN match_method = 'hard_id' THEN 0
      WHEN match_method = 'name_geo' THEN 1
      ELSE 2
    END,
    member_count DESC
),
moved_grid_rows AS (
  DELETE FROM canonical_stations cs
  USING (
    SELECT
      d.canonical_station_id,
      compute_geo_grid_id(d.country::text, d.latitude, d.longitude, d.geom) AS expected_grid_id
    FROM dedup d
  ) expected
  WHERE cs.canonical_station_id = expected.canonical_station_id
    AND cs.grid_id <> expected.expected_grid_id
  RETURNING cs.canonical_station_id
),
upserted AS (
  INSERT INTO canonical_stations AS cs (
    canonical_station_id,
    canonical_name,
    normalized_name,
    country,
    latitude,
    longitude,
    geom,
    grid_id,
    match_method,
    member_count,
    first_seen_snapshot_date,
    last_seen_snapshot_date,
    is_deleted,
    deleted_at,
    updated_at
  )
  SELECT
    d.canonical_station_id,
    d.canonical_name,
    d.normalized_name,
    d.country,
    d.latitude,
    d.longitude,
    d.geom,
    compute_geo_grid_id(d.country::text, d.latitude, d.longitude, d.geom) AS grid_id,
    d.match_method,
    d.member_count,
    d.first_seen_snapshot_date,
    d.last_seen_snapshot_date,
    false,
    NULL,
    now()
  FROM dedup d
  ON CONFLICT (grid_id, canonical_station_id)
  DO UPDATE SET
    canonical_name = EXCLUDED.canonical_name,
    normalized_name = EXCLUDED.normalized_name,
    country = EXCLUDED.country,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    geom = EXCLUDED.geom,
    match_method = EXCLUDED.match_method,
    member_count = EXCLUDED.member_count,
    first_seen_snapshot_date = EXCLUDED.first_seen_snapshot_date,
    last_seen_snapshot_date = EXCLUDED.last_seen_snapshot_date,
    is_deleted = false,
    deleted_at = NULL,
    updated_at = now()
  WHERE cs.canonical_name IS DISTINCT FROM EXCLUDED.canonical_name
     OR cs.normalized_name IS DISTINCT FROM EXCLUDED.normalized_name
     OR cs.country IS DISTINCT FROM EXCLUDED.country
     OR cs.latitude IS DISTINCT FROM EXCLUDED.latitude
     OR cs.longitude IS DISTINCT FROM EXCLUDED.longitude
     OR cs.geom IS DISTINCT FROM EXCLUDED.geom
     OR cs.match_method IS DISTINCT FROM EXCLUDED.match_method
     OR cs.member_count IS DISTINCT FROM EXCLUDED.member_count
     OR cs.first_seen_snapshot_date IS DISTINCT FROM EXCLUDED.first_seen_snapshot_date
     OR cs.last_seen_snapshot_date IS DISTINCT FROM EXCLUDED.last_seen_snapshot_date
     OR cs.is_deleted IS DISTINCT FROM false
     OR cs.deleted_at IS NOT NULL
  RETURNING (xmax = 0) AS inserted
)
SELECT
  (SELECT COUNT(*)::integer FROM dedup) AS seed_rows,
  (SELECT COUNT(*)::integer FROM moved_grid_rows) AS moved_grid_rows,
  (SELECT COUNT(*)::integer FROM upserted WHERE inserted) AS inserted_rows,
  (SELECT COUNT(*)::integer FROM upserted WHERE NOT inserted) AS updated_rows,
  (
    (SELECT COUNT(*) FROM dedup)
    - (SELECT COUNT(*) FROM upserted)
  )::integer AS unchanged_rows;
`;

function log(message) {
  process.stdout.write(`[seed-base-spatial] ${message}\n`);
}

function toUtcIsoDate(now = new Date()) {
  return now.toISOString().slice(0, 10);
}

function isStrictIsoDate(value) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(value || ""))) {
    return false;
  }
  const date = new Date(`${value}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) {
    return false;
  }
  return date.toISOString().slice(0, 10) === value;
}

function flushCsvField(state) {
  state.currentRow.push(state.currentField);
  state.currentField = "";
}

function flushCsvRow(state, text) {
  if (
    state.currentRow.length === 1 &&
    state.currentRow[0] === "" &&
    (state.rows.length === 0 || text.endsWith("\n") || text.endsWith("\r"))
  ) {
    state.currentRow = [];
    return;
  }
  state.rows.push(state.currentRow);
  state.currentRow = [];
}

function handleCsvQuote(text, state, index) {
  if (text[index] !== '"') {
    return null;
  }
  const next = text[index + 1];
  if (state.inQuotes && next === '"') {
    state.currentField += '"';
    return index + 1;
  }
  state.inQuotes = !state.inQuotes;
  return index;
}

function handleCsvSeparator(text, state, index) {
  const ch = text[index];
  if (state.inQuotes) {
    return null;
  }
  if (ch === ",") {
    flushCsvField(state);
    return index;
  }
  if (ch !== "\n" && ch !== "\r") {
    return null;
  }

  let nextIndex = index;
  if (ch === "\r" && text[index + 1] === "\n") {
    nextIndex += 1;
  }
  flushCsvField(state);
  flushCsvRow(state, text);
  return nextIndex;
}

function parseCsv(text) {
  const state = {
    rows: [],
    currentRow: [],
    currentField: "",
    inQuotes: false,
  };

  let index = 0;
  while (index < text.length) {
    const quoteIndex = handleCsvQuote(text, state, index);
    if (quoteIndex !== null) {
      index = quoteIndex + 1;
      continue;
    }

    const separatorIndex = handleCsvSeparator(text, state, index);
    if (separatorIndex !== null) {
      index = separatorIndex + 1;
      continue;
    }

    state.currentField += text[index];
    index += 1;
  }

  flushCsvField(state);
  if (state.currentRow.length > 0) {
    flushCsvRow(state, text);
  }

  return state.rows;
}

function normalizeLookupName(value) {
  const ascii = String(value || "")
    .normalize("NFKD")
    .replaceAll(/[\u0300-\u036f]/g, "")
    .toLowerCase();

  return ascii.replaceAll(/[^a-z0-9]+/g, " ").trim();
}

function normalizeUicCode(raw) {
  const digits = String(raw || "").replaceAll(/\D/g, "");
  if (digits.length < 5 || digits.length > 12) {
    return "";
  }
  const trimmed = digits.replace(/^0+(?=\d)/, "");
  return trimmed.length >= 5 ? trimmed : "";
}

function pickFirstField(record, keys) {
  if (!record || typeof record !== "object") {
    return "";
  }

  const entryMap = new Map();
  for (const [key, value] of Object.entries(record)) {
    entryMap.set(String(key).toLowerCase(), value);
  }

  for (const key of keys) {
    if (entryMap.has(String(key).toLowerCase())) {
      const value = entryMap.get(String(key).toLowerCase());
      if (
        value !== undefined &&
        value !== null &&
        String(value).trim() !== ""
      ) {
        return String(value).trim();
      }
    }
  }

  return "";
}

function toFiniteNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const parsed = Number.parseFloat(String(value));
  return Number.isFinite(parsed) ? parsed : null;
}

function resolveRawUicRows(payload) {
  if (Array.isArray(payload)) {
    return payload;
  }
  if (Array.isArray(payload?.stations)) {
    return payload.stations;
  }
  if (Array.isArray(payload?.records)) {
    return payload.records;
  }
  if (Array.isArray(payload?.data)) {
    return payload.data;
  }
  if (Array.isArray(payload?.items)) {
    return payload.items;
  }
  return [];
}

function parseUicRowsFromJsonPayload(payload, sourceLabel) {
  const rawRows = resolveRawUicRows(payload);

  return rawRows
    .map((row) => {
      const country = pickFirstField(row, [
        "country",
        "country_code",
        "iso2",
        "iso_3166_1_alpha2",
      ]).toUpperCase();
      const uic = normalizeUicCode(
        pickFirstField(row, [
          "uic",
          "uic_code",
          "code",
          "station_code",
          "uic_ref",
        ]),
      );
      const name = pickFirstField(row, [
        "name",
        "station_name",
        "station",
        "title",
      ]);
      const latitude = toFiniteNumber(
        pickFirstField(row, ["lat", "latitude", "y"]),
      );
      const longitude = toFiniteNumber(
        pickFirstField(row, ["lon", "lng", "longitude", "x"]),
      );

      return {
        country,
        uic,
        name,
        latitude,
        longitude,
        sourceLabel,
      };
    })
    .filter((row) => row.country && row.uic);
}

function parseUicRowsFromCsvPayload(csvText, sourceLabel) {
  const rows = parseCsv(csvText);
  if (rows.length === 0) {
    return [];
  }

  const headers = rows[0].map((header) => String(header || "").trim());
  const records = [];

  for (let i = 1; i < rows.length; i += 1) {
    const row = rows[i];
    if (!row || row.length === 0) {
      continue;
    }

    const record = {};
    for (let col = 0; col < headers.length; col += 1) {
      const key = headers[col];
      if (!key) {
        continue;
      }
      record[key] = row[col] === undefined ? "" : String(row[col]).trim();
    }

    const country = pickFirstField(record, [
      "country",
      "country_code",
      "iso2",
      "iso_3166_1_alpha2",
    ]).toUpperCase();
    const uic = normalizeUicCode(
      pickFirstField(record, [
        "uic",
        "uic_code",
        "code",
        "station_code",
        "uic_ref",
      ]),
    );

    if (!country || !uic) {
      continue;
    }

    records.push({
      country,
      uic,
      name: pickFirstField(record, [
        "name",
        "station_name",
        "station",
        "title",
      ]),
      latitude: toFiniteNumber(
        pickFirstField(record, ["lat", "latitude", "y"]),
      ),
      longitude: toFiniteNumber(
        pickFirstField(record, ["lon", "lng", "longitude", "x"]),
      ),
      sourceLabel,
    });
  }

  return records;
}

function parseUicRowsFromText(text, sourceLabel) {
  const trimmed = String(text || "").trim();
  if (!trimmed) {
    return [];
  }

  if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
    let parsed;
    try {
      parsed = JSON.parse(trimmed);
    } catch (err) {
      throw new AppError({
        code: "INVALID_REQUEST",
        message: `UIC payload from ${sourceLabel} is not valid JSON`,
        cause: err,
      });
    }
    return parseUicRowsFromJsonPayload(parsed, sourceLabel);
  }

  return parseUicRowsFromCsvPayload(trimmed, sourceLabel);
}

function extractUicCodesFromTags(tags) {
  if (!tags || typeof tags !== "object") {
    return [];
  }

  const out = new Set();
  for (const key of UIC_TAG_KEYS) {
    const raw = tags[key];
    if (!raw) {
      continue;
    }

    const fragments = String(raw).split(/[\s,;|/]+/g);
    for (const fragment of fragments) {
      const normalized = normalizeUicCode(fragment);
      if (normalized) {
        out.add(normalized);
      }
    }
  }

  return Array.from(out.values());
}

function collectNameTranslations(tags) {
  if (!tags || typeof tags !== "object") {
    return {};
  }

  const out = {};
  for (const [key, value] of Object.entries(tags)) {
    if (!key.startsWith("name:")) {
      continue;
    }
    if (typeof value !== "string") {
      continue;
    }
    const locale = key.slice("name:".length).trim();
    const label = value.trim();
    if (!locale || !label) {
      continue;
    }
    out[locale] = label;
  }
  return out;
}

function selectPrimaryName(tags, country) {
  const priorities = OVERPASS_NAME_PRIORITY[country] || ["name", "name:en"];
  for (const key of priorities) {
    const value = String(tags?.[key] || "").trim();
    if (value) {
      return value;
    }
  }

  const fallback = String(tags?.name || tags?.["name:en"] || "").trim();
  return fallback;
}

function pickCoordinates(element) {
  if (!element || typeof element !== "object") {
    return { latitude: null, longitude: null };
  }

  const latFromNode = toFiniteNumber(element.lat);
  const lonFromNode = toFiniteNumber(element.lon);

  if (latFromNode !== null && lonFromNode !== null) {
    return {
      latitude: latFromNode,
      longitude: lonFromNode,
    };
  }

  const latFromCenter = toFiniteNumber(element.center?.lat);
  const lonFromCenter = toFiniteNumber(element.center?.lon);

  return {
    latitude: latFromCenter,
    longitude: lonFromCenter,
  };
}

function buildOverpassQuery(country, timeoutSec = 240) {
  const iso = String(country || "").toUpperCase();
  return `[out:json][timeout:${timeoutSec}];
area["ISO3166-1"="${iso}"]["boundary"="administrative"]["admin_level"="2"]->.searchArea;
(
  node["railway"~"^(${OVERPASS_STATION_FILTER})$"]["name"](area.searchArea);
  way["railway"~"^(${OVERPASS_STATION_FILTER})$"]["name"](area.searchArea);
  relation["railway"~"^(${OVERPASS_STATION_FILTER})$"]["name"](area.searchArea);
  node["public_transport"="station"]["train"="yes"]["name"](area.searchArea);
  way["public_transport"="station"]["train"="yes"]["name"](area.searchArea);
  relation["public_transport"="station"]["train"="yes"]["name"](area.searchArea);
);
out tags center;`;
}

function formatUtcTimestampSlug(date = new Date()) {
  return date
    .toISOString()
    .replaceAll(/[-:]/g, "")
    .replaceAll(/\.\d{3}Z$/, "Z");
}

async function fileExists(filePath) {
  try {
    await fs.stat(filePath);
    return true;
  } catch (err) {
    if (err?.code === "ENOENT") {
      return false;
    }
    throw err;
  }
}

async function fetchTextWithRetry(url, options = {}) {
  const {
    method = "GET",
    headers = {},
    body,
    retries = 3,
    retryDelayMs = 1500,
  } = options;

  let lastError;

  for (let attempt = 1; attempt <= retries; attempt += 1) {
    try {
      const response = await fetch(url, {
        method,
        headers,
        body,
      });

      if (response.ok) {
        return response.text();
      }

      const text = await response.text();
      const err = new AppError({
        code: "INTERNAL_ERROR",
        message: `HTTP ${response.status} when requesting ${url}`,
        details: {
          status: response.status,
          bodySnippet: text.slice(0, 400),
        },
      });

      if (response.status >= 500 || response.status === 429) {
        lastError = err;
      } else {
        throw err;
      }
    } catch (err) {
      lastError = err;
    }

    if (attempt < retries) {
      await new Promise((resolve) =>
        setTimeout(resolve, retryDelayMs * attempt),
      );
    }
  }

  throw lastError;
}

function parseCountryTokens(raw) {
  return String(raw || "")
    .split(",")
    .map((token) => token.trim().toUpperCase())
    .filter((token) => token.length > 0);
}

function parsePositiveInteger(raw, optionName) {
  const parsed = Number.parseInt(String(raw || ""), 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: `${optionName} must be a non-negative integer`,
    });
  }
  return parsed;
}

function printHelp() {
  process.stdout.write(
    "Usage: scripts/data/seed-base-spatial-data.sh [options]\n\n",
  );
  process.stdout.write(
    "Seed canonical_stations with base OSM/UIC topology (DE/AT/CH) to mitigate cold-start station novelty spikes.\n\n",
  );
  process.stdout.write("Options:\n");
  process.stdout.write(
    "  --country DE|AT|CH       Add one country to scope (repeatable, default: DE,AT,CH)\n",
  );
  process.stdout.write(
    "  --countries DE,AT,CH     Comma-separated country scope override\n",
  );
  process.stdout.write(
    "  --as-of YYYY-MM-DD       Seed snapshot date (default: current UTC date)\n",
  );
  process.stdout.write(
    "  --uic-file PATH          Optional local UIC CSV/JSON file\n",
  );
  process.stdout.write(
    "  --uic-url URL            Optional UIC CSV/JSON URL (downloaded each run)\n",
  );
  process.stdout.write(
    `  --osm-endpoint URL       Overpass API endpoint (default: ${DEFAULT_OVERPASS_ENDPOINT})\n`,
  );
  process.stdout.write(
    "  --overpass-timeout-sec N Overpass query timeout in seconds (default: 240)\n",
  );
  process.stdout.write(
    "  --output-dir PATH        Relative/absolute artifact directory (default: data/raw/base-spatial-seed)\n",
  );
  process.stdout.write(
    "  --offline                Use cached OSM files from output dir only\n",
  );
  process.stdout.write(
    "  --dry-run                Build artifacts and summary without DB writes\n",
  );
  process.stdout.write(
    "  --limit N                Keep only first N deterministic seed rows (debug aid)\n",
  );
  process.stdout.write("  -h, --help               Show this help\n");
}

function readRequiredSeedArg(tokens, index, flagName) {
  const value = String(tokens[index + 1] || "").trim();
  if (!value) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: `Missing value for ${flagName}`,
    });
  }
  return value;
}

function parseSeedArgsToken(parsed, tokens, index) {
  const token = String(tokens[index] || "").trim();

  switch (token) {
    case "-h":
    case "--help":
      parsed.helpRequested = true;
      return index;
    case "--country":
      parsed.countries.push(
        ...parseCountryTokens(readRequiredSeedArg(tokens, index, "--country")),
      );
      return index + 1;
    case "--countries":
      parsed.countries = parseCountryTokens(
        readRequiredSeedArg(tokens, index, "--countries"),
      );
      return index + 1;
    case "--as-of": {
      const value = readRequiredSeedArg(tokens, index, "--as-of");
      if (!isStrictIsoDate(value)) {
        throw new AppError({
          code: "INVALID_REQUEST",
          message: "Invalid --as-of value (expected YYYY-MM-DD)",
        });
      }
      parsed.asOf = value;
      return index + 1;
    }
    case "--uic-file":
      parsed.uicFile = readRequiredSeedArg(tokens, index, "--uic-file");
      return index + 1;
    case "--uic-url":
      parsed.uicUrl = readRequiredSeedArg(tokens, index, "--uic-url");
      return index + 1;
    case "--osm-endpoint":
      parsed.osmEndpoint = readRequiredSeedArg(tokens, index, "--osm-endpoint");
      return index + 1;
    case "--overpass-timeout-sec":
      parsed.overpassTimeoutSec = parsePositiveInteger(
        readRequiredSeedArg(tokens, index, "--overpass-timeout-sec"),
        "--overpass-timeout-sec",
      );
      return index + 1;
    case "--output-dir":
      parsed.outputDir = readRequiredSeedArg(tokens, index, "--output-dir");
      return index + 1;
    case "--offline":
      parsed.offline = true;
      return index;
    case "--dry-run":
      parsed.dryRun = true;
      return index;
    case "--limit":
      parsed.limit = parsePositiveInteger(
        readRequiredSeedArg(tokens, index, "--limit"),
        "--limit",
      );
      return index + 1;
    default:
      throw new AppError({
        code: "INVALID_REQUEST",
        message: `Unknown argument: ${token}`,
      });
  }
}

function parseSeedArgs(args = []) {
  const parsed = {
    helpRequested: false,
    countries: [],
    asOf: toUtcIsoDate(),
    uicFile: "",
    uicUrl: "",
    osmEndpoint: DEFAULT_OVERPASS_ENDPOINT,
    overpassTimeoutSec: 240,
    outputDir: DEFAULT_OUTPUT_DIR,
    offline: false,
    dryRun: false,
    limit: 0,
  };

  const tokens = Array.isArray(args) ? args : [];

  let index = 0;
  while (index < tokens.length) {
    const nextIndex = parseSeedArgsToken(parsed, tokens, index);
    index = nextIndex + 1;
  }

  if (parsed.countries.length === 0) {
    parsed.countries = [...DEFAULT_COUNTRIES];
  }

  parsed.countries = Array.from(new Set(parsed.countries));

  for (const country of parsed.countries) {
    if (!SUPPORTED_COUNTRIES.has(country)) {
      throw new AppError({
        code: "INVALID_REQUEST",
        message: `Invalid country '${country}' (expected DE, AT, CH)`,
      });
    }
  }

  return parsed;
}

function pickBetterUicRecord(current, candidate) {
  if (!current) {
    return candidate;
  }

  const currentScore =
    (current.name ? 1 : 0) +
    (current.latitude !== null && current.longitude !== null ? 1 : 0);
  const candidateScore =
    (candidate.name ? 1 : 0) +
    (candidate.latitude !== null && candidate.longitude !== null ? 1 : 0);

  return candidateScore > currentScore ? candidate : current;
}

function buildUicIndex(rows, allowedCountries) {
  const byCode = new Map();
  const byName = new Map();
  const filtered = [];

  for (const row of rows) {
    if (!allowedCountries.has(row.country)) {
      continue;
    }

    filtered.push(row);

    const codeKey = `${row.country}|${row.uic}`;
    byCode.set(codeKey, pickBetterUicRecord(byCode.get(codeKey), row));

    const normalizedName = normalizeLookupName(row.name);
    if (!normalizedName) {
      continue;
    }

    const nameKey = `${row.country}|${normalizedName}`;
    if (byName.has(nameKey)) {
      const existing = byName.get(nameKey);
      if (!existing) {
        continue;
      }
      if (existing.uic !== row.uic) {
        byName.set(nameKey, null);
        continue;
      }
      byName.set(nameKey, pickBetterUicRecord(existing, row));
      continue;
    }
    byName.set(nameKey, row);
  }

  return {
    byCode,
    byName,
    rowCount: filtered.length,
  };
}

async function loadUicRows(options) {
  const rows = [];
  const sources = [];

  if (options.uicFile) {
    const filePath = path.resolve(options.uicFile);
    const payload = await fs.readFile(filePath, "utf8");
    const sourceLabel = `file:${filePath}`;
    rows.push(...parseUicRowsFromText(payload, sourceLabel));
    sources.push(sourceLabel);
  }

  if (options.uicUrl) {
    const payload = await fetchTextWithRetry(options.uicUrl, {
      method: "GET",
      headers: {
        "user-agent": "TrainscannerBaseSpatialSeeder/1.0",
      },
    });
    const sourceLabel = `url:${options.uicUrl}`;
    rows.push(...parseUicRowsFromText(payload, sourceLabel));
    sources.push(sourceLabel);
  }

  return {
    rows,
    sources,
  };
}

async function loadOverpassPayloadForCountry(country, options) {
  const filePath = path.join(options.outputDir, `osm-overpass-${country}.json`);

  if (options.offline) {
    if (!(await fileExists(filePath))) {
      throw new AppError({
        code: "INVALID_REQUEST",
        message: `--offline set but cache file is missing for ${country}: ${filePath}`,
      });
    }
    const payload = await fs.readFile(filePath, "utf8");
    return {
      json: JSON.parse(payload),
      cachePath: filePath,
      fromCache: true,
    };
  }

  try {
    const query = buildOverpassQuery(country, options.overpassTimeoutSec);
    const payload = await fetchTextWithRetry(options.osmEndpoint, {
      method: "POST",
      headers: {
        "content-type": "text/plain; charset=utf-8",
        "user-agent": "TrainscannerBaseSpatialSeeder/1.0",
      },
      body: query,
      retries: 3,
      retryDelayMs: 2000,
    });

    await fs.writeFile(filePath, payload, "utf8");

    return {
      json: JSON.parse(payload),
      cachePath: filePath,
      fromCache: false,
    };
  } catch (err) {
    if (await fileExists(filePath)) {
      log(
        `Overpass request failed for ${country}; falling back to cached payload at ${filePath}`,
      );
      const payload = await fs.readFile(filePath, "utf8");
      return {
        json: JSON.parse(payload),
        cachePath: filePath,
        fromCache: true,
      };
    }
    throw err;
  }
}

function resolveUicMatch(country, tags, stationName, uicIndex) {
  const codesFromTags = extractUicCodesFromTags(tags);
  const code = codesFromTags[0];
  if (code) {
    const key = `${country}|${code}`;
    const row = uicIndex.byCode.get(key) || null;
    return {
      uicCode: code,
      uicRecord: row,
      source: row ? "tag+uic_feed" : "tag_only",
    };
  }

  const normalizedName = normalizeLookupName(stationName);
  if (!normalizedName) {
    return null;
  }

  const nameKey = `${country}|${normalizedName}`;
  const nameMatchedRow = uicIndex.byName.get(nameKey);
  if (nameMatchedRow) {
    return {
      uicCode: nameMatchedRow.uic,
      uicRecord: nameMatchedRow,
      source: "name+uic_feed",
    };
  }

  return null;
}

function isValidCoordinate(latitude, longitude) {
  return (
    latitude !== null &&
    longitude !== null &&
    latitude >= -90 &&
    latitude <= 90 &&
    longitude >= -180 &&
    longitude <= 180
  );
}

function buildCandidateFromElement(element, country, asOf, uicIndex) {
  const tags = element?.tags;
  if (!tags || typeof tags !== "object") {
    return null;
  }

  const { latitude, longitude } = pickCoordinates(element);
  if (!isValidCoordinate(latitude, longitude)) {
    return null;
  }

  const stationName = selectPrimaryName(tags, country);
  const uicMatch = resolveUicMatch(country, tags, stationName, uicIndex);
  const name = stationName || uicMatch?.uicRecord?.name || "";

  if (!name) {
    return null;
  }

  const uicCode = uicMatch?.uicCode || "";
  const hasHardId = Boolean(uicCode);
  const osmRef = `${element.type || "unknown"}/${element.id || "unknown"}`;

  const canonicalStationId = hasHardId
    ? `cstn_seed_uic_${country.toLowerCase()}_${uicCode}`
    : `cstn_seed_osm_${country.toLowerCase()}_${crypto
        .createHash("sha1")
        .update(`${country}|${osmRef}|${normalizeLookupName(name)}`)
        .digest("hex")
        .slice(0, 20)}`;

  return {
    canonical_station_id: canonicalStationId,
    canonical_name: name,
    country,
    latitude,
    longitude,
    match_method: hasHardId ? "hard_id" : "name_geo",
    member_count: 1,
    first_seen_snapshot_date: asOf,
    last_seen_snapshot_date: asOf,
    uic_code: uicCode,
    uic_match_source: uicMatch?.source || "",
    osm_ref: osmRef,
    name_translations: collectNameTranslations(tags),
  };
}

function preferCanonicalName(current, candidate) {
  const left = String(current || "").trim();
  const right = String(candidate || "").trim();

  if (!left) {
    return right;
  }
  if (!right) {
    return left;
  }

  if (left.length === right.length) {
    if (left <= right) {
      return left;
    }
    return right;
  }

  return right.length > left.length ? right : left;
}

function addCandidate(aggregateMap, candidate) {
  const key = candidate.canonical_station_id;
  const existing = aggregateMap.get(key);

  if (!existing) {
    aggregateMap.set(key, {
      ...candidate,
      _coord_count: 1,
      _source_refs: new Set([candidate.osm_ref]),
      _name_variants: new Set([candidate.canonical_name]),
    });
    return;
  }

  existing.member_count += 1;
  existing.canonical_name = preferCanonicalName(
    existing.canonical_name,
    candidate.canonical_name,
  );

  if (candidate.match_method === "hard_id") {
    existing.match_method = "hard_id";
  }

  if (candidate.uic_code && !existing.uic_code) {
    existing.uic_code = candidate.uic_code;
    existing.uic_match_source = candidate.uic_match_source;
  }

  existing.latitude =
    (existing.latitude * existing._coord_count + candidate.latitude) /
    (existing._coord_count + 1);
  existing.longitude =
    (existing.longitude * existing._coord_count + candidate.longitude) /
    (existing._coord_count + 1);
  existing._coord_count += 1;

  existing._source_refs.add(candidate.osm_ref);
  existing._name_variants.add(candidate.canonical_name);

  for (const [locale, label] of Object.entries(
    candidate.name_translations || {},
  )) {
    if (!existing.name_translations[locale]) {
      existing.name_translations[locale] = label;
    }
  }
}

function finalizeAggregateRows(aggregateMap) {
  return Array.from(aggregateMap.values())
    .map((row) => ({
      canonical_station_id: row.canonical_station_id,
      canonical_name: row.canonical_name,
      country: row.country,
      latitude: Number.isFinite(row.latitude) ? row.latitude : null,
      longitude: Number.isFinite(row.longitude) ? row.longitude : null,
      match_method: row.match_method,
      member_count: row.member_count,
      first_seen_snapshot_date: row.first_seen_snapshot_date,
      last_seen_snapshot_date: row.last_seen_snapshot_date,
      uic_code: row.uic_code,
      uic_match_source: row.uic_match_source,
      source_refs: Array.from(row._source_refs.values()).sort((a, b) =>
        String(a).localeCompare(String(b)),
      ),
      name_variants: Array.from(row._name_variants.values()).sort((a, b) =>
        String(a).localeCompare(String(b)),
      ),
      name_translations: row.name_translations,
    }))
    .sort((a, b) => {
      if (a.country !== b.country) {
        return a.country.localeCompare(b.country);
      }
      return a.canonical_station_id.localeCompare(b.canonical_station_id);
    });
}

function toDbRows(manifestRows) {
  return manifestRows.map((row) => ({
    canonical_station_id: row.canonical_station_id,
    canonical_name: row.canonical_name,
    country: row.country,
    latitude: row.latitude,
    longitude: row.longitude,
    match_method: row.match_method,
    member_count: row.member_count,
    first_seen_snapshot_date: row.first_seen_snapshot_date,
    last_seen_snapshot_date: row.last_seen_snapshot_date,
  }));
}

async function ensureGridFunction(client) {
  const row = await client.queryOne(
    "SELECT to_regprocedure('compute_geo_grid_id(text,double precision,double precision,geometry)') IS NOT NULL AS available",
  );
  if (!row?.available) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message:
        "compute_geo_grid_id(...) is not available. Run DB migration 012_v2_bounding_box_partitioning.sql before seeding.",
    });
  }
}

async function upsertBaseStations(client, seedRows, asOf) {
  if (!Array.isArray(seedRows) || seedRows.length === 0) {
    return {
      seedRows: 0,
      insertedRows: 0,
      updatedRows: 0,
      movedGridRows: 0,
      unchangedRows: 0,
    };
  }

  const result = await client.queryOne(UPSERT_BASE_STATIONS_SQL, {
    rows_json: JSON.stringify(seedRows),
    as_of: asOf,
  });

  return {
    seedRows: Number(result?.seed_rows || 0),
    insertedRows: Number(result?.inserted_rows || 0),
    updatedRows: Number(result?.updated_rows || 0),
    movedGridRows: Number(result?.moved_grid_rows || 0),
    unchangedRows: Number(result?.unchanged_rows || 0),
  };
}

async function writeArtifacts(outputDir, payloads) {
  const stamp = formatUtcTimestampSlug();
  const summaryPath = path.join(outputDir, `seed-summary-${stamp}.json`);
  const manifestPath = path.join(outputDir, `seed-manifest-${stamp}.json`);
  const rowsPath = path.join(outputDir, `seed-rows-${stamp}.json`);

  await fs.writeFile(
    summaryPath,
    `${JSON.stringify(payloads.summary, null, 2)}\n`,
    "utf8",
  );
  await fs.writeFile(
    manifestPath,
    `${JSON.stringify(payloads.manifestRows, null, 2)}\n`,
    "utf8",
  );
  await fs.writeFile(
    rowsPath,
    `${JSON.stringify(payloads.dbRows, null, 2)}\n`,
    "utf8",
  );

  return {
    summaryPath,
    manifestPath,
    rowsPath,
  };
}

async function collectSeedCandidatesByCountry(args, outputDir, uicIndex) {
  const aggregateMap = new Map();
  const osmStatsByCountry = {};

  for (const country of args.countries) {
    log(`Fetching OSM station topology for ${country}`);
    const payload = await loadOverpassPayloadForCountry(country, {
      ...args,
      outputDir,
    });

    const elements = Array.isArray(payload?.json?.elements)
      ? payload.json.elements
      : [];
    osmStatsByCountry[country] = {
      elements: elements.length,
      cachePath: payload.cachePath,
      fromCache: payload.fromCache,
    };

    for (const element of elements) {
      const candidate = buildCandidateFromElement(
        element,
        country,
        args.asOf,
        uicIndex,
      );
      if (candidate) {
        addCandidate(aggregateMap, candidate);
      }
    }

    log(
      `Country ${country}: parsed ${elements.length} OSM elements (${payload.fromCache ? "cache" : "network"})`,
    );
  }

  return {
    aggregateMap,
    osmStatsByCountry,
  };
}

async function upsertSeedRowsIfNeeded(args, rootDir, dbRows) {
  const emptySummary = {
    seedRows: dbRows.length,
    insertedRows: 0,
    updatedRows: 0,
    movedGridRows: 0,
    unchangedRows: 0,
  };

  if (args.dryRun) {
    log("Dry-run enabled; skipped DB writes.");
    return emptySummary;
  }

  if (dbRows.length === 0) {
    return emptySummary;
  }

  const client = createPostgisClient({ rootDir });
  try {
    await client.ensureReady();
    await ensureGridFunction(client);
    const dbSummary = await upsertBaseStations(client, dbRows, args.asOf);
    log(
      `DB upsert complete inserted=${dbSummary.insertedRows} updated=${dbSummary.updatedRows} unchanged=${dbSummary.unchangedRows}`,
    );
    return dbSummary;
  } finally {
    await client.end();
  }
}

function buildSeedSummary({
  runId,
  args,
  uicSources,
  osmStatsByCountry,
  dbRows,
  dbSummary,
}) {
  const hardIdRows = dbRows.filter(
    (row) => row.match_method === "hard_id",
  ).length;
  const nameGeoRows = dbRows.filter(
    (row) => row.match_method === "name_geo",
  ).length;

  return {
    runId,
    generatedAt: new Date().toISOString(),
    asOf: args.asOf,
    countries: args.countries,
    dryRun: args.dryRun,
    offline: args.offline,
    osmEndpoint: args.osmEndpoint,
    uicSources,
    legalIsolation: LEGAL_ISOLATION_NOTE,
    counts: {
      osmElements: Object.values(osmStatsByCountry).reduce(
        (acc, item) => acc + Number(item.elements || 0),
        0,
      ),
      seedRows: dbRows.length,
      hardIdRows,
      nameGeoRows,
      insertedRows: dbSummary.insertedRows,
      updatedRows: dbSummary.updatedRows,
      unchangedRows: dbSummary.unchangedRows,
      movedGridRows: dbSummary.movedGridRows,
    },
    osmStatsByCountry,
  };
}

async function run() {
  const parsedCli = parsePipelineCliArgs(process.argv.slice(2));
  const args = parseSeedArgs(parsedCli.passthroughArgs);

  if (args.helpRequested) {
    printHelp();
    return;
  }

  const rootDir = parsedCli.rootDir || process.cwd();
  const outputDir = path.resolve(rootDir, args.outputDir);
  await fs.mkdir(outputDir, { recursive: true });

  log(
    `Starting seed build for countries=${args.countries.join(",")} asOf=${args.asOf} dryRun=${args.dryRun} offline=${args.offline}`,
  );

  const uicLoad = await loadUicRows({ ...args, outputDir });
  const uicIndex = buildUicIndex(uicLoad.rows, new Set(args.countries));

  if (uicLoad.rows.length === 0) {
    log(
      "No explicit UIC feed provided; hard_id links will rely on OSM UIC tags only.",
    );
  } else {
    log(
      `Loaded ${uicIndex.rowCount} UIC records from ${uicLoad.sources.join(", ")}`,
    );
  }

  const { aggregateMap, osmStatsByCountry } =
    await collectSeedCandidatesByCountry(args, outputDir, uicIndex);

  let manifestRows = finalizeAggregateRows(aggregateMap);
  if (args.limit > 0 && manifestRows.length > args.limit) {
    manifestRows = manifestRows.slice(0, args.limit);
  }

  const dbRows = toDbRows(manifestRows);
  const dbSummary = await upsertSeedRowsIfNeeded(args, rootDir, dbRows);
  const summary = buildSeedSummary({
    runId: parsedCli.runId || "",
    args,
    uicSources: uicLoad.sources,
    osmStatsByCountry,
    dbRows,
    dbSummary,
  });

  const artifactPaths = await writeArtifacts(outputDir, {
    summary,
    manifestRows,
    dbRows,
  });

  const result = {
    ...summary,
    artifacts: artifactPaths,
  };

  process.stdout.write(`${JSON.stringify(result)}\n`);
}

if (require.main === module) {
  const main = async () => {
    try {
      await run();
    } catch (err) {
      printCliError("seed-base-spatial", err, "Seed base spatial data failed");
      process.exit(1);
    }
  };
  void main();
}

module.exports = {
  LEGAL_ISOLATION_NOTE,
  UPSERT_BASE_STATIONS_SQL,
  addCandidate,
  buildOverpassQuery,
  buildUicIndex,
  extractUicCodesFromTags,
  finalizeAggregateRows,
  normalizeLookupName,
  normalizeUicCode,
  parseCsv,
  parseSeedArgs,
  parseUicRowsFromCsvPayload,
  parseUicRowsFromJsonPayload,
  parseUicRowsFromText,
  resolveUicMatch,
  run,
  selectPrimaryName,
};
