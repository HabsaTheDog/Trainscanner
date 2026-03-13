#!/usr/bin/env node

const fs = require("node:fs");

const STATION_TYPES = [
  { id: "Q55488", label: "railway station" },
  { id: "Q719456", label: "bus station" },
  { id: "Q928830", label: "metro station" },
  { id: "Q4663385", label: "tram stop" },
  { id: "Q1339195", label: "subway station" },
  { id: "Q2175765", label: "transit stop" },
];

const COUNTRY_QIDS = {
  AT: "Q40",
  BE: "Q31",
  CH: "Q39",
  CZ: "Q213",
  DE: "Q183",
  FR: "Q142",
  IT: "Q38",
  NL: "Q55",
  PL: "Q36",
};

const DEFAULT_BATCH_SIZE = 250;
const DEFAULT_REQUEST_TIMEOUT_MS = 20000;
const DEFAULT_MAX_PREFIX_LENGTH = 3;
const DEFAULT_TARGET_PREFIX_SIZE = 5000;
const HEAVY_COUNTRY_PREFIX_LENGTHS = {
  DE: 3,
};
const HEAVY_COUNTRY_TARGET_PREFIX_SIZES = {
  DE: 400,
};

function normalizeStationName(value) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFKD")
    .replaceAll(/[\u0300-\u036f]/g, "")
    .replaceAll(/[^a-z0-9]+/g, " ")
    .replaceAll(/\s+/g, " ")
    .trim();
}

function parseArgs(argv = []) {
  const parsed = {
    country: "",
    fixture: "",
    batchSize: DEFAULT_BATCH_SIZE,
    maxPrefixLength: DEFAULT_MAX_PREFIX_LENGTH,
    targetPrefixSize: DEFAULT_TARGET_PREFIX_SIZE,
    timeoutMs: DEFAULT_REQUEST_TIMEOUT_MS,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = String(argv[index] || "");
    switch (token) {
      case "--country":
        parsed.country = String(argv[index + 1] || "")
          .trim()
          .toUpperCase();
        index += 1;
        break;
      case "--fixture":
        parsed.fixture = String(argv[index + 1] || "").trim();
        index += 1;
        break;
      case "--batch-size":
        parsed.batchSize = Number.parseInt(String(argv[index + 1] || ""), 10);
        index += 1;
        break;
      case "--timeout-ms":
        parsed.timeoutMs = Number.parseInt(String(argv[index + 1] || ""), 10);
        index += 1;
        break;
      case "--max-prefix-length":
        parsed.maxPrefixLength = Number.parseInt(
          String(argv[index + 1] || ""),
          10,
        );
        index += 1;
        break;
      case "--target-prefix-size":
        parsed.targetPrefixSize = Number.parseInt(
          String(argv[index + 1] || ""),
          10,
        );
        index += 1;
        break;
      default:
        throw new Error(`Unknown argument: ${token}`);
    }
  }

  if (!parsed.country) {
    throw new Error("--country is required for Wikidata imports");
  }
  if (!Number.isInteger(parsed.batchSize) || parsed.batchSize <= 0) {
    throw new Error("--batch-size must be a positive integer");
  }
  if (!Number.isInteger(parsed.timeoutMs) || parsed.timeoutMs <= 0) {
    throw new Error("--timeout-ms must be a positive integer");
  }
  if (
    !Number.isInteger(parsed.maxPrefixLength) ||
    parsed.maxPrefixLength <= 0
  ) {
    throw new Error("--max-prefix-length must be a positive integer");
  }
  if (
    !Number.isInteger(parsed.targetPrefixSize) ||
    parsed.targetPrefixSize <= 0
  ) {
    throw new Error("--target-prefix-size must be a positive integer");
  }
  parsed.maxPrefixLength =
    HEAVY_COUNTRY_PREFIX_LENGTHS[parsed.country] || parsed.maxPrefixLength;
  parsed.targetPrefixSize =
    HEAVY_COUNTRY_TARGET_PREFIX_SIZES[parsed.country] ||
    parsed.targetPrefixSize;
  return parsed;
}

function resolveCountryQid(country) {
  const qid =
    COUNTRY_QIDS[
      String(country || "")
        .trim()
        .toUpperCase()
    ];
  if (!qid) {
    throw new Error(
      `No Wikidata country entity mapping is configured for ${country}`,
    );
  }
  return qid;
}

function toRows(bindings = [], country) {
  const rowsByExternalId = new Map();

  for (const binding of bindings) {
    const entityUri = String(binding?.item?.value || "").trim();
    const coords = String(binding?.coord?.value || "").trim();
    const match = coords.match(/^Point\(([-0-9.]+) ([-0-9.]+)\)$/);
    if (!entityUri || !match) {
      continue;
    }

    const displayName = String(binding?.itemLabel?.value || "").trim();
    const externalId = entityUri.replace("http://www.wikidata.org/entity/", "");
    if (!displayName || !externalId) {
      continue;
    }

    const subtype = String(binding?.stationTypeLabel?.value || "").trim();
    const row = {
      external_id: externalId,
      display_name: displayName,
      normalized_name: normalizeStationName(displayName),
      country,
      latitude: Number(match[2]),
      longitude: Number(match[1]),
      category: "station",
      subtype,
      source_url: entityUri.replace("http://", "https://"),
      metadata: {
        wikidata_entity: externalId,
        station_type: subtype,
      },
    };

    const existing = rowsByExternalId.get(externalId);
    if (!existing) {
      rowsByExternalId.set(externalId, row);
      continue;
    }

    rowsByExternalId.set(externalId, {
      ...existing,
      subtype: existing.subtype || row.subtype,
      metadata: {
        ...existing.metadata,
        station_type:
          existing.metadata?.station_type || row.metadata.station_type,
      },
    });
  }

  return Array.from(rowsByExternalId.values());
}

function escapeSparqlString(value) {
  return String(value || "")
    .replaceAll("\\", "\\\\")
    .replaceAll('"', '\\"');
}

function buildPrefixFilter(prefix) {
  return prefix
    ? `FILTER(STRSTARTS(?itemId, "${escapeSparqlString(prefix)}"))`
    : "";
}

function buildPrefixCountQuery(parsed, stationType, prefix) {
  const countryQid = resolveCountryQid(parsed.country);
  return `
    SELECT (COUNT(?item) AS ?count) WHERE {
      VALUES ?country { wd:${countryQid} }
      VALUES ?stationType { wd:${stationType.id} }
      ?item wdt:P31 ?stationType;
            wdt:P17 ?country;
            wdt:P625 ?coord.
      BIND(STRAFTER(STR(?item), "Q") AS ?itemId)
      ${buildPrefixFilter(prefix)}
    }
  `;
}

function buildPrefixFetchQuery(parsed, stationType, prefix, limit, offset) {
  const countryQid = resolveCountryQid(parsed.country);
  return `
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT ?item ?itemLabel ?coord ?itemId WHERE {
      VALUES ?country { wd:${countryQid} }
      VALUES ?stationType { wd:${stationType.id} }
      ?item wdt:P31 ?stationType;
            wdt:P17 ?country;
            wdt:P625 ?coord.
      BIND(STRAFTER(STR(?item), "Q") AS ?itemId)
      ${buildPrefixFilter(prefix)}
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    }
    ORDER BY xsd:integer(?itemId)
    LIMIT ${limit}
    OFFSET ${offset}
  `;
}

async function fetchQueryJson(parsed, query, attempt = 1) {
  const url = new URL("https://query.wikidata.org/sparql");
  url.searchParams.set("format", "json");
  url.searchParams.set("query", query);

  let response;
  try {
    response = await fetch(url, {
      headers: {
        Accept: "application/sparql-results+json",
        "User-Agent": "Trainscanner external reference importer",
      },
      signal: AbortSignal.timeout(parsed.timeoutMs),
    });
  } catch (error) {
    if (attempt < 4) {
      await new Promise((resolve) => setTimeout(resolve, attempt * 1500));
      return fetchQueryJson(parsed, query, attempt + 1);
    }
    throw error;
  }

  if (!response.ok) {
    if (attempt < 4 && (response.status === 429 || response.status >= 500)) {
      await new Promise((resolve) => setTimeout(resolve, attempt * 1500));
      return fetchQueryJson(parsed, query, attempt + 1);
    }
    const body = await response.text();
    throw new Error(
      `Wikidata query failed with status ${response.status}: ${body.slice(0, 240)}`,
    );
  }

  return response.json();
}

async function fetchPrefixCount(parsed, stationType, prefix) {
  const payload = await fetchQueryJson(
    parsed,
    buildPrefixCountQuery(parsed, stationType, prefix),
  );
  const value =
    payload?.results?.bindings?.[0]?.count?.value === undefined
      ? null
      : Number.parseInt(String(payload.results.bindings[0].count.value), 10);
  return Number.isInteger(value) && value >= 0 ? value : 0;
}

async function fetchBindingsForPrefix(parsed, stationType, prefix, count) {
  const bindings = [];
  for (let offset = 0; offset < count; offset += parsed.batchSize) {
    const payload = await fetchQueryJson(
      parsed,
      buildPrefixFetchQuery(
        parsed,
        stationType,
        prefix,
        Math.min(parsed.batchSize, count - offset),
        offset,
      ),
    );
    const page = (payload?.results?.bindings || []).map((binding) => ({
      ...binding,
      stationTypeLabel: {
        type: "literal",
        value: stationType.label,
      },
    }));
    if (page.length === 0) {
      break;
    }
    bindings.push(...page);
  }
  return bindings;
}

function nextPrefixDigits(prefix) {
  return prefix ? "0123456789" : "123456789";
}

function logProgress(message) {
  process.stderr.write(`[wikidata] ${message}\n`);
}

async function collectBindingsByPrefix(parsed, stationType, prefix) {
  const count = await fetchPrefixCount(parsed, stationType, prefix);
  if (count === 0) {
    return [];
  }
  if (
    count <= parsed.targetPrefixSize ||
    prefix.length >= parsed.maxPrefixLength
  ) {
    logProgress(
      `country=${parsed.country} type=${stationType.id} prefix=${prefix || "*"} count=${count}`,
    );
    return fetchBindingsForPrefix(parsed, stationType, prefix, count);
  }

  const bindings = [];
  logProgress(
    `country=${parsed.country} type=${stationType.id} splitting_prefix=${prefix || "*"} count=${count}`,
  );
  for (const digit of nextPrefixDigits(prefix)) {
    bindings.push(
      ...(await collectBindingsByPrefix(
        parsed,
        stationType,
        `${prefix}${digit}`,
      )),
    );
  }
  return bindings;
}

async function fetchBindingsForType(parsed, stationType) {
  const bindings = [];
  for (const prefix of nextPrefixDigits("")) {
    bindings.push(
      ...(await collectBindingsByPrefix(parsed, stationType, prefix)),
    );
  }
  return bindings;
}

async function loadBindings(parsed) {
  if (parsed.fixture) {
    const raw = JSON.parse(fs.readFileSync(parsed.fixture, "utf8"));
    if (Array.isArray(raw)) {
      if (raw.length > 0 && raw.every((row) => row?.external_id)) {
        return raw;
      }
      return raw;
    }
    return raw?.results?.bindings || [];
  }

  const bindings = [];
  for (const stationType of STATION_TYPES) {
    bindings.push(...(await fetchBindingsForType(parsed, stationType)));
  }
  return bindings;
}

async function run() {
  const parsed = parseArgs(process.argv.slice(2));
  const bindings = await loadBindings(parsed);
  const rows =
    Array.isArray(bindings) &&
    bindings.length > 0 &&
    bindings.every((row) => row?.external_id)
      ? bindings
      : toRows(bindings, parsed.country);
  process.stdout.write(JSON.stringify(rows));
}

run().catch((error) => {
  process.stderr.write(`${error.message}\n`);
  process.exit(1);
});
