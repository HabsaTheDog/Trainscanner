const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const {
  _internal: {
    preflightSourceAvailability,
    normalizeImportedRows,
    parseRefreshExternalReferenceArgs,
    resolveGeoNamesInputPath,
    resolveSnapshotLabel,
    resolveSourceDescriptors,
  },
} = require("../../src/domains/reference/service");

test("parseRefreshExternalReferenceArgs validates scope flags", () => {
  assert.deepEqual(
    parseRefreshExternalReferenceArgs([
      "--country",
      "de",
      "--as-of",
      "2026-03-10",
      "--source-id",
      "wikidata",
    ]),
    {
      helpRequested: false,
      scope: {
        country: "DE",
        asOf: "2026-03-10",
        sourceId: "wikidata",
      },
    },
  );
});

test("normalizeImportedRows fills normalized names and source metadata", () => {
  const rows = normalizeImportedRows(
    [
      {
        external_id: "Q123",
        display_name: "Wien Hauptbahnhof",
      },
    ],
    {
      country: "AT",
    },
    "wikidata",
  );

  assert.deepEqual(rows, [
    {
      external_id: "Q123",
      display_name: "Wien Hauptbahnhof",
      normalized_name: "wien hauptbahnhof",
      country: "AT",
      latitude: null,
      longitude: null,
      category: "",
      subtype: "",
      source_url: "",
      metadata: {
        imported_source: "wikidata",
      },
    },
  ]);
});

test("resolveSnapshotLabel prefers explicit importer metadata", () => {
  assert.equal(
    resolveSnapshotLabel(
      "wikidata",
      {
        asOf: "2026-03-10",
      },
      {
        snapshot_label: "wikidata-custom",
      },
    ),
    "wikidata-custom",
  );
});

test("resolveGeoNamesInputPath reuses cached country dumps without env configuration", async () => {
  const rootDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "trainscanner-geonames-"),
  );
  const cacheDir = path.join(
    rootDir,
    "data",
    "artifacts",
    "external-references",
    "geonames-at",
  );
  await fs.promises.mkdir(cacheDir, { recursive: true });
  const textPath = path.join(cacheDir, "AT.txt");
  await fs.promises.writeFile(textPath, "fixture\n", "utf8");

  await assert.doesNotReject(async () => {
    const resolved = await resolveGeoNamesInputPath(rootDir, {}, "AT");
    assert.equal(resolved, textPath);
  });
});

test("resolveSourceDescriptors reuses cached Wikidata rows before live refresh", async () => {
  const rootDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "trainscanner-wikidata-cache-"),
  );
  const cacheDir = path.join(
    rootDir,
    "data",
    "artifacts",
    "external-references",
    "wikidata",
  );
  await fs.promises.mkdir(cacheDir, { recursive: true });
  const cachePath = path.join(cacheDir, "de-2026-03-13.json");
  const fixtureRows = [
    {
      external_id: "Q42",
      display_name: "Cached Station",
      normalized_name: "cached station",
      country: "DE",
    },
  ];
  await fs.promises.writeFile(cachePath, JSON.stringify(fixtureRows), "utf8");

  const descriptors = resolveSourceDescriptors(rootDir, {});
  const metadata = descriptors.wikidata.resolveMetadata({
    country: "DE",
    asOf: "2026-03-13",
  });
  const loaded = await descriptors.wikidata.loadRows(
    {
      country: "DE",
      asOf: "2026-03-13",
    },
    { metadata },
  );

  assert.equal(loaded.cacheHit, true);
  assert.deepEqual(loaded.rows, fixtureRows);
});

test("preflightSourceAvailability fails overture and geonames early without local inputs", () => {
  const entries = preflightSourceAvailability(
    "/tmp/repo",
    {},
    {
      country: "DE",
      asOf: "2026-03-13",
    },
    ["overture", "geonames", "wikidata"],
  );

  assert.deepEqual(entries, [
    {
      sourceId: "overture",
      available: false,
      mode: "unavailable",
      reason: "missing_QA_EXTERNAL_REFERENCE_OVERTURE_PATH",
    },
    {
      sourceId: "geonames",
      available: false,
      mode: "unavailable",
      reason: "missing_QA_EXTERNAL_REFERENCE_GEONAMES_PATH",
    },
    {
      sourceId: "wikidata",
      available: true,
      mode: "live",
      reason: "country_scoped_live_refresh",
    },
  ]);
});

test("preflightSourceAvailability rejects live wikidata without country before db work", () => {
  const entries = preflightSourceAvailability(
    "/tmp/repo",
    {},
    {
      country: "",
      asOf: "2026-03-13",
    },
    ["wikidata"],
  );

  assert.deepEqual(entries, [
    {
      sourceId: "wikidata",
      available: false,
      mode: "unavailable",
      reason: "country_required_for_live_wikidata",
    },
  ]);
});
