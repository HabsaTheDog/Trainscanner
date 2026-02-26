const test = require("node:test");
const assert = require("node:assert/strict");

const {
  addCandidate,
  buildUicIndex,
  normalizeUicCode,
  parseCsv,
  parseSeedArgs,
  parseUicRowsFromText,
  resolveUicMatch,
  finalizeAggregateRows,
} = require("../../src/cli/seed-base-spatial-data");

test("parseSeedArgs applies default DE/AT/CH scope", () => {
  const parsed = parseSeedArgs([]);
  assert.deepEqual(parsed.countries, ["DE", "AT", "CH"]);
  assert.match(parsed.asOf, /^\d{4}-\d{2}-\d{2}$/);
});

test("parseSeedArgs rejects invalid country", () => {
  assert.throws(
    () => parseSeedArgs(["--country", "FR"]),
    /Invalid country 'FR'/,
  );
});

test("parseCsv handles quoted commas and escaped quotes", () => {
  const rows = parseCsv('a,b,c\n1,"x,y","z""q"\n');
  assert.deepEqual(rows, [
    ["a", "b", "c"],
    ["1", "x,y", 'z"q'],
  ]);
});

test("parseUicRowsFromText parses CSV and normalizes UIC codes", () => {
  const rows = parseUicRowsFromText(
    "country,uic,name,lat,lon\nDE,008000105,Berlin Hbf,52.525,13.369\n",
    "file:/tmp/uic.csv",
  );

  assert.equal(rows.length, 1);
  assert.equal(rows[0].country, "DE");
  assert.equal(rows[0].uic, "8000105");
  assert.equal(rows[0].name, "Berlin Hbf");
  assert.equal(rows[0].latitude, 52.525);
  assert.equal(rows[0].longitude, 13.369);
});

test("buildUicIndex marks ambiguous name matches as null", () => {
  const rows = [
    {
      country: "DE",
      uic: "8000105",
      name: "Berlin Hbf",
      latitude: 52.525,
      longitude: 13.369,
      sourceLabel: "a",
    },
    {
      country: "DE",
      uic: "8099999",
      name: "Berlin Hbf",
      latitude: null,
      longitude: null,
      sourceLabel: "b",
    },
  ];

  const index = buildUicIndex(rows, new Set(["DE"]));
  assert.equal(index.byName.get("DE|berlin hbf"), null);
});

test("resolveUicMatch prefers explicit OSM UIC tag", () => {
  const index = buildUicIndex(
    [
      {
        country: "DE",
        uic: "8000105",
        name: "Berlin Hbf",
        latitude: 52.525,
        longitude: 13.369,
        sourceLabel: "source",
      },
    ],
    new Set(["DE"]),
  );

  const match = resolveUicMatch(
    "DE",
    { uic_ref: "008000105" },
    "Berlin Hbf",
    index,
  );

  assert.equal(match?.uicCode, "8000105");
  assert.equal(match?.source, "tag+uic_feed");
});

test("normalizeUicCode strips noise and invalid lengths", () => {
  assert.equal(normalizeUicCode(" 008000105 "), "8000105");
  assert.equal(normalizeUicCode("abc"), "");
  assert.equal(normalizeUicCode("1234"), "");
});

test("addCandidate aggregates duplicates and keeps hard_id match method", () => {
  const aggregate = new Map();

  addCandidate(aggregate, {
    canonical_station_id: "cstn_seed_uic_de_8000105",
    canonical_name: "Berlin Hbf",
    country: "DE",
    latitude: 52.52,
    longitude: 13.36,
    match_method: "name_geo",
    member_count: 1,
    first_seen_snapshot_date: "2026-02-23",
    last_seen_snapshot_date: "2026-02-23",
    uic_code: "",
    uic_match_source: "",
    osm_ref: "node/1",
    name_translations: { en: "Berlin Central Station" },
  });

  addCandidate(aggregate, {
    canonical_station_id: "cstn_seed_uic_de_8000105",
    canonical_name: "Berlin Hauptbahnhof",
    country: "DE",
    latitude: 52.53,
    longitude: 13.37,
    match_method: "hard_id",
    member_count: 1,
    first_seen_snapshot_date: "2026-02-23",
    last_seen_snapshot_date: "2026-02-23",
    uic_code: "8000105",
    uic_match_source: "tag+uic_feed",
    osm_ref: "way/2",
    name_translations: { de: "Berlin Hauptbahnhof" },
  });

  const rows = finalizeAggregateRows(aggregate);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].canonical_name, "Berlin Hauptbahnhof");
  assert.equal(rows[0].member_count, 2);
  assert.equal(rows[0].match_method, "hard_id");
  assert.deepEqual(rows[0].source_refs, ["node/1", "way/2"]);
  assert.deepEqual(rows[0].name_variants, [
    "Berlin Hauptbahnhof",
    "Berlin Hbf",
  ]);
});
