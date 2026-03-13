const test = require("node:test");
const assert = require("node:assert/strict");

const {
  _internal: { normalizeRows, parseSummaryJson },
} = require("../../src/data/postgis/repositories/external-reference-repo");

test("normalizeRows coerces importer payloads into repository rows", () => {
  assert.deepEqual(
    normalizeRows([
      {
        external_id: "Q123",
        display_name: "Wien Hauptbahnhof",
        latitude: "48.185",
        longitude: "16.374",
        metadata: {
          source: "fixture",
        },
      },
    ]),
    [
      {
        external_id: "Q123",
        display_name: "Wien Hauptbahnhof",
        normalized_name: "",
        country: "",
        latitude: 48.185,
        longitude: 16.374,
        category: "",
        subtype: "",
        source_url: "",
        metadata: {
          source: "fixture",
        },
      },
    ],
  );
});

test("parseSummaryJson reads trailing JSON summaries from SQL stdout", () => {
  assert.deepEqual(
    parseSummaryJson(
      'NOTICE: example\n{"matched_stations":2,"rows_inserted":3}',
      "TEST",
      "missing summary",
    ),
    {
      matched_stations: 2,
      rows_inserted: 3,
    },
  );
});
