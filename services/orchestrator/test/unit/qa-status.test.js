const test = require("node:test");
const assert = require("node:assert/strict");

const {
  _internal: { parseArgs, applyScopeFilter, summarizeRow },
} = require("../../src/cli/qa-status");

test("qa-status parseArgs supports audit flag", () => {
  const parsed = parseArgs([
    "--country",
    "DE",
    "--as-of",
    "2026-03-15",
    "--source-id",
    "src-1",
    "--with-audit",
    "--json",
  ]);

  assert.equal(parsed.country, "DE");
  assert.equal(parsed.asOf, "2026-03-15");
  assert.equal(parsed.sourceId, "src-1");
  assert.equal(parsed.withAudit, true);
  assert.equal(parsed.json, true);
});

test("qa-status applyScopeFilter respects country, as-of, and source id", () => {
  const rows = [
    {
      stage_id: "merge-queue",
      scope_country: "DE",
      scope_as_of: "2026-03-15",
      scope_source_id: "src-1",
    },
    {
      stage_id: "merge-queue",
      scope_country: "AT",
      scope_as_of: "2026-03-15",
      scope_source_id: "src-1",
    },
  ];

  assert.deepEqual(
    applyScopeFilter(rows, {
      country: "DE",
      asOf: "2026-03-15",
      sourceId: "src-1",
    }),
    [rows[0]],
  );
});

test("qa-status summarizeRow marks stale reference-data rows", () => {
  const staleTimestamp = new Date(
    Date.now() - 9 * 24 * 60 * 60 * 1000,
  ).toISOString();

  assert.deepEqual(
    summarizeRow(
      {
        stage_id: "reference-data",
        scope_key: "DE|2026-03-15",
        scope_country: "DE",
        scope_as_of: "2026-03-15",
        scope_source_id: "",
        status: "ready",
        last_finished_at: staleTimestamp,
        timing_summary: {
          totalDurationMs: 123,
          cacheHit: true,
          skippedUnchanged: false,
        },
        output_summary: {
          timetableTripsTouched: 1,
          timetableTripStopTimesTouched: 0,
        },
      },
      168,
    ),
    {
      stageId: "reference-data",
      scopeKey: "DE|2026-03-15",
      scopeCountry: "DE",
      scopeAsOf: "2026-03-15",
      scopeSourceId: "",
      status: "stale",
      lastFinishedAt: staleTimestamp,
      durationMs: 123,
      cacheHit: true,
      skippedUnchanged: false,
      referenceStale: true,
      timetableTouched: true,
    },
  );
});
