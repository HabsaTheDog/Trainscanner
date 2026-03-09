const test = require("node:test");
const assert = require("node:assert/strict");

const {
  classifyDistanceEvidence,
  isGenericStationName,
  normalizeLooseStationName,
  summarizeEvidenceRows,
  tokenizeLooseStationName,
} = require("../../src/domains/qa/evidence-utils");

test("normalizeLooseStationName expands common abbreviations", () => {
  assert.equal(
    normalizeLooseStationName("München Hbf, Abzw. Ost Str."),
    "munchen hauptbahnhof abzweig ost strasse",
  );
});

test("tokenizeLooseStationName removes low-signal transport boilerplate", () => {
  assert.deepEqual(tokenizeLooseStationName("Berlin Bahnhof Steig 02"), [
    "berlin",
    "steig",
    "02",
  ]);
});

test("isGenericStationName flags lexical platform names and high-frequency names", () => {
  assert.equal(isGenericStationName("Steig 02", 1), true);
  assert.equal(isGenericStationName("Neuhaus", 12), true);
  assert.equal(isGenericStationName("Konstanz Fuerstenberg", 2), false);
});

test("classifyDistanceEvidence maps missing and far distances correctly", () => {
  assert.deepEqual(classifyDistanceEvidence(null), {
    status: "missing",
    distance_status: "missing_coordinates",
    score: 0,
  });
  assert.deepEqual(classifyDistanceEvidence(40), {
    status: "supporting",
    distance_status: "same_location",
    score: 1,
  });
  assert.deepEqual(classifyDistanceEvidence(7000), {
    status: "warning",
    distance_status: "too_far",
    score: 0.05,
  });
});

test("summarizeEvidenceRows builds cluster-level counts and pair summaries", () => {
  const summary = summarizeEvidenceRows([
    {
      source_global_station_id: "a",
      target_global_station_id: "b",
      evidence_type: "name_exact",
      status: "supporting",
      score: 1,
    },
    {
      source_global_station_id: "a",
      target_global_station_id: "b",
      evidence_type: "geographic_distance",
      status: "warning",
      score: 0.05,
    },
    {
      source_global_station_id: "a",
      target_global_station_id: "c",
      evidence_type: "coordinate_quality",
      status: "missing",
      score: 0,
    },
  ]);

  assert.deepEqual(summary.evidenceSummary, {
    supporting: 1,
    warning: 1,
    missing: 1,
    informational: 0,
    status_counts: {
      supporting: 1,
      warning: 1,
      missing: 1,
      informational: 0,
    },
    type_counts: {
      name_exact: 1,
      geographic_distance: 1,
      coordinate_quality: 1,
    },
  });
  assert.equal(summary.pairSummaries.length, 2);
  assert.equal(summary.pairSummaries[0].source_global_station_id, "a");
  assert.equal(summary.pairSummaries[0].target_global_station_id, "b");
  assert.equal(summary.pairSummaries[0].warning_count, 1);
  assert.deepEqual(summary.pairSummaries[0].highlights, {
    evidence_types: ["name_exact", "geographic_distance"],
    distance_status: "",
    shared_signal_count: 0,
  });
});
