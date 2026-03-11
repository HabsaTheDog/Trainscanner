const test = require("node:test");
const assert = require("node:assert/strict");

const {
  classifyDistanceEvidence,
  classifyEvidenceRow,
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

test("classifyEvidenceRow derives canonical category and seed role", () => {
  assert.deepEqual(
    classifyEvidenceRow({
      evidence_type: "name_loose_similarity",
      details: {
        seed_reasons: ["loose_name_geo", "exact_name"],
      },
    }),
    {
      category: "core_match",
      is_seed_rule: true,
      seed_reasons: ["loose_name_geo", "exact_name"],
    },
  );
  assert.deepEqual(
    classifyEvidenceRow({
      evidence_type: "geographic_distance",
      details: {
        seed_reasons: ["loose_name_geo"],
      },
    }),
    {
      category: "core_match",
      is_seed_rule: false,
      seed_reasons: ["loose_name_geo"],
    },
  );
});

test("summarizeEvidenceRows builds cluster-level counts and pair summaries", () => {
  const summary = summarizeEvidenceRows([
    {
      source_global_station_id: "a",
      target_global_station_id: "b",
      evidence_type: "name_exact",
      category: "core_match",
      is_seed_rule: true,
      seed_reasons: ["exact_name"],
      status: "supporting",
      score: 1,
      details: {
        seed_reasons: ["exact_name"],
      },
    },
    {
      source_global_station_id: "a",
      target_global_station_id: "b",
      evidence_type: "geographic_distance",
      category: "core_match",
      is_seed_rule: false,
      seed_reasons: ["exact_name"],
      status: "warning",
      score: 0.05,
      details: {
        seed_reasons: ["exact_name"],
      },
    },
    {
      source_global_station_id: "a",
      target_global_station_id: "c",
      evidence_type: "coordinate_quality",
      category: "risk_conflict",
      is_seed_rule: false,
      seed_reasons: ["loose_name_missing_coords"],
      status: "missing",
      score: 0,
      details: {
        seed_reasons: ["loose_name_missing_coords"],
      },
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
    category_counts: {
      core_match: 2,
      network_context: 0,
      risk_conflict: 1,
    },
    seed_rule_counts: {
      exact_name: 2,
      loose_name_missing_coords: 1,
    },
  });
  assert.equal(summary.pairSummaries.length, 2);
  assert.equal(summary.pairSummaries[0].source_global_station_id, "a");
  assert.equal(summary.pairSummaries[0].target_global_station_id, "b");
  assert.equal(summary.pairSummaries[0].warning_count, 1);
  assert.deepEqual(summary.pairSummaries[0].categories, ["core_match"]);
  assert.deepEqual(summary.pairSummaries[0].seed_reasons, ["exact_name"]);
  assert.deepEqual(summary.pairSummaries[0].highlights, {
    evidence_types: ["name_exact", "geographic_distance"],
    distance_status: "",
    shared_signal_count: 0,
    seed_reasons: ["exact_name"],
  });
  assert.deepEqual(summary.pairSummaries[1].categories, ["risk_conflict"]);
  assert.deepEqual(summary.pairSummaries[1].seed_reasons, [
    "loose_name_missing_coords",
  ]);
  assert.deepEqual(summary.pairSummaries[1].highlights.seed_reasons, [
    "loose_name_missing_coords",
  ]);
});
