const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadRuntimeModule() {
  const repoRoot = path.resolve(__dirname, "../../../..");
  const runtimePath = path.join(
    repoRoot,
    "frontend",
    "src",
    "curation-page-runtime.js",
  );
  return import(pathToFileURL(runtimePath).href);
}

test("formatResultsLabel renders locale-formatted result totals", async () => {
  const { formatResultsLabel } = await loadRuntimeModule();
  assert.equal(formatResultsLabel(105758, "en-US"), "105,758 results");
});

test("formatResultsLabel normalizes missing totals to zero", async () => {
  const { formatResultsLabel } = await loadRuntimeModule();
  assert.equal(formatResultsLabel(undefined, "en-US"), "0 results");
});

test("createMergeFromSelection absorbs selected merge members and raw refs into one merge", async () => {
  const { createMergeFromSelection } = await loadRuntimeModule();
  const workspace = {
    merges: [
      {
        entity_id: "merge_a",
        member_refs: ["raw:station_b", "raw:station_c"],
        display_name: "Station B",
      },
    ],
  };
  const selectedRefs = new Set(["merge:merge_a", "raw:station_a"]);
  const candidates = [
    {
      global_station_id: "station_a",
      display_name: "Station A",
      candidate_rank: 1,
    },
    {
      global_station_id: "station_b",
      display_name: "Station B",
      candidate_rank: 2,
    },
    {
      global_station_id: "station_c",
      display_name: "Station C",
      candidate_rank: 3,
    },
  ];

  const next = createMergeFromSelection(workspace, selectedRefs, candidates);

  assert.equal(next.merges.length, 1);
  assert.deepEqual(next.merges[0].member_refs, [
    "raw:station_a",
    "raw:station_b",
    "raw:station_c",
  ]);
  assert.equal(next.merges[0].display_name, "Station A");
});

test("removeMemberFromMerge drops one merge member and removes undersized merges", async () => {
  const { removeMemberFromMerge } = await loadRuntimeModule();
  const workspace = {
    merges: [
      {
        entity_id: "merge_a",
        member_refs: ["raw:station_a", "raw:station_b", "raw:station_c"],
        display_name: "Station A",
      },
      {
        entity_id: "merge_b",
        member_refs: ["raw:station_x", "raw:station_y"],
        display_name: "Station X",
      },
    ],
  };

  const trimmed = removeMemberFromMerge(workspace, "merge_a", "raw:station_b");
  assert.deepEqual(trimmed.merges[0].member_refs, [
    "raw:station_a",
    "raw:station_c",
  ]);

  const removed = removeMemberFromMerge(trimmed, "merge_b", "raw:station_x");
  assert.equal(removed.merges.length, 1);
  assert.equal(removed.merges[0].entity_id, "merge_a");
});

test("normalizeClusterDetail preserves canonical evidence taxonomy fields", async () => {
  const { normalizeClusterDetail } = await loadRuntimeModule();
  const detail = normalizeClusterDetail({
    cluster_id: "cluster_1",
    country_tags: ["DE"],
    workspace: null,
    candidates: [],
    evidence: [
      {
        evidence_type: "name_exact",
        source_global_station_id: "a",
        target_global_station_id: "b",
        category: "core_match",
        is_seed_rule: true,
        seed_reasons: ["exact_name"],
        status: "supporting",
        score: 1,
        raw_value: 1,
        details: { seed_reasons: ["exact_name"] },
      },
    ],
    evidence_summary: {
      category_counts: {
        core_match: 1,
        network_context: 0,
        risk_conflict: 0,
      },
      seed_rule_counts: {
        exact_name: 1,
      },
    },
    pair_summaries: [
      {
        source_global_station_id: "a",
        target_global_station_id: "b",
        categories: ["core_match"],
        seed_reasons: ["exact_name"],
        score: 1,
        summary: "Signals are mostly supportive",
      },
    ],
    decisions: [],
    edit_history: [],
  });

  assert.equal(detail.evidence[0].category, "core_match");
  assert.equal(detail.evidence[0].is_seed_rule, true);
  assert.deepEqual(detail.evidence[0].seed_reasons, ["exact_name"]);
  assert.deepEqual(detail.evidence_summary.category_counts, {
    core_match: 1,
    network_context: 0,
    risk_conflict: 0,
  });
  assert.deepEqual(detail.evidence_summary.seed_rule_counts, {
    exact_name: 1,
  });
  assert.deepEqual(detail.pair_summaries[0].categories, ["core_match"]);
  assert.deepEqual(detail.pair_summaries[0].seed_reasons, ["exact_name"]);
});
