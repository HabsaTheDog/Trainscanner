const test = require("node:test");
const assert = require("node:assert/strict");

const { buildPromptContext } = require("../../src/domains/ai-evaluation/context-builder");
const { aggregateRunMetrics, comparePrediction } = require("../../src/domains/ai-evaluation/metrics");
const { buildTruthSnapshot } = require("../../src/domains/ai-evaluation/truth");

test("buildPromptContext excludes workspace and decisions while keeping network fields", () => {
  const context = buildPromptContext(
    {
      cluster_id: "cluster-1",
      severity: "high",
      scope_tag: "2026-03-28",
      summary: { hint: "test" },
      workspace: { merges: [["hidden"]] },
      decisions: [{ note: "hidden" }],
      candidates: [
        {
          global_station_id: "station-1",
          display_name: "Vienna Hbf",
          coord_status: "coordinates_present",
          aliases: ["Wien Hbf"],
          provenance: { active_source_ids: ["db"] },
          network_context: { routes: [{ label: "ICE", pattern_hits: 4 }] },
          network_summary: { route_pattern_count: 4 },
          external_reference_summary: { primary_match_count: 1 },
        },
      ],
      evidence_summary: { supporting: 2 },
      pair_summaries: [{ summary: "close match" }],
    },
    {
      context_sections: [
        "cluster_summary",
        "candidate_core",
        "network_context",
        "network_summary",
        "cluster_metadata",
      ],
    },
  );

  assert.equal(context.cluster_summary.hint, "test");
  assert.equal(context.candidate_core[0].display_name, "Vienna Hbf");
  assert.equal(context.network_context[0].network_context.routes[0].label, "ICE");
  assert.equal(context.cluster_metadata.cluster_id, "cluster-1");
  assert.equal("workspace" in context, false);
  assert.equal("decisions" in context, false);
});

test("buildTruthSnapshot canonicalizes rename-only workspaces", () => {
  const truth = buildTruthSnapshot({
    cluster_id: "cluster-2",
    status: "resolved",
    workspace: {
      entities: [],
      merges: [],
      groups: [],
      keep_separate_sets: [],
      renames: [
        {
          ref: "raw:station-a",
          display_name: "New Name",
        },
      ],
      note: "",
    },
  });

  assert.equal(truth.verdict, "rename_only");
  assert.deepEqual(truth.renames, [
    {
      target_ref_type: "raw",
      target_station_ids: ["station-a"],
      display_name: "New Name",
    },
  ]);
});

test("comparePrediction and aggregateRunMetrics summarize exactness and latency", () => {
  const truth = {
    verdict: "merge_only",
    merges: [["a", "b"]],
    groups: [],
    keep_separate_sets: [],
    renames: [],
  };
  const comparison = comparePrediction(truth, {
    verdict: "merge_only",
    merges: [["a", "b"]],
    groups: [],
    keep_separate_sets: [],
    renames: [],
  });

  assert.equal(comparison.verdict_exact, true);
  assert.equal(comparison.strict_exact, true);
  assert.equal(comparison.pairwise_agreement, 1);

  const metrics = aggregateRunMetrics([
    {
      comparison,
      latency_ms: 140,
      token_usage: { total_tokens: 321 },
      estimated_cost_usd: 0.0123,
      item_status: "succeeded",
    },
    {
      comparison: {
        verdict_exact: false,
        strict_exact: false,
        pairwise_agreement: 0.5,
        false_merge: true,
        false_dismiss: false,
      },
      latency_ms: 260,
      token_usage: { total_tokens: 99 },
      estimated_cost_usd: 0.001,
      item_status: "succeeded",
    },
  ]);

  assert.equal(metrics.total_items, 2);
  assert.equal(metrics.scored_items, 2);
  assert.equal(metrics.total_tokens, 420);
  assert.equal(metrics.median_latency_ms, 140);
  assert.equal(metrics.p95_latency_ms, 260);
});
