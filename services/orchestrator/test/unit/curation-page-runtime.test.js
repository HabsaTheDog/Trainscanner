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
    candidates: [
      {
        global_station_id: "station_a",
        display_name: "Station A",
        candidate_rank: 1,
        provider_labels: ["DB Regio"],
        coord_status: "missing_coordinates",
        network_context: {
          routes: [
            { label: "ICE 42", transport_mode: "rail", pattern_hits: 2 },
            { label: "RB 12", transport_mode: "rail", pattern_hits: 2 },
          ],
          incoming: [{ station_name: "Berlin Hbf", pattern_hits: 3 }],
          outgoing: [
            { station_name: "Hamburg Hbf", pattern_hits: 2 },
            { station_name: "Munich Hbf", pattern_hits: 3 },
          ],
          stop_points: ["Platform 1", "Platform 2"],
        },
        network_summary: {
          route_pattern_count: 4,
          incoming_neighbor_count: 1,
          outgoing_neighbor_count: 2,
          stop_point_count: 6,
          provider_source_count: 2,
        },
        external_reference_summary: {
          source_counts: {
            wikidata: 1,
          },
          primary_match_count: 1,
          strong_match_count: 1,
          probable_match_count: 0,
        },
        external_reference_matches: [
          {
            source_id: "wikidata",
            external_id: "Q123",
            display_name: "Station A",
            category: "station",
            lat: 52.52,
            lon: 13.405,
            distance_meters: 23,
            match_status: "strong",
            match_confidence: 0.99,
            source_url: "https://www.wikidata.org/wiki/Q123",
            is_primary: true,
          },
        ],
        provenance: {
          has_active_source_mappings: false,
          active_source_ids: [],
          active_source_labels: [],
          active_stop_place_refs: [],
          historical_source_ids: ["delfi-de"],
          historical_source_labels: ["DELFI"],
          historical_stop_place_refs: ["de:old:123"],
          coord_input_stop_place_refs: ["de:123"],
        },
      },
    ],
    reference_overlay: [
      {
        source_id: "wikidata",
        external_id: "Q123",
        display_name: "Station A",
        category: "station",
        lat: 52.52,
        lon: 13.405,
        source_url: "https://www.wikidata.org/wiki/Q123",
        matched_candidate_ids: ["station_a"],
      },
    ],
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
  assert.deepEqual(detail.candidates[0].network_context.stop_points, [
    "Platform 1",
    "Platform 2",
  ]);
  assert.deepEqual(detail.candidates[0].network_summary, {
    route_pattern_count: 4,
    incoming_neighbor_count: 1,
    outgoing_neighbor_count: 2,
    stop_point_count: 6,
    provider_source_count: 2,
  });
  assert.deepEqual(detail.candidates[0].provenance, {
    has_active_source_mappings: false,
    active_source_ids: [],
    active_source_labels: [],
    active_stop_place_refs: [],
    historical_source_ids: ["delfi-de"],
    historical_source_labels: ["DELFI"],
    historical_stop_place_refs: ["de:old:123"],
    coord_input_stop_place_refs: ["de:123"],
  });
  assert.deepEqual(detail.candidates[0].external_reference_summary, {
    source_counts: {
      wikidata: 1,
    },
    primary_match_count: 1,
    strong_match_count: 1,
    probable_match_count: 0,
  });
  assert.equal(
    detail.candidates[0].external_reference_matches[0].source_id,
    "wikidata",
  );
  assert.equal(detail.reference_overlay[0].source_id, "wikidata");
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

test("fetchClusterDetail requests network context from GraphQL", async () => {
  const { fetchClusterDetail } = await loadRuntimeModule();
  const previousFetch = globalThis.fetch;
  const previousWindow = globalThis.window;
  const queries = [];

  globalThis.window = {
    __CONFIG__: {
      GRAPHQL_URL: "http://example.test/api/graphql",
    },
  };
  globalThis.fetch = async (_url, options = {}) => {
    const payload = JSON.parse(String(options.body || "{}"));
    queries.push(payload.query || "");

    return {
      ok: true,
      async json() {
        return {
          data: {
            globalCluster: {
              cluster_id: "cluster_legacy",
              country_tags: ["DE"],
              workspace: null,
              candidates: [
                {
                  global_station_id: "station_legacy",
                  display_name: "Legacy Station",
                  network_context: {
                    routes: [
                      {
                        label: "ICE 42",
                        transport_mode: "rail",
                        pattern_hits: 1,
                      },
                    ],
                    incoming: [{ station_name: "Berlin Hbf", pattern_hits: 1 }],
                    outgoing: [
                      { station_name: "Hamburg Hbf", pattern_hits: 1 },
                    ],
                    stop_points: [],
                  },
                  network_summary: {
                    route_pattern_count: 1,
                    incoming_neighbor_count: 1,
                    outgoing_neighbor_count: 1,
                    stop_point_count: 2,
                    provider_source_count: 1,
                  },
                },
              ],
              evidence: [],
              evidence_summary: {},
              pair_summaries: [],
              decisions: [],
              edit_history: [],
            },
          },
        };
      },
    };
  };

  try {
    const detail = await fetchClusterDetail("cluster_legacy");

    assert.equal(queries.length, 1);
    assert.match(queries[0], /network_context/);
    assert.match(queries[0], /stop_points/);
    assert.deepEqual(detail.candidates[0].network_context.stop_points, []);
    assert.equal(detail.candidates[0].network_summary.stop_point_count, 2);
  } finally {
    globalThis.fetch = previousFetch;
    globalThis.window = previousWindow;
  }
});

test("pickPrimaryCountry falls back to EU when no country tags exist", async () => {
  const { pickPrimaryCountry } = await loadRuntimeModule();
  assert.equal(pickPrimaryCountry(["AT", "DE"]), "AT");
  assert.equal(pickPrimaryCountry([]), "EU");
  assert.equal(pickPrimaryCountry(null, "ALL"), "ALL");
});

test("resolveDefaultMapStyle prefers explicit styles and otherwise uses dark basemaps", async () => {
  const { resolveDefaultMapStyle } = await loadRuntimeModule();
  const previousMapStyleUrl = globalThis.MAP_STYLE_URL;
  const previousProtomapsApiKey = globalThis.PROTOMAPS_API_KEY;

  try {
    globalThis.MAP_STYLE_URL = "https://example.com/custom-style.json";
    globalThis.PROTOMAPS_API_KEY = "abc123";
    assert.equal(
      resolveDefaultMapStyle(),
      "https://example.com/custom-style.json",
    );

    globalThis.MAP_STYLE_URL = "";
    assert.equal(
      resolveDefaultMapStyle(),
      "https://api.protomaps.com/styles/v4/dark/en.json?key=abc123",
    );

    globalThis.PROTOMAPS_API_KEY = "";
    assert.equal(
      resolveDefaultMapStyle(),
      "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
    );
  } finally {
    globalThis.MAP_STYLE_URL = previousMapStyleUrl;
    globalThis.PROTOMAPS_API_KEY = previousProtomapsApiKey;
  }
});

test("requireSuccessfulMutation rejects missing or unsuccessful mutation payloads", async () => {
  const { requireSuccessfulMutation } = await loadRuntimeModule();

  assert.throws(
    () => requireSuccessfulMutation({}, "saveGlobalClusterWorkspace", "failed"),
    /failed/,
  );
  assert.throws(
    () =>
      requireSuccessfulMutation(
        { saveGlobalClusterWorkspace: { ok: false } },
        "saveGlobalClusterWorkspace",
        "failed",
      ),
    /failed/,
  );

  assert.deepEqual(
    requireSuccessfulMutation(
      { saveGlobalClusterWorkspace: { ok: true, workspace_version: 2 } },
      "saveGlobalClusterWorkspace",
      "failed",
    ),
    { ok: true, workspace_version: 2 },
  );
});

test("requireGraphqlField rejects missing AI responses", async () => {
  const { requireGraphqlField } = await loadRuntimeModule();

  assert.throws(
    () => requireGraphqlField({}, "requestAiScore", "No response from AI."),
    /No response from AI\./,
  );
  assert.deepEqual(
    requireGraphqlField(
      { requestAiScore: { suggested_action: "merge" } },
      "requestAiScore",
      "No response from AI.",
    ),
    { suggested_action: "merge" },
  );
});
