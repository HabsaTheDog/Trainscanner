const test = require("node:test");
const assert = require("node:assert/strict");

const {
  _internal: { mapClusterCandidate },
} = require("../../src/graphql/resolvers");

test("mapClusterCandidate preserves provider feed context alongside provenance", () => {
  const mapped = mapClusterCandidate({
    global_station_id: "station_a",
    display_name: "Station A",
    provider_labels: ["db_regio_feed", "delfi_feed"],
    aliases: ["Station A Hbf"],
    network_summary: {
      provider_source_count: "2",
      route_pattern_count: "4",
    },
    network_context: {
      routes: [{ label: "ICE 42", transport_mode: "rail", pattern_hits: 4 }],
      stop_points: ["Platform 1", "Platform 2"],
    },
    provenance: {
      active_source_ids: ["db_regio_feed"],
      historical_source_ids: ["delfi_feed"],
      historical_stop_place_refs: ["de:old:123"],
      coord_input_stop_place_refs: ["de:123"],
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
        match_status: "strong",
        match_confidence: 0.98,
        is_primary: true,
      },
    ],
  });

  assert.deepEqual(mapped.provider_labels, ["db_regio_feed", "delfi_feed"]);
  assert.equal(mapped.network_summary.provider_source_count, 2);
  assert.deepEqual(mapped.network_context.stop_points, [
    "Platform 1",
    "Platform 2",
  ]);
  assert.deepEqual(mapped.provenance.active_source_ids, ["db_regio_feed"]);
  assert.deepEqual(mapped.provenance.historical_source_ids, ["delfi_feed"]);
  assert.equal(mapped.provenance.has_active_source_mappings, true);
  assert.equal(mapped.external_reference_summary.primary_match_count, 1);
  assert.equal(mapped.external_reference_matches[0].source_id, "wikidata");
});
