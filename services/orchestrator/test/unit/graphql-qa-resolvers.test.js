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
    context_summary: {
      provider_source_count: "2",
      route_count: "4",
    },
    service_context: {
      lines: ["ICE 42"],
      stop_points: ["Platform 1", "Platform 2"],
    },
    provenance: {
      active_source_ids: ["db_regio_feed"],
      historical_source_ids: ["delfi_feed"],
      historical_stop_place_refs: ["de:old:123"],
      coord_input_stop_place_refs: ["de:123"],
    },
  });

  assert.deepEqual(mapped.provider_labels, ["db_regio_feed", "delfi_feed"]);
  assert.equal(mapped.context_summary.provider_source_count, 2);
  assert.deepEqual(mapped.service_context.stop_points, [
    "Platform 1",
    "Platform 2",
  ]);
  assert.deepEqual(mapped.provenance.active_source_ids, ["db_regio_feed"]);
  assert.deepEqual(mapped.provenance.historical_source_ids, ["delfi_feed"]);
  assert.equal(mapped.provenance.has_active_source_mappings, true);
});
