const test = require("node:test");
const assert = require("node:assert/strict");

const {
  normalizeClusterDecision,
  normalizeIsoCountry,
  resolveCandidateDisplayName,
} = require("../../src/domains/qa/v2-contracts");

test("normalizeIsoCountry accepts empty when allowed and valid alpha-2 codes", () => {
  assert.equal(normalizeIsoCountry("", { allowEmpty: true }), "");
  assert.equal(normalizeIsoCountry("de", { allowEmpty: false }), "DE");
  assert.equal(normalizeIsoCountry("fr", { allowEmpty: false }), "FR");
});

test("normalizeClusterDecision validates merge payload and keeps line decision scaffold", () => {
  const payload = normalizeClusterDecision({
    operation: "merge",
    selected_station_ids: ["cstn_a", "cstn_b", "cstn_b"],
    line_decisions: { future_line_groups: [{ id: "l1" }] },
  });

  assert.equal(payload.operation, "merge");
  assert.deepEqual(payload.selectedStationIds, ["cstn_a", "cstn_b"]);
  assert.deepEqual(payload.lineDecisions, {
    future_line_groups: [{ id: "l1" }],
  });
});

test("normalizeClusterDecision carries rename_targets entries", () => {
  const payload = normalizeClusterDecision({
    operation: "merge",
    selected_station_ids: ["cstn_a", "cstn_b"],
    rename_targets: [
      {
        canonical_station_id: "cstn_a",
        rename_to: "Alpha Hub",
      },
    ],
  });

  assert.equal(payload.renameTargets.length, 1);
  assert.equal(payload.renameTargets[0].canonicalStationId, "cstn_a");
  assert.equal(payload.renameTargets[0].renameTo, "Alpha Hub");
});

test("normalizeClusterDecision accepts merge groups without explicit target station", () => {
  const payload = normalizeClusterDecision({
    operation: "merge",
    selected_station_ids: ["cstn_a", "cstn_b"],
    groups: [
      {
        group_label: "merge-selected",
        member_station_ids: ["cstn_a", "cstn_b"],
      },
    ],
  });

  assert.equal(payload.groups.length, 1);
  assert.equal(payload.groups[0].targetCanonicalStationId, "");
  assert.deepEqual(payload.groups[0].memberStationIds, ["cstn_a", "cstn_b"]);
});

test("normalizeClusterDecision rejects split without at least two groups", () => {
  assert.throws(() => {
    normalizeClusterDecision({
      operation: "split",
      groups: [{ group_label: "a", member_station_ids: ["cstn_a"] }],
    });
  }, /split requires at least two groups/);
});

test("normalizeClusterDecision keeps group segment_action walk links for backend transfer writes", () => {
  const payload = normalizeClusterDecision({
    operation: "merge",
    selected_station_ids: ["cstn_a", "cstn_b"],
    groups: [
      {
        group_label: "merge-selected",
        target_canonical_station_id: "cstn_a",
        member_station_ids: ["cstn_a", "cstn_b"],
        segment_action: {
          walk_links: [
            {
              from_segment_id: "seg_a",
              to_segment_id: "seg_b",
              min_walk_minutes: 4,
              bidirectional: true,
            },
          ],
        },
      },
    ],
  });

  assert.equal(payload.groups.length, 1);
  assert.deepEqual(payload.groups[0].segmentAction, {
    walk_links: [
      {
        from_segment_id: "seg_a",
        to_segment_id: "seg_b",
        min_walk_minutes: 4,
        bidirectional: true,
      },
    ],
  });
});

test("normalizeClusterDecision rejects unsupported operation values", () => {
  assert.throws(() => {
    normalizeClusterDecision({
      operation: "rename",
      selected_station_ids: ["cstn_a"],
    });
  }, /operation must be one of 'merge', 'split'/);
});

test("resolveCandidateDisplayName prefers explicit display name and never falls back to raw id", () => {
  assert.equal(
    resolveCandidateDisplayName({
      display_name: "Berlin Hbf",
      canonical_name: "Berlin Hauptbahnhof",
    }),
    "Berlin Hbf",
  );
  assert.equal(
    resolveCandidateDisplayName({
      canonical_name: "Munchen Hbf",
      canonical_station_id: "cstn_hash",
    }),
    "Munchen Hbf",
  );
  assert.equal(
    resolveCandidateDisplayName({ canonical_station_id: "cstn_hash" }),
    "Unnamed station",
  );
});

test("normalizeClusterDecision carries optional group section metadata", () => {
  const payload = normalizeClusterDecision({
    operation: "split",
    selected_station_ids: ["cstn_a", "cstn_b"],
    groups: [
      {
        group_label: "Main Hall",
        section_type: "main",
        section_name: "Main Hall",
        member_station_ids: ["cstn_a"],
      },
      {
        group_label: "Bus Terminal",
        section_type: "bus",
        section_name: "Bus Terminal",
        member_station_ids: ["cstn_b"],
      },
    ],
  });

  assert.equal(payload.groups.length, 2);
  assert.equal(payload.groups[0].sectionType, "main");
  assert.equal(payload.groups[1].sectionType, "bus");
  assert.equal(payload.groups[0].sectionName, "Main Hall");
});
