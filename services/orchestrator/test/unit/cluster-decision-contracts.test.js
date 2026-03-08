const test = require("node:test");
const assert = require("node:assert/strict");

const {
  normalizeGlobalMergeDecision,
  normalizeIsoCountry,
} = require("../../src/domains/qa/cluster-decision-contracts");

test("normalizeIsoCountry accepts empty when allowed and valid alpha-2 codes", () => {
  assert.equal(normalizeIsoCountry("", { allowEmpty: true }), "");
  assert.equal(normalizeIsoCountry("de", { allowEmpty: false }), "DE");
  assert.equal(normalizeIsoCountry("fr", { allowEmpty: false }), "FR");
});

test("normalizeGlobalMergeDecision validates merge payload", () => {
  const payload = normalizeGlobalMergeDecision({
    operation: "merge",
    selected_global_station_ids: ["gstn_a", "gstn_b", "gstn_b"],
  });

  assert.equal(payload.operation, "merge");
  assert.deepEqual(payload.selectedGlobalStationIds, ["gstn_a", "gstn_b"]);
  assert.equal(payload.groups.length, 0);
});

test("normalizeGlobalMergeDecision carries rename_targets entries", () => {
  const payload = normalizeGlobalMergeDecision({
    operation: "rename",
    rename_targets: [
      {
        global_station_id: "gstn_a",
        rename_to: "Alpha Hub",
      },
    ],
  });

  assert.equal(payload.renameTargets.length, 1);
  assert.equal(payload.renameTargets[0].globalStationId, "gstn_a");
  assert.equal(payload.renameTargets[0].renameTo, "Alpha Hub");
});

test("normalizeGlobalMergeDecision validates group members as global ids", () => {
  const payload = normalizeGlobalMergeDecision({
    operation: "merge",
    selected_global_station_ids: ["gstn_a", "gstn_b"],
    groups: [
      {
        group_label: "merge-selected",
        member_global_station_ids: ["gstn_a", "gstn_b"],
      },
    ],
  });

  assert.equal(payload.groups.length, 1);
  assert.deepEqual(payload.groups[0].memberGlobalStationIds, [
    "gstn_a",
    "gstn_b",
  ]);
});

test("normalizeGlobalMergeDecision rejects merge without at least 2 members", () => {
  assert.throws(() => {
    normalizeGlobalMergeDecision({
      operation: "merge",
      selected_global_station_ids: ["gstn_a"],
    });
  }, /merge decisions require at least two selected global stations/);
});

test("normalizeGlobalMergeDecision rejects unsupported operation values", () => {
  assert.throws(() => {
    normalizeGlobalMergeDecision({
      operation: "approve",
      selected_global_station_ids: ["gstn_a", "gstn_b"],
    });
  }, /operation must be one of 'merge', 'split', 'keep_separate', 'rename'/);
});
