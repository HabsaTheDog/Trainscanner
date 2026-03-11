import assert from "node:assert/strict";
import test from "node:test";

import {
  formatCoordinateStatusLabel,
  formatEvidenceCategoryLabel,
  formatEvidenceDetails,
  formatEvidenceStatusLabel,
  formatEvidenceTypeLabel,
  formatEvidenceValue,
  formatLabel,
  formatProviderFeedsTooltip,
  formatSeedReasonLabel,
  getEvidenceCategoryCounts,
  getEvidenceTypeCounts,
  getRowSeedReasons,
  getSeedRuleCounts,
  getSummaryCount,
} from "../src/curation-page-formatters.js";
import {
  BASE_MARKER_SIZE,
  buildMappableItems,
  buildMarkerOverlapLayout,
} from "../src/curation-page-map-utils.js";
import { createUiState, uiReducer } from "../src/curation-page-ui-state.js";

test("curation formatters normalize labels and evidence summaries", () => {
  assert.equal(formatLabel("risk_conflict"), "Risk Conflict");
  assert.equal(formatEvidenceTypeLabel("name_exact"), "Exact Name");
  assert.equal(formatEvidenceStatusLabel("missing_coordinates"), "No Coords");
  assert.equal(
    formatEvidenceCategoryLabel("network_context"),
    "Network Context",
  );
  assert.equal(formatSeedReasonLabel("shared_route"), "Shared Route");
  assert.equal(formatCoordinateStatusLabel("coordinates_present"), "Coords");
  assert.equal(
    formatProviderFeedsTooltip([" DB ", "SBB", "DB", "", null]),
    "Feeds used: DB, SBB",
  );
  assert.equal(formatProviderFeedsTooltip([]), "No feeds available");
  assert.equal(
    formatEvidenceValue({
      evidence_type: "geographic_distance",
      raw_value: 152.2,
    }),
    "152m",
  );
  assert.equal(
    formatEvidenceValue({
      evidence_type: "token_overlap",
      raw_value: 0.88,
    }),
    "88%",
  );
  assert.equal(
    formatEvidenceDetails({
      explanation: "Shared operator labels",
    }),
    "Shared operator labels",
  );
  assert.equal(
    formatEvidenceDetails({
      raw_score: 4,
      same_country: true,
    }),
    "Raw Score: 4 · Same Country: true",
  );
  assert.deepEqual(
    getEvidenceTypeCounts({
      type_counts: {
        shared_route_context: 2,
        name_exact: 4,
      },
    }),
    [
      { type: "name_exact", count: 4 },
      { type: "shared_route_context", count: 2 },
    ],
  );
  assert.deepEqual(
    getEvidenceCategoryCounts({
      category_counts: {
        core_match: 3,
        risk_conflict: 1,
      },
    }),
    [
      { category: "core_match", count: 3 },
      { category: "risk_conflict", count: 1 },
    ],
  );
  assert.deepEqual(
    getSeedRuleCounts({
      seed_rule_counts: {
        loose_name_geo: 1,
        exact_name: 5,
      },
    }),
    [
      { reason: "exact_name", count: 5 },
      { reason: "loose_name_geo", count: 1 },
    ],
  );
  assert.equal(
    getSummaryCount({ status_counts: { supporting: 7 } }, "supporting"),
    7,
  );
  assert.deepEqual(
    getRowSeedReasons({
      details: { seed_reasons: [" exact_name ", "", "shared_route"] },
    }),
    ["exact_name", "shared_route"],
  );
});

test("curation map utils derive approximate coordinates and overlap layout", () => {
  const items = [
    {
      ref: "raw:A",
      display_name: "Vienna Hbf",
      lat: 48.185,
      lon: 16.374,
      candidate: { candidate_rank: 1 },
    },
    {
      ref: "raw:B",
      display_name: "Vienna Hbf",
      lat: 48.186,
      lon: 16.375,
      candidate: { candidate_rank: 2 },
    },
    {
      ref: "raw:C",
      display_name: "Vienna Hbf",
      candidate: { candidate_rank: 3 },
    },
  ];

  const mappable = buildMappableItems(items);
  assert.equal(mappable.length, 3);
  assert.equal(mappable[2].approx, true);
  assert.equal(mappable[2].lat, (48.185 + 48.186) / 2);
  assert.equal(mappable[2].lon, (16.374 + 16.375) / 2);

  const fakeMap = {
    project([lon, lat]) {
      return { x: lon * 100, y: lat * 100 };
    },
  };

  const layout = buildMarkerOverlapLayout(fakeMap, [
    { ref: "raw:A", lat: 48.185, lon: 16.374 },
    { ref: "raw:B", lat: 48.185, lon: 16.374 },
  ]);

  assert.equal(layout.get("raw:A").stackSize, 2);
  assert.equal(layout.get("raw:A").markerSize, BASE_MARKER_SIZE * 2);
  assert.equal(layout.get("raw:B").stackIndex, 1);
});

test("curation ui state reducer keeps selection and focus transitions stable", () => {
  const initial = createUiState();
  const selected = uiReducer(initial, {
    type: "set_selection",
    refs: ["raw:1", "merge:2"],
    lastSelectedIndex: 3,
  });
  assert.deepEqual(Array.from(selected.selectedRefs), ["raw:1", "merge:2"]);
  assert.equal(selected.lastSelectedIndex, 3);

  const toggled = uiReducer(selected, {
    type: "toggle_selection",
    ref: "raw:1",
    index: 4,
  });
  assert.deepEqual(Array.from(toggled.selectedRefs), ["merge:2"]);
  assert.equal(toggled.lastSelectedIndex, 4);

  const focused = uiReducer(initial, {
    type: "focus",
    ref: "group:local-1",
  });
  assert.equal(focused.focusedRef, "group:local-1");
  assert.equal(focused.activeTool, "group");
});
