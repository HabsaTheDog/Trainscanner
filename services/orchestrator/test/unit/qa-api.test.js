const test = require("node:test");
const assert = require("node:assert/strict");

const {
  _internal: {
    buildExternalReferenceEnrichment,
    buildEditHistoryQuery,
    buildWorkspaceMutationResponse,
    normalizeCandidateMetadata,
    normalizeExternalReferenceSummary,
    normalizeEvidenceRow,
    resolveUpdatedBy,
  },
} = require("../../src/domains/qa/api");

test("normalizeEvidenceRow coerces metrics and preserves safe details", () => {
  const normalized = normalizeEvidenceRow({
    evidence_type: "geographic_distance",
    status: "",
    raw_value: "123.4",
    score: "0.75",
    details: {
      distance_meters: 123.4,
      distance_status: "nearby",
    },
  });

  assert.equal(normalized.status, "informational");
  assert.equal(normalized.raw_value, 123.4);
  assert.equal(normalized.score, 0.75);
  assert.deepEqual(normalized.details, {
    distance_meters: 123.4,
    distance_status: "nearby",
  });
  assert.equal(normalized.category, "core_match");
});

test("resolveUpdatedBy falls back to qa_operator and trims aliases", () => {
  assert.equal(resolveUpdatedBy(undefined), "qa_operator");
  assert.equal(resolveUpdatedBy({ updated_by: "  alice  " }), "alice");
  assert.equal(resolveUpdatedBy({ updatedBy: "bob" }), "bob");
});

test("buildWorkspaceMutationResponse shapes stable mutation payloads", () => {
  assert.deepEqual(
    buildWorkspaceMutationResponse("cluster-1", {
      workspaceVersion: 4,
      effectiveStatus: "in_review",
      workspace: { merges: [] },
    }),
    {
      ok: true,
      cluster_id: "cluster-1",
      workspace_version: 4,
      effective_status: "in_review",
      workspace: { merges: [] },
    },
  );
});

test("normalizeCandidateMetadata exposes active and historical provenance", () => {
  const normalized = normalizeCandidateMetadata({
    latitude: null,
    longitude: null,
    metadata: {
      network_context: {
        stop_points: ["Platform 1", " Platform 1 ", "Platform 2"],
      },
      network_summary: {
        provider_source_count: "0",
      },
    },
    active_source_ids: ["db-regio", " db-regio "],
    active_stop_place_refs: ["de:123"],
    historical_source_ids: ["delfi-de"],
    historical_stop_place_refs: ["de:old:123"],
    coord_input_stop_place_refs: ["de:123", ""],
  });

  assert.equal(normalized.coord_status, "missing_coordinates");
  assert.deepEqual(normalized.network_context.stop_points, [
    "Platform 1",
    "Platform 2",
  ]);
  assert.deepEqual(normalized.provenance, {
    active_source_ids: ["db-regio"],
    active_stop_place_refs: ["de:123"],
    historical_source_ids: ["delfi-de"],
    historical_stop_place_refs: ["de:old:123"],
    coord_input_stop_place_refs: ["de:123"],
    has_active_source_mappings: true,
  });
  assert.deepEqual(normalized.external_reference_summary, {
    source_counts: {},
    primary_match_count: 0,
    strong_match_count: 0,
    probable_match_count: 0,
  });
  assert.deepEqual(normalized.external_reference_matches, []);
});

test("normalizeExternalReferenceSummary coerces source counts and match totals", () => {
  assert.deepEqual(
    normalizeExternalReferenceSummary({
      source_counts: {
        wikidata: "2",
      },
      primary_match_count: "1",
      strong_match_count: "2",
      probable_match_count: "0",
    }),
    {
      source_counts: {
        wikidata: 2,
      },
      primary_match_count: 1,
      strong_match_count: 2,
      probable_match_count: 0,
    },
  );
});

test("buildExternalReferenceEnrichment appends candidate provenance, overlay, and evidence", () => {
  const enrichment = buildExternalReferenceEnrichment(
    [
      {
        global_station_id: "station_a",
        display_name: "Vienna Hbf",
      },
      {
        global_station_id: "station_b",
        display_name: "Wien Hauptbahnhof",
      },
    ],
    [
      {
        global_station_id: "station_a",
        reference_station_id: 1,
        source_id: "wikidata",
        external_id: "Q123",
        display_name: "Wien Hauptbahnhof",
        latitude: 48.185,
        longitude: 16.374,
        distance_meters: 45,
        match_status: "strong",
        match_confidence: 0.99,
        is_primary: true,
        source_url: "https://www.wikidata.org/wiki/Q123",
      },
      {
        global_station_id: "station_b",
        reference_station_id: 1,
        source_id: "wikidata",
        external_id: "Q123",
        display_name: "Wien Hauptbahnhof",
        latitude: 48.185,
        longitude: 16.374,
        distance_meters: 35,
        match_status: "strong",
        match_confidence: 0.98,
        is_primary: true,
        source_url: "https://www.wikidata.org/wiki/Q123",
      },
    ],
    [
      {
        source_id: "wikidata",
        external_id: "Q123",
        display_name: "Wien Hauptbahnhof",
        category: "station",
        latitude: 48.185,
        longitude: 16.374,
        source_url: "https://www.wikidata.org/wiki/Q123",
        matched_candidate_ids: ["station_a", "station_b"],
      },
    ],
  );

  assert.equal(
    enrichment.candidates[0].external_reference_summary.strong_match_count,
    1,
  );
  assert.equal(enrichment.reference_overlay.length, 1);
  assert.deepEqual(enrichment.reference_overlay[0].matched_candidate_ids, [
    "station_a",
    "station_b",
  ]);
  assert.ok(
    enrichment.evidence.some(
      (row) => row.evidence_type === "external_reference_same_entity",
    ),
  );
  assert.ok(
    enrichment.evidence.some(
      (row) => row.evidence_type === "external_reference_coverage",
    ),
  );
});

test("buildEditHistoryQuery casts workspace and decision enums to text", () => {
  const query = buildEditHistoryQuery();

  assert.match(query, /v\.action::text AS event_type/);
  assert.match(query, /d\.operation::text AS event_type/);
  assert.match(query, /UNION ALL/);
});
