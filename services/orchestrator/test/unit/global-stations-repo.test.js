const test = require("node:test");
const assert = require("node:assert/strict");

const {
  BUILD_GLOBAL_SQL,
  createGlobalStationsRepo,
  extractInfoFromNotice,
  extractPhaseFromNotice,
} = require("../../src/data/postgis/repositories/global-stations-repo");

test("extractPhaseFromNotice returns global build phases", () => {
  assert.equal(
    extractPhaseFromNotice({
      message: "global_build_phase:loading_latest_stop_places",
    }),
    "loading_latest_stop_places",
  );
  assert.equal(extractPhaseFromNotice("other notice"), "");
});

test("extractInfoFromNotice returns global build info payloads", () => {
  assert.deepEqual(
    extractInfoFromNotice({
      message: "global_build_info:source_row_count=42",
    }),
    {
      key: "source_row_count",
      value: "42",
    },
  );
  assert.equal(extractInfoFromNotice("global_build_phase:finalizing"), null);
});

test("build global stations SQL keeps latest-row dedup for stop places and stop points", () => {
  assert.match(
    BUILD_GLOBAL_SQL,
    /SET LOCAL max_parallel_workers_per_gather = 4;/,
  );
  assert.match(BUILD_GLOBAL_SQL, /SET LOCAL work_mem = '64MB';/);
  assert.match(BUILD_GLOBAL_SQL, /SET LOCAL maintenance_work_mem = '256MB';/);
  assert.match(BUILD_GLOBAL_SQL, /SET LOCAL synchronous_commit = 'OFF';/);
  assert.match(BUILD_GLOBAL_SQL, /SET LOCAL jit = off;/);
  assert.match(
    BUILD_GLOBAL_SQL,
    /PARTITION BY rp\.source_id, rp\.provider_stop_place_ref/,
  );
  assert.match(
    BUILD_GLOBAL_SQL,
    /ORDER BY pd\.snapshot_date DESC, rp\.dataset_id DESC, rp\.updated_at DESC, rp\.stop_place_id DESC/,
  );
  assert.match(
    BUILD_GLOBAL_SQL,
    /PARTITION BY rpp\.source_id, rpp\.provider_stop_point_ref/,
  );
  assert.match(
    BUILD_GLOBAL_SQL,
    /ORDER BY pd\.snapshot_date DESC, rpp\.dataset_id DESC, rpp\.updated_at DESC, rpp\.stop_point_id DESC/,
  );
  assert.match(BUILD_GLOBAL_SQL, /WHERE latest\.row_num = 1;/);
  assert.match(
    BUILD_GLOBAL_SQL,
    /CREATE TEMP TABLE _candidate_parent_evidence AS/,
  );
  assert.match(
    BUILD_GLOBAL_SQL,
    /CREATE TEMP TABLE _candidate_stop_point_evidence AS/,
  );
  assert.match(
    BUILD_GLOBAL_SQL,
    /CREATE TEMP TABLE _candidate_topographic_evidence AS/,
  );
  assert.match(
    BUILD_GLOBAL_SQL,
    /CREATE TEMP TABLE _candidate_coord_resolution AS/,
  );
  assert.match(
    BUILD_GLOBAL_SQL,
    /CREATE TEMP TABLE _cleanup_station_candidates AS/,
  );
  assert.match(
    BUILD_GLOBAL_SQL,
    /CREATE TEMP TABLE _transfer_scope_station_ids AS/,
  );
  assert.match(
    BUILD_GLOBAL_SQL,
    /CREATE TEMP TABLE _transfer_scope_stop_points AS/,
  );
  assert.match(
    BUILD_GLOBAL_SQL,
    /deactivation_reason',\s+'orphaned_without_active_source_or_stop_points'/,
  );
  for (const phase of [
    "initializing",
    "loading_latest_stop_places",
    "loading_latest_stop_points",
    "building_stop_point_evidence",
    "building_parent_evidence",
    "building_sibling_evidence",
    "building_coord_resolution",
    "writing_global_stations",
    "writing_station_mappings",
    "writing_global_stop_points",
    "writing_stop_point_mappings",
    "writing_transfer_edges",
    "finalizing",
  ]) {
    assert.match(BUILD_GLOBAL_SQL, new RegExp(`global_build_phase:${phase}`));
  }
  for (const infoKey of [
    "source_row_count",
    "latest_stop_point_rows",
    "stop_point_evidence_rows",
    "parent_evidence_rows",
    "sibling_evidence_rows",
    "coord_resolution_rows",
    "station_mapping_rows",
    "stop_point_mapping_rows",
    "transfer_scope_station_count",
    "transfer_scope_stop_point_rows",
    "transfer_edge_rows",
  ]) {
    assert.match(BUILD_GLOBAL_SQL, new RegExp(`global_build_info:${infoKey}=`));
  }
  assert.match(BUILD_GLOBAL_SQL, /coord_source/);
  assert.match(BUILD_GLOBAL_SQL, /coord_confidence/);
  assert.doesNotMatch(
    BUILD_GLOBAL_SQL,
    /ON CONFLICT \(from_global_stop_point_id, to_global_stop_point_id\)/,
  );
});

test("buildGlobalStations returns the parsed summary JSON", async () => {
  const calls = [];
  const phases = [];
  const infos = [];
  const client = {
    async runScript(sql, params, options) {
      calls.push({
        sql: String(sql),
        params,
      });
      if (options?.onNotice) {
        options.onNotice({
          message: "global_build_phase:loading_latest_stop_places",
        });
        options.onNotice({
          message: "global_build_info:source_row_count=1",
        });
      }
      return {
        stdout:
          '{"sourceRows":1,"globalStations":1,"stationMappings":1,"globalStopPoints":1,"stopPointMappings":1,"coordSourceSelf":1,"coordSourceParentStopPlace":0,"coordSourceChildStopPoints":0,"coordSourceSiblingStopPlaces":0,"coordSourceTopographicPlaceCluster":0,"coordSourceMissing":0,"coordConfidenceHigh":1,"coordConfidenceMedium":0,"coordConfidenceLow":0,"coordConfidenceUnresolved":0,"coordConflictCount":0,"countryFilter":"DE","asOf":"2026-03-09","sourceScope":"test_source"}',
        stderr: "",
      };
    },
  };

  const repo = createGlobalStationsRepo(client);
  const summary = await repo.buildGlobalStations(
    {
      country: "DE",
      asOf: "2026-03-09",
      sourceId: "test_source",
    },
    {
      onPhase(phase) {
        phases.push(phase);
      },
      onInfo(info) {
        infos.push(info);
      },
    },
  );

  assert.equal(summary.sourceRows, 1);
  assert.equal(summary.stationMappings, 1);
  assert.equal(summary.stopPointMappings, 1);
  assert.equal(summary.coordSourceSelf, 1);
  assert.equal(summary.coordConfidenceHigh, 1);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].params.country_filter, "DE");
  assert.equal(calls[0].params.as_of, "2026-03-09");
  assert.equal(calls[0].params.source_id_scope, "test_source");
  assert.deepEqual(phases, ["loading_latest_stop_places"]);
  assert.deepEqual(infos, [{ key: "source_row_count", value: "1" }]);
});
