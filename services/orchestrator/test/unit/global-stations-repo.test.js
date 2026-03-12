const test = require("node:test");
const assert = require("node:assert/strict");

const {
  BUILD_GLOBAL_SQL,
  createGlobalStationsRepo,
} = require("../../src/data/postgis/repositories/global-stations-repo");

test("build global stations SQL keeps latest-row dedup for stop places and stop points", () => {
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
    /deactivation_reason',\s+'orphaned_without_active_source_or_stop_points'/,
  );
  assert.match(BUILD_GLOBAL_SQL, /coord_source/);
  assert.match(BUILD_GLOBAL_SQL, /coord_confidence/);
});

test("buildGlobalStations returns the parsed summary JSON", async () => {
  const calls = [];
  const client = {
    async runScript(sql, params) {
      calls.push({
        sql: String(sql),
        params,
      });
      return {
        stdout:
          '{"sourceRows":1,"globalStations":1,"stationMappings":1,"globalStopPoints":1,"stopPointMappings":1,"coordSourceSelf":1,"coordSourceParentStopPlace":0,"coordSourceChildStopPoints":0,"coordSourceSiblingStopPlaces":0,"coordSourceTopographicPlaceCluster":0,"coordSourceMissing":0,"coordConfidenceHigh":1,"coordConfidenceMedium":0,"coordConfidenceLow":0,"coordConfidenceUnresolved":0,"coordConflictCount":0,"countryFilter":"DE","asOf":"2026-03-09","sourceScope":"test_source"}',
        stderr: "",
      };
    },
  };

  const repo = createGlobalStationsRepo(client);
  const summary = await repo.buildGlobalStations({
    country: "DE",
    asOf: "2026-03-09",
    sourceId: "test_source",
  });

  assert.equal(summary.sourceRows, 1);
  assert.equal(summary.stationMappings, 1);
  assert.equal(summary.stopPointMappings, 1);
  assert.equal(summary.coordSourceSelf, 1);
  assert.equal(summary.coordConfidenceHigh, 1);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].params.country_filter, "DE");
  assert.equal(calls[0].params.as_of, "2026-03-09");
  assert.equal(calls[0].params.source_id_scope, "test_source");
});
