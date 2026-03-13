const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");

const { createPostgisClient } = require("../../src/data/postgis/client");
const {
  createExternalReferenceRepo,
} = require("../../src/data/postgis/repositories/external-reference-repo");
const {
  createDatabase,
  createDbEnv,
  dropDatabase,
  ensureDockerServiceRunning,
  ensureBootstrapped,
  shouldRunPostgisTests,
} = require("../helpers/postgis-test-db");

test(
  "external reference repository builds matches and overlay points",
  { skip: !shouldRunPostgisTests },
  async () => {
    const servicesRoot = path.resolve(__dirname, "../../..");
    const repoRoot = path.resolve(servicesRoot, "..");
    const dbName = `itest_external_reference_${Date.now()}`;
    const dbEnv = createDbEnv(dbName);
    let client;

    ensureDockerServiceRunning(repoRoot, dbEnv);
    await createDatabase(dbEnv);
    test.after(async () => {
      if (client) {
        await client.end();
      }
      await dropDatabase(dbEnv);
    });
    await ensureBootstrapped(repoRoot, dbEnv);

    client = createPostgisClient({
      rootDir: repoRoot,
      env: dbEnv,
    });
    await client.ensureReady();

    const repo = createExternalReferenceRepo(client);

    await client.runSql(
      `
      INSERT INTO global_stations (
        global_station_id,
        display_name,
        normalized_name,
        country,
        latitude,
        longitude,
        geom,
        station_kind
      )
      VALUES
        (
          'station_a',
          'Central Station',
          normalize_station_name('Central Station'),
          'DE',
          52.5200,
          13.4050,
          ST_SetSRID(ST_MakePoint(13.4050, 52.5200), 4326),
          'station'
        ),
        (
          'station_b',
          'Central Station',
          normalize_station_name('Central Station'),
          'DE',
          52.5201,
          13.4051,
          ST_SetSRID(ST_MakePoint(13.4051, 52.5201), 4326),
          'station'
        )
      `,
    );

    const wikidataImport = await repo.recordImportRun({
      sourceId: "wikidata",
      snapshotLabel: "wikidata-2026-03-10",
      snapshotDate: "2026-03-10",
      country: "DE",
      status: "succeeded",
      metadata: {},
    });

    await repo.replaceImportRows({
      importId: wikidataImport.import_id,
      sourceId: "wikidata",
      rows: [
        {
          external_id: "Q123",
          display_name: "Central Station",
          normalized_name: "central station",
          country: "DE",
          latitude: 52.52005,
          longitude: 13.40505,
          category: "station",
          subtype: "rail_station",
          source_url: "https://www.wikidata.org/wiki/Q123",
          metadata: {},
        },
      ],
    });

    const overtureImport = await repo.recordImportRun({
      sourceId: "overture",
      snapshotLabel: "overture-2026-03-10",
      snapshotDate: "2026-03-10",
      country: "DE",
      status: "succeeded",
      metadata: {},
    });

    await repo.replaceImportRows({
      importId: overtureImport.import_id,
      sourceId: "overture",
      rows: [
        {
          external_id: "ovr-1",
          display_name: "Central East",
          normalized_name: "central east",
          country: "DE",
          latitude: 52.5204,
          longitude: 13.4054,
          category: "station",
          subtype: "train_station",
          source_url: "https://example.test/overture/ovr-1",
          metadata: {},
        },
      ],
    });

    const summary = await repo.buildStationReferenceMatches({
      country: "DE",
      asOf: "2026-03-10",
    });

    assert.equal(summary.matched_stations, 2);
    assert.equal(summary.status_counts.strong, 2);

    const matches = await repo.loadMatchesByStationIds([
      "station_a",
      "station_b",
    ]);
    assert.equal(matches.length >= 2, true);
    assert.equal(
      matches.filter(
        (row) => row.source_id === "wikidata" && row.is_primary === true,
      ).length,
      2,
    );

    const overlay = await repo.loadOverlayByStationIds([
      "station_a",
      "station_b",
    ]);
    assert.equal(overlay.length, 2);
    assert.ok(
      overlay.some((row) => row.external_id === "Q123"),
      "overlay should include matched Wikidata reference",
    );
    assert.ok(
      overlay.some((row) => row.external_id === "ovr-1"),
      "overlay should include nearby unmatched Overture reference",
    );
  },
);
