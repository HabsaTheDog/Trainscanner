const test = require("node:test");
const assert = require("node:assert/strict");
const { execFileSync, spawnSync } = require("node:child_process");
const path = require("node:path");
const { Pool } = require("pg");

const { createPostgisClient } = require("../../src/data/postgis/client");
const {
  createGlobalStationsRepo,
} = require("../../src/data/postgis/repositories/global-stations-repo");

const hasDocker =
  spawnSync("bash", ["-lc", "command -v docker >/dev/null 2>&1"]).status === 0;
const shouldRun = hasDocker && process.env.ENABLE_POSTGIS_TESTS === "1";

function createDbEnv(dbName) {
  return {
    ...process.env,
    CANONICAL_DB_MODE: "docker-compose",
    CANONICAL_DB_DOCKER_PROFILE: "pan-europe-data",
    CANONICAL_DB_DOCKER_SERVICE: "postgis",
    CANONICAL_DB_NAME: dbName,
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function ensureBootstrapped(repoRoot, dbEnv) {
  const scriptPath = path.join(repoRoot, "scripts", "data", "db-bootstrap.sh");

  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      execFileSync("bash", [scriptPath, "--quiet", "--if-ready"], {
        cwd: repoRoot,
        env: dbEnv,
        encoding: "utf8",
      });
      return;
    } catch (error) {
      const output = `${error.stdout || ""}\n${error.stderr || ""}`;
      if (attempt < 3 && output.includes("tuple concurrently updated")) {
        await sleep(500 * attempt);
        continue;
      }
      throw error;
    }
  }
}

async function createDatabase(dbEnv) {
  const pool = new Pool({
    host: dbEnv.CANONICAL_DB_HOST || "localhost",
    port: Number.parseInt(dbEnv.CANONICAL_DB_PORT || "55432", 10),
    user: dbEnv.CANONICAL_DB_USER || "trainscanner",
    password: dbEnv.CANONICAL_DB_PASSWORD || "trainscanner",
    database: "postgres",
  });

  try {
    await pool.query(`CREATE DATABASE "${dbEnv.CANONICAL_DB_NAME}"`);
  } catch (error) {
    if (error?.code !== "42P04") {
      throw error;
    }
  } finally {
    await pool.end();
  }
}

async function dropDatabase(dbEnv) {
  const pool = new Pool({
    host: dbEnv.CANONICAL_DB_HOST || "localhost",
    port: Number.parseInt(dbEnv.CANONICAL_DB_PORT || "55432", 10),
    user: dbEnv.CANONICAL_DB_USER || "trainscanner",
    password: dbEnv.CANONICAL_DB_PASSWORD || "trainscanner",
    database: "postgres",
  });

  try {
    await pool.query(
      `DROP DATABASE IF EXISTS "${dbEnv.CANONICAL_DB_NAME}" WITH (FORCE)`,
    );
  } finally {
    await pool.end();
  }
}

test(
  "buildGlobalStations keeps one active mapping when historical datasets coexist",
  { skip: !shouldRun },
  async () => {
    const servicesRoot = path.resolve(__dirname, "../../..");
    const repoRoot = path.resolve(servicesRoot, "..");
    const dbName = `itest_global_stations_${Date.now()}`;
    const dbEnv = createDbEnv(dbName);
    let client;

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

    const repo = createGlobalStationsRepo(client);
    const sourceId = `integration_hist_${Date.now()}`;
    const placeRef = "place_main";
    const pointRef = "point_main";

    const olderDataset = await client.runSql(
      `
      INSERT INTO provider_datasets (
        source_id,
        provider_slug,
        country,
        format,
        snapshot_date,
        ingestion_status
      )
      VALUES (
        :'source_id',
        'integration-provider',
        'DE',
        'netex',
        DATE '2026-03-04',
        'ingested'
      )
      RETURNING dataset_id
      `,
      { source_id: sourceId },
    );

    const newerDataset = await client.runSql(
      `
      INSERT INTO provider_datasets (
        source_id,
        provider_slug,
        country,
        format,
        snapshot_date,
        ingestion_status
      )
      VALUES (
        :'source_id',
        'integration-provider',
        'DE',
        'netex',
        DATE '2026-03-09',
        'ingested'
      )
      RETURNING dataset_id
      `,
      { source_id: sourceId },
    );

    const olderDatasetId = olderDataset.rows[0].dataset_id;
    const newerDatasetId = newerDataset.rows[0].dataset_id;

    await client.runSql(
      `
      INSERT INTO raw_provider_stop_places (
        stop_place_id,
        dataset_id,
        source_id,
        provider_stop_place_ref,
        country,
        stop_name,
        latitude,
        longitude,
        hard_id,
        raw_payload,
        updated_at
      )
      VALUES
        (
          'old_place_' || :'source_id',
          :'older_dataset_id',
          :'source_id',
          :'place_ref',
          'DE',
          'Historic Central',
          52.5200,
          13.4050,
          'hard-central',
          '{}'::jsonb,
          TIMESTAMPTZ '2026-03-04 09:00:00+00'
        ),
        (
          'new_place_' || :'source_id',
          :'newer_dataset_id',
          :'source_id',
          :'place_ref',
          'DE',
          'Current Central',
          52.5201,
          13.4051,
          'hard-central',
          '{}'::jsonb,
          TIMESTAMPTZ '2026-03-09 09:00:00+00'
        )
      `,
      {
        source_id: sourceId,
        older_dataset_id: olderDatasetId,
        newer_dataset_id: newerDatasetId,
        place_ref: placeRef,
      },
    );

    await client.runSql(
      `
      INSERT INTO raw_provider_stop_points (
        stop_point_id,
        dataset_id,
        source_id,
        provider_stop_point_ref,
        provider_stop_place_ref,
        stop_place_id,
        country,
        stop_name,
        latitude,
        longitude,
        raw_payload,
        updated_at
      )
      VALUES
        (
          'old_point_' || :'source_id',
          :'older_dataset_id',
          :'source_id',
          :'point_ref',
          :'place_ref',
          'old_place_' || :'source_id',
          'DE',
          'Platform Historic',
          52.5200,
          13.4050,
          '{}'::jsonb,
          TIMESTAMPTZ '2026-03-04 09:15:00+00'
        ),
        (
          'new_point_' || :'source_id',
          :'newer_dataset_id',
          :'source_id',
          :'point_ref',
          :'place_ref',
          'new_place_' || :'source_id',
          'DE',
          'Platform Current',
          52.5201,
          13.4051,
          '{}'::jsonb,
          TIMESTAMPTZ '2026-03-09 09:15:00+00'
        )
      `,
      {
        source_id: sourceId,
        older_dataset_id: olderDatasetId,
        newer_dataset_id: newerDatasetId,
        place_ref: placeRef,
        point_ref: pointRef,
      },
    );

    const summary = await repo.buildGlobalStations({
      country: "DE",
      asOf: "2026-03-09",
      sourceId,
    });

    assert.equal(summary.sourceRows, 1);

    const stationMappingCount = await client.runSql(
      `
      SELECT COUNT(*)::integer AS count
      FROM provider_global_station_mappings
      WHERE source_id = :'source_id'
        AND provider_stop_place_ref = :'place_ref'
        AND is_active = true
      `,
      {
        source_id: sourceId,
        place_ref: placeRef,
      },
    );

    const stopPointMappingCount = await client.runSql(
      `
      SELECT COUNT(*)::integer AS count
      FROM provider_global_stop_point_mappings
      WHERE source_id = :'source_id'
        AND provider_stop_point_ref = :'point_ref'
        AND is_active = true
      `,
      {
        source_id: sourceId,
        point_ref: pointRef,
      },
    );

    const latestStation = await client.runSql(
      `
      SELECT gs.display_name
      FROM provider_global_station_mappings m
      JOIN global_stations gs
        ON gs.global_station_id = m.global_station_id
      WHERE m.source_id = :'source_id'
        AND m.provider_stop_place_ref = :'place_ref'
        AND m.is_active = true
      `,
      {
        source_id: sourceId,
        place_ref: placeRef,
      },
    );

    const latestStopPoint = await client.runSql(
      `
      SELECT sp.display_name
      FROM provider_global_stop_point_mappings m
      JOIN global_stop_points sp
        ON sp.global_stop_point_id = m.global_stop_point_id
      WHERE m.source_id = :'source_id'
        AND m.provider_stop_point_ref = :'point_ref'
        AND m.is_active = true
      `,
      {
        source_id: sourceId,
        point_ref: pointRef,
      },
    );

    assert.equal(stationMappingCount.rows[0].count, 1);
    assert.equal(stopPointMappingCount.rows[0].count, 1);
    assert.equal(latestStation.rows[0].display_name, "Current Central");
    assert.equal(latestStopPoint.rows[0].display_name, "Platform Current");
  },
);

test(
  "buildGlobalStations derives child stop place coordinates from child stop points and records provenance",
  { skip: !shouldRun },
  async () => {
    const servicesRoot = path.resolve(__dirname, "../../..");
    const repoRoot = path.resolve(servicesRoot, "..");
    const dbName = `itest_global_station_coords_${Date.now()}`;
    const dbEnv = createDbEnv(dbName);
    let client;

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

    const repo = createGlobalStationsRepo(client);
    const sourceId = `integration_coords_${Date.now()}`;

    const dataset = await client.runSql(
      `
      INSERT INTO provider_datasets (
        source_id,
        provider_slug,
        country,
        format,
        snapshot_date,
        ingestion_status
      )
      VALUES (
        :'source_id',
        'integration-provider',
        'DE',
        'netex',
        DATE '2026-03-09',
        'ingested'
      )
      RETURNING dataset_id
      `,
      { source_id: sourceId },
    );

    const datasetId = dataset.rows[0].dataset_id;

    await client.runSql(
      `
      INSERT INTO raw_provider_stop_places (
        stop_place_id,
        dataset_id,
        source_id,
        provider_stop_place_ref,
        country,
        stop_name,
        latitude,
        longitude,
        parent_stop_place_ref,
        topographic_place_ref,
        hard_id,
        raw_payload,
        updated_at
      )
      VALUES
        (
          'parent_place_' || :'source_id',
          :'dataset_id',
          :'source_id',
          'parent_place',
          'DE',
          'Bruchhausen',
          51.302085,
          7.930336,
          NULL,
          'topo-main',
          'de:05958:32194',
          '{}'::jsonb,
          TIMESTAMPTZ '2026-03-09 09:00:00+00'
        ),
        (
          'child_place_' || :'source_id',
          :'dataset_id',
          :'source_id',
          'child_place',
          'DE',
          'Bruchhausen',
          NULL,
          NULL,
          'parent_place',
          'topo-main',
          'de:05958:32194:0',
          '{}'::jsonb,
          TIMESTAMPTZ '2026-03-09 09:05:00+00'
        )
      `,
      {
        source_id: sourceId,
        dataset_id: datasetId,
      },
    );

    await client.runSql(
      `
      INSERT INTO raw_provider_stop_points (
        stop_point_id,
        dataset_id,
        source_id,
        provider_stop_point_ref,
        provider_stop_place_ref,
        stop_place_id,
        country,
        stop_name,
        latitude,
        longitude,
        topographic_place_ref,
        raw_payload,
        updated_at
      )
      VALUES
        (
          'child_point_1_' || :'source_id',
          :'dataset_id',
          :'source_id',
          'child_point_1',
          'child_place',
          'child_place_' || :'source_id',
          'DE',
          'Bruchhausen',
          51.302152,
          7.930273,
          'topo-main',
          '{}'::jsonb,
          TIMESTAMPTZ '2026-03-09 09:10:00+00'
        ),
        (
          'child_point_2_' || :'source_id',
          :'dataset_id',
          :'source_id',
          'child_point_2',
          'child_place',
          'child_place_' || :'source_id',
          'DE',
          'Bruchhausen',
          51.302017,
          7.930426,
          'topo-main',
          '{}'::jsonb,
          TIMESTAMPTZ '2026-03-09 09:11:00+00'
        )
      `,
      {
        source_id: sourceId,
        dataset_id: datasetId,
      },
    );

    const summary = await repo.buildGlobalStations({
      country: "DE",
      asOf: "2026-03-09",
      sourceId,
    });

    assert.equal(summary.coordSourceChildStopPoints, 1);

    const childStation = await client.runSql(
      `
      SELECT
        gs.latitude,
        gs.longitude,
        gs.metadata ->> 'coord_source' AS coord_source,
        gs.metadata ->> 'coord_confidence' AS coord_confidence,
        gs.metadata ->> 'hierarchy_role' AS hierarchy_role,
        gs.metadata -> 'coord_validation' -> 'warning_codes' AS warning_codes
      FROM provider_global_station_mappings m
      JOIN global_stations gs
        ON gs.global_station_id = m.global_station_id
      WHERE m.source_id = :'source_id'
        AND m.provider_stop_place_ref = 'child_place'
        AND m.is_active = true
      `,
      { source_id: sourceId },
    );

    assert.equal(childStation.rows.length, 1);
    assert.ok(Number(childStation.rows[0].latitude) > 0);
    assert.ok(Number(childStation.rows[0].longitude) > 0);
    assert.equal(childStation.rows[0].coord_source, "child_stop_points");
    assert.equal(childStation.rows[0].coord_confidence, "high");
    assert.equal(childStation.rows[0].hierarchy_role, "child");
  },
);
