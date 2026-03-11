const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");

const { createPostgisClient } = require("../../src/data/postgis/client");
const {
  createGlobalStationsRepo,
} = require("../../src/data/postgis/repositories/global-stations-repo");
const {
  createMergeQueueRepo,
} = require("../../src/data/postgis/repositories/merge-queue-repo");
const {
  createDatabase,
  createDbEnv,
  dropDatabase,
  ensureDockerServiceRunning,
  ensureBootstrapped,
  shouldRunPostgisTests,
} = require("../helpers/postgis-test-db");

test(
  "global rebuild creates a cross-border merge cluster from fixture stations",
  { skip: !shouldRunPostgisTests },
  async () => {
    const servicesRoot = path.resolve(__dirname, "../../..");
    const repoRoot = path.resolve(servicesRoot, "..");
    const dbName = `itest_global_rebuild_cross_border_${Date.now()}`;
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

    const stationsRepo = createGlobalStationsRepo(client);
    const queueRepo = createMergeQueueRepo(client);
    const fixtureSuffix = String(Date.now());
    const sourceIdDe = `fixture_cross_border_de_${fixtureSuffix}`;
    const sourceIdAt = `fixture_cross_border_at_${fixtureSuffix}`;
    const asOf = "2026-03-10";
    const pairSeedInfos = [];

    const datasetRows = await client.runSql(
      `
      INSERT INTO provider_datasets (
        source_id,
        provider_slug,
        country,
        format,
        snapshot_date,
        ingestion_status
      )
      VALUES
        (
          :'source_id_de',
          'fixture-provider',
          'DE',
          'netex',
          :'as_of'::date,
          'ingested'
        ),
        (
          :'source_id_at',
          'fixture-provider',
          'AT',
          'netex',
          :'as_of'::date,
          'ingested'
        )
      RETURNING dataset_id, source_id
      `,
      {
        source_id_de: sourceIdDe,
        source_id_at: sourceIdAt,
        as_of: asOf,
      },
    );

    const datasetIdBySource = new Map(
      datasetRows.rows.map((row) => [row.source_id, row.dataset_id]),
    );

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
        topographic_place_ref,
        raw_payload,
        updated_at
      )
      VALUES
        (
          'fixture_de_place_' || :'suffix',
          :'dataset_id_de',
          :'source_id_de',
          'border_place_de',
          'DE',
          'Grenzbahnhof',
          47.570000,
          9.700000,
          'fixture-topo-de',
          '{}'::jsonb,
          TIMESTAMPTZ '2026-03-10 08:00:00+00'
        ),
        (
          'fixture_at_place_' || :'suffix',
          :'dataset_id_at',
          :'source_id_at',
          'border_place_at',
          'AT',
          'Grenzbahnhof',
          47.570000,
          9.709000,
          'fixture-topo-at',
          '{}'::jsonb,
          TIMESTAMPTZ '2026-03-10 08:01:00+00'
        )
      `,
      {
        suffix: fixtureSuffix,
        dataset_id_de: datasetIdBySource.get(sourceIdDe),
        dataset_id_at: datasetIdBySource.get(sourceIdAt),
        source_id_de: sourceIdDe,
        source_id_at: sourceIdAt,
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
          'fixture_de_point_' || :'suffix',
          :'dataset_id_de',
          :'source_id_de',
          'border_point_de',
          'border_place_de',
          'fixture_de_place_' || :'suffix',
          'DE',
          'Grenzbahnhof Gleis 1',
          47.570050,
          9.700050,
          'fixture-topo-de',
          '{}'::jsonb,
          TIMESTAMPTZ '2026-03-10 08:05:00+00'
        ),
        (
          'fixture_at_point_' || :'suffix',
          :'dataset_id_at',
          :'source_id_at',
          'border_point_at',
          'border_place_at',
          'fixture_at_place_' || :'suffix',
          'AT',
          'Grenzbahnhof Gleis 1',
          47.570050,
          9.709050,
          'fixture-topo-at',
          '{}'::jsonb,
          TIMESTAMPTZ '2026-03-10 08:06:00+00'
        )
      `,
      {
        suffix: fixtureSuffix,
        dataset_id_de: datasetIdBySource.get(sourceIdDe),
        dataset_id_at: datasetIdBySource.get(sourceIdAt),
        source_id_de: sourceIdDe,
        source_id_at: sourceIdAt,
      },
    );

    const stationsSummary = await stationsRepo.buildGlobalStations({ asOf });
    assert.equal(stationsSummary.sourceRows, 2);
    assert.equal(stationsSummary.stationMappings, 2);
    assert.equal(stationsSummary.globalStopPoints, 2);

    const mappedStations = await client.queryRows(
      `
      SELECT DISTINCT global_station_id
      FROM provider_global_station_mappings
      WHERE source_id IN (:'source_id_de', :'source_id_at')
        AND is_active = true
      ORDER BY global_station_id
      `,
      {
        source_id_de: sourceIdDe,
        source_id_at: sourceIdAt,
      },
    );
    assert.equal(mappedStations.length, 2);

    const queueSummary = await queueRepo.rebuildMergeQueue(
      { asOf },
      {
        onInfo(info) {
          pairSeedInfos.push(info);
        },
      },
    );

    assert.equal(queueSummary.scopeCountry, "");
    assert.equal(queueSummary.scopeAsOf, asOf);
    assert.equal(queueSummary.scopeTag, asOf);
    assert.equal(queueSummary.clusters, 1);
    assert.ok(queueSummary.candidates >= 2);
    assert.ok(queueSummary.evidence > 0);
    assert.ok(
      pairSeedInfos.some(
        (info) => info.key === "pair_seeds_total" && Number(info.value) >= 1,
      ),
    );

    const clusterRows = await client.queryRows(
      `
      SELECT
        c.merge_cluster_id,
        c.scope_tag,
        c.candidate_count,
        c.country_tags,
        (
          SELECT COALESCE(json_agg(json_build_object(
            'global_station_id', cc.global_station_id,
            'display_name', cc.display_name,
            'country', cc.country
          ) ORDER BY cc.country, cc.global_station_id), '[]'::json)
          FROM qa_merge_cluster_candidates cc
          WHERE cc.merge_cluster_id = c.merge_cluster_id
        ) AS candidates
      FROM qa_merge_clusters c
      WHERE c.scope_tag = :'scope_tag'
      ORDER BY c.merge_cluster_id
      `,
      { scope_tag: asOf },
    );

    assert.equal(clusterRows.length, 1);
    assert.ok(clusterRows[0].candidate_count >= 2);
    assert.deepEqual([...new Set(clusterRows[0].country_tags)].sort(), [
      "AT",
      "DE",
    ]);
    assert.deepEqual(
      [
        ...new Set(
          clusterRows[0].candidates.map((candidate) => candidate.country),
        ),
      ].sort(),
      ["AT", "DE"],
    );
    assert.ok(
      clusterRows[0].candidates.every(
        (candidate) => candidate.display_name === "Grenzbahnhof",
      ),
    );

    const evidenceRows = await client.queryRows(
      `
      SELECT evidence_type, status, raw_value, details
      FROM qa_merge_cluster_evidence
      WHERE merge_cluster_id = :'merge_cluster_id'
      ORDER BY evidence_type
      `,
      { merge_cluster_id: clusterRows[0].merge_cluster_id },
    );

    assert.ok(
      evidenceRows.some(
        (row) =>
          row.evidence_type === "name_exact" &&
          row.status === "supporting" &&
          Array.isArray(row.details?.seed_reasons) &&
          row.details.seed_reasons.includes("exact_name"),
      ),
    );
    assert.ok(
      evidenceRows.some(
        (row) =>
          row.evidence_type === "country_relation" &&
          row.status === "informational" &&
          row.details?.same_country === false,
      ),
    );
  },
);
