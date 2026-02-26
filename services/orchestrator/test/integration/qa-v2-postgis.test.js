const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const crypto = require("node:crypto");
const { execFileSync, spawnSync } = require("node:child_process");

const {
  getReviewClustersV2,
  getReviewClusterDetailV2,
  postReviewClusterDecisionV2,
  getCuratedStationsV1,
  getCuratedStationDetailV1,
} = require("../../src/domains/qa/api");
const { createPostgisClient } = require("../../src/data/postgis/client");

const hasDocker =
  spawnSync("bash", ["-lc", "command -v docker >/dev/null 2>&1"]).status === 0;
const shouldRun = hasDocker && process.env.ENABLE_POSTGIS_TESTS === "1";

test(
  "qa v2 cluster generation is deterministic and decisions are atomic",
  { skip: !shouldRun },
  async () => {
    const repoRoot = path.resolve(__dirname, "../../..");

    process.env.CANONICAL_DB_MODE = "docker-compose";
    process.env.CANONICAL_DB_DOCKER_PROFILE = "dach-data";
    process.env.CANONICAL_DB_DOCKER_SERVICE = "postgis";

    execFileSync(
      "bash",
      [path.join(repoRoot, "scripts", "data", "db-migrate.sh"), "--quiet"],
      {
        cwd: repoRoot,
        stdio: "inherit",
        env: {
          ...process.env,
          CANONICAL_DB_MODE: "docker-compose",
          CANONICAL_DB_DOCKER_PROFILE: "dach-data",
          CANONICAL_DB_DOCKER_SERVICE: "postgis",
        },
      },
    );

    const client = createPostgisClient({
      rootDir: repoRoot,
      env: {
        ...process.env,
        CANONICAL_DB_MODE: "docker-compose",
        CANONICAL_DB_DOCKER_PROFILE: "dach-data",
        CANONICAL_DB_DOCKER_SERVICE: "postgis",
      },
    });
    await client.ensureReady();

    const runId = crypto.randomUUID();
    const suffix = crypto.randomUUID().slice(0, 8);
    const stationA = `cstn_test_a_${suffix}`;
    const stationB = `cstn_test_b_${suffix}`;
    const sourceA = `qa_source_a_${suffix}`;
    const sourceB = `qa_source_b_${suffix}`;
    const stopA = `stop_a_${suffix}`;
    const stopB = `stop_b_${suffix}`;
    const complexId = `cplx_${suffix}`;
    const segmentA = `seg_${suffix}_a`;
    const segmentB = `seg_${suffix}_b`;
    const issueKey = `duplicate_hard_id|DE|hard_${suffix}|latest`;

    await client.exec(
      `
    BEGIN;

    INSERT INTO import_runs (run_id, pipeline, status, source_id, country, snapshot_date)
    VALUES (:'run_id'::uuid, 'canonical_build', 'succeeded', :'source_a', 'DE', '2026-02-20'::date)
    ON CONFLICT (run_id) DO NOTHING;

    INSERT INTO canonical_stations (
      canonical_station_id,
      canonical_name,
      normalized_name,
      country,
      latitude,
      longitude,
      geom,
      grid_id,
      match_method,
      member_count,
      first_seen_snapshot_date,
      last_seen_snapshot_date,
      last_built_run_id,
      updated_at
    ) VALUES
      (:'station_a', 'Alpha Hub', normalize_station_name('Alpha Hub'), 'DE', 48.1001, 11.5001, ST_SetSRID(ST_MakePoint(11.5001, 48.1001), 4326), compute_geo_grid_id('DE', 48.1001, 11.5001, ST_SetSRID(ST_MakePoint(11.5001, 48.1001), 4326)), 'hard_id', 1, '2026-02-20', '2026-02-20', :'run_id'::uuid, now()),
      (:'station_b', 'Alpha Hub North', normalize_station_name('Alpha Hub North'), 'DE', 48.1012, 11.5012, ST_SetSRID(ST_MakePoint(11.5012, 48.1012), 4326), compute_geo_grid_id('DE', 48.1012, 11.5012, ST_SetSRID(ST_MakePoint(11.5012, 48.1012), 4326)), 'hard_id', 1, '2026-02-20', '2026-02-20', :'run_id'::uuid, now())
    ON CONFLICT (grid_id, canonical_station_id) DO NOTHING;

    INSERT INTO netex_stops_staging (
      import_run_id,
      source_id,
      country,
      provider_slug,
      snapshot_date,
      source_stop_id,
      stop_name,
      latitude,
      longitude,
      grid_id,
      hard_id,
      raw_payload
    ) VALUES
      (:'run_id'::uuid, :'source_a', 'DE', 'qa-provider', '2026-02-20'::date, :'stop_a', 'Alpha Hub', 48.1001, 11.5001, compute_geo_grid_id('DE', 48.1001, 11.5001, NULL::geometry), :'hard_id', '{"lines": ["S1", "ICE-1"], "language": "de"}'::jsonb),
      (:'run_id'::uuid, :'source_b', 'DE', 'qa-provider', '2026-02-20'::date, :'stop_b', 'Alpha Hub North', 48.1012, 11.5012, compute_geo_grid_id('DE', 48.1012, 11.5012, NULL::geometry), :'hard_id', '{"lines": ["S1"], "language": "de"}'::jsonb)
    ON CONFLICT (grid_id, source_id, snapshot_date, source_stop_id) DO NOTHING;

    INSERT INTO canonical_station_sources (
      canonical_station_id,
      source_id,
      source_stop_id,
      country,
      snapshot_date,
      match_method,
      hard_id,
      import_run_id,
      updated_at
    ) VALUES
      (:'station_a', :'source_a', :'stop_a', 'DE', '2026-02-20'::date, 'hard_id', :'hard_id', :'run_id'::uuid, now()),
      (:'station_b', :'source_b', :'stop_b', 'DE', '2026-02-20'::date, 'hard_id', :'hard_id', :'run_id'::uuid, now())
    ON CONFLICT (source_id, source_stop_id) DO UPDATE SET
      canonical_station_id = EXCLUDED.canonical_station_id,
      hard_id = EXCLUDED.hard_id,
      updated_at = now();

    INSERT INTO canonical_review_queue (
      issue_key,
      country,
      canonical_station_id,
      issue_type,
      severity,
      status,
      details,
      provenance_source,
      provenance_run_tag,
      first_detected_at,
      last_detected_at,
      created_at,
      updated_at
    ) VALUES (
      :'issue_key',
      'DE',
      NULL,
      'duplicate_hard_id',
      'high',
      'open',
      jsonb_build_object('hardId', :'hard_id', 'canonicalStationIds', jsonb_build_array(:'station_a', :'station_b')),
      'integration-test',
      'latest',
      now(),
      now(),
      now(),
      now()
    )
    ON CONFLICT (issue_key) DO UPDATE SET
      status = 'open',
      details = EXCLUDED.details,
      provenance_run_tag = 'latest',
      updated_at = now();

    INSERT INTO qa_station_complexes_v2 (
      complex_id,
      country,
      complex_name,
      display_name,
      metadata
    ) VALUES (
      :'complex_id',
      'DE',
      'Alpha Hub Complex',
      'Alpha Hub Complex',
      '{}'::jsonb
    )
    ON CONFLICT (complex_id) DO NOTHING;

    INSERT INTO qa_station_segments_v2 (
      segment_id,
      complex_id,
      canonical_station_id,
      segment_name,
      segment_type,
      latitude,
      longitude,
      geom,
      metadata
    ) VALUES
      (:'segment_a', :'complex_id', :'station_a', 'Alpha Hub Main Segment', 'main_hall', 48.1001, 11.5001, ST_SetSRID(ST_MakePoint(11.5001, 48.1001), 4326), '{}'::jsonb),
      (:'segment_b', :'complex_id', :'station_b', 'Alpha Hub Bus Segment', 'bus_station', 48.1012, 11.5012, ST_SetSRID(ST_MakePoint(11.5012, 48.1012), 4326), '{}'::jsonb)
    ON CONFLICT (segment_id) DO NOTHING;

    COMMIT;
    `,
      {
        run_id: runId,
        station_a: stationA,
        station_b: stationB,
        source_a: sourceA,
        source_b: sourceB,
        stop_a: stopA,
        stop_b: stopB,
        complex_id: complexId,
        segment_a: segmentA,
        segment_b: segmentB,
        hard_id: `hard_${suffix}`,
        issue_key: issueKey,
      },
    );

    await client.exec(`SELECT qa_rebuild_station_clusters_v2('DE', NULL);`);
    const firstRows = await client.queryRows(
      `SELECT cluster_id FROM qa_station_clusters_v2 WHERE country = 'DE' AND scope_tag = 'latest' ORDER BY cluster_id`,
    );

    await client.exec(`SELECT qa_rebuild_station_clusters_v2('DE', NULL);`);
    const secondRows = await client.queryRows(
      `SELECT cluster_id FROM qa_station_clusters_v2 WHERE country = 'DE' AND scope_tag = 'latest' ORDER BY cluster_id`,
    );

    assert.deepEqual(
      firstRows.map((row) => row.cluster_id),
      secondRows.map((row) => row.cluster_id),
    );

    const listUrl = new URL(
      "http://localhost/api/qa/v2/clusters?country=DE&scope_tag=latest&limit=20",
    );
    const clusters = await getReviewClustersV2(listUrl);
    const cluster = clusters.find(
      (row) =>
        Array.isArray(row.candidates) &&
        row.candidates.some(
          (candidate) => candidate.canonical_station_id === stationA,
        ),
    );
    assert.ok(cluster, "expected seeded cluster in v2 list response");
    assert.equal(Object.hasOwn(cluster, "queue_items"), false);

    const detail = await getReviewClusterDetailV2(cluster.cluster_id);
    assert.ok(Array.isArray(detail.candidates));
    assert.ok(
      detail.candidates.some(
        (candidate) => candidate.canonical_station_id === stationA,
      ),
    );
    assert.equal(Object.hasOwn(detail, "queue_items"), false);
    const candidateWithService = detail.candidates.find(
      (candidate) => candidate.canonical_station_id === stationA,
    );
    assert.ok(candidateWithService);
    assert.ok(Array.isArray(candidateWithService.service_context.lines));
    assert.ok(Array.isArray(candidateWithService.service_context.incoming));
    assert.ok(Array.isArray(candidateWithService.service_context.outgoing));

    await assert.rejects(
      () =>
        postReviewClusterDecisionV2(cluster.cluster_id, {
          operation: "merge",
          selected_station_ids: [stationA, "cstn_outside_scope"],
          requested_by: "integration_reviewer",
        }),
      /not part of cluster/,
    );

    const decision = await postReviewClusterDecisionV2(cluster.cluster_id, {
      operation: "merge",
      selected_station_ids: [stationA, stationB],
      groups: [
        {
          group_label: "merge-selected",
          member_station_ids: [stationA, stationB],
          segment_action: {
            walk_links: [
              {
                from_segment_id: segmentA,
                to_segment_id: segmentB,
                min_walk_minutes: 4,
                bidirectional: true,
              },
            ],
          },
        },
      ],
      note: "Integration merge check",
    });

    assert.equal(decision.ok, true);
    assert.equal(decision.operation, "merge");
    assert.ok(decision.decision_id);

    const mergeDecisionMemberActions = await client.queryRows(
      `
    SELECT action, COUNT(*)::integer AS items
    FROM qa_station_cluster_decision_members_v2
    WHERE decision_id = :'decision_id'
    GROUP BY action
    ORDER BY action
    `,
      {
        decision_id: decision.decision_id,
      },
    );
    assert.equal(mergeDecisionMemberActions.length, 1);
    assert.equal(mergeDecisionMemberActions[0].action, "merge_member");
    assert.equal(mergeDecisionMemberActions[0].items, 2);

    const queueRows = await client.queryRows(
      `
    SELECT status
    FROM canonical_review_queue
    WHERE review_item_id IN (
      SELECT review_item_id
      FROM qa_station_cluster_queue_items_v2
      WHERE cluster_id = :'cluster_id'
    )
    `,
      { cluster_id: cluster.cluster_id },
    );
    assert.ok(queueRows.length > 0);
    assert.ok(queueRows.every((row) => row.status === "resolved"));

    const overrideRows = await client.queryRows(
      `
    SELECT operation, source_canonical_station_id, target_canonical_station_id
    FROM canonical_station_overrides
    WHERE operation = 'merge'
      AND source_canonical_station_id = :'source'
      AND target_canonical_station_id = :'target'
    `,
      {
        source: stationB,
        target: stationA,
      },
    );
    assert.equal(overrideRows.length, 0);

    const walkLinks = await client.queryRows(
      `
    SELECT from_segment_id, to_segment_id, min_walk_minutes
    FROM qa_station_segment_links_v2
    WHERE (from_segment_id = :'segment_a' AND to_segment_id = :'segment_b')
       OR (from_segment_id = :'segment_b' AND to_segment_id = :'segment_a')
    ORDER BY from_segment_id, to_segment_id
    `,
      {
        segment_a: segmentA,
        segment_b: segmentB,
      },
    );
    assert.equal(walkLinks.length, 2);
    assert.ok(walkLinks.every((row) => row.min_walk_minutes === 4));

    const mergeCuratedRows = await client.queryRows(
      `
    SELECT curated_station_id, status, derived_operation
    FROM qa_curated_stations_v1
    WHERE primary_cluster_id = :'cluster_id'
      AND status = 'active'
    ORDER BY curated_station_id
    `,
      {
        cluster_id: cluster.cluster_id,
      },
    );
    assert.equal(mergeCuratedRows.length, 1);
    assert.equal(mergeCuratedRows[0].derived_operation, "merge");

    const mergeCuratedMembers = await client.queryRows(
      `
    SELECT canonical_station_id, member_role
    FROM qa_curated_station_members_v1
    WHERE curated_station_id = :'curated_station_id'
    ORDER BY canonical_station_id
    `,
      {
        curated_station_id: mergeCuratedRows[0].curated_station_id,
      },
    );
    assert.equal(mergeCuratedMembers.length, 2);
    assert.ok(
      mergeCuratedMembers.some((row) => row.canonical_station_id === stationA),
    );
    assert.ok(
      mergeCuratedMembers.some((row) => row.canonical_station_id === stationB),
    );

    const curatedListUrl = new URL(
      `http://localhost/api/qa/v2/curated-stations?cluster_id=${encodeURIComponent(cluster.cluster_id)}&status=active&limit=20`,
    );
    const curatedList = await getCuratedStationsV1(curatedListUrl);
    assert.equal(curatedList.length, 1);
    assert.equal(curatedList[0].derived_operation, "merge");
    assert.ok(Array.isArray(curatedList[0].members));
    assert.equal(curatedList[0].members.length, 2);

    const curatedDetail = await getCuratedStationDetailV1(
      curatedList[0].curated_station_id,
    );
    assert.equal(
      curatedDetail.curated_station_id,
      curatedList[0].curated_station_id,
    );
    assert.ok(Array.isArray(curatedDetail.members));
    assert.ok(Array.isArray(curatedDetail.field_provenance));
    assert.ok(Array.isArray(curatedDetail.lineage));

    const applied = await postReviewClusterDecisionV2(cluster.cluster_id, {
      operation: "split",
      selected_station_ids: [stationA, stationB],
      groups: [
        {
          group_label: "Main Hall",
          section_type: "main",
          section_name: "Main Hall",
          target_canonical_station_id: stationA,
          member_station_ids: [stationA],
          segment_action: {
            walk_links: [
              {
                from_segment_id: segmentA,
                to_segment_id: segmentB,
                min_walk_minutes: 5,
                bidirectional: true,
              },
            ],
          },
        },
        {
          group_label: "Bus Terminal",
          section_type: "bus",
          section_name: "Bus Terminal",
          target_canonical_station_id: stationB,
          member_station_ids: [stationB],
        },
      ],
      note: "Group modeling check via direct decision",
      requested_by: "integration_reviewer",
    });
    assert.equal(applied.ok, true);
    assert.ok(applied.decision_id);

    const groupedRows = await client.queryRows(
      `
    SELECT g.group_id, g.display_name, s.section_id, s.section_type
    FROM qa_station_groups_v2 g
    JOIN qa_station_group_sections_v2 s
      ON s.group_id = g.group_id
    WHERE g.cluster_id = :'cluster_id'
      AND g.is_active = true
    ORDER BY s.section_id
    `,
      {
        cluster_id: cluster.cluster_id,
      },
    );
    assert.ok(groupedRows.length >= 2);

    const splitCuratedRows = await client.queryRows(
      `
    SELECT status, derived_operation, COUNT(*)::integer AS items
    FROM qa_curated_stations_v1
    WHERE primary_cluster_id = :'cluster_id'
    GROUP BY status, derived_operation
    ORDER BY status, derived_operation
    `,
      {
        cluster_id: cluster.cluster_id,
      },
    );
    assert.ok(
      splitCuratedRows.some(
        (row) =>
          row.status === "active" &&
          row.derived_operation === "split" &&
          row.items >= 2,
      ),
    );
    assert.ok(
      splitCuratedRows.some(
        (row) =>
          row.status === "superseded" && row.derived_operation === "merge",
      ),
    );

    const splitCuratedListUrl = new URL(
      `http://localhost/api/qa/v2/curated-stations?cluster_id=${encodeURIComponent(cluster.cluster_id)}&status=active&limit=20`,
    );
    const splitCuratedList = await getCuratedStationsV1(splitCuratedListUrl);
    assert.ok(splitCuratedList.length >= 2);
    assert.ok(
      splitCuratedList.every((row) => row.derived_operation === "split"),
    );

    const sectionLinks = await client.queryRows(
      `
    SELECT l.from_section_id, l.to_section_id, l.min_walk_minutes
    FROM qa_station_group_section_links_v2 l
    JOIN qa_station_group_sections_v2 s
      ON s.section_id = l.from_section_id
    JOIN qa_station_groups_v2 g
      ON g.group_id = s.group_id
    WHERE g.cluster_id = :'cluster_id'
      AND g.is_active = true
    `,
      {
        cluster_id: cluster.cluster_id,
      },
    );
    assert.ok(Array.isArray(sectionLinks));
    assert.ok(
      sectionLinks.every(
        (row) => Number.parseInt(String(row.min_walk_minutes || 0), 10) >= 0,
      ),
    );
  },
);
