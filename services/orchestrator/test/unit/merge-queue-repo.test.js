const test = require("node:test");
const assert = require("node:assert/strict");

const {
  BUILD_MERGE_QUEUE_SQL,
  DEFAULT_MERGE_QUEUE_MAINTENANCE_WORK_MEM,
  DEFAULT_MERGE_QUEUE_MAX_PARALLEL_WORKERS,
  DEFAULT_MERGE_QUEUE_WORK_MEM,
  createMergeQueueRepo,
  extractInfoFromNotice,
  extractPhaseFromNotice,
} = require("../../src/data/postgis/repositories/merge-queue-repo");

test("extractPhaseFromNotice returns merge queue phases", () => {
  assert.equal(
    extractPhaseFromNotice({
      message: "merge_queue_phase:building_pair_seeds",
    }),
    "building_pair_seeds",
  );
  assert.equal(
    extractPhaseFromNotice("merge_queue_phase:finalizing"),
    "finalizing",
  );
  assert.equal(extractPhaseFromNotice({ message: "other notice" }), "");
});

test("extractInfoFromNotice returns merge queue info payloads", () => {
  assert.deepEqual(
    extractInfoFromNotice({
      message: "merge_queue_info:pair_seeds_total=42",
    }),
    {
      key: "pair_seeds_total",
      value: "42",
    },
  );
  assert.equal(extractInfoFromNotice("merge_queue_phase:finalizing"), null);
});

test("build sql preserves phase markers and ported evidence primitives", () => {
  assert.match(
    BUILD_MERGE_QUEUE_SQL,
    new RegExp(
      `SET LOCAL max_parallel_workers_per_gather = ${DEFAULT_MERGE_QUEUE_MAX_PARALLEL_WORKERS};`,
    ),
  );
  assert.match(
    BUILD_MERGE_QUEUE_SQL,
    new RegExp(`SET LOCAL work_mem = '${DEFAULT_MERGE_QUEUE_WORK_MEM}';`),
  );
  assert.match(
    BUILD_MERGE_QUEUE_SQL,
    new RegExp(
      `SET LOCAL maintenance_work_mem = '${DEFAULT_MERGE_QUEUE_MAINTENANCE_WORK_MEM}';`,
    ),
  );

  for (const phase of [
    "initializing",
    "building_station_context",
    "building_pair_seeds",
    "building_components",
    "writing_pair_ledger",
    "writing_clusters",
    "writing_candidates",
    "writing_evidence",
    "finalizing",
  ]) {
    assert.match(
      BUILD_MERGE_QUEUE_SQL,
      new RegExp(`merge_queue_phase:${phase}`),
    );
  }

  for (const infoNotice of [
    "pair_seeds_exact_name",
    "pair_seeds_loose_name_geo",
    "pair_seeds_missing_coords",
    "pair_seeds_shared_route",
    "pair_seeds_shared_adjacent",
    "pair_seeds_total",
  ]) {
    assert.match(
      BUILD_MERGE_QUEUE_SQL,
      new RegExp(`merge_queue_info:${infoNotice}=`),
    );
  }

  assert.doesNotMatch(
    BUILD_MERGE_QUEUE_SQL,
    /ADD COLUMN IF NOT EXISTS status text/,
  );
  assert.doesNotMatch(
    BUILD_MERGE_QUEUE_SQL,
    /ADD COLUMN IF NOT EXISTS raw_value numeric/,
  );
  assert.doesNotMatch(
    BUILD_MERGE_QUEUE_SQL,
    /CREATE OR REPLACE FUNCTION qa_loose_station_name/,
  );
  assert.match(BUILD_MERGE_QUEUE_SQL, /_scope_stations/);
  assert.match(
    BUILD_MERGE_QUEUE_SQL,
    /WHERE gs\.is_active = true\s+AND EXISTS \(\s+SELECT 1\s+FROM provider_global_station_mappings m\s+WHERE m\.global_station_id = gs\.global_station_id\s+AND m\.is_active = true\s+\)/,
  );
  assert.match(BUILD_MERGE_QUEUE_SQL, /_pair_seed_reasons/);
  assert.match(BUILD_MERGE_QUEUE_SQL, /_station_context_geo_tile_idx/);
  assert.match(BUILD_MERGE_QUEUE_SQL, /primary_rare_token/);
  assert.match(BUILD_MERGE_QUEUE_SQL, /pair_seeds_shared_route/);
  assert.match(BUILD_MERGE_QUEUE_SQL, /pair_seeds_shared_adjacent/);
  assert.match(BUILD_MERGE_QUEUE_SQL, /_cluster_station_context/);
  assert.match(BUILD_MERGE_QUEUE_SQL, /INSERT INTO qa_merge_eligible_pairs/);
  assert.match(BUILD_MERGE_QUEUE_SQL, /eligiblePairs/);
  assert.match(BUILD_MERGE_QUEUE_SQL, /name_exact/);
  assert.match(BUILD_MERGE_QUEUE_SQL, /generic_name_penalty/);
});

test("rebuildMergeQueue ensures evidence columns before running the script", async () => {
  const calls = [];
  const infos = [];
  const client = {
    async runSql(sql) {
      calls.push(String(sql).trim().replaceAll(/\s+/g, " "));
      return { rows: [] };
    },
    async runScript(_sql, _params, options) {
      if (options?.onNotice) {
        options.onNotice({ message: "merge_queue_info:pair_seeds_total=7" });
      }
      return {
        stdout:
          '{"scopeCountry":"","scopeAsOf":"","scopeTag":"latest","clusters":0,"candidates":0,"evidence":0,"eligiblePairs":0}',
        stderr: "",
      };
    },
  };

  const repo = createMergeQueueRepo(client);
  const result = await repo.rebuildMergeQueue(
    {},
    {
      onInfo(info) {
        infos.push(info);
      },
    },
  );

  assert.equal(result.scopeTag, "latest");
  assert.equal(calls.length, 3);
  assert.match(calls[0], /ADD COLUMN IF NOT EXISTS status text/);
  assert.match(calls[1], /ADD COLUMN IF NOT EXISTS raw_value numeric/);
  assert.match(calls[2], /CREATE TABLE IF NOT EXISTS qa_merge_eligible_pairs/);
  assert.deepEqual(infos, [{ key: "pair_seeds_total", value: "7" }]);
});
