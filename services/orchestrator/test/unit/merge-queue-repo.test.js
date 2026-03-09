const test = require("node:test");
const assert = require("node:assert/strict");

const {
  BUILD_MERGE_QUEUE_SQL,
  createMergeQueueRepo,
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

test("build sql preserves phase markers and ported evidence primitives", () => {
  for (const phase of [
    "initializing",
    "building_station_context",
    "building_pair_seeds",
    "building_components",
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

  assert.match(BUILD_MERGE_QUEUE_SQL, /ADD COLUMN IF NOT EXISTS status text/);
  assert.match(
    BUILD_MERGE_QUEUE_SQL,
    /ADD COLUMN IF NOT EXISTS raw_value numeric/,
  );
  assert.match(BUILD_MERGE_QUEUE_SQL, /qa_loose_station_name/);
  assert.match(BUILD_MERGE_QUEUE_SQL, /_pair_seeds/);
  assert.match(BUILD_MERGE_QUEUE_SQL, /name_exact/);
  assert.match(BUILD_MERGE_QUEUE_SQL, /generic_name_penalty/);
});

test("rebuildMergeQueue ensures evidence columns before running the script", async () => {
  const calls = [];
  const client = {
    async runSql(sql) {
      calls.push(String(sql).trim().replace(/\s+/g, " "));
      return { rows: [] };
    },
    async runScript() {
      return {
        stdout:
          '{"scopeCountry":"","scopeAsOf":"","scopeTag":"latest","clusters":0,"candidates":0,"evidence":0}',
        stderr: "",
      };
    },
  };

  const repo = createMergeQueueRepo(client);
  const result = await repo.rebuildMergeQueue({});

  assert.equal(result.scopeTag, "latest");
  assert.equal(calls.length, 2);
  assert.match(calls[0], /ADD COLUMN IF NOT EXISTS status text/);
  assert.match(calls[1], /ADD COLUMN IF NOT EXISTS raw_value numeric/);
});
