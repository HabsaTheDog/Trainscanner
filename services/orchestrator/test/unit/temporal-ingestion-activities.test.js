const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");

const {
  createIngestionActivities,
} = require("../../src/temporal/activities/ingestion");

test("ingestion activities only pass --as-of to global model shell wrappers", async () => {
  const calls = [];
  const repoRoot = path.resolve(__dirname, "..", "..", "..", "..");
  const activities = createIngestionActivities(null, {
    rootDir: repoRoot,
    execFileAsync: async (file, args, options) => {
      calls.push({ file, args, options });
      return { stdout: "ok", stderr: "" };
    },
  });

  await activities.buildGlobalModel([
    "--country",
    "DE",
    "--source-id",
    "x",
    "--as-of",
    "2026-03-10",
  ]);

  assert.equal(calls.length, 2);
  assert.equal(calls[0].file, "bash");
  assert.equal(calls[1].file, "bash");
  assert.equal(
    calls[0].args[0],
    path.join(repoRoot, "scripts", "data", "build-global-stations.sh"),
  );
  assert.equal(
    calls[1].args[0],
    path.join(repoRoot, "scripts", "data", "build-global-merge-queue.sh"),
  );
  assert.deepEqual(calls[0].args.slice(1), ["--as-of", "2026-03-10"]);
  assert.deepEqual(calls[1].args.slice(1), ["--as-of", "2026-03-10"]);
  assert.equal(calls[0].options.cwd, path.join(repoRoot, "scripts", "data"));
  assert.equal(calls[1].options.cwd, path.join(repoRoot, "scripts", "data"));
});
