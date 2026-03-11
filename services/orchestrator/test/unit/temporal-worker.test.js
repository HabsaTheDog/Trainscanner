const test = require("node:test");
const assert = require("node:assert/strict");

const {
  buildWorkerConnectionOptions,
  buildWorkerOptions,
} = require("../../src/temporal/worker");

test("buildWorkerConnectionOptions resolves the Temporal address", () => {
  assert.deepEqual(
    buildWorkerConnectionOptions({ TEMPORAL_ADDRESS: "temporal:7233" }),
    { address: "temporal:7233" },
  );
  assert.deepEqual(buildWorkerConnectionOptions({}), {
    address: "localhost:7233",
  });
});

test("buildWorkerOptions wires queue, workflows, and activities", () => {
  const connection = {};
  const dbClient = {};
  const config = {};
  const options = buildWorkerOptions(connection, dbClient, config);

  assert.equal(options.connection, connection);
  assert.equal(options.namespace, "default");
  assert.equal(options.taskQueue, "review-pipeline");
  assert.match(options.workflowsPath, /workflows/);
  assert.equal(typeof options.activities, "object");
});
