import assert from "node:assert/strict";
import test from "node:test";

import {
  buildNativeConnectionOptions,
  buildWorkerOptions,
} from "../src/worker";

test("buildNativeConnectionOptions resolves the Temporal address", () => {
  assert.deepEqual(
    buildNativeConnectionOptions({ TEMPORAL_ADDRESS: "temporal:7233" }),
    { address: "temporal:7233" },
  );
  assert.deepEqual(buildNativeConnectionOptions({}), {
    address: "localhost:7233",
  });
});

test("buildWorkerOptions returns the expected queue and namespace", () => {
  const connection = {} as never;
  const options = buildWorkerOptions(connection);

  assert.equal(options.connection, connection);
  assert.equal(options.namespace, "default");
  assert.equal(options.taskQueue, "entity-update");
  assert.match(options.workflowsPath, /workflows/);
  assert.equal(typeof options.activities, "object");
});
