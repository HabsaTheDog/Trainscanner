import assert from "node:assert/strict";
import test from "node:test";

import {
  buildConnectionOptions,
  buildWorkflowStartOptions,
} from "../src/client";

test("buildConnectionOptions resolves the Temporal address", () => {
  assert.deepEqual(
    buildConnectionOptions({ TEMPORAL_ADDRESS: "temporal:7233" }),
    {
      address: "temporal:7233",
    },
  );
  assert.deepEqual(buildConnectionOptions({}), {
    address: "localhost:7233",
  });
});

test("buildWorkflowStartOptions creates stable workflow input", () => {
  const options = buildWorkflowStartOptions(1234567890);
  assert.equal(options.taskQueue, "entity-update");
  assert.equal(options.workflowId, "test-station-workflow-1234567890");
  assert.deepEqual(options.args, [
    { stationId: "8000105", name: "Frankfurt (Main) Hbf" },
  ]);
});
