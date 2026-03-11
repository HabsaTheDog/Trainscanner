import assert from "node:assert/strict";
import test from "node:test";

import { formatLogLine } from "../src/logging";

test("formatLogLine emits structured control-plane log entries", () => {
  const line = formatLogLine("info", "worker started", {
    taskQueue: "entity-update",
  });
  const parsed = JSON.parse(line.trim());

  assert.equal(parsed.level, "info");
  assert.equal(parsed.service, "control-plane");
  assert.equal(parsed.message, "worker started");
  assert.deepEqual(parsed.context, { taskQueue: "entity-update" });
  assert.equal(typeof parsed.ts, "string");
});
