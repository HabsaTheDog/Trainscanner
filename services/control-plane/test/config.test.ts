import assert from "node:assert/strict";
import test from "node:test";

import { resolveTemporalAddress } from "../src/config";

test("resolveTemporalAddress uses env override when present", () => {
  assert.equal(
    resolveTemporalAddress({ TEMPORAL_ADDRESS: " temporal:7233 " }),
    "temporal:7233",
  );
  assert.equal(resolveTemporalAddress({}), "localhost:7233");
});
