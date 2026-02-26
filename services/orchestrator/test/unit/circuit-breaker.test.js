const test = require("node:test");
const assert = require("node:assert/strict");

const { createCircuitBreaker } = require("../../src/core/circuit-breaker");

test("circuit breaker opens after threshold failures", async () => {
  const breaker = createCircuitBreaker({
    name: "test-breaker",
    failureThreshold: 2,
    cooldownMs: 1000,
  });

  await assert.rejects(
    breaker.execute(async () => {
      throw new Error("first");
    }),
  );

  await assert.rejects(
    breaker.execute(async () => {
      throw new Error("second");
    }),
  );

  await assert.rejects(
    breaker.execute(async () => {
      return "never";
    }),
    (err) => {
      assert.equal(err.code, "CIRCUIT_OPEN");
      return true;
    },
  );
});
