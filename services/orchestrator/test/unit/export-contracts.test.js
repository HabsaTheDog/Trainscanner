const test = require("node:test");
const assert = require("node:assert/strict");

const {
  deterministicObjectHash,
} = require("../../src/domains/export/contracts");

test("deterministicObjectHash is stable for key order permutations", () => {
  const a = {
    profile: "p1",
    rowCounts: {
      stops: 2,
      trips: 3,
    },
    asOf: "2026-02-19",
  };

  const b = {
    asOf: "2026-02-19",
    rowCounts: {
      trips: 3,
      stops: 2,
    },
    profile: "p1",
  };

  assert.equal(deterministicObjectHash(a), deterministicObjectHash(b));
});
