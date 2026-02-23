const test = require("node:test");
const assert = require("node:assert/strict");

const { isStrictIsoDate } = require("../../src/core/date");

test("isStrictIsoDate accepts valid calendar dates", () => {
  assert.equal(isStrictIsoDate("2026-02-23"), true);
  assert.equal(isStrictIsoDate("2024-02-29"), true);
});

test("isStrictIsoDate rejects invalid calendar dates", () => {
  assert.equal(isStrictIsoDate("2026-02-30"), false);
  assert.equal(isStrictIsoDate("2025-02-29"), false);
  assert.equal(isStrictIsoDate("2026-13-01"), false);
  assert.equal(isStrictIsoDate("2026/02/23"), false);
});
