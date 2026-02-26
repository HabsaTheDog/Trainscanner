const test = require("node:test");
const assert = require("node:assert/strict");

const {
  validateCompileGtfsRequest,
} = require("../../src/domains/export/compile-contracts");

test("validateCompileGtfsRequest applies defaults and normalizes aliases", () => {
  const result = validateCompileGtfsRequest({
    profile: "demo_profile",
    tier: "high_speed",
    country: "de",
  });

  assert.equal(result.profile, "demo_profile");
  assert.equal(result.tier, "high-speed");
  assert.equal(result.country, "DE");
  assert.match(result.asOf, /^\d{4}-\d{2}-\d{2}$/);
});

test("validateCompileGtfsRequest rejects invalid tier", () => {
  assert.throws(
    () =>
      validateCompileGtfsRequest({
        profile: "demo_profile",
        tier: "express",
        asOf: "2026-02-20",
      }),
    /Field 'tier' must be one of/,
  );
});
