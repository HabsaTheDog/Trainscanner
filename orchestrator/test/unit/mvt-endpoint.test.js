/**
 * test/unit/mvt-endpoint.test.js
 *
 * Tests for the MVT tile-serving domain module (src/domains/qa/mvt.js).
 * Uses a mock PostGIS client – no real DB connection required.
 */

const test = require("node:test");
const assert = require("node:assert/strict");

const {
  serveMvtTile,
  validateTileCoords,
} = require("../../src/domains/qa/mvt");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Build a mock client that records the SQL queries it was called with and
 * returns configurable results.
 */
function makeMockClient({ canonicalTileHex = "", stagingTileHex = "" } = {}) {
  const calls = [];

  let callIndex = 0;
  return {
    calls,
    queryOne: async (sql, params) => {
      calls.push({ sql: sql.trim(), params });
      // Alternate between canonical and staging tile responses
      const i = callIndex++;
      const hex = i === 0 ? canonicalTileHex : stagingTileHex;
      return { tile: hex || null };
    },
  };
}

// ---------------------------------------------------------------------------
// validateTileCoords
// ---------------------------------------------------------------------------

test("validateTileCoords accepts valid z/x/y", () => {
  assert.doesNotThrow(() => validateTileCoords(10, 549, 335)); // Berlin
  assert.doesNotThrow(() => validateTileCoords(0, 0, 0));
  assert.doesNotThrow(() => validateTileCoords(20, 0, 0));
});

test("validateTileCoords rejects z out of range", () => {
  assert.throws(() => validateTileCoords(-1, 0, 0), /z must be/i);
  assert.throws(() => validateTileCoords(21, 0, 0), /z must be/i);
});

test("validateTileCoords rejects x out of range for given z", () => {
  // At z=0 only tile 0/0 exists
  assert.throws(() => validateTileCoords(0, 1, 0), /x out of range/i);
});

test("validateTileCoords rejects negative y", () => {
  assert.throws(() => validateTileCoords(10, 0, -1), /y out of range/i);
});

// ---------------------------------------------------------------------------
// serveMvtTile
// ---------------------------------------------------------------------------

test("serveMvtTile returns a Buffer", async () => {
  const client = makeMockClient();
  const result = await serveMvtTile(client, { z: 10, x: 549, y: 335 });
  assert.ok(Buffer.isBuffer(result), "result should be a Buffer");
});

test("serveMvtTile calls queryOne twice (canonical + staging layers)", async () => {
  const client = makeMockClient();
  await serveMvtTile(client, { z: 10, x: 549, y: 335 });
  assert.equal(client.calls.length, 2, "should call queryOne exactly twice");
});

test("serveMvtTile SQL contains ST_AsMVT for canonical layer", async () => {
  const client = makeMockClient();
  await serveMvtTile(client, { z: 10, x: 549, y: 335 });
  const [firstCall] = client.calls;
  assert.ok(
    firstCall.sql.includes("ST_AsMVT"),
    "first query should use ST_AsMVT",
  );
  assert.ok(
    firstCall.sql.includes("'canonical'"),
    "first query should specify canonical layer",
  );
});

test("serveMvtTile SQL contains ST_TileEnvelope with correct z/x/y", async () => {
  const client = makeMockClient();
  await serveMvtTile(client, { z: 10, x: 549, y: 335 });
  for (const call of client.calls) {
    assert.ok(
      call.sql.includes("ST_TileEnvelope"),
      "should use ST_TileEnvelope",
    );
    assert.equal(call.params.z, 10, "z should be 10");
    assert.equal(call.params.x, 549, "x should be 549 (Berlin)");
    assert.equal(call.params.y, 335, "y should be 335 (Berlin)");
  }
});

test("serveMvtTile SQL contains ST_AsMVT for staging layer", async () => {
  const client = makeMockClient();
  await serveMvtTile(client, { z: 10, x: 549, y: 335 });
  const [, secondCall] = client.calls;
  assert.ok(
    secondCall.sql.includes("ST_AsMVT"),
    "second query should use ST_AsMVT",
  );
  assert.ok(
    secondCall.sql.includes("'staging'"),
    "second query should specify staging layer",
  );
});

test("serveMvtTile concatenates tile buffers from both layers", async () => {
  // Use two 4-byte hex sequences so we get concrete non-empty buffers
  const canonicalHex = "deadbeef";
  const stagingHex = "cafebabe";
  const client = makeMockClient({
    canonicalTileHex: canonicalHex,
    stagingTileHex: stagingHex,
  });
  const buf = await serveMvtTile(client, { z: 10, x: 549, y: 335 });
  const expected = Buffer.concat([
    Buffer.from(canonicalHex, "hex"),
    Buffer.from(stagingHex, "hex"),
  ]);
  assert.deepEqual(
    buf,
    expected,
    "should concatenate canonical and staging tile bytes",
  );
});

test("serveMvtTile returns empty Buffer when both layers are empty", async () => {
  const client = makeMockClient({ canonicalTileHex: "", stagingTileHex: "" });
  const buf = await serveMvtTile(client, { z: 10, x: 549, y: 335 });
  assert.equal(buf.length, 0, "empty tile should have length 0");
});

test("serveMvtTile throws on invalid tile coordinates", async () => {
  const client = makeMockClient();
  await assert.rejects(
    () => serveMvtTile(client, { z: 99, x: 0, y: 0 }),
    /z must be/i,
  );
  assert.equal(
    client.calls.length,
    0,
    "should not call DB if coords are invalid",
  );
});
