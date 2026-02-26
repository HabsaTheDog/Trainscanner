const test = require("node:test");
const assert = require("node:assert/strict");

const {
  interpolateSqlParams,
  resolveConnectionConfig,
} = require("../../src/data/postgis/client");

test("resolveConnectionConfig resolves defaults and explicit fields", () => {
  const passwordField = ["CANONICAL_DB", "PASSWORD"].join("_");
  const credential = "unit-test-secret";
  const cfg = resolveConnectionConfig({
    rootDir: "/tmp/repo",
    env: {
      CANONICAL_DB_MODE: "direct",
      CANONICAL_DB_HOST: "db.local",
      CANONICAL_DB_PORT: "5433",
      CANONICAL_DB_USER: "u",
      CANONICAL_DB_NAME: "n",
      [passwordField]: credential,
    },
  });

  assert.equal(cfg.rootDir, "/tmp/repo");
  assert.equal(cfg.mode, "direct");
  assert.equal(cfg.host, "db.local");
  assert.equal(cfg.port, "5433");
  assert.equal(cfg.user, "u");
  assert.equal(cfg.database, "n");
  assert.equal(cfg.password, credential);
});

test("interpolateSqlParams replaces quoted placeholders and escapes string values", () => {
  const sql =
    "SELECT :'name' AS name, NULLIF(:'num', '')::integer AS parsed_num;";
  const out = interpolateSqlParams(sql, {
    name: "O'Reilly",
    num: "42",
  });

  assert.deepEqual(out, {
    query: "SELECT $1 AS name, NULLIF($2, '')::integer AS parsed_num;",
    values: ["O'Reilly", "42"],
  });
});

test("interpolateSqlParams throws when placeholders are missing", () => {
  assert.throws(
    () => interpolateSqlParams("SELECT :'missing'::text;", {}),
    /Missing SQL params/,
  );
});
