const test = require("node:test");
const assert = require("node:assert/strict");

const {
  interpolateSqlParams,
  parseRowsFromJsonOutput,
  resolveConnectionConfig,
} = require("../../src/data/postgis/client");

test("parseRowsFromJsonOutput parses last json line", () => {
  const rows = parseRowsFromJsonOutput('NOTICE something\n[{"a":1},{"a":2}]\n');
  assert.equal(rows.length, 2);
  assert.equal(rows[0].a, 1);
});

test("parseRowsFromJsonOutput parses multiline json arrays", () => {
  const rows = parseRowsFromJsonOutput('[{"a":1},\n {"a":2}]\n');
  assert.equal(rows.length, 2);
  assert.equal(rows[1].a, 2);
});

test("resolveConnectionConfig resolves defaults and explicit fields", () => {
  const cfg = resolveConnectionConfig({
    rootDir: "/tmp/repo",
    env: {
      CANONICAL_DB_MODE: "direct",
      CANONICAL_DB_HOST: "db.local",
      CANONICAL_DB_PORT: "5433",
      CANONICAL_DB_USER: "u",
      CANONICAL_DB_NAME: "n",
      CANONICAL_DB_PASSWORD: "p",
    },
  });

  assert.equal(cfg.rootDir, "/tmp/repo");
  assert.equal(cfg.mode, "direct");
  assert.equal(cfg.host, "db.local");
  assert.equal(cfg.port, "5433");
  assert.equal(cfg.user, "u");
  assert.equal(cfg.database, "n");
  assert.equal(cfg.password, "p");
});

test("interpolateSqlParams replaces quoted placeholders and escapes string values", () => {
  const sql =
    "SELECT :'name' AS name, NULLIF(:'num', '')::integer AS parsed_num;";
  const out = interpolateSqlParams(sql, {
    name: "O'Reilly",
    num: "42",
  });

  assert.equal(
    out,
    "SELECT 'O''Reilly' AS name, NULLIF('42', '')::integer AS parsed_num;",
  );
});

test("interpolateSqlParams throws when placeholders are missing", () => {
  assert.throws(
    () => interpolateSqlParams("SELECT :'missing'::text;", {}),
    /Missing SQL params/,
  );
});
