const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const path = require("node:path");

const { AppError } = require("../../src/core/errors");
const {
  isFreshStationCacheEntry,
  pruneCacheEntries,
  readZipStatOrThrow,
  resolveStaticAssetPath,
  sendError,
} = require("../../src/server");
const { mkTempDir } = require("../helpers/test-utils");

function createResponseRecorder() {
  return {
    statusCode: 0,
    headers: {},
    body: "",
    writeHead(statusCode, headers) {
      this.statusCode = statusCode;
      this.headers = headers;
    },
    end(body) {
      this.body = body ? String(body) : "";
    },
  };
}

test("resolveStaticAssetPath keeps requests inside the frontend root", () => {
  const resolved = resolveStaticAssetPath("/tmp/frontend", "/assets/app.js");
  assert.equal(resolved.forbidden, false);
  assert.equal(
    resolved.filePath,
    path.resolve("/tmp/frontend", "assets/app.js"),
  );

  const traversal = resolveStaticAssetPath(
    "/tmp/frontend",
    "/../../secret.txt",
  );
  assert.equal(traversal.forbidden, true);
});

test("isFreshStationCacheEntry validates signature and ttl", () => {
  const entry = {
    signature: "zip:1:2",
    cachedAt: 1000,
  };

  assert.equal(isFreshStationCacheEntry(entry, "zip:1:2", 500, 1200), true);
  assert.equal(isFreshStationCacheEntry(entry, "zip:9:9", 500, 1200), false);
  assert.equal(isFreshStationCacheEntry(entry, "zip:1:2", 100, 1200), false);
});

test("pruneCacheEntries drops stale entries before trimming least recently used", () => {
  const cache = new Map([
    [
      "stale",
      {
        cachedAt: 100,
        lastAccessAt: 100,
      },
    ],
    [
      "older",
      {
        cachedAt: 950,
        lastAccessAt: 200,
      },
    ],
    [
      "recent",
      {
        cachedAt: 980,
        lastAccessAt: 300,
      },
    ],
  ]);

  pruneCacheEntries(cache, {
    ttlMs: 100,
    maxEntries: 1,
    now: 1000,
  });

  assert.deepEqual(Array.from(cache.keys()), ["recent"]);
});

test("sendError serializes AppError payloads with optional details", () => {
  const response = createResponseRecorder();

  sendError(
    response,
    new AppError({
      code: "INVALID_REQUEST",
      message: "Bad input",
      details: { field: "country" },
    }),
    { includeDetails: true, extra: { requestId: "req-1" } },
  );

  assert.equal(response.statusCode, 400);
  assert.match(response.headers["content-type"] || "", /application\/json/);
  assert.deepEqual(JSON.parse(response.body), {
    error: "Bad input",
    errorCode: "INVALID_REQUEST",
    details: { field: "country" },
    requestId: "req-1",
  });
});

test("readZipStatOrThrow returns file stats and rejects missing artifacts", async () => {
  const tempDir = await mkTempDir("server-zip-stat-");
  const zipPath = path.join(tempDir, "fixture.zip");
  await fs.writeFile(zipPath, "zip-data", "utf8");

  const stat = await readZipStatOrThrow("fixture", zipPath);
  assert.equal(stat.isFile(), true);

  await assert.rejects(
    readZipStatOrThrow("missing", path.join(tempDir, "missing.zip")),
    (error) =>
      error instanceof AppError &&
      error.code === "PROFILE_ARTIFACT_MISSING" &&
      /missing\.zip/.test(error.message),
  );
});
