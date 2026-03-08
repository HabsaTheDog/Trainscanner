const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadRuntimeModule() {
  const repoRoot = path.resolve(__dirname, "../../../..");
  const runtimePath = path.join(
    repoRoot,
    "frontend",
    "src",
    "curation-page-runtime.js",
  );
  return import(pathToFileURL(runtimePath).href);
}

test("formatResultsLabel renders locale-formatted result totals", async () => {
  const { formatResultsLabel } = await loadRuntimeModule();
  assert.equal(formatResultsLabel(105758, "en-US"), "105,758 results");
});

test("formatResultsLabel normalizes missing totals to zero", async () => {
  const { formatResultsLabel } = await loadRuntimeModule();
  assert.equal(formatResultsLabel(undefined, "en-US"), "0 results");
});
