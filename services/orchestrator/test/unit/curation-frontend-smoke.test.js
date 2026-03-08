const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const path = require("node:path");

test("curation frontend includes required component structure and runtime utilities", async () => {
  const repoRoot = path.resolve(__dirname, "../../../..");
  const html = await fs.readFile(
    path.join(repoRoot, "frontend", "curation.html"),
    "utf8",
  );
  const pageJsx = await fs.readFile(
    path.join(repoRoot, "frontend", "src", "curation-page.jsx"),
    "utf8",
  );
  const runtimeJs = await fs.readFile(
    path.join(repoRoot, "frontend", "src", "curation-page-runtime.js"),
    "utf8",
  );
  const entryJsx = await fs.readFile(
    path.join(repoRoot, "frontend", "src", "curation-page-entry.jsx"),
    "utf8",
  );

  // Entry point still loads via module import
  const hasModuleImport = /import\s+"\.\/src\/curation-page-entry\.jsx";/.test(
    html,
  );
  assert.ok(
    hasModuleImport,
    "curation.html must import curation-page-entry.jsx",
  );

  // Entry point renders CurationPage
  assert.match(entryJsx, /CurationPage/);
  assert.match(entryJsx, /createRoot/);

  // JSX has critical UI element IDs
  assert.match(pageJsx, /id="countryFilter"/);
  assert.match(pageJsx, /<option value="">All<\/option>/);
  assert.match(pageJsx, /<option value="DE">DE<\/option>/);
  assert.match(pageJsx, /<option value="AT">AT<\/option>/);
  assert.match(pageJsx, /<option value="CH">CH<\/option>/);
  assert.match(pageJsx, /id="statusFilter"/);
  assert.match(pageJsx, /id="toolMergeBtn"/);
  assert.match(pageJsx, /id="toolSplitBtn"/);
  assert.match(pageJsx, /id="toolGroupBtn"/);
  assert.match(pageJsx, /id="resolveConflictBtn"/);
  assert.match(pageJsx, /id="mapModeDefaultBtn"/);
  assert.match(pageJsx, /id="mapModeSatelliteBtn"/);
  assert.match(pageJsx, /id="editNoteInput"/);
  assert.match(pageJsx, /id="editMergeRenameInput"/);
  assert.match(pageJsx, /id="groupPairWalkList"/);
  assert.match(pageJsx, /id="selectedServiceIncoming"/);

  // Runtime uses shared graphql.js and global merge GraphQL API model
  assert.match(runtimeJs, /import.*graphqlQuery.*from.*"\.\/graphql"/);
  assert.match(runtimeJs, /globalClusters/);
  assert.match(runtimeJs, /total_count/);
  assert.match(runtimeJs, /items\s*\{/);
  assert.match(runtimeJs, /globalCluster/);
  assert.match(runtimeJs, /submitGlobalMergeDecision/);
  assert.match(runtimeJs, /selected_global_station_ids/);
  assert.match(runtimeJs, /fetchClusters/);
  assert.match(runtimeJs, /country:\s*filters\.country\s*\|\|\s*null/);
  assert.match(runtimeJs, /fetchClusterDetail/);
  assert.match(runtimeJs, /buildResolvePayload/);
  assert.match(runtimeJs, /resolveDefaultMapStyle/);
  assert.match(runtimeJs, /rename_targets/);

  // JSX uses React hooks (not imperative DOM manipulation)
  assert.match(pageJsx, /useState/);
  assert.match(pageJsx, /useEffect/);
  assert.match(pageJsx, /useCallback/);

  // Old patterns should NOT exist
  assert.doesNotMatch(runtimeJs, /document\.getElementById/);
  assert.doesNotMatch(runtimeJs, /document\.createElement/);
  assert.doesNotMatch(runtimeJs, /addEventListener/);
  assert.doesNotMatch(runtimeJs, /initCurationApp/);
});
