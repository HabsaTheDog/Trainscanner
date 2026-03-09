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
  assert.match(pageJsx, /id="contextualActionBar"/);
  assert.match(pageJsx, /id="mergeSelectedActionBtn"/);
  assert.match(pageJsx, /id="createGroupActionBtn"/);
  assert.match(pageJsx, /id="keepSeparateActionBtn"/);
  assert.match(pageJsx, /id="resolveClusterBtn"/);
  assert.match(pageJsx, /id="dismissClusterBtn"/);
  assert.match(pageJsx, /id="mapModeDefaultBtn"/);
  assert.match(pageJsx, /id="mapModeSatelliteBtn"/);
  assert.match(pageJsx, /id="mergeToolTabBtn"/);
  assert.match(pageJsx, /id="groupToolTabBtn"/);
  assert.match(pageJsx, /id="groupEditorPanel"/);
  assert.match(pageJsx, /id="groupTransferMatrix"/);
  assert.match(pageJsx, /id="saveStateIndicator"/);
  assert.match(pageJsx, /id="evidencePanel"/);
  assert.match(pageJsx, /id="historyPanel"/);
  assert.match(pageJsx, /formatEvidenceTypeLabel/);
  assert.match(pageJsx, /formatEvidenceStatusLabel/);
  assert.match(pageJsx, /curation-status-pill/);
  assert.match(pageJsx, /curation-context-chips/);
  assert.match(pageJsx, /type="checkbox"/);
  assert.match(pageJsx, /checked=\{selected\}/);
  assert.match(pageJsx, /data-station-id=\{item\.ref\}/);
  assert.match(pageJsx, /buildMarkerOverlapLayout/);
  assert.match(pageJsx, /curation-marker__selection-ring/);
  assert.match(pageJsx, /useReducer/);
  assert.match(pageJsx, /saveState/);

  // Runtime uses shared graphql.js and global merge GraphQL API model
  assert.match(runtimeJs, /import.*graphqlQuery.*from.*"\.\/graphql\.js"/);
  assert.match(runtimeJs, /globalClusters/);
  assert.match(runtimeJs, /total_count/);
  assert.match(runtimeJs, /items\s*\{/);
  assert.match(runtimeJs, /globalCluster/);
  assert.match(runtimeJs, /saveGlobalClusterWorkspace/);
  assert.match(runtimeJs, /undoGlobalClusterWorkspace/);
  assert.match(runtimeJs, /resetGlobalClusterWorkspace/);
  assert.match(runtimeJs, /reopenGlobalCluster/);
  assert.match(runtimeJs, /resolveGlobalCluster/);
  assert.match(runtimeJs, /workspace_version/);
  assert.match(runtimeJs, /effective_status/);
  assert.match(runtimeJs, /service_context/);
  assert.match(runtimeJs, /context_summary/);
  assert.match(runtimeJs, /coord_status/);
  assert.match(runtimeJs, /fetchClusters/);
  assert.match(runtimeJs, /country:\s*filters\.country\s*\|\|\s*null/);
  assert.match(runtimeJs, /fetchClusterDetail/);
  assert.match(runtimeJs, /details/);
  assert.match(runtimeJs, /raw_value/);
  assert.match(runtimeJs, /pair_summaries/);
  assert.match(runtimeJs, /evidence_summary/);
  assert.match(runtimeJs, /createMergeFromSelection/);
  assert.match(runtimeJs, /createGroupFromSelection/);
  assert.match(runtimeJs, /buildRailItems/);
  assert.match(runtimeJs, /resolveDefaultMapStyle/);
  assert.match(runtimeJs, /resolveSatelliteMapStyle/);
  assert.match(runtimeJs, /normalizeWorkspace/);

  // JSX uses React hooks
  assert.match(pageJsx, /useState/);
  assert.match(pageJsx, /useEffect/);
  assert.match(pageJsx, /useCallback/);
  assert.match(pageJsx, /map\.setStyle\(nextStyle\)/);
  assert.match(pageJsx, /dispatch\(\{ type: "map_mode", mode: "default" \}\)/);
  assert.match(
    pageJsx,
    /dispatch\(\{ type: "map_mode", mode: "satellite" \}\)/,
  );

  // Old tool-strip patterns should NOT exist
  assert.doesNotMatch(pageJsx, /id="toolMergeBtn"/);
  assert.doesNotMatch(pageJsx, /id="toolSplitBtn"/);
  assert.doesNotMatch(pageJsx, /id="toolGroupBtn"/);
  assert.doesNotMatch(runtimeJs, /document\.getElementById/);
  assert.doesNotMatch(runtimeJs, /document\.createElement/);
  assert.doesNotMatch(runtimeJs, /addEventListener/);
  assert.doesNotMatch(runtimeJs, /initCurationApp/);
});
