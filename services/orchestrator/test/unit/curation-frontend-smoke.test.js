const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const path = require("node:path");

test("curation frontend includes staged conflict editor and map mode hooks", async () => {
  const repoRoot = path.resolve(__dirname, "../../../..");
  const html = await fs.readFile(
    path.join(repoRoot, "frontend", "curation.html"),
    "utf8",
  );
  const pageJsx = await fs.readFile(
    path.join(repoRoot, "frontend", "src", "curation-page.jsx"),
    "utf8",
  );
  const logicJs = await fs.readFile(
    path.join(repoRoot, "frontend", "src", "curation-page-runtime.js"),
    "utf8",
  );

  const hasLegacyScriptTag = /src="\/src\/curation-page-entry\.jsx"/.test(html);
  const hasModuleImport = /import\s+"\.\/src\/curation-page-entry\.jsx";/.test(
    html,
  );
  assert.ok(hasLegacyScriptTag || hasModuleImport);

  assert.match(pageJsx, /id="mapModeDefaultBtn"/);
  assert.match(pageJsx, /id="mapModeSatelliteBtn"/);
  assert.match(pageJsx, /id="toolMergeBtn"/);
  assert.match(pageJsx, /id="toolSplitBtn"/);
  assert.match(pageJsx, /id="toolGroupBtn"/);
  assert.match(pageJsx, /id="resolveConflictBtn"/);
  assert.match(pageJsx, /id="scopeTagFilter"/);
  assert.match(pageJsx, /id="groupPairWalkList"/);
  assert.match(pageJsx, /id="selectedServiceIncoming"/);
  assert.match(pageJsx, /id="editPayloadPreview"/);
  assert.doesNotMatch(pageJsx, /Linked Queue Items/);
  assert.doesNotMatch(pageJsx, /Keep Separate/);

  assert.match(logicJs, /\/api\/qa\/clusters/);
  assert.match(logicJs, /\/api\/qa\/curated-stations/);
  assert.match(logicJs, /renderCuratedCandidatesInline/);
  assert.match(logicJs, /renderDraftMergesInline/);
  assert.match(logicJs, /MAP_MODE_SESSION_KEY/);
  assert.match(logicJs, /sessionStorage\.setItem\(MAP_MODE_SESSION_KEY/);
  assert.match(logicJs, /resolveConflict/);
  assert.match(logicJs, /rename_targets/);
  assert.doesNotMatch(logicJs, /keep_separate/);
  assert.doesNotMatch(logicJs, /queue_items/);
});
