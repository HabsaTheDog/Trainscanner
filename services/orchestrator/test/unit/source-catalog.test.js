const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");

const {
  formatSourceDisplayLabel,
  resetSourceCatalogCache,
  resolveSourceLabels,
} = require("../../src/domains/source-discovery/catalog");

test("resolveSourceLabels maps configured source ids to readable feed labels", async () => {
  const tempRoot = await fs.mkdtemp(
    path.join(os.tmpdir(), "trainscanner-source-catalog-"),
  );
  await fs.mkdir(path.join(tempRoot, "config"), { recursive: true });
  await fs.mkdir(path.join(tempRoot, "scripts"), { recursive: true });
  await fs.mkdir(path.join(tempRoot, "services", "orchestrator"), {
    recursive: true,
  });
  await fs.writeFile(
    path.join(tempRoot, "config", "europe-data-sources.json"),
    JSON.stringify({
      schemaVersion: "2.0.0",
      sources: [
        {
          id: "at_oebb_mmtis_netex",
          country: "AT",
          provider: "OeBB-Infrastruktur AG",
          datasetName: "MMTIS NeTEx",
          format: "netex",
          accessType: "public",
          downloadMethod: "manual_redirect",
          downloadUrlOrEndpoint: "https://example.invalid/feed",
        },
      ],
    }),
  );

  resetSourceCatalogCache();

  assert.equal(
    formatSourceDisplayLabel({
      provider: "OeBB-Infrastruktur AG",
      datasetName: "MMTIS NeTEx",
    }),
    "OeBB-Infrastruktur AG - MMTIS NeTEx",
  );
  assert.deepEqual(
    resolveSourceLabels(
      ["at_oebb_mmtis_netex", "unknown_feed", "at_oebb_mmtis_netex"],
      { rootDir: tempRoot },
    ),
    ["OeBB-Infrastruktur AG - MMTIS NeTEx", "unknown_feed"],
  );
});
