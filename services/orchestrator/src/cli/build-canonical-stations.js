#!/usr/bin/env node
const { buildCanonicalStations } = require("../domains/canonical/service");
const { parsePipelineCliArgs, printCliError } = require("./pipeline-common");

function run() {
  return (async () => {
    try {
      const parsed = parsePipelineCliArgs(process.argv.slice(2));
      await buildCanonicalStations({
        rootDir: parsed.rootDir,
        runId: parsed.runId,
        args: parsed.passthroughArgs,
      });
    } catch (err) {
      printCliError("build-canonical", err, "Build canonical stations failed");
      process.exit(1);
    }
  })();
}

void run();
