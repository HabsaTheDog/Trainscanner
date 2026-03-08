#!/usr/bin/env node
const { fetchSources } = require("../domains/source-discovery/service");
const { parsePipelineCliArgs, printCliError } = require("./pipeline-common");

function run() {
  return (async () => {
    try {
      const parsed = parsePipelineCliArgs(process.argv.slice(2));
      await fetchSources({
        rootDir: parsed.rootDir,
        runId: parsed.runId,
        args: parsed.passthroughArgs,
      });
    } catch (err) {
      printCliError("fetch-sources", err, "Fetch source datasets failed");
      process.exit(1);
    }
  })();
}

void run();
