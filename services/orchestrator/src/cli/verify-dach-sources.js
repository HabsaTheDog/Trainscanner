#!/usr/bin/env node
const { verifySources } = require("../domains/source-discovery/service");
const { parsePipelineCliArgs, printCliError } = require("./pipeline-common");

function run() {
  return (async () => {
    try {
      const parsed = parsePipelineCliArgs(process.argv.slice(2));
      await verifySources({
        rootDir: parsed.rootDir,
        runId: parsed.runId,
        args: parsed.passthroughArgs,
      });
    } catch (err) {
      printCliError("verify-dach", err, "Verify DACH sources failed");
      process.exit(1);
    }
  })();
}

void run();
