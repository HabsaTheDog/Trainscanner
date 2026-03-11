#!/usr/bin/env node
const { buildGlobalStations } = require("../domains/global/service");
const { parsePipelineCliArgs, printCliError } = require("./pipeline-common");

async function runCli() {
  try {
    const parsed = parsePipelineCliArgs(process.argv.slice(2));
    await buildGlobalStations({
      rootDir: parsed.rootDir,
      runId: parsed.runId || "",
      args: parsed.passthroughArgs,
    });
  } catch (err) {
    printCliError("build-global-stations", err, "Build global stations failed");
    return 1;
  }
  return 0;
}

void (async () => {
  process.exitCode = await runCli();
})();
