#!/usr/bin/env node
const { buildGlobalStations } = require("../domains/global/service");
const { parsePipelineCliArgs, printCliError } = require("./pipeline-common");

async function run() {
  try {
    const parsed = parsePipelineCliArgs(process.argv.slice(2));
    await buildGlobalStations({
      rootDir: parsed.rootDir,
      runId: parsed.runId || "",
      args: parsed.passthroughArgs,
    });
  } catch (err) {
    printCliError("build-global-stations", err, "Build global stations failed");
    process.exit(1);
  }
}

void run();
