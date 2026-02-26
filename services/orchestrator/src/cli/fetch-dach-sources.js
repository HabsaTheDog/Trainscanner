#!/usr/bin/env node
const { fetchSources } = require("../domains/source-discovery/service");
const { parsePipelineCliArgs, printCliError } = require("./pipeline-common");

async function run() {
  try {
    const parsed = parsePipelineCliArgs(process.argv.slice(2));
    await fetchSources({
      rootDir: parsed.rootDir,
      runId: parsed.runId,
      args: parsed.passthroughArgs,
    });
  } catch (err) {
    printCliError("fetch-dach", err, "Fetch DACH sources failed");
    process.exit(1);
  }
}

void run(); // NOSONAR - CommonJS entrypoint cannot use top-level await.
