#!/usr/bin/env node
const { ingestNetex } = require("../domains/ingest/service");
const { parsePipelineCliArgs, printCliError } = require("./pipeline-common");

async function run() {
  try {
    const parsed = parsePipelineCliArgs(process.argv.slice(2));
    await ingestNetex({
      rootDir: parsed.rootDir,
      runId: parsed.runId,
      args: parsed.passthroughArgs,
    });
  } catch (err) {
    printCliError("ingest-netex", err, "Ingest NeTEx failed");
    process.exit(1);
  }
}

void run();
