#!/usr/bin/env node
const { ingestNetex } = require("../domains/ingest/service");
const { parsePipelineCliArgs, printCliError } = require("./pipeline-common");

function printUsage() {
  process.stdout.write("Usage: scripts/data/ingest-netex.sh [options]\n");
}

function run() {
  return (async () => {
    try {
      const parsed = parsePipelineCliArgs(process.argv.slice(2));
      if (
        parsed.passthroughArgs.includes("--help") ||
        parsed.passthroughArgs.includes("-h")
      ) {
        printUsage();
        return;
      }
      await ingestNetex({
        rootDir: parsed.rootDir,
        runId: parsed.runId,
        args: parsed.passthroughArgs,
      });
    } catch (err) {
      printCliError("ingest-netex", err, "Ingest NeTEx failed");
      process.exit(1);
    }
  })();
}

void run();
