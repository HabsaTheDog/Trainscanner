#!/usr/bin/env node
const { buildGlobalMergeQueue } = require("../domains/global/service");
const { parsePipelineCliArgs, printCliError } = require("./pipeline-common");

async function run() {
  try {
    const parsed = parsePipelineCliArgs(process.argv.slice(2));
    await buildGlobalMergeQueue({
      rootDir: parsed.rootDir,
      runId: parsed.runId || "",
      args: parsed.passthroughArgs,
    });
  } catch (err) {
    printCliError(
      "build-global-merge-queue",
      err,
      "Build global merge queue failed",
    );
    process.exit(1);
  }
}

void run();
