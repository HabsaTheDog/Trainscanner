#!/usr/bin/env node
const { buildReviewQueue } = require("../domains/canonical/service");
const { parsePipelineCliArgs, printCliError } = require("./pipeline-common");

async function run() {
  try {
    const parsed = parsePipelineCliArgs(process.argv.slice(2));
    await buildReviewQueue({
      rootDir: parsed.rootDir,
      runId: parsed.runId,
      args: parsed.passthroughArgs,
    });
  } catch (err) {
    printCliError("build-review-queue", err, "Build review queue failed");
    process.exit(1);
  }
}

void run();
