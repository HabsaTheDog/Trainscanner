#!/usr/bin/env node
const { reportReviewQueue } = require("../domains/qa/service");
const { parsePipelineCliArgs, printCliError } = require("./pipeline-common");

function run() {
  return (async () => {
    try {
      const parsed = parsePipelineCliArgs(process.argv.slice(2));
      await reportReviewQueue({
        rootDir: parsed.rootDir,
        runId: parsed.runId,
        args: parsed.passthroughArgs,
      });
    } catch (err) {
      printCliError("report-review-queue", err, "Report review queue failed");
      process.exit(1);
    }
  })();
}

void run();
