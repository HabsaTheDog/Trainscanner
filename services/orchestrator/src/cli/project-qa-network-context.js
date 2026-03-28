#!/usr/bin/env node
const {
  projectQaNetworkContext,
} = require("../domains/qa/pipeline-stage-service");
const { parsePipelineCliArgs, printCliError } = require("./pipeline-common");

async function runCli() {
  try {
    const parsed = parsePipelineCliArgs(process.argv.slice(2));
    await projectQaNetworkContext({
      rootDir: parsed.rootDir,
      runId: parsed.runId || "",
      args: parsed.passthroughArgs,
    });
  } catch (error) {
    printCliError(
      "project-qa-network-context",
      error,
      "QA network projection failed",
    );
    return 1;
  }

  return 0;
}

void runCli().then((exitCode) => {
  process.exitCode = exitCode;
});
