#!/usr/bin/env node
const { buildGlobalMergeQueue } = require("../domains/global/service");
const { parsePipelineCliArgs, printCliError } = require("./pipeline-common");

async function runCli() {
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
    return 1;
  }
  return 0;
}

async function main() {
  process.exitCode = await runCli();
}

void main();
