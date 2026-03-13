#!/usr/bin/env node
const { refreshExternalReferences } = require("../domains/reference/service");
const { parsePipelineCliArgs, printCliError } = require("./pipeline-common");

async function runCli() {
  try {
    const parsed = parsePipelineCliArgs(process.argv.slice(2));
    await refreshExternalReferences({
      rootDir: parsed.rootDir,
      runId: parsed.runId || "",
      args: parsed.passthroughArgs,
    });
  } catch (error) {
    printCliError(
      "refresh-external-references",
      error,
      "External reference refresh failed",
    );
    return 1;
  }

  return 0;
}

function startCli() {
  void runCli().then((exitCode) => {
    process.exitCode = exitCode;
  });
}

startCli();
