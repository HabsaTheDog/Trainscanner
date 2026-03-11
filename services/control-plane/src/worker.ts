import { NativeConnection, Worker } from "@temporalio/worker";
import * as activities from "./activities";
import { resolveTemporalAddress } from "./config";
import { logError, logInfo } from "./logging";

export function buildNativeConnectionOptions(
  env: NodeJS.ProcessEnv = process.env,
) {
  return {
    address: resolveTemporalAddress(env),
  };
}

export function buildWorkerOptions(connection: NativeConnection) {
  return {
    connection,
    namespace: "default",
    taskQueue: "entity-update",
    workflowsPath: require.resolve("./workflows"),
    activities,
  };
}

export async function run() {
  const connection = await NativeConnection.connect(
    buildNativeConnectionOptions(),
  );

  const worker = await Worker.create(buildWorkerOptions(connection));

  logInfo("Temporal worker started", { taskQueue: "entity-update" });
  await worker.run();
}

export async function runCli() {
  try {
    await run();
    return 0;
  } catch (err) {
    logError("Temporal worker failed", {
      error: err instanceof Error ? err.message : String(err),
    });
    return 1;
  }
}

async function main() {
  process.exitCode = await runCli();
}

if (require.main === module) {
  void main();
}
