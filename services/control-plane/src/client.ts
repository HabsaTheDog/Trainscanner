import { Client, Connection } from "@temporalio/client";
import { resolveTemporalAddress } from "./config";
import { logError, logInfo } from "./logging";
import {
  processStationEntityWorkflow,
  type StationEntityParams,
} from "./workflows/processStationEntityWorkflow";

export function buildConnectionOptions(env: NodeJS.ProcessEnv = process.env) {
  return {
    address: resolveTemporalAddress(env),
  };
}

export function buildWorkflowStartOptions(now = Date.now()): {
  taskQueue: string;
  workflowId: string;
  args: [StationEntityParams];
} {
  return {
    taskQueue: "entity-update",
    workflowId: `test-station-workflow-${now}`,
    args: [{ stationId: "8000105", name: "Frankfurt (Main) Hbf" }],
  };
}

export async function run() {
  const connection = await Connection.connect(buildConnectionOptions());
  const client = new Client({
    connection,
  });

  const handle = await client.workflow.start(
    processStationEntityWorkflow,
    buildWorkflowStartOptions(),
  );

  logInfo("Started workflow", { workflowId: handle.workflowId });
  const result = await handle.result();
  logInfo("Workflow result received", {
    workflowId: handle.workflowId,
    result,
  });
}

export async function runCli() {
  try {
    await run();
    return 0;
  } catch (err) {
    logError("Workflow client failed", {
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
