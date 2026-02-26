import { Client, Connection } from "@temporalio/client";
import { processStationEntityWorkflow } from "./workflows/processStationEntityWorkflow";

async function run() {
  try {
    // Connect to the default Server location
    const connection = await Connection.connect({ address: "localhost:7233" });

    // In production, instantiate the Client using a namespace
    const client = new Client({
      connection,
      // namespace: 'foo.bar', // connects to 'default' namespace if omitted
    });

    const handle = await client.workflow.start(processStationEntityWorkflow, {
      taskQueue: "entity-update",
      // In practice, use a meaningful business ID, like a station ID
      workflowId: `test-station-workflow-${Date.now()}`,
      args: [{ stationId: "8000105", name: "Frankfurt (Main) Hbf" }],
    });

    console.log(`Started workflow ${handle.workflowId}`);

    // Optional: wait for result
    const result = await handle.result();
    console.log("Workflow result:", result);
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

void run(); // NOSONAR - CommonJS entrypoint cannot use top-level await.
