import { NativeConnection, Worker } from "@temporalio/worker";
import * as activities from "./activities";

async function run() {
  try {
    // Establish connection to Temporal server
    const connection = await NativeConnection.connect({
      address: process.env.TEMPORAL_ADDRESS || "localhost:7233",
    });

    const worker = await Worker.create({
      connection,
      namespace: "default",
      taskQueue: "entity-update",
      // In TS, workflows are loaded from the source file path
      workflowsPath: require.resolve("./workflows"),
      activities,
    });

    console.log("Temporal Worker started on taskQueue: entity-update");

    // Start accepting tasks on the `entity-update` queue
    await worker.run();
  } catch (err) {
    console.error("Terminal worker failed:", err);
    process.exit(1);
  }
}

void run();
