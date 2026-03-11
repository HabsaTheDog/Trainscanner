import { logInfo } from "../logging";
import type { StationEntityParams } from "../workflows/processStationEntityWorkflow";

// Stubs for the overarching entity update workflow

export async function insertEntityToDb(
  params: StationEntityParams,
): Promise<void> {
  logInfo("Inserting entity to DB", {
    stationId: params.stationId,
    name: params.name || "Unknown",
  });
  // Database logic goes here (e.g., PostGIS UPSERT)
  // An UPSERT (INSERT ... ON CONFLICT) guarantees idempotency
  await new Promise((resolve) => setTimeout(resolve, 500)); // Simulate async work
}

export async function queueRustParserJob(stationId: string): Promise<string> {
  logInfo("Queueing Rust parser job", { stationId });
  // Queue logic here, e.g. pushing a message to RabbitMQ/Redis or calling rust service directly
  await new Promise((resolve) => setTimeout(resolve, 300));
  return `job-${stationId}-${Date.now()}`;
}

export async function notifyPythonAiWorker(
  stationId: string,
  jobId: string,
): Promise<void> {
  logInfo("Notifying Python AI worker", { stationId, jobId });
  // Here we would call the Python AI scoring endpoint, using `fetch` or Axios
  await new Promise((resolve) => setTimeout(resolve, 200));
}
