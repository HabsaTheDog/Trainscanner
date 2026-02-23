import { proxyActivities } from "@temporalio/workflow";
// Only import the activity types from the activities file
import type * as activities from "../activities";

// Set up the activities with a retry policy
const { insertEntityToDb, queueRustParserJob, notifyPythonAiWorker } =
  proxyActivities<typeof activities>({
    startToCloseTimeout: "1 minute",
    retry: {
      initialInterval: "1s",
      backoffCoefficient: 2,
      maximumInterval: "30s",
      maximumAttempts: 5, // retry 5 times before failing
    },
  });

export interface StationEntityParams {
  stationId: string;
  name?: string;
}

export async function processStationEntityWorkflow(
  params: StationEntityParams,
): Promise<string> {
  // 1. Insert Entity to Database
  await insertEntityToDb(params);

  // 2. Queue the Rust SAX parser job to extract relevant data for this station
  const jobId = await queueRustParserJob(params.stationId);

  // 3. Notify the Python AI Worker that this entity is ready for scoring/metadata enhancement
  await notifyPythonAiWorker(params.stationId, jobId);

  return "Workflow completed successfully for station " + params.stationId;
}
