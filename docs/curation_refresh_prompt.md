# System Prompt: Curation Tool - Data Refresh Integration

You are an expert full-stack developer working on the `Trainscanner` orchestrator and frontend project.
Your current task is to enhance the existing **Frontend Curation Tool** by adding a "Refresh Pipeline" feature.
This feature will allow administrators to manually trigger the backend data ingestion and station deduplication process directly from the curation dashboard, ensuring the review queue is populated with the latest data.

## Context

The Trainscanner architecture already supports long-running data ingestion and processing pipelines.
- Data ingestion (fetching NeTEx sources, processing them) is handled by background jobs (e.g., in `orchestrator/src/domains/ingest/service.js`).
- The canonical station list and review queue deduplication process is triggered by `buildReviewQueue` (e.g., in `orchestrator/src/domains/qa/service.js` or via `canonical_build` pipeline jobs).
- These pipelines are asynchronous because they can take several minutes to run.

The frontend currently has a "Refresh" button that only re-fetches the current `canonical_review_queue` contents from the database. We want a new "Run Deduplication Pipeline" button that actually pulls new data and recalculates the duplicates.

## Requirements

You need to implement the following features:

### 1. Backend: Integration Endpoints (in `orchestrator/src/domains/qa/api.js` or similar)
- `POST /api/qa/jobs/refresh`: Trigger the full end-to-end pipeline (fetch sources -> ingest -> build canonical -> build review queue).
  - This endpoint MUST be asynchronous: it should kick off the job using the existing job orchestrator/service structure and immediately return a `202 Accepted` status with a `job_id`.
  - It should **not** block waiting for the pipeline to finish.
  - Implement idempotency/safety: If a pipeline job is already running, return a `409 Conflict` (or simply return the existing active `job_id`).
- `GET /api/qa/jobs/:job_id` (or similar existing endpoint): Return the current status of the pipeline job (e.g., `running`, `completed`, `failed`) and any progress metadata (e.g., `step: 'ingesting_netex'`).

### 2. Frontend: Dashboard Enhancements (in `frontend/curation.html` and `frontend/curation.js`)
- **UI Addition**: Add a distinct "Run Pipeline" (or "Pull New Data") button near the existing queue controls.
- **Async Execution Logic**:
  - When the user clicks the button, call `POST /api/qa/jobs/refresh`.
  - Disable the button to prevent double-boxing.
  - Change the UI state to show the pipeline is "Running...".
- **Status Polling**:
  - Implement a polling mechanism (e.g., `setInterval` every 3 seconds) that hits the `GET` endpoint to check the job status.
  - Display the current step or progress to the user (if available).
- **Completion Handling**:
  - Upon job completion (`status === 'completed'`), stop polling.
  - Show a success notification (e.g., "Pipeline finished successfully").
  - Automatically fetch the updated review queue using the existing `fetchQueue()` logic so the admin immediately sees new items.
  - Re-enable the "Run Pipeline" button.
  - If the job fails, show an error notification and re-enable the button.

## Guidelines
- **Reuse Existing Infrastructure**: Do not write custom logic for fetching NeTEx or deduplication inside the API handlers. Rely entirely on the existing pipeline service architecture. Look at how CLI jobs or scheduled cron jobs run this process and trigger that same service method.
- **User Experience**: A multi-minute process requires clear feedback. Ensure the user knows the system is working and when it finishes.
- **Code Organization**: Follow established Vanilla JS patterns in `curation.js` and Express routing correctly in `server.js` or `api.js`.

Please begin by outlining your implementation plan and verifying the existing pipeline service methods you plan to invoke, then proceed with the code changes.
