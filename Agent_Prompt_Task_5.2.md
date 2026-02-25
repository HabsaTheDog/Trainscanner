# Agent Prompt: Task 5.2 - Bulk Approval QA Frontend

**Context & Objective:**
You are an AI agent tasked with executing Phase 5, Task 5.2 of the Trainscanner V2 migration.
Your goal is to build the QA Operator Interface frontend — allowing operators to review, visually inspect, and bulk-approve or reject AI station matches from a MapLibre map.

**Current State Reference:**
- **Backend is ready.** The GraphQL API is fully wired in `orchestrator/src/graphql/schema.js` and `resolvers.js`. The relevant queries/mutations to use are:
  -  `lowConfidenceQueue(limit, offset)` — fetches pending matches
  - `approveAiMatch(clusterId, evidenceId)` — approves a match
  - `rejectAiMatch(clusterId, evidenceId)` — rejects a match
  - `overrideAiMatch(clusterId, evidenceId, targetClusterId)` — overrides with a manual target
- **Frontend scaffold exists.** There is already a React/Vite project in the `frontend/` directory. An existing `CurationPage.jsx` handles some cluster views.
- **The frontend currently has a `curation.html` and `index.html` entry point.** Build on the existing structure.

**Your Instructions:**

1. **Low-Confidence Queue View:**
   - In `frontend/src/`, either update `CurationPage.jsx` or create a new `QAQueuePage.jsx`.
   - Fetch data from the `lowConfidenceQueue` GraphQL query.
   - Display a sortable table or list showing each low-confidence match (station name, AI confidence score, suggested action, cluster ID).

2. **MapLibre Map Integration:**
   - When an operator clicks on a queue item, show a **MapLibre GL** map panel side-by-side with the list.
   - Render the candidate stations as markers or circles with their coordinates so operators can visually verify the merge makes sense spatially.
   - Use Vector Tile endpoints (if `pg_tileserv` was configured in Task 5.1) or fallback to fetching individual GeoJSON points from the GraphQL backend.

3. **Bulk Region Approval:**
   - Implement a "Select Region" tool on the map (e.g., a bbox draw or polygon selection) that allows operators to select all visible low-confidence items at once and batch-approve or batch-reject them via the GraphQL mutations.

4. **Transfer Matrix Override (Top 100 Mega-Hubs):**
   - Add a separate "Manual Walk-Time Override" section for the top 100 EU mega-hubs (e.g., Paris CDG, Frankfurt, Amsterdam Centraal). 
   - This can be a simple, separate table that allows operators to directly enter or edit walking times (in minutes) between platforms for the hub, storing overrides in the database.

**Deliverables:**
- Updated or new React component(s) in `frontend/src/`.
- Any associated state management, GraphQL query helpers, and CSS updates in `frontend/src/styles.css`.
- Instructions on how to run the frontend locally (`npm run dev`).

Please begin your analysis of the existing `frontend/` codebase and implement the QA frontend.
