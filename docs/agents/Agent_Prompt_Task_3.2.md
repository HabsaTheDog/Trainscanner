# Agent Prompt: Task 3.2 - Batching & Staggered Execution

**Context & Objective:**
You are an AI agent tasked with executing Phase 3, Task 3.2 of the Trainscanner V2 migration.
The goal is to prevent the new Temporal control plane from being overloaded. During major schedule updates (like the European "December Timetable Shock"), millions of stations and routes shift simultaneously. Spawning a separate Temporal workflow for every single entity immediately will overwhelm the Temporal database and crush the downstream Rust/Python workers.

**Current State Reference:**

- Phase 3, Task 1 is complete: the core Temporal worker and `processStationEntityWorkflow` are established in the `control-plane/` directory.
- Review the `processStationEntityWorkflow` to understand what an individual entity update looks like.

**Your Instructions:**

1. **Implement a Batching Ingestion Trigger:**
   - Create a new module or script in the `control-plane/` (e.g., `src/trigger.ts` or `src/batchScheduler.ts`) that acts as the entry point for massive data drops.
   - Instead of immediately firing `client.workflow.start()` for millions of entities in a `for` loop, you need to batch them.

2. **The "October Slow Burn" Logic (Staggered Execution):**
   - The trigger script should take a list of (dummy or database-queried) `canonical_station_id`s that need updating.
   - Implement logic to group these IDs into manageable chunks (e.g., batches of 1000).
   - Use Temporal's scheduled execution features or simple delay logic to stagger these batches over hours or days. For example, assign batch 1 to start now, batch 2 to start in 5 minutes, batch 3 in 10 minutes, etc. (This simulates the "slow burn" approach to stretch the compute load).

3. **Workflow Refactoring (Optional but Recommended):**
   - You may modify `processStationEntityWorkflow` (or create a new `processStationBatchWorkflow`) to accept an array of station IDs instead of a single ID, reducing the total workflow count in Temporal. If you do this, ensure the activities loop over the array safely and track partial failures so idempotency isn't lost.

4. **Integration & Validation:**
   - Write a test runner script (`npm run simulate:december-drop`) that simulates receiving 50,000 station updates and correctly chunks and staggers their workflow executions into the future without crashing the Temporal client.

**Deliverables:**

- The new TypeScript batching/scheduler module.
- Any necessary modifications to `processStationEntityWorkflow`.
- The simulation script added to `package.json`.

Please begin your analysis and implement the Batching & Staggered Execution layer.
