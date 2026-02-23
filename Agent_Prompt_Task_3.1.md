# Agent Prompt: Task 3.1 - Temporal.io Setup

**Context & Objective:**
You are an AI agent tasked with executing Phase 3, Task 3.1 of the Trainscanner V2 migration.
Your goal is to replace the fragile, native Node.js orchestrator with a robust distributed system using Temporal.io as the new Control Plane. This will allow for distributed syncing, idempotency, staggered retries, and test coordination.

**Current State Reference:**

- The Ingestion pipeline (Rust XML streaming & Data Normalization) from Phase 2 is complete.
- We have the `orchestrator/` folder built in Node.js/TypeScript. Inside `orchestrator/src/temporal/`, there is likely some placeholder boilerplate.
- The `control-plane/` folder might also exist and have a `package.json`.
- The Rust Ingestion worker already has a `[dependencies.temporal-sdk]` and basic runtime setup configured from previous steps.

**Your Instructions:**

1. **Environment & Worker Registration:**
   - Establish a clean Temporal.io setup. Decide if the main Temporal orchestration logic should live in `orchestrator/src/temporal` or the `control-plane/` directory, and consolidate as necessary.
   - Define the main Temporal Client connection.
   - Register a primary Node.js/TypeScript worker that will manage the overarching "Entity Update" workflows.

2. **Define Idempotent Workflows:**
   - Create a core workflow (e.g., `processStationEntityWorkflow`).
   - The workflow must be strictly idempotent: if an entity update fails midway through, Temporal must be able to resume or retry it without creating duplicate or corrupt data in the PostGIS database.

3. **Activity Interfacing:**
   - Define the TypeScript Activity stubs that will map to the actual work (e.g., calling the database to insert an entity, placing jobs into the Rust SAX parser queue, or notifying the Python AI worker).
   - Ensure you leverage Temporal's robust retry mechanisms for all network/DB-bound activities.

4. **Integration & Validation:**
   - Provide a `docker-compose.yml` snippet or instructions on how to spin up a local Temporal development cluster (`temporalite` or the official docker image) if it isn't already present.
   - Write a simple script (e.g., `npm run start:worker`) to boot your Temporal worker, and another script `npm run trigger:test` to manually queue a test workflow to prove it works.

**Deliverables:**

- Code implementation for the core Temporal workflows and TypeScript worker.
- Updated `package.json` with Temporal dependencies (`@temporalio/client`, `@temporalio/worker`, `@temporalio/workflow`, `@temporalio/activity`).
- Instructions on how to run local tests.

Please begin your analysis and implement the Temporal.io Setup.
