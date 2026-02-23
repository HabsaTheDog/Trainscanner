# Trainscanner V2: European Scale Agent Migration Plan

This document breaks down the "Project Scope" into sequential, isolated tasks. You can assign these tasks to AI agents one by one. Each phase builds upon the previous one to safely migrate the database and architecture to a scalable, Europe-wide solution.

## Phase 1: Storage Foundation & Spatial Seeding
**Goal:** Establish a robust, scalable spatial database before ingesting heavy data.

- [ ] **Task 1.1: PostGIS & Geographic Partitioning Setup**
  - **Agent Prompt:** "Migrate the current database schema to use PostGIS with explicit Geographic Table Partitioning (e.g., bounding box grids). Ensure the setup supports multi-node scaling (like AWS Aurora/Citus) to prevent spatial locking during global queries."
- [ ] **Task 1.2: Implement Tombstoning & Stable IDs**
  - **Agent Prompt:** "Update the database schema and models to support 'Tombstoning' (soft-deletions) for canonical IDs to prevent unannounced station deletions from breaking historical routing."
- [ ] **Task 1.3: Seed Base Spatial Data (Cold Start Mitigation)**
  - **Agent Prompt:** "Create a script to pre-seed the PostGIS database with known, stable datasets (UIC codes, OpenRailwayMap stations). Ensure strict legal isolation from proprietary schedule DBs to avoid ODbL license virality."

## Phase 2: Ingestion & Normalization Engine
**Goal:** Handle massive, malformed XML datasets without crashing.

- [ ] **Task 2.1: Rust SAX Stream Workers**
  - **Agent Prompt:** "Implement a dedicated Rust worker utilizing SAX streams to parse massive NeTEx/GTFS datasets. This must be a memory-efficient, event-driven parser that avoids loading entire XML files into memory."
- [ ] **Task 2.2: Data Normalization Schema**
  - **Agent Prompt:** "Introduce an abstract 'Data Normalization Schema' step post-parsing. It must strictly map all times to Vienna (CET/CEST), flag anomalies, and handle the 'Provider Weighting' hierarchy to auto-trust local infrastructure owners for cross-border data conflicts."

## Phase 3: Control Plane & Orchestration
**Goal:** Replace the fragile Node.js orchestrator with a robust distributed system.

- [ ] **Task 3.1: Temporal.io Setup**
  - **Agent Prompt:** "Set up Temporal.io to act as the new Control Plane. Implement the core worker registration and create idempotent workflows for entity updates."
- [ ] **Task 3.2: Batching & Staggered Execution**
  - **Agent Prompt:** "Refactor the ingestion trigger to heavily batch entity updates before passing them to Temporal. Implement an 'October slow burn' logic to stretch compute load, preventing workflow bloat in the orchestration DB."

## Phase 4: Local AI & Spatial Inference
**Goal:** Process novel stations efficiently using local context, avoiding live API latency.

- [ ] **Task 4.1: Local vLLM Worker Setup**
  - **Agent Prompt:** "Create a Python/FastAPI worker node designed to run quantized local LLMs (vLLM). Connect it to the Temporal control plane via message queues."
- [ ] **Task 4.2: Multilingual Alias & Entity Merging**
  - **Agent Prompt:** "Implement the AI logic to compute 'Merge Confidence Scores' for novel stations. The AI must explicitly query the local OSM database for multi-language tags (`name:en`, `name:fr`) to prevent duplication in border regions."

## Phase 5: QA & Operator Resolutions
**Goal:** Enable humans to efficiently verify the AI's low-confidence matches.

- [ ] **Task 5.1: Vector Tile Map & GraphQL Backend**
  - **Agent Prompt:** "Set up a QA Dashboard backend using GraphQL/tRPC and configure dynamic vector tile serving (e.g., pg_tileserv) from our PostGIS database."
- [ ] **Task 5.2: Bulk Approval & Transfer Matrix**
  - **Agent Prompt:** "Build the QA frontend in React/MapLibre. Implement pattern-based, region-wide bulk approvals. Add a 'Transfer Matrix' override system prioritizing manual walk-time curation for the top 100 EU mega-hubs."

## Phase 6: Testing Grid & Artifact Compilation
**Goal:** Validate the final GTFS feeds dynamically without cost-prohibitive compute spikes.

- [ ] **Task 6.1: Hierarchical Artifact Compilation**
  - **Agent Prompt:** "Create the compilation pipeline that emits optimized GTFS artifacts. It must split networks into hierarchical tiers (High-Speed, Regional, Local) to prevent routing engine memory bloat."
- [ ] **Task 6.2: Ephemeral K8s MOTIS Testing Grid**
  - **Agent Prompt:** "Implement a Kubernetes orchestration script to dynamically spin up ephemeral MOTIS testing pods. Create two test modes: dynamic micro-graphs (affected bbox + padding) for local updates, and isolated sparse macro-graphs for high-speed networks."

## Suggested Agent Workflow:
1. Provide the agent with the `Project Scope.md` file for context context.
2. Copy/Paste the specific **Agent Prompt** from the checklist above.
3. Once the agent completes a task, review the code, run standard tests, and merge before assigning the next task on the list.
