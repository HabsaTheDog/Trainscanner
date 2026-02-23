# Agent Prompt: Task 5.1 - Vector Tile Map & GraphQL Backend

**Context & Objective:**
You are an AI agent tasked with executing Phase 5, Task 5.1 of the Trainscanner V2 migration.
Your goal is to scaffold the backend infrastructure for the **QA Dashboard**. This dashboard allows human operators to visually verify AI decisions (especially low-confidence matches) before they're merged. To do this, the dashboard needs to quickly query the staging vs. canonical databases and render them on a dynamic map.

**Current State Reference:**

- The overall control flow relies on Temporal (Node.js/`control-plane`).
- You have an existing `frontend/` directory and a PostGIS Database (`db/`).
- The AI Worker (from Phase 4) is correctly analyzing nodes and updating database entries.

**Your Instructions:**

1. **Backend Integration (GraphQL or tRPC):**
   - In the Node.js backend (`orchestrator/` or a dedicated `qa-backend/` service), set up a GraphQL or tRPC router.
   - You must expose endpoints to fetch the "Low Confidence Queue" (e.g., fetching matches from PostgreSQL where AI confidence < 0.90).
   - Expose endpoints to explicitly _Approve_, _Reject_, or _Override_ an AI match, updating the PostGIS db state accordingly.

2. **Dynamic MVT (Map Vector Tiles) Servings:**
   - To render millions of stations on the frontend without crashing the browser, we need Vector Tiles.
   - Configure a dynamic MVT server to read directly from PostGIS. Standard approaches include:
     - Adding `pg_tileserv` via `docker-compose`.
     - Or creating a custom Node.js endpoint utilizing PostGIS's powerful `ST_AsMVT` and `ST_AsMVTGeom` functions to serve `.pbf` tiles dynamically from the `canonical_stations` and `netex_stops_staging` partitioned tables.

3. **Validation:**
   - Write standard tests ensuring the "Low Confidence Queue" endpoints return the expected format.
   - If using `ST_AsMVT`, provide a test script to prove tiles can be generated over a known bounding box (e.g., Berlin or Vienna).

**Deliverables:**

- The new/updated Node.js backend files exposing the QA operations.
- The Vector Tile serving infrastructure (whether via Docker `pg_tileserv` or raw SQL `ST_AsMVT` routes).
- Updated documentation instructing how to boot this new QA service.

Please begin your analysis and implement the Vector Tile & GraphQL Backend.
