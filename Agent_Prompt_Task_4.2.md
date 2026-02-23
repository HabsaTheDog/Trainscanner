# Agent Prompt: Task 4.2 - Multilingual Alias & Entity Merging

**Context & Objective:**
You are an AI agent tasked with executing Phase 4, Task 4.2 of the Trainscanner V2 migration.
Your goal is to implement the actual Spatial Inference logic within the newly created Python AI Worker. The system needs to compute "Merge Confidence Scores" for novel stations coming from the NeTEx ingestion stream (`netex_stops_staging`).

**Current State Reference:**

- The Python AI worker scaffolding (`workers/python-ai/`) exists and is integrated with Temporal.
- The `calculate_merge_score` activity is currently returning dummy JSON data.
- The PostgreSQL database has geographic partitioning and tombstoning active.

**Your Instructions:**

1. **Connect to PostgreSQL/PostGIS:**
   - Update `workers/python-ai/activities.py` to include a PostgreSQL connection (using `asyncpg` or `psycopg2`).
   - The AI worker must be able to query both the `netex_stops_staging` table (for the novel entity properties) and the `canonical_stations` table (to find potential geographic matches).

2. **Querying Local OSM (Multi-Language Alias Check):**
   - The biggest issue with European rail data is border aliases (e.g., "Bozen" vs "Bolzano").
   - When calculating a match, the logic must explicitly query local OSM datasets or the PostGIS base spatial mapping to look for multi-language tags (`name:en`, `name:fr`, `name:de`, `name:it`).
   - If the incoming schedule name matches ANY of the OSM alias names for a geographically close canonical station, the confidence score should be extremely high.

3. **Implement the Local LLM Fallback (vLLM/Ollama):**
   - If exact ID or geographic alias matches fail, pass the raw string and coordinates to the local LLM running via the `LocalLLMWrapper`.
   - The LLM should be prompted with a strict JSON-schema instructing it to evaluate whether the `stop_name` from staging logically refers to the `canonical_name` from the database, returning a confidence score (0.0 to 1.0).

4. **Return the Confidence Score:**
   - The Python Temporal activity should return the final structured decision (`confidence` score, matched `canonical_station_id`, and whether to `merge` or `insert_new`) to the TypeScript Orchestrator.

**Deliverables:**

- The fully updated `activities.py` containing the PostGIS queries and local LLM prompting logic.
- Documentation in the README (if necessary) on any new environment variables (like `DATABASE_URL`).

Please begin your reasoning and implement the spatial inference matching logic.
