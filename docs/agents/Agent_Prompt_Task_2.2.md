# Agent Prompt: Task 2.2 - Data Normalization Schema

**Context & Objective:**
You are an AI agent tasked with executing Phase 2, Task 2.2 of the Trainscanner V2 migration.
Your goal is to build an abstract "Data Normalization Schema" step that runs immediately after the raw XML data has been parsed and ingested into `netex_stops_staging`. Raw European rail schedules are notoriously messy, containing broken schemas, incorrect timezones, and overlapping data drops at borders.

**Current State Reference:**

- The raw SAX parsing (Task 2.1) is now successfully extracting NeTEx to database staging tables (`netex_stops_staging`).
- The overall control pipeline is located in the Node.js `orchestrator/` folder.
- You must decide whether to implement this normalizer as a downstream Rust worker or as a TypeScript module within the Orchestrator (e.g., inside `orchestrator/src/domains/` or `orchestrator/src/data/`). Pick whichever fits best with the existing control flow.

**Your Instructions:**

1. **Timezone Enforcement:**
   - The normalization step must strictly parse any timestamps in the raw payload and convert/map them to a unified Vienna (CET/CEST) timezone.
   - Discard or flag timestamp anomalies (e.g., dates too far in the past/future).

2. **Provider Weighting (Cross-Border Logic):**
   - Different agencies often upload data for shared border stations (e.g., DB and ÖBB both upload data for Salzburg Hbf).
   - Define a "Provider Weighting" dictionary or hierarchy (e.g., ÖBB data is trusted more in Austria, DB data in Germany).
   - The normalizer must read from `netex_stops_staging` and apply this ruleset to resolve data conflicts. Which provider's "stop name" or "coordinates" should win for a given `canonical_station_id`?

3. **Anomaly Detection & Flagging:**
   - Implement structural anomaly detection (e.g., coordinates sitting in the ocean, missing translations for major hubs, invalid UIC codes).
   - Add a "QA_Flag" or "Anomaly_Score" column to the `netex_stops_staging` schema or a related `qa_anomalies` table to log rejected/flagged entries without hard-crashing the pipeline.

4. **Integration & Validation:**
   - Write tests or a script demonstrating how a conflicted data payload is resolved using the Provider Weighting system.
   - If modifying database schemas, write a new SQL migration in `db/migrations`.

**Deliverables:**

- Code implementation for the Data Normalization pipeline (Rust or TypeScript).
- Validation tests or instructions.
- Any corresponding SQL migrations.

Please begin your analysis and implement the Data Normalization Schema.
