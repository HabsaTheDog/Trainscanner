# Agent Prompt: Task 1.2 - Seed Base Spatial Data (Cold Start Mitigation)

**Context & Objective:**
You are an AI agent tasked with executing Phase 1, Task 1.2 of the Trainscanner V2 migration.
Your goal is to create a robust spatial data seeding pipeline. Trainscanner currently lacks base topology/stations for Europe, meaning the first time it ingests operator schedules, the "Cold Start" AI will flag millions of stations as "novel" and fail under load. You need to pre-seed the PostGIS database with known, stable datasets.

**Current State Reference:**

- The database has been successfully migrated to use Geographic Grid Partitioning in `canonical_stations` and `netex_stops_staging`.
- Tombstoning (`is_deleted`, `deleted_at`) is active.
- Relevant current scripts/workers: Review the `workers/` or `scripts/` directories to see how the project currently handles data ingestion (e.g., Rust SAX workers or Node routines).

**Your Instructions:**

1. **Analyze Project Structure:**
   - Review the codebase to determine the best place for a one-off database seeding script. If a `scripts/` or `db/seeds/` directory exists, use it. If a Rust or TS worker is more appropriate based on existing patterns, use that.

2. **Acquire Base Datasets:**
   - The script needs to download and parse base truth data. The two primary sources for European rail are:
     - **UIC (International Union of Railways) Codes:** For authoritative station IDs.
     - **OpenRailwayMap (ORM) / OSM:** For accurate geospatial bounding boxes and multilingual names (`name:en`, `name:fr`, etc.).

3. **Develop the Seeder Logic:**
   - Write a script that downloads (or points to a local static bundle of) these datasets.
   - For each valid European rail station (focusing initially on DE, AT, CH corridors), construct a valid `canonical_station` record.
   - **Crucial Rule:** OSM data is licensed under ODbL (Share-Alike). To prevent this license from "infecting" proprietary schedule data, you must ensure strict legal isolation. Do NOT mix proprietary schedule payload data directly into the OSM-derived topology fields. Maintain loose pointers (e.g., `canonical_station_id`) between the schedule staging and the OSM topology.

4. **Insert into Grid Partitions:**
   - Ensure your insertion logic correctly computes the `grid_id` (using the same logic deployed in the recent `012_v2_bounding_box_partitioning.sql` migration) so that inserts route to the correct PostGIS hash partitions.
   - Set the `match_method` to `hard_id` or `name_geo` depending on the confidence of your seed data.

5. **Validation:**
   - The script should be idempotent (safe to run multiple times without duplicating stations).
   - Provide a `README.md` or instructional block on exactly how to execute this seeding script locally.

**Deliverables:**

- The new database seeding script (Node.js, Python, or Rust, matching the repo's preferred toolchain).
- Documentation on how to run it.

Please begin your analysis and create the seeding script.
