# Agent Prompt: Task 1.1 - PostGIS & Geographic Partitioning Setup

**Context & Objective:**
You are an AI agent tasked with executing Phase 1, Task 1.1 of the Trainscanner V2 migration. 
Your goal is to prepare the PostgreSQL database for Europe-wide scaling by implementing strict Geographic Table Partitioning (via bounding box grids or spatial hashes) to replace the current basic List partitioning, which limits scaling and could cause spatial locking.

**Current State Reference:**
- Review the latest migration file: `db/migrations/011_v2_spatial_partitioning_and_tombstoning.sql`. 
- Notice that the table currently uses `PARTITION BY LIST (country)` limited to 'DE', 'AT', 'CH'.
- Tombstoning (`is_deleted`, `deleted_at`) has already been added to the base schema.

**Your Instructions:**

1. **Analyze Current Schema:**
   - Review the existing `canonical_stations` and `netex_stops_staging` schema definitions in the `db/migrations` folder.

2. **Refactor Partitioning Strategy:**
   - Change the partitioning strategy from `LIST (country)` to a highly scalable Geographic Grid Partitioning. 
   - A standard approach is to use a computed `grid_id` (e.g., based on Geohash or a fixed latitude/longitude bounding box grid) instead of strictly relying on `country`. This prevents hot-spotting and prepares the data to be distributed across multi-node Postgres clusters like Citus or Aurora.
   - Example: Add a persisted generated column `grid_id` based on `(ST_GeoHash(geom, 3))` or similar bounding box logic, and partition by `LIST (grid_id)` or `HASH (grid_id)`.

3. **Multi-Node Compatibility:**
   - Ensure the new schema completely avoids Foreign Keys pointing directly to distributed/partitioned tables (which is already somewhat handled, but needs careful review).
   - Ensure the `PRIMARY KEY` of the partitioned tables includes the partitioning key (e.g., `grid_id, canonical_station_id`).

4. **Create the Migration File:**
   - Create a new SQL migration file (e.g., `db/migrations/012_v2_bounding_box_partitioning.sql`).
   - Write the SQL up/down logic to migrate existing data from the country-list partitions into the new grid-based partitions. 
   - Ensure you properly copy/insert the existing data and rebuild the GiST spatial indexes on the new partitions.

5. **Validation:**
   - Ensure that after the migration, performing an `EXPLAIN` query using a bounding box `ST_Intersects` correctly prunes partitions.

**Deliverables:**
- The new database migration `.sql` file.
- Any necessary model/type updates in the application schema (if ORM/Codegen is relying on the specific primary keys, e.g., in `orchestrator/` or `services/`).

Please begin your analysis and create the migration file.
