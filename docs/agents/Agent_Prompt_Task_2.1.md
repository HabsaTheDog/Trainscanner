# Agent Prompt: Task 2.1 - Rust SAX Stream Workers

**Context & Objective:**
You are an AI agent tasked with executing Phase 2, Task 2.1 of the Trainscanner V2 migration.
Your goal is to replace the dummy/placeholder XML parsing logic in the existing Rust Temporal worker with a robust, memory-efficient SAX parser that can handle massive (multi-gigabyte) NeTEx/GTFS XML datasets without running out of memory.

**Current State Reference:**

- The database storage foundation and spatial seeding (Phase 1) are complete.
- Review the file `workers/rust-ingestion/src/activities/netex.rs`.
- Look at the `extract_netex_stops` function. Currently, there is a comment block stating `// ... Deep SAX logic here ...` and it just outputs "Sample Station" into the database `COPY` stream.
- The project likely uses or should use `quick_xml` (or similar) for event-driven parsing.

**Your Instructions:**

1. **Analyze Existing Code:**
   - Read `workers/rust-ingestion/src/activities/netex.rs` to understand the inputs and the existing `COPY` stream logic writing to `netex_stops_staging`.
   - Ensure you check `workers/rust-ingestion/Cargo.toml` to make sure the necessary XML streaming dependencies (like `quick-xml`) are included. If not, add them.

2. **Implement Event-Driven SAX Parsing:**
   - Replace the dummy loop over `entries_to_scan` with actual `quick_xml::Reader` logic.
   - The parser must **NOT** load the entire XML into memory (do not use DOM parsing). Instead, it must stream through the file using events (Start, End, Text, Empty).
   - Target NeTEx elements like `Site`, `StopPlace`, or `ScheduledStopPoint`.
   - Extract relevant properties required by the `netex_stops_staging` table (e.g., ID, Name, Latitude, Longitude).

3. **Data Formatting & Database Insertion:**
   - Continue using the high-performance PostgreSQL `COPY` stream already established in `netex.rs`.
   - Format the extracted values to correctly match the `COPY` statement format. Remember to handle proper escaping for CSV format (especially for names that might contain quotes or commas).
   - Compute the `grid_id` dynamically using the existing `compute_grid_id` function.

4. **Robust Error Handling:**
   - Ensure that malformed inner nodes do not crash the entire worker. Log warnings for skipped nodes where possible.
   - Check if the incoming ZIP file format is handled correctly.

5. **Validation:**
   - Provide a way to test this worker locally (e.g., a dummy test or instructions on how to call the Temporal activity directly from a test harness).
   - Build the worker `cargo build --features temporal-worker` or run `cargo check` to ensure there are no compilation errors.

**Deliverables:**

- The updated `workers/rust-ingestion/src/activities/netex.rs` with functional SAX parsing.
- Any updates to `Cargo.toml`.
- Brief documentation on how to verify the parser locally.

Please begin your analysis and implement the streaming parser.
