# Agent Prompt: Task 6.1 - Hierarchical Artifact Compilation

**Context & Objective:**
You are an AI agent tasked with executing Phase 6, Task 6.1 of the Trainscanner V2 migration.
Your goal is to create a compilation pipeline that emits optimized, **tier-constrained** GTFS artifacts from the canonical PostGIS data. Routing engines like MOTIS will choke if fed a single monolithic European graph, so network data must be split into hierarchical tiers.

**Current State Reference:**

- There is an existing GTFS export script at `scripts/qa/export-canonical-gtfs.py` that builds `stops.txt`, `routes.txt`, `trips.txt`, `stop_times.txt`, `transfers.txt`, `agency.txt`, and `calendar.txt` inside a deterministic ZIP.
- The canonical data lives in the `canonical_stations` PostGIS table (grid-partitioned).
- The system normalizes times to Vienna (CET/CEST).

**Your Instructions:**

1. **Define Network Tiers:**
   - Implement a classification system for network types:
     - **Tier 1 — High-Speed / Long-Distance** (ICE, TGV, Railjet, Frecciarossa, etc.)
     - **Tier 2 — Regional / Intercity** (RE, RB, IR, IC)
     - **Tier 3 — Local / S-Bahn / Metro** (S-Bahn, U-Bahn, Tram)
   - Classification can be based on route type codes (GTFS `route_type`), operator/agency names, or explicit tagging from the staging data.

2. **Refactor the Export Pipeline:**
   - Extend or refactor `scripts/qa/export-canonical-gtfs.py` to accept a `--tier` argument (e.g., `--tier high-speed`, `--tier regional`, `--tier local`, or `--tier all`).
   - When a tier is specified, the export must filter the canonical data to only emit stops, routes, and trips belonging to that tier.
   - The `--tier all` option should produce a combined feed but with tier metadata tags preserved in agency or route descriptions for MOTIS to process selectively.

3. **Temporal Workflow Integration:**
   - Create a new Temporal activity or workflow (in `control-plane/` or `orchestrator/src/temporal/`) called `compileGtfsArtifact` that triggers the Python export script with the appropriate tier argument.
   - The workflow should emit artifacts to a designated output directory (e.g., `data/artifacts/`) and log the compilation result.

4. **Validation:**
   - Run `python scripts/qa/export-canonical-gtfs.py --tier regional` (or equivalent) against the existing database and verify the output ZIP is valid (contains the required GTFS text files and they are non-empty).

**Deliverables:**

- The updated `scripts/qa/export-canonical-gtfs.py` with tier-based filtering.
- The new Temporal activity/workflow for triggering compilation.
- Brief documentation on how to compile artifacts for each tier.

Please begin your analysis and implement the Hierarchical Artifact Compilation pipeline.
