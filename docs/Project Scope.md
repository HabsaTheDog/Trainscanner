# Trainscanner Curation & QA Pipeline (V2 - European Scale)

## 1. Vision & Purpose
This project is the **automated data orchestration and intelligence backbone** for a future user-facing application—a "Skyscanner for European Trains."

Before providing seamless multi-national train routing to end-users, the chaotic, fragmented, and overlapping landscape of European railway data (NeTEx, GTFS) must be unified. **This developer tool is the automated data factory.** It guarantees the GTFS data feeding the end-user app is pristine, accurately merged, and empirically functional, relying on agentic AI workflows and hierarchical network structuring to process data at a continental scale.

## 2. Core Ideology
1. **Agentic AI Entity Resolution (via Local Data):** Move away from purely manual curation and static algorithms. The system uses AI Agents equipped with local geocoding tools (e.g., self-hosted Nominatim/Pelias, PostGIS with European OSM dumps) to actively investigate and automatically merge stations across borders, avoiding crippling API bottlenecks and costs.
2. **Deterministic Schema Mapping (AI-Generated):** To avoid live hallucinations during routine data processing, AI is *not* used in the live ingestion stream. Instead, AI assists developers in generating and maintaining **deterministic mapping scripts** for proprietary regional feeds, ensuring the actual ingestion pipeline is 100% reproducible, fast, and safe.
3. **Idempotent Updates & Stable IDs:** Once the pipeline establishes a canonical entity (e.g., "Paris Gare de Lyon"), stable IDs are locked. Subsequent remote feed updates go through a deterministic diffing mechanism. AI Agents are only triggered for *new* topological entities or significant geographic shifts, protecting the system from redundant re-verifications.
4. **Hierarchical Network Tiering & Boundary Nodes:** Transit networks are automatically classified and split into operational tiers (Tier 1: High-Speed/Long Distance, Tier 2: Regional, Tier 3: Local). The system ensures "Transfer/Boundary Nodes" are perfectly synchronized across artifacts, allowing for highly optimized, memory-efficient routing over massive geographic areas without losing precise local connections.
5. **Bulk-Resolution Human-in-the-Loop:** The React Frontend QA Dashboard is an **Exception Management System** built for scale. Domain experts resolve the hardest edge cases, aided by the AI's audit trail. Crucially, the UI supports bulk-pattern resolution, allowing humans to apply one logic fix to hundreds of similar structural anomalies.
6. **Continuous Layered Validation (MOTIS Engine):** A GTFS dataset is only good if it is traversable. The orchestrator pushes generated artifacts to a horizontally scaled cluster of MOTIS transit routing engines to validate both internal tier consistency and the critical cross-tier transfer boundaries before marking datasets "Production Ready."

## 3. System Architecture
- **Orchestrator (Node.js/Express & Temporal/Workflow Engine):** The control plane. It coordinates the fetching of remote snapshots, triggers deterministic mappers and AI investigation workers, serves the QA UI backend, and manages the distributed testing cluster.
- **AI Investigation Agents (Python/LangChain):** A specialized worker layer where AI agents evaluate ambiguous proximal stations using cached, local spatial tools to calculate a Merge Confidence Score and leave an audit trail for human reviewers.
- **Frontend QA Dashboard (React/Vite & MapLibre):** An exception-handling workspace. It surfaces anomalous clusters on an interactive map and provides bulk-execution tools for operators to resolve repetitive data errors at scale based on AI recommendations.
- **Data Staging (PostGIS):** A robust spatial database storing topologies, runtime geometries, AI confidence scores, stable canonical IDs, and maintaining the hierarchical tags (Long Distance vs. Local) needed for optimized export.
- **Layered MOTIS Testing Grid:** An auto-scaling integration layer. Different MOTIS instances handle specific network tiers (e.g., a fast "European Backbone" router paired with containerized "Local" routers) to execute parallel REST endpoint (`/api/routes`) regressions, explicitly testing the boundary hand-offs.

## 4. The Standard Operating Workflow
1. **Parallel Raw Discovery:** CRON jobs reach out to official European data hubs to asynchronously pull down raw schedules.
2. **Deterministic Diffing & Staging:** Incoming feeds are mapped into PostGIS using AI-generated, human-approved deterministic parsers. Only genuine topological changes or net-new stations bypass the stable ID cache.
3. **Agentic Canonicalization & Tiering:** The AI Agents evaluate all novel or unmapped proximal stations against local OSM infrastructure. High-confidence matches are automatically merged. Routes and stations receive hierarchical tags. Complex splits are actively researched and queued.
4. **Bulk Exception Curation:** Administrators review the low-confidence queue, applying bulk-pattern resolutions to systematic mapping errors or manually resolving single complex hubs using the AI's investigation notes.
5. **Hierarchical GTFS Artifact Compilation:** The system compiles optimized, layered `canonical_runtime` GTFS artifacts, ensuring boundary overlap for transfers while keeping artifact sizes strictly constrained to their tiers.
6. **Integration Smoke Testing:** Artifacts are deployed to the tier-specific testing grid. If cross-border regressions and hybrid tier-to-tier routes meet the SLA, the artifact is marked as "Production Ready."

## 5. AI Developer Instructions
When interacting with this codebase, AI Assistants must understand that:
- This is an **automated pipeline focusing on massive scale and deterministic data flow**. It uses Node.js for orchestration, PostGIS for spatial truth, and Python/Agentic frameworks *strictly* for asynchronous anomaly resolution and parser generation.
- The pipeline assumes a **Hierarchical Data Model**, where GTFS data is deliberately split by transit type to prevent memory bloat in routing engines. Boundary nodes must maintain absolute continuity.
- Development should prioritize local infrastructure (self-hosted spatial queries) over external APIs, and QA UI features must embrace bulk-action paradigms to prevent human bottlenecks.
