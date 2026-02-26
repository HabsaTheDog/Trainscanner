# Trainscanner Curation & QA Pipeline (V2 - European Scale)

## 1. Vision & Architecture

**Goal:** Automated data orchestration backbone for a "Skyscanner for European Trains", merging fragmented railway data (NeTEx, GTFS) into unified, traversable datasets via deterministic pipelines and local AI.

- **Control Plane (Temporal.io + TypeScript Workers):** Orchestrator replacing native Node.js to manage distributed syncing, idempontency, staggered retries, and test coordination.
- **Ingestion Workers (Golang/Rust):** High-performance parsing microservices utilizing SAX streams to convert massive XML datasets into canonical nodes without memory starvation.
- **AI & Spatial Inference (Python/FastAPI/Langchain):** Dedicated GPU worker nodes running quantized local LLMs (vLLM) to compute "Merge Confidence Scores" for novel stations. Connects to control plane via message queues.
- **Data Staging (PostgreSQL/PostGIS):** Cloud-managed spatial truth DB relying on explicit Geographic Table Partitioning. (Topologies, stable canonical IDs, hierarchical tags, temporal versioning/tombstoning).
- **QA Dashboard (React/MapLibre & GraphQL/tRPC):** Operator interface resolving low-confidence AI matches. Map data served dynamically via vector tile servers (e.g., pg_tileserv).
- **Testing Grid (Kubernetes + MOTIS):** Auto-scaling integration testing via ephemeral K8s pods spinning up localized micro-graphs per region.

## 2. Core Principles

- **Local, Deterministic AI:** AI uses local PostGIS/OSM to bypass API latency. No live LLM calls in daily streams.
- **Idempotent Updates:** AI exclusively processes novel entities; known entities rely on stable IDs and diffing.
- **Hierarchical Tiering:** Networks split (High-Speed, Regional, Local) to prevent routing engine memory bloat.
- **Layered Validation:** Output artifacts are strictly verified across MOTIS engine clusters before release.

## 3. Operating Workflow

1. **Fetch:** Async CRON pulls raw temporal schedules.
2. **Normalize & Stage:** Parsers map incoming data; strictly normalize to Vienna (CET/CEST) timezone; known entities bypass AI.
3. **Canonicalize:** AI merges novel properties using local OSM; constructs multilingual alias dictionaries to prevent duplication.
4. **QA Handling:** Human-in-the-loop bulk resolutions for the low-confidence queue.
5. **Compilation:** System emits optimized, tier-constrained GTFS artifacts.
6. **Integration Testing:** Artifacts subjected to MOTIS cross-tier regression checks.

## 4. Bottlenecks & Strategic Mitigations

- **"Cold Start" Compute:** Initial European scan flags millions of novel entities.
  - _Fix:_ Pre-seed PostGIS with known datasets (UIC codes, OpenRailwayMap).
- **Temporal Workflow Bloat:** Spawning workflows per-entity overloads the orchestration DB.
  - _Fix:_ Heavily batch entity updates before passing to Temporal; control plane written in TS/Go to manage staggered execution gracefully.
- **NeTEx XML Bloat:** Massive XML structures crash standard parsers.
  - _Fix:_ Dedicated Rust worker microservices enforcing memory-efficient, event-driven (SAX) XML streaming exclusively.
- **The Standardization Illusion:** 30+ national operators frequently break GTFS/NeTEx schemas.
  - _Fix:_ Introduce an abstract Data Normalization Schema for pre-stage anomaly detection.
- **Compute Spikes (MOTIS I/O):** Testing the full continental graph per update is cost-prohibitive.
  - _Fix:_ Kubernetes (K8s) dynamically spinning up ephemeral MOTIS testing pods. Two low-RAM test types: dynamic micro-graphs (affected bbox + padding) for Local/Regional updates, and isolated sparse macro-graphs for High-Speed/Long-Distance networks.
- **The December Timetable Shock:** Continental schedules shift simultaneously every December.
  - _Fix:_ Implement an October "slow burn" pre-ingestion environment to stretch the compute load.
- **PostGIS Spatial Locking:** Global spatial queries cause massive table locks.
  - _Fix:_ Implement declarative Geographic Table Partitioning aggressively (e.g., via bounding box grids) and migrate to managed multi-node Postgres (AWS Aurora/Citus) for horizontal scaling.
- **Cross-Border Data Conflicts:** Overlapping national datasets disagree on shared hubs.
  - _Fix:_ Orchestrator enforces a "Provider Weighting" hierarchy to auto-trust the local infrastructure owner.
- **Multilingual Border Complexity:** Aliases in border regions (e.g., Bozen/Bolzano) cause duplication.
  - _Fix:_ AI explicitly queries local OSM datasets for pre-mapped multi-language tags (`name:en`, `name:fr`) before creating new entities.
- **Transfer Realism (Mega-Hubs):** AI misses indoor walking distances or temporary closures.
  - _Fix:_ QA Transfer Matrix override prioritizing manual walk-time curation for the top 100 EU hubs.
- **QA Scaling Limit:** Even a 1% AI failure rate creates 50k+ manual tasks.
  - _Fix:_ Prioritize pattern-based, region-wide bulk approvals over single-station curation.
- **The ODbL License Virus:** Merging OSM topology with proprietary schedules triggers "share-alike" clauses.
  - _Fix:_ Enforce strict legal isolation between Navigation (OSM) and Schedule DBs; use loose canonical ID pointers.
- **Data Depreciation:** Unannounced station deletions break historical routing.
  - _Fix:_ Implement "Tombstoning" (soft-deletions) for canonical IDs.
- **Pricing Integration:** Dynamic yield pricing breaks static routing models.
  - _Fix:_ Defer native pricing to V3 or abstract via a secondary async pricing cache heuristic.
- **Real-Time Streams (GTFS-RT):** Highly fragmented and unstable continental support.
  - _Fix:_ Exclude from core curation; treat as a distinct secondary overlay post-static stabilization.
