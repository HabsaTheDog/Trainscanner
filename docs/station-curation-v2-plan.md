# Station Curation V2 Plan

## Goals

Deliver a robust, Europe-ready station curation and dedup workflow that:

- Preserves existing runtime contracts (`/api/routes` stays independent from QA curation)
- Keeps existing QA refresh async/non-blocking semantics (`/api/qa/jobs/refresh`, `/api/qa/jobs/:job_id`)
- Adds cluster-first curation with auditable naming, segment context, and service context

## Non-Goals For This Pass

- No production wiring of OJP/stitching prototypes into `/api/routes`
- No full line-dedup execution engine yet (only first-class model seam and API scaffolding)

## Current-State Findings

- Curation UI is cluster-first (`frontend/curation.js` using `GET /api/qa/v2/clusters`)
- User-facing naming still surfaces opaque canonical IDs/hashes
- No cluster abstraction with grouped decision workflow
- No service/line context payloads in curation API
- No station-complex/segment data model
- Country checks and workflows are DACH hard-coded in several legacy paths

## Target V2 Architecture

### 1) Data model (additive)

Add new v2 tables while keeping old tables unchanged:

- `qa_station_clusters_v2`
- `qa_station_cluster_candidates_v2`
- `qa_station_cluster_evidence_v2`
- `qa_station_cluster_decisions_v2`
- `qa_station_cluster_decision_members_v2`
- `qa_station_naming_overrides_v2`
- `qa_station_display_names_v2` (materialized cache/snapshot for UI friendliness)
- `qa_station_complexes_v2`
- `qa_station_segments_v2`
- `qa_station_segment_links_v2`
- `qa_station_candidate_services_v2`
- `canonical_line_identities_v2` (seam for future line dedup)
- `station_segment_line_links_v2`

All new tables are idempotent (`CREATE TABLE IF NOT EXISTS`, safe indexes) and include audit fields (`created_at/by`, `updated_at/by`, optional reason metadata).

### 2) Naming model

- Keep canonical machine IDs stable (`canonical_station_id` untouched)
- Add display naming policy and provenance:
  - display name
  - language
  - source type (`canonical`, `source_stop`, `manual_override`, `composed`)
  - explanation metadata in JSON
- Add alias support via array/json structures in candidate payload
- Ensure UI primary labels always prefer display name, never raw IDs

### 3) Cluster generation

Extend review-queue build to also build deterministic v2 clusters:

- Deterministic keying by sorted member IDs + scope tag
- Evidence graph includes: name similarity, distance, hard-ID collisions, provider overlap, service overlap
- Candidate context includes source/provider, coordinates, naming metadata, segment hints, service coverage metadata
- Build remains idempotent by scope and deterministic for same inputs

### 4) V2 API

Use v2 curation endpoints:

- `GET /api/qa/v2/clusters`
- `GET /api/qa/v2/clusters/:cluster_id`
- `POST /api/qa/v2/clusters/:cluster_id/decisions`

Decision payload supports:

- merge selected candidates
- keep separate
- split into groups
- rename target groups
- complex/segment annotations
- future line decision scaffold fields

All responses keep machine-readable `errorCode` via existing error handling and include correlation id header inherited from server middleware.

### 5) Frontend curation UX (cluster-first)

- Move from queue-item list to cluster list
- Candidate cards show:
  - readable display name + aliases
  - provider/source
  - coordinates
  - evidence summary
  - service/line context and confidence/completeness
  - segment/complex context
- Add selection/grouping interactions:
  - merge group
  - keep separate
  - split groups
  - rename groups
- Keep async pipeline polling non-blocking

### 6) Map enhancements

- Keep MapLibre GL JS
- Add basemap toggle (default + satellite)
- Persist map mode in session storage
- Preserve fallback style behavior if no style/key configured

### 7) Europe-ready strategy

- Keep DACH defaults in scripts/UI shortcuts
- Remove hard DACH assumptions in new v2 contracts where safe:
  - v2 country filter accepts ISO-3166 alpha-2 (`[A-Z]{2}`)
  - language fields generic (BCP-47-ish text)
  - scoring/threshold configs read from JSON config with DACH defaults

## Migration And Rollout Phases

### Phase 0: Compatibility Baseline

- Add migration and new backend utilities without changing runtime routing contracts
- Validate no regressions in existing `/api/routes`

### Phase 1: V2 Read Path

- Build/populate v2 clusters during review queue build
- Expose `GET /api/qa/v2/clusters` + detail endpoint
- Move frontend read path fully to v2 clusters

### Phase 2: V2 Decision Writes

- Add atomic decision submission endpoint with audit rows
- Persist v2 decisions directly into v2 decision, group, naming, and curated projection tables
- Keep linked queue-item handling backend-internal (no queue-item blocks in v2 cluster API/UI payloads)

### Phase 3: UX Upgrade

- Replace curation UI list/detail/actions with cluster-first workflow
- Add basemap toggle + service/segment visualization

### Phase 4: Hardening

- Add determinism/unit/integration tests for naming, clustering, decision atomicity, payload contracts, frontend smoke
- Update docs and operator command examples

## Safety And Rollback

- Additive schema only, no destructive drops
- If v2 cluster population fails, legacy queue still works
- Decision endpoint uses transactions and explicit validation before writes

## Test Strategy

- Unit:
  - naming resolution strategy
  - cluster evidence scoring deterministic ordering
  - decision payload validation
- Integration:
  - v2 API list/detail/decision contracts
  - transactional writes + audit records
- Frontend smoke:
  - cluster load
  - map style toggle persistence
  - decision action roundtrip (mocked fetch)

## Implemented Now vs Deferred (planned)

Implemented in this change:

- Additive v2 schema + migrations
- Deterministic cluster generation + evidence/context materialization
- v2 cluster API read/write endpoints
- Cluster-first curation UI including map toggle and session-persisted mode
- Naming metadata and auditable rename path
- Segment/complex model + decision payload support
- Line identity seam tables and payload scaffolding

Deferred to follow-up:

- Full automatic line dedup merge engine
- Advanced multilingual ranking beyond base alias/language fields
- richer segment-walk routing simulation beyond transfer-link metadata capture
