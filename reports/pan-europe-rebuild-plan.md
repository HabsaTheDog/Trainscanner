# Hard-Cutover Pan-European Rebuild Plan

## Scope

- Replace legacy country-scoped canonical/QA/export architecture in one branch.
- Break API/UI contracts now; no compatibility mode.
- Remove legacy canonical code paths in the same cutover.

## Current Blockers

- Semantic station identity is country-scoped (`DE|AT|CH`) across schema and contracts.
- QA clustering is country-bounded and cannot model cross-border merge candidates.
- Transfer behavior depends on country-scoped defaults instead of explicit global transfer edges.
- GTFS export is synthetic bridge generation rather than timetable/trip preservation.

## Target Layers

1. `provider_datasets`
2. `raw_provider_stop_places`
3. `raw_provider_stop_points`
4. `global_stations`
5. `global_stop_points`
6. `provider_global_station_mappings`
7. `provider_global_stop_point_mappings`
8. `timetable_trips`
9. `timetable_trip_stop_times`
10. `transfer_edges`
11. `qa_merge_clusters`
12. `qa_merge_cluster_candidates`
13. `qa_merge_cluster_evidence`
14. `qa_merge_decisions`
15. `qa_merge_decision_members`

Rules:

- `country` is metadata/filter context only, not semantic identity.
- Routing artifacts must preserve real timetable edges and explicit transfer edges.
- Synthetic bridge routes are removed.

## Phases

### Phase 0: Freeze + Deletion Map

- Objective: lock destructive boundaries and execution order.
- Reuse: existing repo architecture docs.
- Replace: none.
- Parallel: none.
- Risks: partial implementation drift.
- Acceptance: this file + checklist + phase-1 handoff exist and are actionable.

#### Phase 0 Deletion Map (Hard-Cutover Scope)

Schema/runtime objects to remove from active code paths:

- `canonical_stations`
- `canonical_station_sources`
- `canonical_review_queue`
- `canonical_station_overrides`
- `station_transfer_rules` + country-default transfer helper
- legacy QA cluster stack (`qa_station_clusters`, `qa_station_cluster_*`, `qa_station_groups`, `qa_curated_*`)
- synthetic canonical refresh/materialization functions tied to country-scoped identity

Code/scripts to remove or replace:

- `services/orchestrator/src/domains/canonical/*`
- `services/orchestrator/src/data/postgis/repositories/canonical-stations-repo.js`
- `services/orchestrator/src/data/postgis/repositories/review-queue-repo.js`
- `scripts/data/build-canonical-stations.sh`
- `scripts/data/build-review-queue.sh`
- synthetic bridge exporter logic in `scripts/qa/export-canonical-gtfs.py`

### Phase 1: Schema Replacement Foundation

- Objective: replace canonical/QA schema with pan-European global schema.
- Files: `db/schema.sql`, `scripts/data/db-bootstrap.sh`.
- Replace: canonical/review-queue cluster schema and synthetic assumptions.
- Dependencies: Phase 0.
- Risks: monolithic bootstrap order.
- Acceptance:
  - new global tables exist,
  - legacy canonical/cluster tables removed,
  - bootstrap succeeds.
- Validation:
  - `scripts/data/db-bootstrap.sh`
  - `SELECT to_regclass(...)` checks for global tables only.

### Phase 2: Ingestion + Mapping Refactor

- Objective: ingest raw stop places/points and timetable facts into new schema.
- Files:
  - `scripts/data/netex_extract_stops.py`
  - `scripts/data/ingest-netex.impl.sh`
  - `services/orchestrator/src/data/postgis/repositories/netex-stops-repo.js`
  - `services/orchestrator/src/domains/ingest/contracts.js`
  - `services/orchestrator/src/domains/source-discovery/contracts.js`
  - `config/europe-data-sources.json`
- Dependencies: Phase 1.
- Reuse: fetch/auth shell scaffolding and orchestrator job runner.
- Replace: DACH-only enums and stop-only model.
- Parallel: none.
- Risks: provider variance and ingest volume.
- Acceptance:
  - cross-border data populates new raw/mapping tables,
  - FK integrity holds,
  - idempotent re-run does not duplicate active mappings.

### Phase 3: QA Backend + UI Rewrite

- Objective: global cross-border merge curation API/UI.
- Files:
  - `services/orchestrator/src/domains/qa/api.js`
  - `services/orchestrator/src/graphql/schema.js`
  - `services/orchestrator/src/graphql/resolvers.js`
  - `services/orchestrator/src/domains/qa/cluster-decision-contracts.js`
  - `frontend/src/curation-page.jsx`
  - `frontend/src/curation-page-runtime.js`
- Dependencies: Phase 2.
- Reuse: merge/split decision flow concepts.
- Replace: country-scoped cluster semantics.
- Parallel: none.
- Risks: operator retraining after contract break.
- Acceptance:
  - global clusters support mixed-country candidates,
  - decisions persist to new `qa_merge_*` artifacts.

### Phase 4: Timetable-Preserving GTFS Export

- Objective: emit GTFS from real timetable + explicit transfer edges.
- Files:
  - `scripts/qa/export-canonical-gtfs.py` (rewritten as pan-European exporter)
  - `scripts/qa/build-profile.sh`
  - `services/orchestrator/src/temporal/activities/compile.js`
  - `services/orchestrator/test/e2e/export-determinism.e2e.test.js`
- Dependencies: Phase 3.
- Reuse: deterministic zip + validation harness.
- Replace: synthetic bridge generation.
- Parallel: none.
- Risks: export size and schedule edge cases.
- Acceptance:
  - `trips.txt`/`stop_times.txt` come from timetable facts,
  - `transfers.txt` comes from `transfer_edges`,
  - synthetic bridge marker removed.

### Phase 5: MOTIS Integration + Cross-Border Regression

- Objective: pan-European artifact is sole runtime profile.
- Files:
  - `config/gtfs-profiles.json`
  - `services/orchestrator/src/gtfs-profile-resolver.js`
  - `services/orchestrator/test/routes/regression_cases.json`
  - `scripts/qa/run-route-regression.sh`
  - `scripts/run-motis-k8s-test.sh`
- Dependencies: Phase 4.
- Reuse: regression and k8s MOTIS harnesses.
- Replace: DACH-only assumptions and baselines.
- Parallel: none.
- Risks: runtime/memory pressure.
- Acceptance:
  - cross-border regression suite passes,
  - micro+macro MOTIS validation passes.

### Phase 6: Legacy Purge Completion

- Objective: remove remaining legacy canonical code/scripts/tests.
- Delete/replace:
  - `services/orchestrator/src/data/postgis/repositories/canonical-stations-repo.js`
  - `services/orchestrator/src/data/postgis/repositories/review-queue-repo.js`
  - `services/orchestrator/src/domains/canonical/service.js`
  - `scripts/data/build-canonical-stations.sh`
  - `scripts/data/build-review-queue.sh`
  - synthetic-export legacy fixtures/tests.
- Dependencies: Phase 5.
- Acceptance:
  - no active runtime references to legacy canonical path,
  - docs/tests match pan-European model.
- Validation:
  - `rg -n "canonical_stations|canonical_review_queue|build-canonical-stations|build-review-queue|group-aware-synthetic-journeys-from-canonical-stops" .`

## First Implementation Slice

1. Add planning artifacts.
2. Rewrite `db/schema.sql` for global model.
3. Update `scripts/data/db-bootstrap.sh` validation to global tables.
4. Bootstrap clean DB and record proof.
5. Do not modify `AGENTS.md`.

## Surprising Constraints

- `db/schema.sql` was a flattened monolithic migration script and is order-sensitive.
- DACH assumptions are duplicated in SQL, shell scripts, JS contracts, and profiles.
- Legacy `canonical_station_sources` keying by `(source_id, source_stop_id)` dropped historical snapshot dimensionality.
- Existing exporter tests were built around synthetic bridge behavior and require rewrite.
