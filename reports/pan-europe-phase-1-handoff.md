# Phase 1 Handoff - Immediate Hard Cutover Start

## Objective

Replace the legacy canonical/QA schema foundation with the pan-European global model in one pass.

## Required Edits

1. Rewrite `db/schema.sql`.
2. Update `scripts/data/db-bootstrap.sh` validation checks.

## What To Build First

Implement these schema objects first, then indexes/FKs:

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

Keep:

- `CREATE EXTENSION IF NOT EXISTS postgis;`
- minimal operational tables needed by orchestrator runtime (`pipeline_jobs`, `system_state`).

Drop from active schema:

- legacy `canonical_*` tables
- legacy `qa_station_*`/`qa_curated_*` cluster model
- legacy country-scoped transfer/canonical review artifacts

## No-Touch Boundaries During This Slice

- Do not change frontend yet.
- Do not change GTFS exporter logic yet.
- Do not change `AGENTS.md`.

## Validation Commands

1. `scripts/data/db-bootstrap.sh`
2. SQL checks:
   - `SELECT to_regclass('provider_datasets');`
   - `SELECT to_regclass('global_stations');`
   - `SELECT to_regclass('timetable_trip_stop_times');`
   - `SELECT to_regclass('qa_merge_clusters');`
3. Confirm legacy tables removed from schema by checking:
   - `SELECT to_regclass('canonical_stations');` returns null
   - `SELECT to_regclass('canonical_review_queue');` returns null

## Done Criteria

- Bootstrap succeeds from clean DB.
- All global model tables exist.
- Legacy canonical schema objects are absent.
- `scripts/data/db-bootstrap.sh` validation matches new schema.
