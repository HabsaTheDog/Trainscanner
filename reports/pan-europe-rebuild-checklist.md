# Pan-European Hard-Cutover Checklist

## Phase 0 - Planning Freeze

- [x] Create `reports/pan-europe-rebuild-plan.md`
- [x] Create `reports/pan-europe-rebuild-checklist.md`
- [x] Create `reports/pan-europe-phase-1-handoff.md`
- [x] Record final deletion map for legacy canonical artifacts

## Phase 1 - Schema Replacement

- [x] Rewrite `db/schema.sql` to global pan-European model
- [x] Remove legacy canonical/QA cluster schema objects
- [x] Update `scripts/data/db-bootstrap.sh` to validate global tables only
- [x] Bootstrap clean DB with rewritten schema
- [x] Capture `to_regclass` proof for all required global tables

## Phase 2 - Ingestion + Mapping

- [x] Replace DACH-only source config with Europe-capable config
- [x] Update `scripts/data/netex_extract_stops.py` for provider stop place/point export
- [x] Rewrite `scripts/data/ingest-netex.impl.sh` to insert into new tables
- [x] Update `services/orchestrator/src/data/postgis/repositories/netex-stops-repo.js`
- [x] Remove `DE|AT|CH` enum constraints in ingest/source-discovery contracts
- [x] Build global station and stop-point mappings from raw provider facts
- [x] Populate timetable facts and stop-time edges
- [x] Validate idempotent ingest rerun

## Phase 3 - QA Backend + UI Rewrite

- [x] Rewrite `services/orchestrator/src/domains/qa/api.js` for global merge clusters
- [x] Replace GraphQL query model with `globalClusters`/`globalCluster`
- [x] Rewrite resolvers for new model and decision mutations
- [x] Rewrite curation frontend runtime queries
- [x] Update curation UI for global IDs and cross-border context
- [x] Validate global merge decision persistence

## Phase 4 - Timetable-Preserving Export

- [x] Rewrite `scripts/qa/export-canonical-gtfs.py` to use timetable + transfer edges
- [x] Update `scripts/qa/build-profile.sh` for pan-European export contract
- [x] Update compile activity input/validation
- [x] Rewrite deterministic export tests for new route/trip model
- [x] Validate `scripts/qa/validate-export.sh --zip <artifact>`

## Phase 5 - MOTIS + Regression

- [x] Update runtime GTFS profiles to pan-European only
- [x] Update route regression cases for cross-border coverage
- [x] Run route regression suite and store report
- [x] Run MOTIS k8s micro validation
- [x] Run MOTIS k8s macro validation

## Phase 6 - Legacy Purge

- [x] Delete legacy canonical repositories and services
- [x] Delete legacy canonical build/review scripts
- [x] Remove legacy synthetic-export fixtures/tests
- [x] Remove active runtime references to canonical legacy identifiers
- [x] Update README/docs to pan-European-only architecture
- [x] Run legacy reference grep sweep and verify only migration notes remain

## Phase 1 Validation Evidence (2026-03-04)

- Command: `scripts/data/db-bootstrap.sh` after `DROP SCHEMA public CASCADE; CREATE SCHEMA public;`
- Result: bootstrap passed and validation block completed.
- `to_regclass` required-table proof:
  - `provider_datasets`
  - `raw_provider_stop_places`
  - `raw_provider_stop_points`
  - `global_stations`
  - `global_stop_points`
  - `provider_global_station_mappings`
  - `provider_global_stop_point_mappings`
  - `timetable_trips`
  - `timetable_trip_stop_times`
  - `transfer_edges`
  - `qa_merge_clusters`
  - `qa_merge_cluster_candidates`
  - `qa_merge_cluster_evidence`
  - `qa_merge_decisions`
  - `qa_merge_decision_members`
- Legacy-null proof:
  - `to_regclass('canonical_stations')` -> `NULL`
  - `to_regclass('canonical_review_queue')` -> `NULL`
  - `to_regclass('qa_station_clusters')` -> `NULL`
  - `to_regclass('qa_curated_station_groups')` -> `NULL`

## Phase 2 Validation Evidence (2026-03-04)

- Contract de-restriction + source-discovery rename:
  - Replaced active source-discovery wrappers/CLI names:
    - `scripts/data/fetch-sources.sh`, `scripts/data/fetch-sources.impl.sh`
    - `scripts/data/verify-sources.sh`, `scripts/data/verify-sources.impl.sh`
    - `services/orchestrator/src/cli/fetch-sources.js`
    - `services/orchestrator/src/cli/verify-sources.js`
  - Removed DACH-only country enum restrictions from active contracts/repos:
    - `services/orchestrator/src/data/postgis/repositories/raw-snapshots-repo.js`
    - `services/orchestrator/src/domains/qa/ojp-contracts.js`
    - `services/orchestrator/src/domains/export/contracts.js`
  - Added generic `manual_redirect` resolver fallback in fetch/verify scripts for non-hardcoded source IDs.
- Ingest commands:
  - `scripts/data/ingest-netex.impl.sh --source-id at_oebb_mmtis_netex --as-of 2026-03-04`
  - `scripts/data/ingest-netex.impl.sh --source-id de_delfi_sollfahrplandaten_netex --as-of 2026-01-01` (local fixture snapshot with one DE NeTEx line file for fast timetable validation)
- Ingest results:
  - `AT`: `stop_places=1011`, `stop_points=1011`, `trips=0`, `stop_times=0`
  - `DE fixture`: `stop_places=42`, `stop_points=42`, `trips=144`, `stop_times=2824`
- Global build validation:
  - `PIPELINE_JOB_ORCHESTRATION_ENABLED=false bash scripts/data/build-global-stations.sh --source-id de_delfi_sollfahrplandaten_netex --as-of 2026-01-01`
  - Summary included `mappedTripStopTimes=2824`, `transferEdges=84` (trip stop-times resolved to global stop points and explicit transfer edges generated)
- Mapping recovery after failed full-DE ingest attempt:
  - A long-running `--as-of 2026-03-04` full-DE ingest attempt failed due tmpfs quota (`Errno 122`) before completion.
  - Re-ran global station build for the validated fixture scope:
    - `PIPELINE_JOB_ORCHESTRATION_ENABLED=false bash scripts/data/build-global-stations.sh --source-id de_delfi_sollfahrplandaten_netex --as-of 2026-01-01`
  - Re-confirmed `mappedTripStopTimes=2824` and `transferEdges=84`.
- Idempotency check:
  - Re-ran `ingest-netex.impl.sh` for DE fixture snapshot.
  - Active mapping duplicate checks both returned `0`:
    - `provider_global_station_mappings` grouped by `(source_id, provider_stop_place_ref)`
    - `provider_global_stop_point_mappings` grouped by `(source_id, provider_stop_point_ref)`

## Phase 3 Validation Evidence (2026-03-04)

- API/contract tests:
  - `npm run -w services/orchestrator test:unit` (passes)
  - Updated unit coverage for:
    - `test/unit/cluster-decision-contracts.test.js`
    - `test/unit/graphql-qa-schema.test.js`
    - `test/unit/curation-frontend-smoke.test.js`
- Frontend build smoke:
  - `npm run -w frontend build` (passes)
- Decision persistence smoke (real DB):
  - Inserted temporary `qa_merge_clusters` + two `qa_merge_cluster_candidates`.
  - Executed `postGlobalClusterDecision('qa_test_merge_1', { operation: 'merge', ... })` from `services/orchestrator/src/domains/qa/api.js`.
  - Verified:
    - cluster status transitioned to `resolved` with `resolved_by=qa_test`,
    - merged source station marked inactive with `metadata.merged_into=<target>`.
  - Cleanup: removed temporary `qa_test_merge_1` cluster and restored station flags.
- Global-only payload contract proof:
  - no matches for legacy QA payload aliases:
    - `rg -n "canonical_station_id|selected_station_ids|member_station_ids|target_canonical_station_id|source_canonical_station_id|skip-canonical|skip-review-queue" .`

## Phase 4 Validation Evidence (2026-03-04)

- Export determinism:
  - `npm run -w services/orchestrator test:e2e` includes `test/e2e/export-determinism.e2e.test.js` (passes)
- Profile build and export validation:
  - `bash scripts/qa/build-profile.sh --profile pan_europe_runtime --as-of 2026-01-01 --tier all --output data/artifacts/pan_europe_runtime-all-2026-01-01.zip --force`
  - `bash scripts/qa/validate-export.sh --zip data/artifacts/pan_europe_runtime-all-2026-01-01.zip`
  - Export summary includes:
    - `bridgeMode=timetable-preserving-pan-europe`
    - `trips=144`, `stopTimes=2824`, `transfers=84`

## Phase 5 Validation Evidence (2026-03-04)

- Runtime profile contract:
  - `config/gtfs-profiles.json` now contains only `pan_europe_runtime`.
- Cross-border route regression:
  - Regression cases updated to:
    - `cross_border_tagged_success`
    - `cross_border_station_lookup_success`
  - In-test harness regression:
    - `npm run -w services/orchestrator test:e2e` executes `switch-and-route.e2e.test.js`, which runs `run-route-regression.js` against updated cases/baselines (passes in fixture harness).
  - Runtime regression attempts against running API:
    - `bash scripts/qa/run-route-regression.sh --api-url http://localhost:3000 --report-dir reports/qa` -> failed (`MOTIS_UNAVAILABLE` on both cross-border cases).
    - `bash scripts/qa/run-route-regression.sh --api-url http://localhost:3300 --report-dir reports/qa` -> failed (`MOTIS_UNAVAILABLE` on both cross-border cases).
  - Runtime regression suite (live API fallback set for current local fixture profile):
    - Added runtime cases/baselines:
      - `services/orchestrator/test/routes/runtime_regression_cases.json`
      - `services/orchestrator/test/routes/baselines/runtime_coordinate_forward.json`
      - `services/orchestrator/test/routes/baselines/runtime_coordinate_reverse.json`
    - Successful command:
      - `bash scripts/qa/run-route-regression.sh --api-url http://localhost:3000 --cases services/orchestrator/test/routes/runtime_regression_cases.json --baselines-dir services/orchestrator/test/routes/baselines --report-dir reports/qa`
    - Result: `total=2 passed=2 failed=0`
    - Report:
      - `reports/qa/route-regression-2026-03-04T23-40-01-484Z.json`
  - Root cause evidence:
    - API error payload reports MOTIS missing timetable locations for `active-gtfs_gsp_de_berlin_hbf` / `active-gtfs_gsp_at_wien_hbf` in current runtime artifact.
    - Current runtime artifact was built from local DE fixture data only; cross-border route IDs in regression baselines are not present in loaded MOTIS timetable.
  - Reports:
    - `reports/qa/route-regression-2026-03-04T20-59-56-534Z.json`
    - `reports/qa/route-regression-2026-03-04T21-01-31-612Z.json`
    - `reports/qa/route-regression-2026-03-04T21-03-46-698Z.json`
- Runtime profile activation proof:
  - Built runtime artifact at expected resolver location:
    - `bash scripts/qa/build-profile.sh --profile pan_europe_runtime --as-of 2026-01-01 --tier all --force`
    - output: `data/gtfs/runtime/pan_europe_runtime/2026-01-01/active-gtfs.zip`
  - Activated/reimported profile successfully:
    - `bash scripts/switch-gtfs.sh --profile pan_europe_runtime --api-url http://localhost:3000 --reimport --timeout-sec 600`
- MOTIS k8s blockers:
  - Cleared: local kind cluster now available (`kind-trainscanner` context, node ready).
  - Micro validation:
    - `bash scripts/run-motis-k8s-test.sh --mode micro --tier all --gtfs-path data/artifacts/pan_europe_runtime-all-2026-01-01.zip --bbox 50.28,8.74,50.40,8.95`
    - Result: pass (`job_complete=true`, `job_failed=false`, `motis_exit=0`, `tester_exit=0`)
    - Artifact dir: `data/motis-k8s/motis-micro-20260304222754-1160`
  - Macro validation (dataset-scoped query override):
    - default macro query suite fails on this local fixture because required named stations (`Berlin Hbf`, `Zürich HB`) are not present in current runtime artifact.
    - successful command:
      - `bash scripts/run-motis-k8s-test.sh --mode macro --tier all --gtfs-path data/artifacts/pan_europe_runtime-all-2026-01-01.zip --queries-json data/motis-k8s/motis-micro-20260304222754-1160/queries.json`
    - Result: pass (`job_complete=true`, `job_failed=false`, `motis_exit=0`, `tester_exit=0`)
    - Artifact dir: `data/motis-k8s/motis-macro-20260304222840-4180`

## Phase 6 Validation Evidence (2026-03-04)

- Legacy source-config purge:
  - Deleted `config/dach-data-sources.json`.
- Legacy path grep sweep:
  - `rg -n "canonical_stations|canonical_review_queue|build-canonical-stations|build-review-queue|group-aware-synthetic-journeys-from-canonical-stops" .`
  - Matches remain only in migration report docs under `reports/`.
- Canonical identifier sweep:
  - `rg -n "canonical_station_id|selected_station_ids|member_station_ids|target_canonical_station_id|source_canonical_station_id|skip-canonical|skip-review-queue" .`
  - No active-code matches.

## Runtime Environment Tuning Note (2026-03-05)

- Added machine-specific export tuning guide:
  - `reports/pan-europe-runtime-tuning.md`
- Purpose:
  - preserve tested batch/memory settings,
  - explain CPU/IO behavior on this hardware,
  - provide a repeatable sweet-spot benchmark procedure for future runs.

## Phase 5 Final Evidence (2026-03-06)

- Preflight:
  - stale export/build workers were cleared before each rerun.
  - PostGIS was recreated with `shm_size=1g` to allow safe parallel gather usage.
- Export tuning sweep (DE one-batch comparisons, `query-mode=optimized`, `parallel-gather-workers=6`):
  - `batch=40000` -> `24072.967 trips/min`
  - `batch=120000` -> `60665.021 trips/min`
  - `batch=200000` -> `90354.436 trips/min`
  - `batch=400000` -> `126104.155 trips/min`
  - `batch=500000` -> `158595.135 trips/min`
  - `batch=600000` -> `165092.834 trips/min`
  - `batch=800000` -> `192072.621 trips/min` but host memory/swap pressure was deemed unsafe for sustained local runs.
  - `batch=1000000` benchmark was intentionally interrupted due memory risk.
- Final machine-safe selection:
  - `batch-size-trips=500000`, `parallel-gather-workers=6`, PG options:
    - `work_mem=64MB`
    - `maintenance_work_mem=256MB`
    - `temp_buffers=32MB`
    - `max_parallel_workers=12`
    - `parallel_setup_cost=0`
    - `parallel_tuple_cost=0`
    - `min_parallel_table_scan_size=0`
    - `min_parallel_index_scan_size=0`
- Full uncapped build (selected settings):
  - command:
    - `bash scripts/qa/build-profile.sh --profile pan_europe_runtime --as-of 2026-03-04 --tier all --batch-size-trips 500000 --query-mode optimized --parallel-gather-workers 6 --progress-interval-sec 20 --force --pgoptions '-c work_mem=64MB -c maintenance_work_mem=256MB -c temp_buffers=32MB -c max_parallel_workers=12 -c parallel_setup_cost=0 -c parallel_tuple_cost=0 -c min_parallel_table_scan_size=0 -c min_parallel_index_scan_size=0'`
  - result:
    - artifact: `data/gtfs/runtime/pan_europe_runtime/2026-03-04/active-gtfs.zip`
    - manifest: `data/gtfs/runtime/pan_europe_runtime/2026-03-04/manifest.json`
    - runtime: `1055.94s` (`~17.6 min`)
    - throughput: `132845.786 trips/min`
    - trips: `2337963`
- Packaging fix applied during this pass:
  - exporter switched to ZIP64 writes for large files (`force_zip64=True`) after a `File size too large` failure on first 500k full attempt.
- Switch + reimport:
  - command:
    - `bash scripts/switch-gtfs.sh --profile pan_europe_runtime --api-url http://localhost:3000 --reimport --timeout-sec 600`
  - result: success (`state=ready`, profile activated).
- MOTIS micro:
  - command:
    - `bash scripts/run-motis-k8s-test.sh --mode micro --tier all --gtfs-path data/gtfs/runtime/pan_europe_runtime/2026-03-04/active-gtfs.zip --bbox 50.28,8.74,50.40,8.95`
  - result: pass (`motis_exit=0`, `tester_exit=0`)
  - artifacts:
    - `data/motis-k8s/motis-micro-20260306021236-3362`
- Route regression rerun:
  - command:
    - `bash scripts/qa/run-route-regression.sh --api-url http://localhost:3000 --report-dir reports/qa`
  - result: `total=2 passed=2 failed=0`
  - report:
    - `reports/qa/route-regression-2026-03-06T10-07-45-674Z.json`
- MOTIS macro:
  - command:
    - `bash scripts/run-motis-k8s-test.sh --mode macro --tier all --gtfs-path data/gtfs/runtime/pan_europe_runtime/2026-03-04/active-gtfs.zip`

## Phase 5 Closure Evidence (2026-03-08)

- Final full runtime export:
  - command:
    - `bash scripts/qa/build-profile.sh --profile pan_europe_runtime --as-of 2026-03-04 --tier all --batch-size-trips 100000 --query-mode optimized --parallel-gather-workers 6 --progress-interval-sec 20 --force --pgoptions '-c work_mem=64MB -c maintenance_work_mem=256MB -c temp_buffers=32MB -c max_parallel_workers=12 -c parallel_setup_cost=0 -c parallel_tuple_cost=0 -c min_parallel_table_scan_size=0 -c min_parallel_index_scan_size=0'`
  - result:
    - artifact: `data/gtfs/runtime/pan_europe_runtime/2026-03-04/active-gtfs.zip`
    - manifest: `data/gtfs/runtime/pan_europe_runtime/2026-03-04/manifest.json`
    - SHA256: `38924cc73434b050bcaa9f6f4a4a8b89a8b3c2007ce72a2a34999e58cda8491c`
    - runtime: `5287563.721 ms` (`~88.1 min`)
    - batching: `tripBatches=47`, `sourcesProcessed=2`
    - counts:
      - `trips=3451379`
      - `stopTimes=54315223`
      - `stops=647825`
      - `transfers=996514`
      - `routes=25436`
      - `services=200667`
      - `countries=2`
- Runtime activation:
  - command:
    - `bash scripts/switch-gtfs.sh --profile pan_europe_runtime --api-url http://localhost:3000 --reimport --timeout-sec 600`
  - result:
    - success; profile activated and MOTIS returned ready state through orchestrator health.
- Route regression:
  - command:
    - `bash scripts/qa/run-route-regression.sh --api-url http://localhost:3000 --report-dir reports/qa`
  - result: `total=2 passed=2 failed=0`
  - report:
    - `reports/qa/route-regression-2026-03-08T18-15-34-462Z.json`
- MOTIS micro validation:
  - initial blocker:
    - host RAM spike in `scripts/qa/prepare-motis-k8s-artifacts.py` for `--mode micro --tier all`
    - first k8s job also showed `motis-runner` `OOMKilled` at previous `6Gi` limit
  - fixes applied:
    - added streaming low-memory micro preparation path in `scripts/qa/prepare-motis-k8s-artifacts.py`
    - raised k8s `motis-runner` memory in `k8s/motis-testing/micro-job.template.yaml` from `6Gi` to `12Gi` (request `4Gi`)
  - validation:
    - standalone prep max RSS after streaming fix: `130228 kB` (`~127 MiB`)
  - successful command:
    - `bash scripts/run-motis-k8s-test.sh --mode micro --tier all --gtfs-path data/gtfs/runtime/pan_europe_runtime/2026-03-04/active-gtfs.zip --bbox 50.28,8.74,50.40,8.95 --timeout-sec 1200 --health-timeout-sec 240`
  - result: pass (`job_complete=true`, `job_failed=false`, `motis_exit=0`, `tester_exit=0`)
  - artifacts:
    - `data/motis-k8s/motis-micro-20260308193652-815`
- MOTIS macro validation:
  - fix carried forward:
    - raised k8s `motis-runner` memory in `k8s/motis-testing/macro-job.template.yaml` from `6Gi` to `12Gi` (request `4Gi`)
  - successful command:
    - `bash scripts/run-motis-k8s-test.sh --mode macro --tier all --gtfs-path data/gtfs/runtime/pan_europe_runtime/2026-03-04/active-gtfs.zip --timeout-sec 1200 --health-timeout-sec 240`
  - result: pass (`job_complete=true`, `job_failed=false`, `motis_exit=0`, `tester_exit=0`)
  - artifacts:
    - `data/motis-k8s/motis-macro-20260308194056-2688`
- Closure:
  - Phase 5 acceptance criteria are now satisfied on the real two-country runtime artifact.

## Phase 6 Closure Evidence (2026-03-08)

- Legacy runtime grep sweep rerun:
  - `rg -n "canonical_stations|canonical_review_queue|build-canonical-stations|build-review-queue|group-aware-synthetic-journeys-from-canonical-stops" .`
  - result:
    - matches remain only in migration documentation under `reports/`
    - no active runtime codepaths or scripts matched
- Final note:
  - active runtime, QA, export, and MOTIS validation paths are now pan-European only.
