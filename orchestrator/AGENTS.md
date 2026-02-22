# Orchestrator Agent Notes

## Runtime

- Plain Node.js (no heavy framework).
- Entry point: `orchestrator/src/server.js`.
- Repo-root startup shortcuts are available via `npm run dev -- --profile <name>` and `npm run stop` (wrappers around `scripts/*.sh`).

## Core files

- `src/server.js`: HTTP API + static hosting (`frontend/dist`) + station resolution
- `src/switcher.js`: profile switch state machine
- `src/profile-resolver.js`: profile normalization + static/runtime artifact resolution
- `src/motis.js`: MOTIS health/route calls + Docker restart API
- `src/lock.js`: filesystem lock (`state/gtfs-switch.lock`) with stale-lock cleanup
- `src/config.js`: env/file path configuration (default `FRONTEND_DIR=frontend/dist`)
- `src/core/`: shared schema validation, error taxonomy, ID/correlation helpers
- `src/core/job-orchestrator.js`: idempotent DB-backed pipeline job orchestration with retry/resume
- `src/core/circuit-breaker.js`: transient-failure circuit breaker for external calls
- `src/core/metrics.js`: in-process Prometheus metric collectors
- `src/domains/`: domain boundaries (`source-discovery`, `ingest`, `canonical`, `export`, `switch-runtime`, `routing`, `qa`)
- `src/cli/`: thin wrappers for config validation, profile runtime helpers, route regression, and DACH data flows
- `src/core/pipeline-runner.js`: shared runId/logging wrapper for CLI-triggered pipeline commands
- `src/data/postgis/system-state.js`: DB-backed `system_state` read/write helpers with graceful fallback behavior upstream
- `src/domains/source-discovery/service.js`: service entrypoints for fetch/verify flows
- `src/domains/ingest/service.js`: service entrypoint for NeTEx ingest flow
- `src/domains/canonical/service.js`: service entrypoints for canonical build + review queue build
- `src/domains/qa/service.js`: service entrypoint for review queue reporting
- `src/domains/qa/api.js`: v2 cluster decision endpoints + async curation refresh pipeline job API
- `src/domains/qa/v2-contracts.js`: v2 cluster decision normalization helpers
- `src/domains/qa/curated-projection.js`: additive curated-entity projection builder/writer (decision dual-write foundation)
- Container runtime expects repo-root layout (`/app/orchestrator`, `/app/scripts`, `/app/db`) for script execution paths.

## API contracts

- `GET /api/gtfs/profiles`
- `POST /api/gtfs/activate` body `{ "profile": "..." }`
- `GET /api/gtfs/status`
- `GET /api/gtfs/stations`
- `GET /api/qa/v2/clusters`
- `GET /api/qa/v2/clusters/:cluster_id`
- `POST /api/qa/v2/clusters/:cluster_id/decisions`
- `GET /api/qa/v2/curated-stations`
- `GET /api/qa/v2/curated-stations/:curated_station_id`
- `POST /api/qa/jobs/refresh`
- `GET /api/qa/jobs/:job_id`
- `GET /health`
- `GET /metrics`
- `POST /api/routes`

Error contract:

- failures include `errorCode`
- responses include `x-correlation-id` header

## `/api/routes` expectations

- Enforce `ready` state before querying MOTIS.
- Normalize station input against active profile station index.
- Prefer MOTIS stop IDs in `tag_stopId` format.
- Default dataset tag comes from `MOTIS_DATASET_TAG` (default `active-gtfs`).
- Return `routeRequestResolved` and recent `motisAttempts` for debugging.

## State machine contract

- Valid states: `idle|switching|importing|restarting|ready|failed`.
- Persist every transition primarily to PostGIS `system_state.gtfs_switch_status`; fallback to `state/gtfs-switch-status.json` when DB persistence is unavailable.
- Persist active profile marker primarily to PostGIS `system_state.active_gtfs`; fallback to `state/active-gtfs.json` (legacy config path auto-migrates).
- Prevent concurrent switches with lock file and clear stale locks when safe.
- Switch requests are idempotent for same in-flight/active profile (`reused`/`noop`, `runId`).
- Keep static profile activation behavior backward-compatible while supporting runtime descriptor profiles (`runtime.mode=canonical-export`).
- When runtime descriptors are used, resolve to concrete artifact paths deterministically before copying to `data/motis/active-gtfs.zip`.

## Editing rules

- Keep behavior deterministic and debuggable.
- Avoid introducing async races in switch flow.
- Keep domain contracts validated at module boundaries.
- If request mapping changes, update docs (`README.md` + AGENTS files) in same change.

## DACH data-pipeline boundary

- `scripts/data/fetch-dach-sources.sh`, `scripts/data/verify-dach-sources.sh`, `scripts/data/db-migrate.sh`, `scripts/data/ingest-netex.sh`, and `scripts/data/build-canonical-stations.sh` are separate from orchestrator runtime.
- `scripts/data/build-review-queue.sh`, `scripts/data/run-station-review-pipeline.sh`, `scripts/data/refresh-station-review.sh`, `scripts/data/report-review-queue.sh`, `scripts/data/reset-station-review-data.sh`, `scripts/data/test-ojp-feeders.sh`, `scripts/data/check-ojp-feeders-mock.sh`, and `scripts/data/run-stitch-prototype.sh` are also separate from orchestrator runtime.
- `scripts/data/build-review-queue.sh` materializes deterministic queue rows plus v2 cluster tables in one run.
- DACH script entrypoints are stable wrappers; Node CLIs/services are the default orchestration path.
- Pipeline jobs are DB-backed (`pipeline_jobs`) and idempotent by `(job_type, idempotency_key)`; duplicate in-flight/completed starts must reuse prior outcome.
- Pending pipeline job rows (`queued|retry_wait`) are resumable for the same idempotency key and must continue from stored checkpoint context.
- Per-`job_type` running limits come from `PIPELINE_JOB_MAX_CONCURRENT` and must be enforced atomically in DB claim logic.
- Running-slot races while claiming `status=running` must be surfaced as `JOB_BACKPRESSURE`.
- Do not couple DACH retrieval/ingest/canonical build into `/api/routes` or GTFS switch flow in this MVP slice.
- Keep curation endpoints (`/api/qa/v2/clusters`, `/api/qa/v2/clusters/:cluster_id`, `/api/qa/v2/clusters/:cluster_id/decisions`, `/api/qa/jobs/refresh`, `/api/qa/jobs/:job_id`) and curation dashboard concerns out of `/api/routes` and GTFS switch flow.
- Keep cluster list/detail payloads free of deprecated linked queue-item blocks; queue resolution remains backend-internal.
- Keep backend payloads stable for guided frontend UX; avoid forcing raw/internal IDs as primary user-facing labels in curation responses.
- Preserve raw candidate coordinates in cluster detail payloads; frontend map disambiguates same-coordinate nodes as concentric rings without coordinate offsets.
- Keep curated projection membership (`GET /api/qa/v2/curated-stations`) and cluster candidate IDs stable so frontend can hide merge-member candidates from standalone cards/markers and show them under derived `Members (N)` disclosures only.
- Frontend curation is cluster/edit focused; pipeline refresh execution belongs to terminal tooling (`scripts/data/run-station-review-pipeline.sh`, `scripts/data/refresh-station-review.sh`) or API automation.
- V2 cluster decision groups may carry `segment_action.walk_links`; API must validate cluster scope and upsert `qa_station_segment_links_v2` atomically with the decision.
- Decision endpoint should accept one synthesized staged-editor payload (`operation=merge|split`) and optional `rename_targets`, with atomic writes.
- Group operations persist into `qa_station_groups_v2` + section/link tables and are exported as group-aware GTFS stop structures.
- V2 decision writes also dual-write additive curated projection rows (`qa_curated_*_v1`) so reviewer-confirmed entities remain separate from raw/canonical ingestion state.
- V2 `merge` decisions represent creation of a new curated entity; do not require explicit merge-target selection in API contracts.
- V2 decisions must not write legacy `canonical_station_overrides`.
- Merge decision-member audit rows should be stored as neutral `merge_member` actions (no target/source role split).
- `GET /api/qa/jobs/:job_id` should include checkpoint-derived progress for automation/ops polling, including live `download_progress` during `fetching_sources`.
- `POST /api/qa/jobs/refresh` scope (`country`, optional) is execution scope only; do not couple it to cluster list filters (`country|status|scope_tag`) in API contracts.
- Do not couple OJP feeder probing or stitching prototype into `/api/routes` or GTFS switch flow in this MVP slice.
- Canonical -> GTFS runtime export is script-driven in `scripts/qa/`; orchestrator consumes produced artifacts only.
