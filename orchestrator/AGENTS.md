# Orchestrator Agent Notes

## Runtime

- Plain Node.js (no heavy framework).
- Entry point: `orchestrator/src/server.js`.

## Core files

- `src/server.js`: HTTP API + static hosting + station resolution
- `src/switcher.js`: profile switch state machine
- `src/profile-resolver.js`: profile normalization + static/runtime artifact resolution
- `src/motis.js`: MOTIS health/route calls + Docker restart API
- `src/lock.js`: filesystem lock (`state/gtfs-switch.lock`) with stale-lock cleanup
- `src/config.js`: env/file path configuration
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
- `src/domains/qa/api.js`: review queue/override handlers + async curation refresh pipeline job API
- Container runtime expects repo-root layout (`/app/orchestrator`, `/app/scripts`, `/app/db`) for legacy script execution paths.

## API contracts

- `GET /api/gtfs/profiles`
- `POST /api/gtfs/activate` body `{ "profile": "..." }`
- `GET /api/gtfs/status`
- `GET /api/gtfs/stations`
- `GET /api/qa/queue`
- `POST /api/qa/overrides`
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
- `scripts/data/build-review-queue.sh`, `scripts/data/apply-station-overrides.sh`, `scripts/data/report-review-queue.sh`, `scripts/data/test-ojp-feeders.sh`, `scripts/data/check-ojp-feeders-mock.sh`, and `scripts/data/run-stitch-prototype.sh` are also separate from orchestrator runtime.
- DACH script entrypoints are stable wrappers; Node CLIs/services are the default orchestration path and invoke `scripts/data/*.legacy.sh` compatibility implementations.
- Pipeline jobs are DB-backed (`pipeline_jobs`) and idempotent by `(job_type, idempotency_key)`; duplicate in-flight/completed starts must reuse prior outcome.
- Pending pipeline job rows (`queued|retry_wait`) are resumable for the same idempotency key and must continue from stored checkpoint context.
- Per-`job_type` running limits come from `PIPELINE_JOB_MAX_CONCURRENT` and must be enforced atomically in DB claim logic.
- Running-slot races while claiming `status=running` must be surfaced as `JOB_BACKPRESSURE`.
- Do not couple DACH retrieval/ingest/canonical build into `/api/routes` or GTFS switch flow in this MVP slice.
- Keep curation endpoints (`/api/qa/queue`, `/api/qa/overrides`, `/api/qa/jobs/refresh`, `/api/qa/jobs/:job_id`) and curation dashboard concerns out of `/api/routes` and GTFS switch flow.
- `GET /api/qa/jobs/:job_id` should include checkpoint-derived progress suitable for frontend UX, including live `download_progress` during `fetching_sources`.
- Do not couple OJP feeder probing or stitching prototype into `/api/routes` or GTFS switch flow in this MVP slice.
- Canonical -> GTFS runtime export is script-driven in `scripts/qa/`; orchestrator consumes produced artifacts only.
