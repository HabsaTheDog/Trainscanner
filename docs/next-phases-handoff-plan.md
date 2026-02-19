# Next Phases Handoff Plan (For Implementation Agent)

Date: 2026-02-19
Owner handoff target: next implementing agent
Prerequisite: Phase 1 and Phase 2 are complete (`docs/refactor-plan.md`)

## Implementation Status (2026-02-19)

- Phase 3A delivered: script entrypoints under `scripts/data/` are wrapper-only and forward to Node CLIs/services.
- Phase 3B delivered: PostGIS client + repository layer exists under `orchestrator/src/data/postgis/` with parameterized queries, transaction helper, and schema-validated return shapes.
- Phase 3C delivered: `pipeline_jobs` migration + DB-backed idempotent/retry/resume orchestrator in `orchestrator/src/core/job-orchestrator.js`.
- Phase 3D delivered: deterministic/e2e suites run in orchestrator tests and nightly CI publishes QA artifacts.
- Phase 4A delivered: `/metrics`, structured event schema fields, and pipeline KPI reporting.
- Phase 4B delivered: DB-backed multi-runner protection (`pipeline_jobs` uniqueness + backpressure) and source-discovery circuit breaker.
- Phase 4C delivered (MVP slice): station-index cache hardening + DB index coverage and nightly determinism gates.

## Mission

Implement deferred Phase 3 and Phase 4 work without breaking current contracts:

- Keep existing API endpoints and shell command names stable
- Keep switch states unchanged: `idle|switching|importing|restarting|ready|failed`
- Keep Map stack as MapLibre
- Keep NeTEx-first policy and explicit GTFS fallback policy

## Current baseline (already in place)

- Core/shared modules exist under `orchestrator/src/core/`
- Domain contract modules exist under `orchestrator/src/domains/`
- CI fast and nightly workflows exist
- Route smoke/regression fixtures exist under `tests/routes/`
- Required docs policy is enforced in CI

## Phase 3 Implementation Plan

### Phase 3A: Replace large shell logic with domain services

Goal: move remaining heavy logic from shell scripts into tested Node modules/CLIs while keeping script entrypoints as wrappers.

Scope:

- `scripts/data/fetch-dach-sources.sh`
- `scripts/data/verify-dach-sources.sh`
- `scripts/data/ingest-netex.sh`
- `scripts/data/build-canonical-stations.sh`
- `scripts/data/build-review-queue.sh`
- `scripts/data/report-review-queue.sh`

Target modules to add:

- `orchestrator/src/domains/source-discovery/service.js`
- `orchestrator/src/domains/ingest/service.js`
- `orchestrator/src/domains/canonical/service.js`
- `orchestrator/src/domains/qa/service.js`
- CLI wrappers in `orchestrator/src/cli/` for each flow

Required result:

- Shell scripts only parse args and forward to CLI wrappers
- Business logic and orchestration live in Node modules
- Error codes are machine-readable and logged with `runId`

Tests required:

- Unit tests for each service module
- Integration tests for each CLI wrapper contract

Definition of done:

- Existing command names and flags still work
- Script behavior matches previous outputs/errors for normal paths
- New code path is default (no feature flag fallback left)

### Phase 3B: Typed PostGIS data access layer

Goal: remove large inline SQL blocks from orchestration code and centralize DB access.

Target structure:

- `orchestrator/src/data/postgis/client.js`
- `orchestrator/src/data/postgis/repositories/`
  - `import-runs-repo.js`
  - `raw-snapshots-repo.js`
  - `netex-stops-repo.js`
  - `canonical-stations-repo.js`
  - `review-queue-repo.js`

Requirements:

- Parameterized queries only
- Shared transaction helper (`withTransaction`)
- Stable return shapes validated with schema checks

Tests required:

- Integration tests against local `postgis` service
- Migration compatibility tests (`db/migrations/*.sql` + repository calls)

Definition of done:

- No large SQL heredocs remain in pipeline orchestration modules
- Repositories cover all used DB operations in pipeline flows

### Phase 3C: Idempotent job orchestration + retry/resume

Goal: make fetch/ingest/canonical/export jobs resumable and idempotent.

Additions:

- New migration(s): `db/migrations/003_job_orchestration.sql` (or equivalent)
- Job table(s) with:
  - `job_id`, `job_type`, `idempotency_key`, `status`, `attempt`, `started_at`, `ended_at`, `error_code`, `error_message`, `run_context`
- Orchestration helper module:
  - `orchestrator/src/core/job-orchestrator.js`

Behavior rules:

- Same idempotency key returns existing in-flight/completed job outcome
- Retry strategy: bounded exponential backoff for transient errors
- Resume strategy: continue from last durable checkpoint when possible

Tests required:

- Unit tests for retry/backoff logic
- Integration tests for idempotency key reuse and resume behavior

Definition of done:

- Re-running the same export/ingest job is safe and deterministic
- Failures record structured job state and error code

### Phase 3D: Full pipeline e2e + determinism regression

Goal: verify end-to-end path with stronger automated coverage.

Add e2e coverage for:

- fetch/verify
- ingest
- canonical build
- export
- activate profile
- route smoke

Add deterministic checks:

- Same scope/input => same artifact hash
- Stable review queue output ordering for same `--as-of`

Artifacts:

- JSON reports written to `reports/qa/`

Definition of done:

- Nightly workflow runs full e2e/regression and publishes reports
- Determinism checks fail on drift

## Phase 4 Implementation Plan

### Phase 4A: Observability and telemetry hardening

Add:

- Metrics endpoint (`/metrics`) for orchestrator
- Standard event schema for logs (service, runId, correlationId, errorCode, latency)
- Pipeline KPI reports (duration, throughput, failure rate)

Tests:

- Unit tests for metric collectors
- Integration tests for metrics exposure

### Phase 4B: Multi-runner safety and backpressure

Add:

- Distributed lock support for pipeline jobs (DB lock or external lock)
- Queue/backpressure limits for concurrent job types
- Circuit breakers around external source/OJP calls

Tests:

- Concurrency tests simulating multiple job start attempts

### Phase 4C: Performance + scale

Add:

- Station index caching improvements for large GTFS files
- DB index review and query plan tuning
- Memory/CPU profiling for ingest and canonical build

Targets:

- route lookup latency p95 under defined threshold
- canonical build runtime reduced for large snapshots

## CI rollout plan for next agent

1. Extend fast PR workflow to include new unit suites for service/repository layers.
2. Extend nightly workflow with full DB-backed e2e orchestration tests.
3. Keep docs update contract gate active.
4. Publish QA artifacts from all new long-running suites.

## Risk controls

- Keep wrappers backward-compatible until equivalent tests pass.
- Do not remove legacy paths before contract test parity.
- For each migrated script, maintain a temporary compatibility snapshot test.

## Handoff checklist for implementation agent

1. Read `docs/refactor-plan.md` and this file.
2. Implement Phase 3A one script family at a time (source-discovery -> ingest -> canonical -> qa).
3. Add repository layer before refactoring canonical SQL-heavy flows.
4. Introduce job orchestration migration after repository layer is stable.
5. Land Phase 3 with passing fast + nightly CI.
6. Start Phase 4 only after Phase 3 determinism/e2e gates are green.

## Required docs update with each phase

When behavior/contracts/scripts change, update in same PR:

- `README.md`
- `AGENTS.md`
- `orchestrator/AGENTS.md`
- `frontend/AGENTS.md`
