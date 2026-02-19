# Trainscanner Refactor Plan

Date: 2026-02-19

## Goals

Move from MVP-style implementation to production-grade architecture for:

- DACH dataset pipeline (source discovery, ingest, canonicalization, export, QA)
- GTFS runtime switching/orchestration
- MOTIS routing orchestration

without breaking current operator workflows.

## Target Architecture

### Domain modules

A shared domain layer is introduced under `orchestrator/src/domains/` with explicit boundaries:

- `source-discovery`: DACH source config contracts, policy checks, deterministic source selection
- `ingest`: ingest option contracts and run metadata contracts
- `canonical`: canonical build/review scope contracts
- `export`: deterministic export contracts + manifest/hash normalization
- `switch-runtime`: profile/state/contracts + idempotent switch jobs
- `routing`: station input normalization, station index loading, route request resolution
- `qa`: route smoke/regression contracts and report writers

### Core platform modules

A shared core layer under `orchestrator/src/core/` provides:

- schema-based validation primitives
- structured logging with correlation/run IDs
- explicit error taxonomy (`errorCode` + HTTP mapping)
- deterministic primitives (atomic JSON IO, stable hashing helpers)

### Runtime/API layer

`orchestrator/src/server.js` remains the entrypoint for backward compatibility, but uses extracted domain/core modules.

### Script layer

Existing shell entrypoints remain stable (`scripts/...`) but become thin wrappers around tested modules where practical.

## Phased Migration

### Phase 1 (implemented in this change)

Foundation + runtime hardening:

- Add core error taxonomy and machine-readable API error payloads
- Add schema-based config/env validation for orchestrator runtime and config files used by runtime scripts
- Add structured logger with correlation/run IDs
- Extract and harden `switch-runtime` domain (idempotent activation semantics + stable state transitions)
- Extract and harden `routing` domain (normalization + station resolution as tested module)
- Preserve all existing endpoints and status vocabulary

### Phase 2 (implemented in this change)

Pipeline/QA modularization + deterministic regression harness:

- Introduce domain contract modules for `source-discovery`, `ingest`, `canonical`, `export`, `qa`
- Move duplicated ad-hoc script logic into reusable Node modules/CLIs
- Add deterministic route regression suite with fixtures:
  - `tests/routes/smoke_cases.json`
  - `tests/routes/regression_cases.json`
  - `tests/routes/baselines/*.json`
- Add report output contract under `reports/qa/`
- Add CI quality gates (fast PR + nightly full)

### Phase 3 (deferred)

Deep pipeline runtime decomposition:

- Replace remaining large shell flows (`fetch-dach-sources.sh`, `verify-dach-sources.sh`, `ingest-netex.sh`, canonical tooling) with thin wrappers over domain services
- Add typed DB access adapters for all pipeline SQL flows
- Add retry/backoff orchestration across fetch/ingest/canonical/export jobs with resume markers

### Phase 4 (deferred)

Operational hardening:

- distributed locking/backpressure for multi-runner environments
- richer metrics export and centralized log sink integration
- performance optimization for large-station indexes and high-concurrency routing

## Backward Compatibility Strategy

- Keep all existing shell command names and core API endpoints unchanged
- Keep status values unchanged: `idle|switching|importing|restarting|ready|failed`
- Preserve `state/active-gtfs.json` as source of truth; continue legacy migration from `config/active-gtfs.json`
- Preserve Map stack (MapLibre) and frontend contracts
- Maintain NeTEx-first policy; GTFS fallback remains explicit-by-config only

## Rollback Strategy

- Module extraction is additive and behind compatibility wrappers
- In case of failure, revert to previous scripts/server behavior by:
  - restoring pre-refactor `orchestrator/src/server.js` + switcher module
  - disabling new CI jobs while keeping legacy workflows
- No destructive data migration introduced in Phase 1/2
- State files remain backward-readable JSON

## Risk Matrix

| Risk | Impact | Likelihood | Mitigation |
|---|---|---:|---|
| Contract mismatch between scripts and new modules | High | Medium | Keep wrappers thin, add integration tests for CLI contracts |
| Behavioral drift in switch readiness gating | High | Medium | Add integration/e2e tests covering switch + `/api/routes` gating |
| Schema validation too strict for existing configs | Medium | Medium | Normalize legacy shapes and emit explicit validation errors |
| CI runtime growth | Medium | Medium | Split fast PR and full/nightly suites |
| Determinism regressions in export/route baselines | High | Low | Hash-based determinism tests + committed baselines |

## Quality Gates

Fast PR pipeline:

- lint/syntax checks
- contract validation checks
- unit tests
- minimal integration checks

Nightly/full pipeline:

- full integration + e2e + regression checks
- report artifact publication from `reports/qa/`

## Acceptance Mapping

- Plan document: this file (`docs/refactor-plan.md`)
- Phase 1/2 code + tests: included in this change
- Determinism checks: export hash tests + route baseline regression tests
- Docs policy: updates included for `README.md`, `AGENTS.md`, `orchestrator/AGENTS.md`, `frontend/AGENTS.md`
