## Project Notes For Future Agents

Scope: repository root only. Ignore `archive(ignore)/` for active implementation.

## Primary goal

Maintain and extend the MOTIS GTFS-switch MVP for fast dataset testing/debugging.

## Active architecture

- `orchestrator/`: plain Node.js API server and switch workflow
- `orchestrator/src/core/`: shared schema validation, errors, IDs, logging primitives
- `orchestrator/src/domains/`: domain contracts/modules (`source-discovery|ingest|canonical|export|switch-runtime|routing|qa`)
- `orchestrator/src/cli/`: thin CLI wrappers around tested modules
- `frontend/`: static UI (no framework) with route summary + MapLibre map
- `config/`: GTFS profile definitions
- `config/dach-data-sources.json`: official DACH source registry
- `config/ojp-endpoints.json`: OJP feeder endpoint/auth scaffolding
- `config/ojp-endpoints.mock.json`: local OJP mock fixture config for deterministic checks
- `scripts/data/`: DACH source fetch/verify + NeTEx ingest/canonical/QA/OJP/stitch scripts
- `scripts/qa/`: canonical -> GTFS runtime export, validation, and fixture QA scripts
- `db/migrations/`: PostGIS schema migrations for canonical station layer
- `.github/workflows/ojp-mock-feeder-check.yml`: CI smoke check for deterministic OJP mock probe
- `.github/workflows/qa-export-check.yml`: lightweight canonical export determinism check
- `.github/workflows/ci-pr.yml`: fast PR quality gates (lint/contracts/unit+integration)
- `.github/workflows/ci-nightly.yml`: full/nightly integration + e2e + report artifacts
- `docker-compose.yml`: optional `postgis` service (`dach-data` profile) with named volume persistence
- `state/`: switch lock, status, and logs (Note: Planned migration to Postgres `system_state` table; see `docs/state_migration_prompt.md`)
- `data/motis/`: generated MOTIS runtime data
- `data/gtfs/runtime/`: generated deterministic GTFS runtime artifacts
- `reports/qa/`: generated QA/regression reports
- `docs/curation_tool_prompt.md`: planned frontend Curation Tool prompt
- `docs/state_migration_prompt.md`: planned Postgres state migration prompt

## Core behavior that must remain true

- Frontend remains reachable while profile switching/restart is running.
- Only one switch can run at a time (lock file).
- Idempotent switch requests for the same in-flight/active profile return reused/noop semantics with `runId`.
- Switch states are persisted (`idle|switching|importing|restarting|ready|failed`).
- Active profile runtime marker is persisted in `state/active-gtfs.json` (legacy `config/active-gtfs.json` may exist).
- Route endpoint is blocked unless system state is `ready`.
- Station autocomplete comes from active GTFS profile.
- Route station inputs are normalized before MOTIS call.
- API error payloads expose machine-readable `errorCode`; responses carry `x-correlation-id`.
- Static GTFS profiles keep working unchanged; runtime descriptor profiles must resolve deterministically to concrete artifacts before activation.

## DACH data pipeline contract

- Scope includes:
- discovery/retrieval of official raw DACH sources
- NeTEx ingest into PostGIS staging
- canonical station build with provenance mapping
- Script entrypoints under `scripts/data/*.sh` remain stable and are thin wrappers to Node CLIs under `orchestrator/src/cli/`.
- Legacy shell implementations are retained in `scripts/data/*.legacy.sh` for compatibility while Node services are the default entry path.
- Pipeline jobs are idempotent by `(job_type, idempotency_key)` and pending states (`queued|retry_wait`) must be resumable.
- Pipeline job concurrency is configurable per `job_type` (`PIPELINE_JOB_MAX_CONCURRENT`) and must be enforced atomically.
- Multi-runner running-slot races must surface as `JOB_BACKPRESSURE` (not generic internal errors).
- Prefer NeTEx; GTFS requires explicit `fallbackReason` per source.
- No runtime auto-fallback from NeTEx to GTFS for the same source.
- Raw snapshots must stay local under `data/raw/<country>/<provider>/<format>/<YYYY-MM-DD>/`.
- Each fetch run must write a `manifest.json` with retrieval metadata + hash.
- PostGIS is mandatory for canonical layer (`canonical_stations`, `canonical_station_sources`).
- PostGIS curation/stitch prep tables are part of the same DACH slice (`canonical_review_queue`, `canonical_station_overrides`, `station_transfer_rules`, `ojp_stop_refs`).
- Selected `format=netex` ingest must fail hard on parse/source errors (non-zero exit).
- DACH scope remains `DE|AT|CH`.

## Canonical QA + curation contract

- Review queue generation must be deterministic per scope (`latest` or explicit `--as-of`).
- Manual overrides must be auditable in DB (`canonical_station_overrides`) and applied explicitly.
- Queue/report/override tooling must remain script-driven and reversible.
- A Frontend Curation Tool is planned to replace manual CSV curation (see `docs/curation_tool_prompt.md`).

## OJP + stitching boundary

- OJP feeder probing is configuration-driven via `config/ojp-endpoints.json` and `.env`; no secrets in repo.
- Stitching prototype is offline/service-layer only and outputs JSON for manual inspection.
- Do not wire OJP/stitching prototype into production `/api/routes` unless explicitly requested.

## MOTIS routing contract in this MVP

- `/api/routes` should resolve user input to MOTIS stop IDs in `tag_stopId` format.
- Default dataset tag is `active-gtfs`.
- Debug output should include `routeRequestResolved` and attempted MOTIS request variants.
- OJP/stitching prototype remains out of production `/api/routes`.

## Map stack contract

- Frontend map stack is **MapLibre GL JS**.
- Protomaps is preferred when a key is configured in `frontend/config.js`.
- Do not switch to Leaflet unless explicitly requested by the user.

## Key commands

- `scripts/run-test-env.sh --profile <name>`
- `scripts/stop-test-env.sh`
- `scripts/setup.sh --profile <name>`
- `scripts/up.sh --profile <name>`
- `scripts/init-motis.sh --profile <name>`
- `scripts/check-motis-data.sh`
- `scripts/validate-config.sh [--only profiles|dach|ojp|ojp-mock]`
- `scripts/switch-gtfs.sh --profile <name>`
- `scripts/find-working-route.sh --max-attempts <n>`
- `scripts/data/verify-dach-sources.sh`
- `scripts/data/fetch-dach-sources.sh --as-of <YYYY-MM-DD>`
- `scripts/data/db-migrate.sh`
- `scripts/data/ingest-netex.sh --country <DE|AT|CH> --as-of <YYYY-MM-DD>`
- `scripts/data/build-canonical-stations.sh --as-of <YYYY-MM-DD>`
- `scripts/data/report-canonical.sh`
- `scripts/data/build-review-queue.sh --as-of <YYYY-MM-DD>`
- `scripts/data/apply-station-overrides.sh [--csv /absolute/path/overrides.csv]`
- `scripts/data/report-review-queue.sh`
- `scripts/data/test-ojp-feeders.sh --country <DE|AT|CH>`
- `scripts/data/check-ojp-feeders-mock.sh`
- `scripts/data/run-stitch-prototype.sh --country <DE|AT|CH>`
- `scripts/qa/build-profile.sh --profile <name> --as-of <YYYY-MM-DD>`
- `scripts/qa/validate-export.sh --zip /absolute/or/relative/path/to/active-gtfs.zip`
- `scripts/qa/seed-export-fixture.sh --as-of <YYYY-MM-DD>`
- `scripts/qa/run-route-smoke.sh --api-url <url>`
- `scripts/qa/run-route-regression.sh --api-url <url>`
- `scripts/qa/report-pipeline-kpis.sh [--window-hours <n>] [--job-type <type>]`

## Documentation policy (required)

When behavior, endpoints, scripts, or map stack change, update all relevant docs in the same change:

- `README.md`
- `AGENTS.md`
- `frontend/AGENTS.md`
- `orchestrator/AGENTS.md`

Keep docs command-accurate and copy/paste runnable.
