## Project Notes For Future Agents

Scope: repository root only. Ignore `archive(ignore)/` for active implementation.

## Primary goal

Maintain and extend the MOTIS GTFS-switch MVP for fast dataset testing/debugging.

## Active architecture

- `orchestrator/`: plain Node.js API server and switch workflow
- `orchestrator/src/core/`: shared schema validation, errors, IDs, logging primitives
- `orchestrator/src/domains/`: domain contracts/modules (`source-discovery|ingest|canonical|export|switch-runtime|routing|qa`)
- `orchestrator/src/cli/`: thin CLI wrappers around tested modules
- `frontend/`: React + Vite multi-page UI (`/` + `/curation.html`) with route summary + MapLibre map + QA curation dashboard
- `config/`: GTFS profile definitions
- `config/dach-data-sources.json`: official DACH source registry
- `config/ojp-endpoints.json`: OJP feeder endpoint/auth scaffolding
- `config/ojp-endpoints.mock.json`: local OJP mock fixture config for deterministic checks
- `scripts/data/`: DACH source fetch/verify + NeTEx ingest/canonical/QA/OJP/stitch scripts
- `scripts/qa/`: canonical -> GTFS runtime export, validation, and fixture QA scripts
- `db/migrations/`: PostGIS schema migrations for canonical station layer + `system_state`
- `.github/workflows/ojp-mock-feeder-check.yml`: CI smoke check for deterministic OJP mock probe
- `.github/workflows/qa-export-check.yml`: lightweight canonical export determinism check
- `.github/workflows/ci-pr.yml`: fast PR quality gates (lint/contracts/unit+integration)
- `.github/workflows/ci-nightly.yml`: full/nightly integration + e2e + report artifacts
- `docker-compose.yml`: optional `postgis` service (`dach-data` profile) with named volume persistence
- Docker orchestrator build context is repo root so runtime image includes `orchestrator/`, `scripts/`, and `db/` paths used by QA refresh pipeline.
- `state/`: switch lock, status, and logs (filesystem fallback when `system_state` DB persistence is unavailable)
- `data/motis/`: generated MOTIS runtime data
- `data/gtfs/runtime/`: generated deterministic GTFS runtime artifacts
- `reports/qa/`: generated QA/regression reports
- `docs/curation_tool_prompt.md`: planned richer curation workflow prompt
- `docs/state_migration_prompt.md`: migration notes for moving remaining state flows fully into Postgres

## Core behavior that must remain true

- Frontend remains reachable while profile switching/restart is running.
- Only one switch can run at a time (lock file).
- Idempotent switch requests for the same in-flight/active profile return reused/noop semantics with `runId`.
- Switch states are persisted (`idle|switching|importing|restarting|ready|failed`) primarily in PostGIS `system_state.gtfs_switch_status`, with filesystem fallback.
- Active profile runtime marker is persisted primarily in PostGIS `system_state.active_gtfs`, with fallback in `state/active-gtfs.json` (legacy `config/active-gtfs.json` may exist).
- Route endpoint is blocked unless system state is `ready`.
- Station autocomplete comes from active GTFS profile.
- Route station inputs are normalized before MOTIS call.
- API error payloads expose machine-readable `errorCode`; responses carry `x-correlation-id`.
- Static GTFS profiles keep working unchanged; runtime descriptor profiles must resolve deterministically to concrete artifacts before activation.
- QA curation endpoints remain separate from production routing flow.
- Station-review API is v2-only for curation: `GET /api/qa/v2/clusters`, `GET /api/qa/v2/clusters/:cluster_id`, `POST /api/qa/v2/clusters/:cluster_id/decisions`.
- Curated projection read endpoints are additive for reviewer-confirmed entities: `GET /api/qa/v2/curated-stations`, `GET /api/qa/v2/curated-stations/:curated_station_id`.
- Refresh endpoint contracts remain stable: `POST /api/qa/jobs/refresh`, `GET /api/qa/jobs/:job_id`.
- Curation refresh job runs asynchronously and must not block the frontend; duplicate trigger requests should reuse the active `qa.refresh-pipeline` job.
- Refresh job polling payloads should expose enough checkpoint metadata for live frontend progress (including source download bytes during `fetching_sources`).

## DACH data pipeline contract

- Scope includes:
- discovery/retrieval of official raw DACH sources
- NeTEx ingest into PostGIS staging
- canonical station build with provenance mapping
- Script entrypoints under `scripts/data/*.sh` remain stable and are thin wrappers to Node CLIs under `orchestrator/src/cli/`.
- Pipeline jobs are idempotent by `(job_type, idempotency_key)` and pending states (`queued|retry_wait`) must be resumable.
- Pipeline job concurrency is configurable per `job_type` (`PIPELINE_JOB_MAX_CONCURRENT`) and must be enforced atomically.
- Multi-runner running-slot races must surface as `JOB_BACKPRESSURE` (not generic internal errors).
- Prefer NeTEx; GTFS requires explicit `fallbackReason` per source.
- No runtime auto-fallback from NeTEx to GTFS for the same source.
- Raw snapshots must stay local under `data/raw/<country>/<provider>/<format>/<YYYY-MM-DD>/`.
- Each fetch run must write a `manifest.json` with retrieval metadata + hash.
- PostGIS is mandatory for canonical layer (`canonical_stations`, `canonical_station_sources`).
- PostGIS curation/stitch prep tables are v2-first (`canonical_review_queue`, `qa_station_clusters_v2`, `qa_station_cluster_candidates_v2`, `qa_station_cluster_evidence_v2`, `qa_station_cluster_decisions_v2`, `qa_station_groups_v2`, `qa_station_group_sections_v2`, `qa_station_group_section_members_v2`, `qa_station_group_section_links_v2`, `qa_station_display_names_v2`, `qa_station_complexes_v2`, `qa_station_segments_v2`, `canonical_line_identities_v2`, `station_transfer_rules`, `ojp_stop_refs`).
- Curated projection foundation tables are additive and decision-linked (`qa_curated_stations_v1`, `qa_curated_station_members_v1`, `qa_curated_station_lineage_v1`, `qa_curated_station_field_provenance_v1`) to keep reviewer-confirmed entities separate from immutable source/canonical inputs.
- Selected `format=netex` ingest must fail hard on parse/source errors (non-zero exit).
- DACH defaults remain `DE|AT|CH` for source discovery/ingest workflows, while v2 curation schema/endpoints are ISO alpha-2 ready.

## Canonical QA + curation contract

- Review queue and v2 cluster generation must be deterministic per scope (`latest` or explicit `--as-of`).
- V2 naming decisions must be auditable (`qa_station_naming_overrides_v2`) and linked to cluster decisions.
- Queue/report tooling must remain script-driven and reversible.
- Frontend curation dashboard (`/curation.html`) is cluster-first over `/api/qa/v2/clusters`, `/api/qa/v2/clusters/:cluster_id`, and `/api/qa/v2/clusters/:cluster_id/decisions`.
- Frontend curation dashboard UX should remain reviewer-first (plain-language edit controls, selected-node visibility, staged merge/split/group editing, inline rename pencil actions).
- V2 `merge` decisions should create new curated entities (`qa_curated_*_v1`) and must not require a user-selected merge target.
- V2 decisions must not write legacy `canonical_station_overrides`.
- Decision-member audit rows for merge should use neutral member semantics (`action=merge_member`) instead of source/target roles.
- Curated merged/grouped entities should appear inline in the candidate list as expandable derived cards with composition/provenance.
- When derived `merge` items are present, member candidates should be hidden from standalone candidate cards/markers and remain visible only inside the derived card `Members (N)` disclosure.
- Curation map candidate markers that share the same coordinates should remain at exact coordinates and be visually disambiguated with concentric selectable rings.
- Curation should expose segment walking-link authoring in the edit workflow (preset + manual pair mode) and persist links through v2 cluster decisions/group modeling.
- Candidate provenance/coverage should stay explicit in UI (`provider/source labels` + `service_context.completeness` notes).
- Curation list filters (`country|status|scope_tag`) are cluster-browsing controls only; refresh pipeline scope is a CLI/API concern.
- Deprecated linked queue-item blocks should stay out of v2 cluster list/detail API payloads and out of curation UI.
- CSV/script workflows remain supported; planned richer curation flow is tracked in `docs/curation_tool_prompt.md`.

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
- Protomaps is preferred when a key is configured in `frontend/public/config.js`.
- Curation map supports a runtime basemap toggle (`default` and `satellite`) and persists mode in browser session storage.
- Do not switch to Leaflet unless explicitly requested by the user.

## Key commands

- `npm run dev -- --profile <name>`
- `npm run stop`
- `npm run switch -- --profile <name>`
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
- `scripts/data/run-station-review-pipeline.sh [--country <DE|AT|CH>] [--as-of <YYYY-MM-DD>] [--skip-migrate]`
- `scripts/data/refresh-station-review.sh [--country <DE|AT|CH>] [--as-of <YYYY-MM-DD>] [--source-id <id>]`
- `scripts/data/report-review-queue.sh`
- `scripts/data/reset-station-review-data.sh --yes`
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
- `docs/AGENTS.md`
- `docs/documentation-standard.md`
- `tests/AGENTS.md` (when test contract text changes)

Keep docs command-accurate and copy/paste runnable.
