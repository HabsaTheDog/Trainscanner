# MOTIS GTFS Switch

 GTFS profile switching and route debugging with MOTIS.

## Scope

- Active code is in repo root.
- `archive(ignore)/` is historical and not part of the active runtime.

## What works

- MOTIS in Docker (`motis`)
- Orchestrator API + React/Vite frontend (`frontend`, served by `orchestrator`)
- Named GTFS profiles via `config/gtfs-profiles.json`
- Active profile runtime state in PostGIS `system_state.active_gtfs` with filesystem fallback (`state/active-gtfs.json`, legacy `config/active-gtfs.json` auto-migration)
- Switch state machine with lock + status persistence
- Idempotent switch semantics with per-run IDs (`runId`) and deterministic status persistence
- Route query endpoint with station-resolution and MOTIS adapter
- Structured logs with correlation IDs and machine-readable API error codes (`errorCode`)
- Frontend profile switcher, status badge, autocomplete, route summary, map, raw JSON, and QA curation dashboard (`/curation.html`, cluster-first v2 workflow)
- Schema-based config validation for GTFS profiles, DACH sources, and OJP endpoint configs

## Quickstart

Main command (recommended):

```bash
npm run dev -- --profile sample_de
```

Direct script form (equivalent):

```bash
scripts/run-test-env.sh --profile sample_de
```

Open:

- Frontend: `http://localhost:3000`
- MOTIS direct: `http://localhost:8080`

Stop:

```bash
npm run stop
```

Direct script form:

```bash
scripts/stop-test-env.sh
```

## GTFS profiles

Edit `config/gtfs-profiles.json`:

```json
{
  "profiles": {
    "sample_de": { "zipPath": "data/gtfs/de_fv.zip" },
    "de_full": { "zipPath": "data/gtfs/de_full.zip" },
    "canonical_de_runtime": {
      "runtime": {
        "mode": "canonical-export",
        "profile": "canonical_de_runtime",
        "asOf": "latest",
        "country": "DE"
      }
    }
  }
}
```

Profile entry modes:

- Static: `zipPath` or `zip`
- Runtime descriptor: `runtime.mode=canonical-export` (+ optional `profile`, `asOf`, `country`, `artifactPath`)

Runtime descriptor resolution keeps static behavior backward-compatible and lets switch/init consume canonical export artifacts from `data/gtfs/runtime/...`.

## Runtime state

- `system_state` (PostGIS): primary persistence for `gtfs_switch_status` and `active_gtfs` when DB connectivity is available
- `state/gtfs-switch-status.json`: filesystem fallback for switch status (`idle|switching|importing|restarting|ready|failed`) with `runId` + `requestedProfile`
- `state/gtfs-switch.lock`: concurrency lock
- `state/gtfs-switch.log`: step logs and failures
- `state/active-gtfs.json`: filesystem fallback marker for active GTFS profile (legacy `config/active-gtfs.json` auto-migrates)
- `data/motis/`: generated MOTIS runtime data

## API

### `GET /api/gtfs/profiles`

```bash
curl -s http://localhost:3000/api/gtfs/profiles | jq
```

### `POST /api/gtfs/activate`

```bash
curl -s -X POST http://localhost:3000/api/gtfs/activate \
  -H 'Content-Type: application/json' \
  -d '{"profile":"sample_de"}' | jq
```

Response behavior:

- New switch accepted: HTTP `202` with `runId`
- Duplicate request for in-flight profile: HTTP `202` with `reused=true`
- Profile already active/ready: HTTP `200` with `noop=true`

### `GET /api/gtfs/status`

```bash
curl -s http://localhost:3000/api/gtfs/status | jq
```

### `GET /api/gtfs/stations`

Autocomplete source from active profile:

```bash
curl -s "http://localhost:3000/api/gtfs/stations?q=munchen&limit=20" | jq
```

### `GET /api/qa/v2/clusters`

Cluster-first curation feed (preferred for UI) with optional filters:

- `country=<ISO alpha-2>`
- `status=open|in_review|resolved|dismissed`
- `scope_tag=<latest|YYYY-MM-DD>`
- `limit=<1..200>`

V2 curation filters are ISO alpha-2 ready (for Europe expansion) while DACH scripts remain the default operational pipeline.

```bash
curl -s "http://localhost:3000/api/qa/v2/clusters?country=DE&status=open&limit=25" | jq
```

### `GET /api/qa/v2/clusters/:cluster_id`

Cluster detail payload includes candidate naming metadata, evidence links, real derived incoming/outgoing service context, segment context, and decision history:

```bash
curl -s "http://localhost:3000/api/qa/v2/clusters/<cluster_id>" | jq
```

`service_context.completeness.status` is explicit:
- `none`: no source payload rows were available for that candidate
- `full`: incoming and outgoing service direction context could both be derived
- `partial`: line/service fields were extracted, but not full directional coverage
- `incomplete`: source rows exist but expected line/service keys were missing

### `POST /api/qa/v2/clusters/:cluster_id/decisions`

Submit one final cluster-level decision (`merge|split`) from the staged conflict editor workflow:

```bash
curl -s -X POST "http://localhost:3000/api/qa/v2/clusters/<cluster_id>/decisions" \
  -H 'Content-Type: application/json' \
  -d '{
    "operation":"merge",
    "selected_station_ids":["cstn_a","cstn_b"],
    "groups":[
      {
        "group_label":"merge-selected",
        "member_station_ids":["cstn_a","cstn_b"],
        "rename_to":"Winterthur Main Station",
        "segment_action":{
          "walk_links":[
            {"from_segment_id":"seg_main","to_segment_id":"seg_bus","min_walk_minutes":4,"bidirectional":true}
          ]
        }
      }
    ],
    "rename_targets":[
      {"canonical_station_id":"cstn_a","rename_to":"Winterthur Main Concourse"}
    ],
    "note":"same logical station",
    "line_decisions":{"future_line_groups":[]}
  }' | jq
```

For `operation=merge`, a target station is not required in the payload. V2 merge decisions create a new curated entity (`qa_curated_*_v1`) and do not write legacy `canonical_station_overrides` rows.

### `GET /api/qa/v2/curated-stations`

Additive curated projection list (reviewer-confirmed entities) with optional filters:

- `country=<ISO alpha-2>`
- `status=active|superseded`
- `cluster_id=<cluster_id>`
- `limit=<1..200>`

```bash
curl -s "http://localhost:3000/api/qa/v2/curated-stations?cluster_id=<cluster_id>&status=active&limit=25" | jq
```

### `GET /api/qa/v2/curated-stations/:curated_station_id`

Curated entity detail including members, field provenance, and lineage:

```bash
curl -s "http://localhost:3000/api/qa/v2/curated-stations/<curated_station_id>" | jq
```

### `scripts/data/run-station-review-pipeline.sh` (one command: migrate + refresh)

Run the full station-review pipeline with explicit step logs and fail-fast diagnostics:

```bash
scripts/data/run-station-review-pipeline.sh
scripts/data/run-station-review-pipeline.sh --country CH
scripts/data/run-station-review-pipeline.sh --country DE --as-of 2026-02-20
scripts/data/run-station-review-pipeline.sh --skip-migrate --from-step canonical
```

### `scripts/data/refresh-station-review.sh` (manual refresh/debug stages)

Run the full station-review data refresh in the terminal with live logs:

```bash
scripts/data/refresh-station-review.sh
scripts/data/refresh-station-review.sh --country DE --as-of 2026-02-20
scripts/data/refresh-station-review.sh --source-id de_delfi_sollfahrplandaten_netex --only fetch,ingest
scripts/data/refresh-station-review.sh --from-step canonical --to-step review-queue
```

### `POST /api/qa/jobs/refresh`

Trigger the asynchronous refresh pipeline (`fetch -> ingest -> canonical -> review queue`) via API automation:

```bash
curl -s -X POST http://localhost:3000/api/qa/jobs/refresh \
  -H 'Content-Type: application/json' \
  -d '{"country":"DE"}' | jq
```

The endpoint responds immediately with HTTP `202` and `job_id` (also mirrored in `job.job_id`).
Frontend curation no longer starts pipeline jobs directly; use `scripts/data/run-station-review-pipeline.sh` for full local runs (or `scripts/data/refresh-station-review.sh` for step-level debugging).

If you run via Docker Compose and just pulled code changes, rebuild the orchestrator service first:

```bash
docker compose up -d --build orchestrator
```

### `GET /api/qa/jobs/:job_id`

Poll refresh pipeline status:

```bash
curl -s http://localhost:3000/api/qa/jobs/<job_id> | jq
```

While `step=fetching_sources`, payloads include `download_progress` with live source/file/bytes fields
(`source_id`, `source_index`, `total_sources`, `downloaded_bytes`, `total_bytes`) for UI progress bars.

Note: A failed fetch step usually indicates missing source auth env vars (for example `DE_DELFI_SOLLFAHRPLANDATEN_NETEX_USERNAME`/`..._PASSWORD` or cookie/header equivalents).

### `POST /api/routes`

```bash
curl -s -X POST http://localhost:3000/api/routes \
  -H 'Content-Type: application/json' \
  -d '{"origin":"München Hbf [198175]","destination":"Augsburg Hbf [179149]","datetime":"2026-02-20T18:00:00Z"}' | jq
```

Notes:

- Route search is only accepted in status `ready`.
- The orchestrator resolves station input to MOTIS stop IDs in `tag_stopId` format.
- Default tag is `active-gtfs`, so resolved IDs look like `active-gtfs_198175`.
- Responses include `routeRequestResolved` for debugging what was actually sent.
- Error responses include `errorCode` for machine-readable handling.
- Responses include `x-correlation-id` header for log tracing.

### `GET /health`

```bash
curl -s http://localhost:3000/health | jq
```

### `GET /metrics`

Prometheus-style runtime metrics:

```bash
curl -s http://localhost:3000/metrics
```

## Frontend

Build/runtime:

- Stack: **React + Vite** (multi-page: `/` and `/curation.html`)
- Build output: `frontend/dist` (served by orchestrator static hosting)
- Docker image build runs frontend build automatically.

Local frontend build command:

```bash
cd frontend
npm ci
npm run build
```

### Features

- Profile dropdown + activate button
- Live switch status badge
- Station autocomplete
- Route summary (cleaned display)
- Interactive map
- Collapsible raw JSON response
- QA curation dashboard at `http://localhost:3000/curation.html`
- Curation list filters (`country`, `status`, `scope`) are for cluster browsing only (pipeline refresh runs via terminal command)
- Fixed-top staged conflict editor with tools (`Merge`, `Split`, `Group`) and one-shot `Resolve Conflict`
- Selected-node workflow is explicit on cards and map markers (selected/inactive/merged states)
- Curated merged/grouped entities render inline in the **Candidates** list as expandable first-class cards with member provenance.
- When a derived `merge` exists, its member candidates are hidden from standalone cards/markers and shown only inside the derived card `Members (N)` disclosure.
- Resolve payload preview updates immediately from staged local draft state
- Group workflow models one user-facing station with sections (`main|secondary|subway|bus|tram|other`) and optional custom section names
- Group section walk links are auto-generated pairwise at 5 minutes by default and can be edited before `Resolve Conflict`
- Candidate cards show source-feed provenance and coverage notes inline

### Map stack

The frontend uses **MapLibre GL JS** and follows your planned stack direction.

- Preferred style source: Protomaps (if key is configured)
- Fallback style: MapLibre-compatible public style URLs (when no key is set)

Runtime config file: `frontend/public/config.js`

```js
window.PROTOMAPS_API_KEY = '';
window.MAP_STYLE_URL = '';
window.SATELLITE_MAP_STYLE_URL = '';
```

Behavior:

- If `MAP_STYLE_URL` is set, it is used directly.
- Else if `PROTOMAPS_API_KEY` is set, Protomaps style URL is used.
- Else fallback style is used.
- Curation view has a default/satellite basemap toggle and persists selected mode for the browser session.

## Script reference

### `scripts/run-test-env.sh`

Primary local dev/test command.

```bash
scripts/run-test-env.sh --profile sample_de
```

NPM shortcut from repo root:

```bash
npm run dev -- --profile sample_de
```

Useful options:

- `--no-build`
- `--logs`
- `--wait-sec <n>`
- `--osm-url <url>`
- `--osm-file <path>`
- `--force-osm-download`

### `scripts/setup.sh`

One-command setup (download/copy OSM, bootstrap MOTIS data, start compose):

```bash
scripts/setup.sh --profile sample_de
```

Useful options:

- `--detach`
- `--no-build`
- `--no-start`
- `--skip-import`
- `--osm-url <url>`
- `--osm-file <path>`

### `scripts/up.sh`

Compose startup wrapper with preflight + optional auto-init.

```bash
scripts/up.sh --profile sample_de
```

### `scripts/init-motis.sh`

Manual bootstrap for MOTIS data/config/import:

```bash
scripts/init-motis.sh --profile sample_de
```

Useful options:

- `--skip-import`
- `--osm-file <path>`
- `--motis-image <image>`

Profile resolution supports both static `zipPath` profiles and runtime descriptor profiles (`mode=canonical-export`).

### `scripts/check-motis-data.sh`

Preflight checks for required MOTIS files.

```bash
scripts/check-motis-data.sh
```

### `scripts/switch-gtfs.sh`

Switch active profile via API and wait for terminal state.

```bash
scripts/switch-gtfs.sh --profile sample_de
```

Optional full reimport before activation:

```bash
scripts/switch-gtfs.sh --profile sample_de --reimport
```

Optional post-activation smoke gate (strict by default):

```bash
scripts/switch-gtfs.sh --profile sample_de --smoke-gate --smoke-max-attempts 180
```

Smoke options:

- `--smoke-gate`
- `--smoke-strict` / `--smoke-nonstrict`
- `--smoke-max-attempts <n>`
- `--smoke-target-date YYYY-MM-DD`

### `scripts/find-working-route.sh`

Automated route smoke finder from active GTFS.

```bash
scripts/find-working-route.sh --target-date 2026-02-20 --max-attempts 300
```

It generates real candidate pairs from GTFS and tests `/api/routes` until it finds non-empty itineraries.

### `scripts/validate-config.sh`

Validate schema contracts for config files:

```bash
scripts/validate-config.sh
```

Target one config contract:

```bash
scripts/validate-config.sh --only profiles
scripts/validate-config.sh --only dach
scripts/validate-config.sh --only ojp
scripts/validate-config.sh --only ojp-mock
```

### `scripts/qa/build-profile.sh`

Deterministic canonical -> GTFS runtime export builder (group-aware):
- grouped stations export as one user-facing stop
- internal group sections remain exported as child stops
- section walk links export to `transfers.txt`

```bash
scripts/qa/build-profile.sh --profile canonical_de_runtime --as-of 2026-02-19
```

### `scripts/qa/validate-export.sh`

GTFS artifact validation gate (core tables + optional transfer link integrity checks).

```bash
scripts/qa/validate-export.sh --zip data/gtfs/runtime/canonical_de_runtime/2026-02-19/active-gtfs.zip
```

### `scripts/qa/run-route-smoke.sh`

Run deterministic smoke route checks from fixture cases/baselines:

```bash
scripts/qa/run-route-smoke.sh --api-url http://localhost:3000
```

### `scripts/qa/run-route-regression.sh`

Run full route regression suite and write QA report to `reports/qa/`:

```bash
scripts/qa/run-route-regression.sh --api-url http://localhost:3000
```

### `scripts/qa/report-pipeline-kpis.sh`

Write pipeline KPI report (`reports/qa/pipeline-kpis.json`) from `pipeline_jobs`:

```bash
scripts/qa/report-pipeline-kpis.sh --window-hours 24
```

Pipeline orchestration semantics:

- `pipeline_jobs` is idempotent by `(job_type, idempotency_key)`.
- Duplicate starts for an in-flight/completed key reuse prior outcome.
- Failed terminal jobs are replayed as the original failure for the same key (no implicit rerun).
- Pending jobs (`queued`/`retry_wait`) are resumable for the same key and continue from persisted checkpoint context.
- Running-slot race conflicts are normalized to `JOB_BACKPRESSURE` instead of generic internal errors.
- Per-`job_type` concurrency uses `PIPELINE_JOB_MAX_CONCURRENT` and is enforced atomically in DB claim logic.

### DACH official source discovery/retrieval

This repo now includes a separate raw-source layer for official DACH datasets (`DE`, `AT`, `CH`).
It does not change MOTIS GTFS-switch runtime behavior.

- Source registry: `config/dach-data-sources.json`
- Source docs: `docs/dach-official-sources.md`
- Raw fetch script: `scripts/data/fetch-dach-sources.sh`
- Source verification script: `scripts/data/verify-dach-sources.sh`
- Node CLI wrappers: `orchestrator/src/cli/fetch-dach-sources.js`, `orchestrator/src/cli/verify-dach-sources.js`
- Domain service module: `orchestrator/src/domains/source-discovery/service.js`

Validate source config, policy, and reachability:

```bash
scripts/data/verify-dach-sources.sh
```

Verify one source at a time:

```bash
scripts/data/verify-dach-sources.sh --source-id de_delfi_sollfahrplandaten_netex
scripts/data/verify-dach-sources.sh --source-id at_oebb_mmtis_netex
scripts/data/verify-dach-sources.sh --source-id ch_opentransportdata_timetable_netex
```

Fetch latest raw snapshots (local-only storage):

```bash
scripts/data/fetch-dach-sources.sh
```

Fetch one source at a time:

```bash
scripts/data/fetch-dach-sources.sh --source-id de_delfi_sollfahrplandaten_netex
scripts/data/fetch-dach-sources.sh --source-id at_oebb_mmtis_netex
scripts/data/fetch-dach-sources.sh --source-id ch_opentransportdata_timetable_netex
```

Deterministic replay mode with explicit date:

```bash
scripts/data/fetch-dach-sources.sh --as-of 2026-02-01
```

Note:

- Some official sources are access-controlled. Configure login/session secrets in `.env` (for example `DE_DELFI_SOLLFAHRPLANDATEN_NETEX_COOKIE=...`) before running fetch/verify.
- For automatic runs, prefer username/password env vars (for example `DE_DELFI_SOLLFAHRPLANDATEN_NETEX_USERNAME` and `DE_DELFI_SOLLFAHRPLANDATEN_NETEX_PASSWORD`) so scripts can refresh the session.
- DE DELFI currently publishes a ZIP filename that does not contain `netex` in the name (`...fahrplaene_gesamtdeutschland.zip`); this can appear as a warning in verify output and is expected.

### DACH NeTEx -> PostGIS canonical stations (MVP slice)

This repo now includes a PostGIS-backed NeTEx ingest and canonical station layer for DACH (`DE`, `AT`, `CH`).
This slice is local-first and is now the source for canonical->GTFS runtime export artifacts.

- Migrations: `db/migrations/`
- DB helper scripts: `scripts/data/`
- Optional compose service profile for PostGIS: `dach-data`
- PostGIS persistence in compose uses a Docker named volume: `postgis_data` (avoids root-owned host bind artifacts in `data/postgis/`)
- Node CLI wrappers:
  - `orchestrator/src/cli/ingest-netex.js`
  - `orchestrator/src/cli/build-canonical-stations.js`
  - `orchestrator/src/cli/build-review-queue.js`
  - `orchestrator/src/cli/refresh-station-review.js`
  - `orchestrator/src/cli/report-review-queue.js`
- Domain service modules:
  - `orchestrator/src/domains/ingest/service.js`
  - `orchestrator/src/domains/canonical/service.js`
  - `orchestrator/src/domains/qa/service.js`

Run migrations (creates PostGIS extension + required tables):

```bash
scripts/data/db-migrate.sh
```

Ingest one snapshot (hard-fails on NeTEx parse/source errors, no GTFS fallback):

```bash
scripts/data/ingest-netex.sh --country CH --as-of 2026-02-19
scripts/data/ingest-netex.sh --country AT --as-of 2026-02-19
```

Build canonical stations from staging:

```bash
scripts/data/build-canonical-stations.sh --as-of 2026-02-19
```

Report and checks:

```bash
scripts/data/report-canonical.sh
scripts/data/check-canonical-pipeline.sh --min-canonical 1
```

Run the station-review refresh pipeline end-to-end with terminal logs:

```bash
scripts/data/run-station-review-pipeline.sh
scripts/data/run-station-review-pipeline.sh --country CH --as-of 2026-02-19
scripts/data/run-station-review-pipeline.sh --skip-migrate --from-step canonical

scripts/data/refresh-station-review.sh
scripts/data/refresh-station-review.sh --country CH --as-of 2026-02-19
scripts/data/refresh-station-review.sh --source-id ch_opentransportdata_timetable_netex --only fetch,ingest
scripts/data/refresh-station-review.sh --from-step canonical
```

### Canonical -> GTFS runtime export (deterministic)

Build deterministic runtime GTFS artifact from canonical data:

```bash
scripts/qa/build-profile.sh --profile canonical_de_runtime --as-of 2026-02-19
```

Output (default):

- `data/gtfs/runtime/<profile>/<YYYY-MM-DD>/active-gtfs.zip`
- `data/gtfs/runtime/<profile>/<YYYY-MM-DD>/manifest.json`

Validate export explicitly:

```bash
scripts/qa/validate-export.sh --zip data/gtfs/runtime/canonical_de_runtime/2026-02-19/active-gtfs.zip
```

Activate exported runtime profile:

```bash
scripts/init-motis.sh --profile canonical_de_runtime
scripts/switch-gtfs.sh --profile canonical_de_runtime --smoke-gate --smoke-strict
```

Fail-fast behavior:

- Export fails when canonical scope has no snapshots/stops.
- Export fails when coordinates are missing/invalid.
- Export uses a clearly marked MVP bridge mode (`group-aware-synthetic-journeys-from-canonical-stops`) and never emits empty required GTFS files.

### End-to-end workflow (raw -> canonical -> activated runtime)

```bash
# 1) Fetch official raw snapshots
scripts/data/fetch-dach-sources.sh --as-of 2026-02-19

# 2) Ingest NeTEx into PostGIS staging
scripts/data/ingest-netex.sh --country DE --as-of 2026-02-19

# 3) Build canonical stations
scripts/data/build-canonical-stations.sh --as-of 2026-02-19

# 4) Build deterministic runtime GTFS artifact
scripts/qa/build-profile.sh --profile canonical_de_runtime --as-of 2026-02-19 --country DE

# 5) Validate artifact explicitly (optional extra gate)
scripts/qa/validate-export.sh --zip data/gtfs/runtime/canonical_de_runtime/2026-02-19/active-gtfs.zip

# 6) Activate profile in runtime + smoke gate
scripts/init-motis.sh --profile canonical_de_runtime
scripts/switch-gtfs.sh --profile canonical_de_runtime --smoke-gate --smoke-strict
```

### Canonical station QA + manual curation workflow

Manual confirmation is deterministic and DB-auditable through v2 cluster tables plus additive curated projection tables.
This workflow is separate from runtime `/api/routes`.

- Review queue table: `canonical_review_queue`
- V2 cluster tables: `qa_station_clusters_v2`, `qa_station_cluster_candidates_v2`, `qa_station_cluster_evidence_v2`, `qa_station_cluster_decisions_v2`
- V2 group modeling tables: `qa_station_groups_v2`, `qa_station_group_sections_v2`, `qa_station_group_section_members_v2`, `qa_station_group_section_links_v2`
- V2 naming audit tables: `qa_station_display_names_v2`, `qa_station_naming_overrides_v2`
- Segment/complex tables: `qa_station_complexes_v2`, `qa_station_segments_v2`, `qa_station_segment_links_v2`
- Line dedup seam tables: `canonical_line_identities_v2`, `station_segment_line_links_v2`
- Curated projection foundation tables (reviewer-confirmed entity layer): `qa_curated_stations_v1`, `qa_curated_station_members_v1`, `qa_curated_station_lineage_v1`, `qa_curated_station_field_provenance_v1`
- Frontend dashboard: `http://localhost:3000/curation.html`
  - Cluster list: `GET /api/qa/v2/clusters`
    - Dashboard defaults cluster browsing to `scope_tag=latest` (switch to "All scopes" only when needed)
  - Cluster detail: `GET /api/qa/v2/clusters/:cluster_id`
  - Cluster decision submit: `POST /api/qa/v2/clusters/:cluster_id/decisions`
  - Curated projection read path (merged/grouped results): `GET /api/qa/v2/curated-stations`, `GET /api/qa/v2/curated-stations/:curated_station_id`
  - V2 decisions create curated station entities and do not write legacy `canonical_station_overrides`
- Refresh/build command for review data (terminal-first):
  - `scripts/data/run-station-review-pipeline.sh`
  - `scripts/data/refresh-station-review.sh`
  - Optional scope/debug flags: `--country <DE|AT|CH>`, `--as-of <YYYY-MM-DD>`, `--source-id <id>`, `--only <steps>`, `--from-step`, `--to-step`
- Dashboard includes a map basemap toggle (default/satellite) with session persistence.
- When multiple candidates share identical coordinates, the map keeps exact position and renders candidates as concentric selectable rings.
- Dashboard UI is guided and reviewer-first:
  - select candidates with `Select All`/`Clear`
  - see merged/grouped results inline in the candidate list as expandable derived cards with composition/provenance
  - merged member candidates are removed from the standalone list/map and remain visible only under each derived card `Members (N)` section
  - merge tab assumes one resulting name and does not require a manual merge-target selector
  - stage local edits with tools (`Merge`, `Split`, `Group`) and explicit selected-node visibility
  - merge updates local draft state and can be renamed inline via pencil before resolve
  - group workflow models one user-facing station with typed sections and optional custom names
  - pairwise walk links default to 5 minutes and are editable before resolve
  - advanced JSON fields are hidden behind optional disclosure

Naming model in v2:

- `canonical_station_id` remains the stable machine identifier.
- Human-facing labels come from `qa_station_display_names_v2.display_name`.
- Naming provenance (`strategy`, `reason`, source refs, aliases) is stored in `qa_station_display_names_v2` and surfaced in cluster candidate payloads.
- Manual naming decisions are auditable in `qa_station_naming_overrides_v2` and linked to cluster decisions.

Build review queue items:

```bash
scripts/data/build-review-queue.sh
scripts/data/build-review-queue.sh --country CH --as-of 2026-02-19
```

Reset station-review tables (destructive; intended for local test environments):

```bash
scripts/data/reset-station-review-data.sh --yes
```

Report queue coverage + open/resolved items:

```bash
scripts/data/report-review-queue.sh
scripts/data/report-review-queue.sh --country AT --as-of 2026-02-19 --limit 25
```

### OJP feeder scaffolding (standalone, not runtime-wired)

- Endpoint config: `config/ojp-endpoints.json`
- Canonical -> OJP reference map table: `ojp_stop_refs`
- Probe script: `scripts/data/test-ojp-feeders.sh`

Configure provider endpoint + auth env vars in `.env` first, then probe:

```bash
scripts/data/test-ojp-feeders.sh --country DE
scripts/data/test-ojp-feeders.sh --provider-id at_ojp_primary --case-index 0
```

Probe with canonical IDs resolved via `ojp_stop_refs`:

```bash
scripts/data/test-ojp-feeders.sh \
  --provider-id ch_ojp_primary \
  --from-canonical-id cstn_example_from \
  --to-canonical-id cstn_example_to \
  --departure-time 2026-02-20T08:00:00Z
```

Deterministic local happy-path check with built-in mock endpoint:

```bash
scripts/data/check-ojp-feeders-mock.sh
```

GitHub Actions CI job (already included in repo):

- `.github/workflows/ojp-mock-feeder-check.yml`
- `.github/workflows/qa-export-check.yml`

Manual probe against mock config:

```bash
OJP_ENDPOINTS_CONFIG=config/ojp-endpoints.mock.json \
  scripts/data/test-ojp-feeders.sh --provider-id de_ojp_mock_local
```

### Offline stitching prototype (standalone service-layer experiment)

- Transfer rules table: `station_transfer_rules`
- Prototype runner: `scripts/data/run-stitch-prototype.sh`
- Core stitch logic: `scripts/data/stitch-prototype.js`
- Sample inputs:
  - `scripts/data/samples/ojp-feeder-sample.json`
  - `scripts/data/samples/motis-backbone-sample.json`

Run the prototype against sample feeder + backbone data:

```bash
scripts/data/run-stitch-prototype.sh --country DE --top-n 5
```

Run with custom sample files:

```bash
scripts/data/run-stitch-prototype.sh \
  --ojp-json /absolute/path/ojp-feeders.json \
  --motis-json /absolute/path/motis-backbone.json \
  --country CH \
  --as-of 2026-02-19 \
  --top-n 10 \
  --output state/stitch-report-ch.json
```

Output is a ranked JSON report with transfer-risk flags:

- `tight_connection`
- `long_wait`
- `invalid_time_order`

Low-memory run mode (recommended for large CH NeTEx snapshots):

```bash
nice -n 15 ionice -c3 scripts/data/ingest-netex.sh --country CH --as-of 2026-02-19
```

## Domain architecture

Core runtime/domain modules live in `orchestrator/src/`:

- `core/`: schema validation, error taxonomy, ID/correlation helpers
- `domains/source-discovery/`: DACH source contract validation
- `domains/ingest/`: ingest run/option contracts
- `domains/canonical/`: canonical scope contracts
- `domains/export/`: deterministic export/hash contracts
- `domains/switch-runtime/`: profile + switch contracts/state behavior
- `domains/routing/`: station normalization + route request contracts
- `domains/qa/`: QA case/baseline/report contracts
- `cli/`: thin entrypoints for config validation, profile runtime helpers, route regression

## Automated testing

Fast local checks:

```bash
scripts/validate-config.sh
cd orchestrator && npm run check && npm run test:unit && npm run test:integration
```

Full local suite (includes e2e + regression report generation under `reports/qa/`):

```bash
cd orchestrator && npm test
```

## CI quality gates

- Fast PR: `.github/workflows/ci-pr.yml`
- Full/nightly: `.github/workflows/ci-nightly.yml`
- Legacy focused checks remain:
  - `.github/workflows/qa-export-check.yml`
  - `.github/workflows/ojp-mock-feeder-check.yml`

PR CI fails when scoped runtime/script/config changes are missing required docs updates (`README.md`, `AGENTS.md`, `orchestrator/AGENTS.md`, `frontend/AGENTS.md`).

## Environment variables (orchestrator)

Configured in `docker-compose.yml` by default:

- `MOTIS_BASE_URL`
- `MOTIS_HEALTH_PATH`
- `MOTIS_HEALTH_ACCEPT_404`
- `MOTIS_ROUTE_PATH`
- `MOTIS_DATASET_TAG` (default `active-gtfs`)
- `MOTIS_RESTART_MODE`
- `MOTIS_DOCKER_SOCKET_PATH`
- `MOTIS_DOCKER_API_VERSION`
- `MOTIS_CONTAINER_NAME`
- `MOTIS_READY_TIMEOUT_MS`
- `MOTIS_HEALTH_POLL_INTERVAL_MS`
- `GTFS_SWITCH_LOCK_STALE_MS` (default `1800000`, 30 minutes)
- `METRICS_ENABLED` (default `true`, controls `/metrics`)
- `STATION_INDEX_CACHE_MAX_ENTRIES` (default `8`)
- `STATION_INDEX_CACHE_TTL_MS` (default `300000`)
- `PIPELINE_JOB_MAX_CONCURRENT` (default `1`, effective per-`job_type` running limit)
- `PIPELINE_JOB_MAX_ATTEMPTS` (default `3`)
- `PIPELINE_JOB_ORCHESTRATION_ENABLED` (default `true`)

## Environment variables (canonical PostGIS slice)

- `CANONICAL_DB_MODE` (`auto|direct|docker-compose`, default `auto`)
- `CANONICAL_DB_MODE=auto` probes direct `psql` connectivity first; if no explicit direct target is configured and direct probe fails, it falls back to compose service `postgis`
- If explicit direct settings are provided (`CANONICAL_DB_URL`/`DATABASE_URL`/`CANONICAL_DB_HOST`/`PGHOST` etc.) and direct probe fails in `auto`, scripts fail fast instead of silently switching DB targets
- `CANONICAL_DB_URL` (optional full connection URL)
- `CANONICAL_DB_HOST` (default `localhost`)
- `CANONICAL_DB_PORT` (default `5432`)
- `CANONICAL_DB_USER` (default `trainscanner`)
- `CANONICAL_DB_PASSWORD` (default `trainscanner`)
- `CANONICAL_DB_NAME` (default `trainscanner`)
- `CANONICAL_DB_DOCKER_PROFILE` (default `dach-data`)
- `CANONICAL_DB_DOCKER_SERVICE` (default `postgis`)
- `CANONICAL_DB_READY_TIMEOUT_SEC` (default `90`)
- `ENABLE_POSTGIS_TESTS=1` enables DB-backed integration tests in CI/nightly

## Environment variables (OJP feeder scaffolding)

Set per-provider auth using each feeder's `envPrefix` in `config/ojp-endpoints.json`.

Example for `envPrefix=OJP_DE_PRIMARY`:

- `OJP_DE_PRIMARY_BEARER_TOKEN` (for `authMode=bearer`)
- `OJP_DE_PRIMARY_API_KEY` and optional `OJP_DE_PRIMARY_API_KEY_HEADER` (for `authMode=api_key`)
- `OJP_DE_PRIMARY_USERNAME` and `OJP_DE_PRIMARY_PASSWORD` (for `authMode=basic`)
- `OJP_DE_PRIMARY_HEADER` (for `authMode=header`)
- `OJP_ENDPOINTS_CONFIG` (optional path override for feeder endpoint config; useful for mock/CI checks)

## Troubleshooting

### MOTIS loop: `could not read config file at data/config.yml`

Run:

```bash
scripts/setup.sh --profile <name>
```

(or `scripts/init-motis.sh --profile <name>` if OSM already exists), then restart compose.

### Bootstrap error: `exec: "config": executable file not found`

Use explicit CLI binary:

```bash
MOTIS_CLI_BIN=motis scripts/setup.sh --profile <name>
```

### Import error: `tiles profile ... does not exist`

MVP bootstrap disables street/geocoding/tiles by default. If you need them, set:

```bash
MOTIS_DISABLE_STREET_FEATURES=false
```

and provide valid tile profile config for your MOTIS build.

### Route returns empty itineraries

Use resolved/tagged stop IDs (`active-gtfs_<stop_id>`) and check `routeRequestResolved` in response.

### `init-motis.sh` warns about `state/active-gtfs.json` permissions

If you see a warning like:

```text
Warning: could not update .../state/active-gtfs.json due to permissions (EACCES)
```

the GTFS artifact copy/config/import still completed. In this case, run profile activation via API (`scripts/switch-gtfs.sh --profile <name>`) so orchestrator refreshes runtime state files.

### Docker restart API version error

Set/override `MOTIS_DOCKER_API_VERSION` or keep `auto` (default).

## Known limitations

- No auth/rate-limit layer yet.
- No realtime GTFS-RT/OJP enrichment yet.
- Frontend map currently draws first returned itinerary only.
- Fallback map style is not Protomaps-branded unless key/style is configured.
- OJP feeder probing + stitching prototype are not wired into production `/api/routes` in this MVP slice.
