# MOTIS GTFS Switch

 GTFS profile switching and route debugging with MOTIS.

## Scope

- Active code is in repo root.
- `archive(ignore)/` is historical and not part of the active runtime.

## What works

- MOTIS in Docker (`motis`)
- Orchestrator API + static frontend (`orchestrator`)
- Named GTFS profiles via `config/gtfs-profiles.json`
- Active profile runtime state in `state/active-gtfs.json` (auto-migrates legacy `config/active-gtfs.json`)
- Switch state machine with lock + status persistence
- Route query endpoint with station-resolution and MOTIS adapter
- Frontend profile switcher, status badge, autocomplete, route summary, map, raw JSON

## Quickstart

Main command (recommended):

```bash
scripts/run-test-env.sh --profile sample_de
```

Open:

- Frontend: `http://localhost:3000`
- MOTIS direct: `http://localhost:8080`

Stop:

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

Active profile runtime state is tracked in `state/active-gtfs.json`.

## Runtime files

- `state/gtfs-switch-status.json`: switch status (`idle|switching|importing|restarting|ready|failed`)
- `state/gtfs-switch.lock`: concurrency lock
- `state/gtfs-switch.log`: step logs and failures
- `state/active-gtfs.json`: active GTFS profile marker used by orchestrator/scripts
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

### `GET /api/gtfs/status`

```bash
curl -s http://localhost:3000/api/gtfs/status | jq
```

### `GET /api/gtfs/stations`

Autocomplete source from active profile:

```bash
curl -s "http://localhost:3000/api/gtfs/stations?q=munchen&limit=20" | jq
```

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

### `GET /health`

```bash
curl -s http://localhost:3000/health | jq
```

## Frontend

### Features

- Profile dropdown + activate button
- Live switch status badge
- Station autocomplete
- Route summary (cleaned display)
- Interactive map
- Collapsible raw JSON response

### Map stack

The frontend uses **MapLibre GL JS** and follows your planned stack direction.

- Preferred style source: Protomaps (if key is configured)
- Fallback style: OpenFreeMap style URL (when no key is set)

Runtime config file: `frontend/config.js`

```js
window.PROTOMAPS_API_KEY = '';
window.MAP_STYLE_URL = '';
```

Behavior:

- If `MAP_STYLE_URL` is set, it is used directly.
- Else if `PROTOMAPS_API_KEY` is set, Protomaps style URL is used.
- Else fallback style is used.

## Script reference

### `scripts/run-test-env.sh`

Primary local dev/test command.

```bash
scripts/run-test-env.sh --profile sample_de
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

### `scripts/qa/build-profile.sh`

Deterministic canonical -> GTFS runtime export builder.

```bash
scripts/qa/build-profile.sh --profile canonical_de_runtime --as-of 2026-02-19
```

### `scripts/qa/validate-export.sh`

GTFS artifact validation gate.

```bash
scripts/qa/validate-export.sh --zip data/gtfs/runtime/canonical_de_runtime/2026-02-19/active-gtfs.zip
```

### DACH official source discovery/retrieval

This repo now includes a separate raw-source layer for official DACH datasets (`DE`, `AT`, `CH`).
It does not change MOTIS GTFS-switch runtime behavior.

- Source registry: `config/dach-data-sources.json`
- Source docs: `docs/dach-official-sources.md`
- Raw fetch script: `scripts/data/fetch-dach-sources.sh`
- Source verification script: `scripts/data/verify-dach-sources.sh`

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
- Export uses a clearly marked MVP bridge mode (`synthetic-journeys-from-canonical-stops`) and never emits empty required GTFS files.

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

Manual confirmation is deterministic and DB-auditable through queue + overrides tables.
This workflow is separate from runtime `/api/routes`.

- Queue table: `canonical_review_queue`
- Override table: `canonical_station_overrides`

Build review queue items from canonical data:

```bash
scripts/data/build-review-queue.sh
scripts/data/build-review-queue.sh --country CH --as-of 2026-02-19
```

Apply approved overrides (table-backed):

```bash
scripts/data/apply-station-overrides.sh
scripts/data/apply-station-overrides.sh --country DE --as-of 2026-02-19
```

Import overrides from CSV and apply in one run:

```bash
scripts/data/apply-station-overrides.sh --csv /absolute/path/overrides.csv
```

CSV headers (required):

```text
operation,country,source_canonical_station_id,target_canonical_station_id,source_id,source_stop_id,new_canonical_name,reason,requested_by,approved_by,external_ref
```

Starter template CSV:

```bash
cat scripts/data/samples/overrides.example.csv
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

### Docker restart API version error

Set/override `MOTIS_DOCKER_API_VERSION` or keep `auto` (default).

## Known limitations

- No auth/rate-limit layer yet.
- No realtime GTFS-RT/OJP enrichment yet.
- Frontend map currently draws first returned itinerary only.
- Fallback map style is not Protomaps-branded unless key/style is configured.
- OJP feeder probing + stitching prototype are not wired into production `/api/routes` in this MVP slice.
