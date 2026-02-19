# MOTIS GTFS Switch MVP

MVP for fast GTFS profile switching and route debugging with MOTIS.

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
    "de_full": { "zipPath": "data/gtfs/de_full.zip" }
  }
}
```

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

### `scripts/find-working-route.sh`

Automated route smoke finder from active GTFS.

```bash
scripts/find-working-route.sh --target-date 2026-02-20 --max-attempts 300
```

It generates real candidate pairs from GTFS and tests `/api/routes` until it finds non-empty itineraries.

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
This slice is local-first and does not rewire MOTIS GTFS switch runtime.

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
