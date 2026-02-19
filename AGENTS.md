## Project Notes For Future Agents

Scope: repository root only. Ignore `archive(ignore)/` for active implementation.

## Primary goal

Maintain and extend the MOTIS GTFS-switch MVP for fast dataset testing/debugging.

## Active architecture

- `orchestrator/`: plain Node.js API server and switch workflow
- `frontend/`: static UI (no framework) with route summary + MapLibre map
- `config/`: GTFS profile definitions
- `config/dach-data-sources.json`: official DACH source registry
- `scripts/data/`: DACH source fetch/verify + NeTEx ingest/canonical scripts
- `db/migrations/`: PostGIS schema migrations for canonical station layer
- `docker-compose.yml`: optional `postgis` service (`dach-data` profile) with named volume persistence
- `state/`: switch lock, status, and logs
- `data/motis/`: generated MOTIS runtime data

## Core behavior that must remain true

- Frontend remains reachable while profile switching/restart is running.
- Only one switch can run at a time (lock file).
- Switch states are persisted (`idle|switching|importing|restarting|ready|failed`).
- Active profile runtime marker is persisted in `state/active-gtfs.json` (legacy `config/active-gtfs.json` may exist).
- Route endpoint is blocked unless system state is `ready`.
- Station autocomplete comes from active GTFS profile.
- Route station inputs are normalized before MOTIS call.

## DACH data pipeline contract

- Scope includes:
- discovery/retrieval of official raw DACH sources
- NeTEx ingest into PostGIS staging
- canonical station build with provenance mapping
- Prefer NeTEx; GTFS requires explicit `fallbackReason` per source.
- No runtime auto-fallback from NeTEx to GTFS for the same source.
- Raw snapshots must stay local under `data/raw/<country>/<provider>/<format>/<YYYY-MM-DD>/`.
- Each fetch run must write a `manifest.json` with retrieval metadata + hash.
- PostGIS is mandatory for canonical layer (`canonical_stations`, `canonical_station_sources`).
- Selected `format=netex` ingest must fail hard on parse/source errors (non-zero exit).
- DACH scope remains `DE|AT|CH`.

## MOTIS routing contract in this MVP

- `/api/routes` should resolve user input to MOTIS stop IDs in `tag_stopId` format.
- Default dataset tag is `active-gtfs`.
- Debug output should include `routeRequestResolved` and attempted MOTIS request variants.

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
- `scripts/switch-gtfs.sh --profile <name>`
- `scripts/find-working-route.sh --max-attempts <n>`
- `scripts/data/verify-dach-sources.sh`
- `scripts/data/fetch-dach-sources.sh --as-of <YYYY-MM-DD>`
- `scripts/data/db-migrate.sh`
- `scripts/data/ingest-netex.sh --country <DE|AT|CH> --as-of <YYYY-MM-DD>`
- `scripts/data/build-canonical-stations.sh --as-of <YYYY-MM-DD>`
- `scripts/data/report-canonical.sh`

## Documentation policy (required)

When behavior, endpoints, scripts, or map stack change, update all relevant docs in the same change:

- `README.md`
- `AGENTS.md`
- `frontend/AGENTS.md`
- `orchestrator/AGENTS.md`

Keep docs command-accurate and copy/paste runnable.
