## Project Notes For Future Agents

Scope: repository root only. Ignore `archive(ignore)/` for active implementation.

## Primary goal

Maintain and extend the MOTIS GTFS-switch MVP for fast dataset testing/debugging.

## Active architecture

- `orchestrator/`: plain Node.js API server and switch workflow
- `frontend/`: static UI (no framework) with route summary + MapLibre map
- `config/`: GTFS profile definitions and active profile
- `state/`: switch lock, status, and logs
- `data/motis/`: generated MOTIS runtime data

## Core behavior that must remain true

- Frontend remains reachable while profile switching/restart is running.
- Only one switch can run at a time (lock file).
- Switch states are persisted (`idle|switching|importing|restarting|ready|failed`).
- Route endpoint is blocked unless system state is `ready`.
- Station autocomplete comes from active GTFS profile.
- Route station inputs are normalized before MOTIS call.

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

## Documentation policy (required)

When behavior, endpoints, scripts, or map stack change, update all relevant docs in the same change:

- `README.md`
- `AGENTS.md`
- `frontend/AGENTS.md`
- `orchestrator/AGENTS.md`

Keep docs command-accurate and copy/paste runnable.
