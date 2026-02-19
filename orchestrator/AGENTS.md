# Orchestrator Agent Notes

## Runtime

- Plain Node.js (no heavy framework).
- Entry point: `orchestrator/src/server.js`.

## Core files

- `src/server.js`: HTTP API + static hosting + station resolution
- `src/switcher.js`: profile switch state machine
- `src/motis.js`: MOTIS health/route calls + Docker restart API
- `src/lock.js`: filesystem lock (`state/gtfs-switch.lock`)
- `src/config.js`: env/file path configuration

## API contracts

- `GET /api/gtfs/profiles`
- `POST /api/gtfs/activate` body `{ "profile": "..." }`
- `GET /api/gtfs/status`
- `GET /api/gtfs/stations`
- `GET /health`
- `POST /api/routes`

## `/api/routes` expectations

- Enforce `ready` state before querying MOTIS.
- Normalize station input against active profile station index.
- Prefer MOTIS stop IDs in `tag_stopId` format.
- Default dataset tag comes from `MOTIS_DATASET_TAG` (default `active-gtfs`).
- Return `routeRequestResolved` and recent `motisAttempts` for debugging.

## State machine contract

- Valid states: `idle|switching|importing|restarting|ready|failed`.
- Persist every transition to `state/gtfs-switch-status.json`.
- Prevent concurrent switches with lock file.

## Editing rules

- Keep behavior deterministic and debuggable.
- Avoid introducing async races in switch flow.
- If request mapping changes, update docs (`README.md` + AGENTS files) in same change.
