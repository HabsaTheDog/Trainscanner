# Orchestrator Agent Notes

## Runtime

- Plain Node.js (no heavy framework).
- Entry point: `orchestrator/src/server.js`.

## Core files

- `src/server.js`: HTTP API + static hosting + station resolution
- `src/switcher.js`: profile switch state machine
- `src/motis.js`: MOTIS health/route calls + Docker restart API
- `src/lock.js`: filesystem lock (`state/gtfs-switch.lock`) with stale-lock cleanup
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
- Persist active profile marker to `state/active-gtfs.json` (legacy config path auto-migrates).
- Prevent concurrent switches with lock file and clear stale locks when safe.

## Editing rules

- Keep behavior deterministic and debuggable.
- Avoid introducing async races in switch flow.
- If request mapping changes, update docs (`README.md` + AGENTS files) in same change.

## DACH raw-source boundary

- `scripts/data/fetch-dach-sources.sh` and `scripts/data/verify-dach-sources.sh` are separate from orchestrator runtime.
- Do not couple DACH raw retrieval into `/api/routes` or GTFS switch flow in this MVP.
