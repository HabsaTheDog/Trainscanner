# MOTIS GTFS Switch

GTFS profile switching and route debugging with MOTIS.

## Quickstart

```bash
# Start dev environment (Frontend: http://localhost:3000, MOTIS: http://localhost:8080)
npm run dev -- --profile sample_de

# Stop environment
npm run stop
```

## Architecture & State

- **Frontend**: React + Vite (MapLibre GL JS mapping).
- **Backend**: Node.js Orchestrator API.
- **Routing Engine**: MOTIS in Docker.
- **State**: PostgreSQL (PostGIS) for `system_state` and canonical GTFS data, with JSON file fallbacks in `state/`.
- **Profiles**: Configured in `config/gtfs-profiles.json`. Includes static ZIPs or dynamic runtime exports.

## Core Features

- Deterministic GTFS profile switching, validation, and concurrent switch locking.
- Station autocomplete and route debugging endpoints.
- QA Curation Dashboard (`/curation.html`) to merge/split duplicate or vague stations.
- Deterministic data pipeline: DACH raw feeds -> NeTEx ingest -> Canonical PostGIS -> Runtime GTFS.

## API & Scripts Reference

**Key API Endpoints:**

- `GET /api/gtfs/profiles`, `GET /api/gtfs/status`
- `POST /api/gtfs/activate` (Switch profile)
- `GET /api/gtfs/stations`, `POST /api/routes`
- `GET /api/qa/v2/clusters`, `POST /api/qa/v2/clusters/:id/decisions` (QA Curation)

**Key Scripts (`scripts/`):**

- `npm run dev -- --profile <name>` / `scripts/run-test-env.sh --profile <name>`: Start test env.
- `scripts/switch-gtfs.sh --profile <name>`: Switch active profile via API.
- `scripts/data/run-station-review-pipeline.sh`: Run full QA ingestion/review pipeline.
- `scripts/qa/build-profile.sh`: Build deterministic profile from Canonical PostGIS.
- `scripts/data/fetch-dach-sources.sh`: Fetch raw DACH data snapshots.
- `scripts/data/seed-base-spatial-data.sh`: Pre-seed `canonical_stations` from OSM/UIC base topology (cold-start mitigation).
