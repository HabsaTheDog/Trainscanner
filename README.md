# MOTIS GTFS Switch

GTFS profile switching and route debugging with MOTIS.

## Quickstart

```bash
# Install Node workspaces once (frontend + orchestrator + control-plane)
npm ci

# Start dev environment (Frontend: http://localhost:3000, MOTIS: http://localhost:8080)
npm run dev -- --profile sample_de

# Stop environment
npm run stop
```

## Architecture & State

- **Node Tooling**: npm workspaces with root task runner (`frontend`, `services/orchestrator`, `services/control-plane`).
- **Frontend**: React + Vite (MapLibre GL JS mapping).
- **Backend**: Node.js Orchestrator API.
- **Routing Engine**: MOTIS in Docker.
- **State**: PostgreSQL (PostGIS) for `system_state` and canonical GTFS data, with JSON file fallbacks in `services/orchestrator/state/`.
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
- `POST /api/gtfs/compile` (Trigger tiered GTFS artifact compilation workflow)
- `GET /api/gtfs/stations`, `POST /api/routes`
- `GET /api/qa/v2/clusters`, `POST /api/qa/v2/clusters/:id/decisions` (QA Curation)

**Key Scripts (`scripts/`):**

- `npm run dev -- --profile <name>` / `scripts/run-test-env.sh --profile <name>`: Start test env.
- `scripts/switch-gtfs.sh --profile <name>`: Switch active profile via API.
- `scripts/data/run-station-review-pipeline.sh`: Run full QA ingestion/review pipeline.
- `scripts/qa/build-profile.sh`: Build deterministic profile from Canonical PostGIS.
- `scripts/data/fetch-dach-sources.sh`: Fetch raw DACH data snapshots.
- `scripts/data/seed-base-spatial-data.sh`: Pre-seed `canonical_stations` from OSM/UIC base topology (cold-start mitigation).

## Tiered GTFS Artifact Compilation

Compile tier-constrained GTFS artifacts directly from canonical PostGIS:

```bash
# Tier 1
python3 scripts/qa/export-canonical-gtfs.py --from-db --as-of 2026-02-20 --tier high-speed

# Tier 2
python3 scripts/qa/export-canonical-gtfs.py --from-db --as-of 2026-02-20 --tier regional

# Tier 3
python3 scripts/qa/export-canonical-gtfs.py --from-db --as-of 2026-02-20 --tier local

# Combined feed (tier metadata in routes.txt route_desc)
python3 scripts/qa/export-canonical-gtfs.py --from-db --as-of 2026-02-20 --tier all
```

Default outputs are written to `data/artifacts/` unless `--output-zip` / `--summary-json` are passed.

Temporal integration:

- Workflow name: `compileGtfsArtifact`
- Task queue: `review-pipeline`
- Activity name: `compileGtfsArtifact`

## Ephemeral K8s MOTIS Testing

Run disposable Kubernetes jobs for post-compilation route validation:

```bash
# Micro graph: bbox-scoped local/regional regression checks
scripts/run-motis-k8s-test.sh \
  --mode micro \
  --gtfs-path data/gtfs/runtime/de/2026-02-20/active-gtfs.zip \
  --tier regional \
  --bbox "48.05,11.35,48.30,11.75"

# Macro graph: sparse high-speed cross-border checks
scripts/run-motis-k8s-test.sh \
  --mode macro \
  --gtfs-path data/artifacts/canonical-high-speed-2026-02-20.zip \
  --tier high-speed
```

Artifacts and generated query suites are written to `data/motis-k8s/<job-name>/`.
See `k8s/motis-testing/README.md` for `kind` / `minikube` path-mapping details.
