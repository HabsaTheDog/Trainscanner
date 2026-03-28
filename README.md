# MOTIS GTFS Switch

GTFS profile switching and route debugging with MOTIS.

## Quickstart

```bash
# Install Node workspaces once (frontend + orchestrator + control-plane)
npm ci

# Start dev environment without rebuilding images on every run
npm run dev -- --profile pan_europe_runtime

# Force a fresh image rebuild when Docker inputs changed
npm run start:build -- --profile pan_europe_runtime

# Stop environment
npm run stop
```

## QA Runs

The local QA flow mirrors the checked-in CI gates as closely as possible:

```bash
# Install/update dependencies used by the quality gates
npm ci --no-fund --no-audit
python3 -m pip install -r services/ai-scoring/requirements.txt

# Root static checks (Biome, Rust, Python, ShellCheck, TS, Markdown, orchestrator, control-plane)
npm run check

# Config contract validation
./scripts/validate-config.sh

# Orchestrator tests
npm run test:orchestrator

# Focused export determinism gate used by dedicated export QA workflow
node --test services/orchestrator/test/e2e/export-determinism.e2e.test.js

# Security audit gates used by CI PR fast
npm audit --audit-level=high
(cd services/rust-ingestion-worker && cargo audit)
```

`npm run check` is an ordered aggregate of:

- `npm run check:js`
- `npm run check:rs`
- `npm run check:py`
- `npm run check:sh`
- `npm run check:types`
- `npm run check:md`
- `npm run check:orchestrator`
- `npm run check:control-plane`

The dedicated export QA workflow runs `node --test services/orchestrator/test/e2e/export-determinism.e2e.test.js`, and the nightly workflow keeps the same focused export determinism gate alongside the broader orchestrator suite.

## Architecture & State

- **Node Tooling**: npm workspaces with root task runner (`frontend`, `services/orchestrator`, `services/control-plane`).
- **Frontend**: React + Vite (MapLibre GL JS mapping).
- **Backend**: Node.js Orchestrator API.
- **Routing Engine**: MOTIS in Docker.
- **State**: PostgreSQL (PostGIS) for `system_state` and pan-European station/timetable/QA data, with JSON file fallbacks in `services/orchestrator/state/`.
- **Profiles**: Configured in `config/gtfs-profiles.json`. Pan-European runtime export descriptor only.

## Core Features

- Deterministic GTFS profile switching, validation, and concurrent switch locking.
- Station autocomplete and route debugging endpoints.
- QA Curation Dashboard (`/curation.html`) to merge/split duplicate or vague stations.
- Deterministic data pipeline: provider raw feeds -> NeTEx ingest -> global PostGIS model -> runtime GTFS.

## API & Scripts Reference

**Key API Endpoints:**

- `GET /api/gtfs/profiles`, `GET /api/gtfs/status`
- `POST /api/gtfs/activate` (Switch profile)
- `POST /api/gtfs/compile` (Trigger tiered GTFS artifact compilation workflow)
- `GET /api/gtfs/stations`, `POST /api/routes`
- `GET /api/qa/global-clusters`, `POST /api/qa/global-clusters/:id/decisions` (QA Curation)

**Key Scripts (`scripts/`):**

- `npm run dev -- --profile <name>` / `scripts/run-test-env.sh --profile <name>`: Start test env.
- `scripts/switch-gtfs.sh --profile <name>`: Switch active profile via API.
- `scripts/data/db-bootstrap.sh`: Initialize the full PostGIS schema from scratch.
- `scripts/data/run-station-review-pipeline.sh`: Run full QA ingestion/review pipeline.
- `scripts/qa/build-profile.sh`: Build deterministic profile from pan-European timetable and transfer facts.
- `scripts/data/fetch-sources.sh`: Fetch configured pan-European raw source snapshots.

## Tiered GTFS Artifact Compilation

Compile tier-constrained GTFS artifacts directly from pan-European timetable facts:

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
  --gtfs-path data/gtfs/runtime/pan_europe_runtime/2026-02-20/active-gtfs.zip \
  --tier regional \
  --bbox "48.05,11.35,48.30,11.75"

# Macro graph: sparse high-speed cross-border checks
scripts/run-motis-k8s-test.sh \
  --mode macro \
  --gtfs-path data/artifacts/pan-europe-high-speed-2026-02-20.zip \
  --tier high-speed
```

Artifacts and generated query suites are written to `data/motis-k8s/<job-name>/`.
See `k8s/motis-testing/README.md` for `kind` / `minikube` path-mapping details.
