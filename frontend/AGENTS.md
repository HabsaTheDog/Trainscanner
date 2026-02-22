# Frontend Agent Notes

## Scope

- `frontend/index.html`
- `frontend/curation.html`
- `frontend/public/config.js`
- `frontend/src/HomePage.jsx`
- `frontend/src/CurationPage.jsx`
- `frontend/src/main-home.jsx`
- `frontend/src/main-curation.jsx`
- `frontend/src/styles.css`
- `frontend/src/legacy/home-logic.js`
- `frontend/src/legacy/curation-logic.js`
- `frontend/vite.config.js`
- `frontend/package.json`

## Stack

- Frontend is **React + Vite** (multi-page build).
- Output is built to `frontend/dist` and served by orchestrator static hosting.
- Home entry: `/` (`index.html` -> `src/main-home.jsx`)
- Curation entry: `/curation.html` (`curation.html` -> `src/main-curation.jsx`)
- Repo-root startup shortcuts are available via `npm run dev -- --profile <name>` and `npm run stop` (wrappers around `scripts/*.sh`).

## Required UX behavior

- Show GTFS profiles from `GET /api/gtfs/profiles`.
- Treat profile metadata fields (`sourceType`, `runtime`, `resolutionError`) as optional and non-blocking for rendering.
- Activate profile via `POST /api/gtfs/activate`.
- Poll `GET /api/gtfs/status` and render status badge.
- Load station suggestions from `GET /api/gtfs/stations`.
- Disable route form unless status is `ready`.
- Keep UI responsive while switching/importing/restarting.
- Treat API `errorCode` and `x-correlation-id` as optional debug metadata when showing failures.
- Keep the home page link to `/curation.html` available.
- Curation dashboard is cluster-first and loads items from `GET /api/qa/v2/clusters` (optional `country=<ISO alpha-2>`, `status`, `scope_tag`).
- Cluster detail is loaded from `GET /api/qa/v2/clusters/:cluster_id`.
- Cluster decisions submit to `POST /api/qa/v2/clusters/:cluster_id/decisions` with one final payload (`operation=merge|split`).
- Curated merged/grouped result read path is additive via `GET /api/qa/v2/curated-stations` (optionally `cluster_id`, `status`) and `GET /api/qa/v2/curated-stations/:curated_station_id`.
- Render curated merged/grouped results inline in the main candidate list as expandable derived cards (not as a separate standalone panel).
- Merge-derived member candidates must be hidden from standalone cards/map markers and shown only inside each derived card `Members (N)` disclosure.
- Merge UX should not expose a manual merge-target selector; merge creates a new derived entity and only needs selected members + resulting name.
- Keep curation UX reviewer-first and intuitive (guided edit wording, simple candidate selection, obvious selected/inactive card + marker states, explicit impact preview, advanced payload hidden behind optional details).
- Keep overlapping candidate markers on the curation map distinguishable with concentric selectable rings at the exact shared coordinate.
- Keep fixed-top staged conflict editor workflow (`Merge`, `Split`, `Group`) usable for iterative reviewer loops.
- Keep selected-node workflow clear across map markers and candidate cards before resolving.
- Support inline rename pencil actions on candidate/draft derived names (draft-only until resolve).
- Keep split/group workflow usable for large complexes (create/delete groups and add current selection into existing groups).
- Support pairwise walking-link authoring in the group panel with default `min_walk_minutes=5` before resolve.
- Candidate cards should show provenance (`source feeds/provider labels`) and coverage clarity from `service_context.completeness` (including `none|partial|incomplete` notes).
- Keep cluster list filters (`country`, `status`, `scope_tag`) focused on cluster browsing only.
- Do not add pipeline refresh execution controls to frontend curation; terminal/ops workflow uses `scripts/data/run-station-review-pipeline.sh` (or `scripts/data/refresh-station-review.sh` for step-level debugging).
- Default curation cluster browsing should stay anchored to `scope_tag=latest` unless a reviewer explicitly opts into all scopes.

## Route view requirements

- Show a compact itinerary summary (not only raw JSON).
- Keep raw response available in a collapsible section.
- Render first itinerary on map.

## Map requirements

- Use **MapLibre GL JS**.
- Prefer Protomaps style when `window.PROTOMAPS_API_KEY` is configured.
- Keep fallback style so map still renders without a key.
- Support curation basemap toggle (`default` + `satellite`) with session persistence.
- Runtime config is in `frontend/public/config.js`.

## Constraints

- Keep React components thin and declarative; feature logic stays in `src/legacy/*-logic.js` unless a broader refactor is explicitly requested.
- Do not hardcode profile names.
- Preserve status vocabulary exactly.
- Preserve route payload contract: `origin`, `destination`, `datetime`.
- Treat active profile as API-owned runtime state (server-side `system_state.active_gtfs` with filesystem fallback).
- Keep DACH PostGIS/NeTEx canonical pipeline concerns out of frontend runtime unless explicitly requested.
- Treat DACH `scripts/data/*.sh` commands as backend/ops tooling (now CLI-backed wrappers), not frontend runtime dependencies.
- Treat backend pipeline orchestration semantics (`JOB_BACKPRESSURE`, resumable `queued|retry_wait` jobs, per-type concurrency limits) as ops concerns, not frontend state.
- Keep curation interaction limited to QA API contracts; do not call `/api/routes` from curation workflow.
- Backend may dual-write curated projection tables (`qa_curated_*_v1`) as an additive seam; frontend should remain cluster-endpoint driven unless explicitly migrated.
- Keep OJP feeder probing and stitching prototype concerns out of frontend runtime unless explicitly requested.
- Keep local OJP mock fixture checks (`scripts/data/check-ojp-feeders-mock.sh`) out of frontend runtime unless explicitly requested.
