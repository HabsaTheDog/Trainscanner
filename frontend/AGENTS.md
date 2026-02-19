# Frontend Agent Notes

## Scope

- `frontend/index.html`
- `frontend/app.js`
- `frontend/styles.css`
- `frontend/config.js`

## Required UX behavior

- Show GTFS profiles from `GET /api/gtfs/profiles`.
- Activate profile via `POST /api/gtfs/activate`.
- Poll `GET /api/gtfs/status` and render status badge.
- Load station suggestions from `GET /api/gtfs/stations`.
- Disable route form unless status is `ready`.
- Keep UI responsive while switching/importing/restarting.

## Route view requirements

- Show a compact itinerary summary (not only raw JSON).
- Keep raw response available in a collapsible section.
- Render first itinerary on map.

## Map requirements

- Use **MapLibre GL JS**.
- Prefer Protomaps style when `window.PROTOMAPS_API_KEY` is configured.
- Keep fallback style so map still renders without a key.
- Runtime config is in `frontend/config.js`.

## Constraints

- Keep implementation framework-free and easy to debug.
- Do not hardcode profile names.
- Preserve status vocabulary exactly.
- Preserve route payload contract: `origin`, `destination`, `datetime`.
- Treat active profile as API-owned runtime state (stored server-side in `state/active-gtfs.json`).
