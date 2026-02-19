# Migration Notes (Phase 1 + 2)

Date: 2026-02-19

## Backward-compatible behavior kept

- Status vocabulary unchanged: `idle|switching|importing|restarting|ready|failed`
- `/api/routes` readiness gate unchanged (`ready` required)
- Active profile persistence remains `state/active-gtfs.json` (legacy auto-migration still supported)
- Existing script entrypoint names remain valid
- Frontend map stack remains MapLibre

## Additive behavior changes

- `POST /api/gtfs/activate` now returns:
  - `runId` for accepted switches
  - `reused=true` for duplicate in-flight requests of the same profile
  - `noop=true` when requested profile is already active/ready
- API failures now include `errorCode`
- Responses include `x-correlation-id` header
- Config schema validation is now enforced via `scripts/validate-config.sh` and CI
- `scripts/qa/build-profile.sh` now has idempotent skip behavior when existing manifest+artifact already match requested scope (override with `--force`)

## New commands

- `scripts/validate-config.sh`
- `scripts/qa/run-route-smoke.sh`
- `scripts/qa/run-route-regression.sh`
