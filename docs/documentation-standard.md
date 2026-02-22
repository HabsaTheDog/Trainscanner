# Documentation Standard

## General

- Update docs in the same change as behavior/contract updates.
- Use runnable commands and concrete endpoint paths.
- Keep terminology consistent with code (`cluster`, `candidate`, `group`, `Resolve Conflict`).

## Curation API/UI contract

- Describe cluster payloads without deprecated linked queue-item blocks.
- Describe decision writes as one final staged-editor submission to `POST /api/qa/v2/clusters/:cluster_id/decisions`.
- Allowed staged-editor operation outcomes: `merge` or `split`.
- If rename handling is documented, include `rename_targets` as the canonical-station rename transport and note it is draft-only until final resolve.
- Keep MapLibre references intact; do not document Leaflet migration unless explicitly requested.

## Validation commands

When curation behavior changes, documentation updates should be validated with:

```bash
cd frontend && npm run build
cd orchestrator && npm run check
cd orchestrator && npm run test:unit
npm run check:docs
```
