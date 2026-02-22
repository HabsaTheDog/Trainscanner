# Docs Agent Notes

## Scope

- `docs/*.md`
- Cross-repo contract docs that describe curation API/UI behavior.

## Required when curation workflow changes

- Keep endpoint docs aligned with runtime behavior:
  - `GET /api/qa/v2/clusters`
  - `GET /api/qa/v2/clusters/:cluster_id`
  - `POST /api/qa/v2/clusters/:cluster_id/decisions`
- Document staged conflict editor semantics:
  - local draft edits only until `Resolve Conflict`
  - one final POST payload (`operation=merge|split`)
  - inline rename pencil behavior and `rename_targets`
  - group lifecycle (create/delete/add selected members)
  - pairwise walk-time defaults (`min_walk_minutes=5`) and editability
- Do not reintroduce deprecated linked queue-item sections in curation UI/API docs.
- Keep command snippets copy-paste runnable.
