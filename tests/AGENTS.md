# Tests Agent Notes

## Scope

- `orchestrator/test/**`
- Root `tests/**` fixtures/docs when API payload contracts change.

## Curation contract checks

- Frontend smoke tests should assert staged conflict editor hooks (`Merge|Split|Group`, `Resolve Conflict`, no linked queue section).
- QA API unit/integration tests should align with:
  - single decision payload submission (`operation=merge|split`)
  - optional `rename_targets`
  - no `queue_items` blocks in v2 cluster list/detail responses
- Preserve checks that decision writes are atomic and merge-member audit semantics remain neutral (`action=merge_member`).
