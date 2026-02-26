# Trainscanner Project Cleanup Plan

This document outlines a step-by-step plan to clean up the Trainscanner repository, improve structural consistency, and remove unused/legacy code. It is designed to be handed off to an implementation agent.

## 1. Root Directory De-cluttering
The root directory currently contains many planning documents, transient logs, and agent prompts that obscure the core project files.

**Action Items:**
- **Create a `docs/planning/` directory:**
  - Move `Project Scope.md` to `docs/planning/`.
  - Move `Agent_Migration_Plan.md` to `docs/planning/`.
- **Create a `docs/agents/` directory:**
  - Move all `Agent_Prompt_Task_*.md` files (12 files) into `docs/agents/`.
  - Move `AGENTS.md` into `docs/agents/`.
- **Delete Transient Files:**
  - Remove `clippy_out.txt` (a temporary Rust linter output log).
  - Ensure `.ruff_cache/` is added to `.gitignore` and removed from tracking if committed.

## 2. Service & Worker Consolidation
The project currently splits its backend components arbitrarily between root-level folders (`orchestrator`, `control-plane`), a `workers/` directory, and a `services/` directory. This creates confusion.

**Action Items:**
- **Establish a single `services/` (or `apps/`) monorepo structure:**
  - Move `orchestrator/` -> `services/orchestrator/`
  - Move `control-plane/` -> `services/control-plane/`
  - Move `workers/python-ai/` -> `services/python-ai-worker/`
  - Move `workers/rust-ingestion/` -> `services/rust-ingestion-worker/`
  - Keep `services/ai-scoring/` as is.
- **Remove empty directories:**
  - Once the `workers/` directory is empty, delete it.
- **Update references:**
  - Update `docker-compose.yml` to reflect the new paths for any referenced services.
  - Update root `package.json` workspaces (if used) to point to `services/*` and `frontend`.

## 3. Legacy Code Removal
The frontend contains explicitly marked legacy code that is likely no longer used since the migration to React/Vite (`.jsx` files).

**Action Items:**
- **Evaluate and Delete Legacy Frontend Code:**
  - Inspect `frontend/src/legacy/curation-logic.js` and `frontend/src/legacy/home-logic.js`.
  - If their logic has been fully ported to `CurationPage.jsx` and `HomePage.jsx` (which is highly likely), safely delete the `frontend/src/legacy/` directory.

## 4. Data and State Directory Cleanup
The `data/` directory contains typos and manual testing folders that lack standardization.

**Action Items:**
- **Rename poorly named test directories:**
  - Rename `data/gtfs for testing (manualy downloaded)/` to a standardized name like `data/test-fixtures/gtfs/`.
- **Clean up the `state/` directory:**
  - There is a root `state/` directory and also `orchestrator/state/gtfs-switch-status.json`. Consolidate these state files into a single location or explicitly move them to the respective service's local state folder.
- **Gitignore Review:**
  - Ensure `data/raw/`, `data/gtfs/`, `data/postgis/`, and any other data-heavy folders are correctly listed in `.gitignore` to prevent massive accidental commits. Ensure `target/` in Rust and `__pycache__` / `.ruff_cache` in Python are ignored globally.

## 5. Testing and Scripts Standardization
Scripts and tests are slightly scattered. 

**Action Items:**
- **Root Tests vs. Service Tests:**
  - Move root `tests/routes/` to the appropriate service (e.g., `services/orchestrator/test/` or `services/control-plane/tests/`) or establish a dedicated root `e2e-tests/` directory if they are true end-to-end tests.
  - Remove `tests/AGENTS.md` (or move to `docs/agents/`).

## Execution Sequence for the Implementing Agent
1. **Move Documentation:** Relocate Markdown files from the root to `docs/`.
2. **Consolidate Services:** Move all backend components into the unified `services/` folder and update `docker-compose.yml`.
3. **Delete Legacy:** Remove `frontend/src/legacy/` and `clippy_out.txt`.
4. **Fix Data Folders:** Rename the manually downloaded GTFS folder.
5. **Update Paths & Ignore Rules:** Ensure all import paths, script paths, and `.gitignore` rules align with the new structure. Ensure a `git status` check is clean and all tests still pass.
