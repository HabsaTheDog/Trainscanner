# Agent Prompt: Task 6.2 - Ephemeral K8s MOTIS Testing Grid

**Context & Objective:**
You are an AI agent tasked with executing Phase 6, Task 6.2 — the FINAL task of the Trainscanner V2 migration.
Your goal is to implement a Kubernetes orchestration layer that dynamically spins up ephemeral MOTIS routing-engine pods for automated integration testing of the compiled GTFS artifacts. Testing the full European graph at once is cost-prohibitive; instead, we use two test modes targeting specific regions.

**Current State Reference:**

- Task 6.1 is complete. The GTFS export script (`scripts/qa/export-canonical-gtfs.py`) now emits tier-constrained GTFS ZIPs (with `--tier high-speed`, `--tier regional`, `--tier local`, `--tier all`).
- There are existing MOTIS scripts in the repo:
  - `scripts/init-motis.sh` — initializes MOTIS with GTFS data
  - `scripts/check-motis-data.sh` — validates MOTIS data state
  - `scripts/run-test-env.sh` — boots a test environment
  - `scripts/find-working-route.sh` — finds valid routes in MOTIS
- The project uses Docker Compose (`docker-compose.yml`) and has a CI pipeline (`.github/workflows/ci-pr.yml`).

**Your Instructions:**

1. **Create K8s Job/Pod Templates:**
   - Create a new directory `k8s/motis-testing/` (or similar).
   - Write Kubernetes Job manifests (YAML) for two test modes:
     - **Micro-Graph Mode (Local/Regional updates):** Accepts parameters for an affected bounding box (lat/lon bbox + padding). The MOTIS pod loads only the subset GTFS feed covering this region. Used for quick regression checks after localized data updates.
     - **Sparse Macro-Graph Mode (High-Speed/Long-Distance):** Loads the High-Speed tier GTFS feed across the full European extent but with sparse data (no local stops). Used to verify cross-border long-distance routing integrity.

2. **Orchestration Script:**
   - Write a script (Bash or Python, e.g., `scripts/run-motis-k8s-test.sh`) that:
     - Takes arguments: `--mode micro|macro`, `--bbox "lat1,lon1,lat2,lon2"` (for micro mode), `--tier <tier>`, `--gtfs-path <path>`.
     - Applies the correct K8s Job template.
     - Waits for the pod to complete and collects the exit code & logs.
     - Cleans up (deletes) the ephemeral pod/job after the test.

3. **MOTIS Regression Test Logic:**
   - Inside the K8s job, the MOTIS container should:
     - Import the provided GTFS ZIP.
     - Run a set of predefined test queries (e.g., "route from Berlin Hbf to Wien Hbf", "route from Zürich HB to München Hbf").
     - Validate that all test queries return valid routes that are not empty.
     - Exit with code 0 on success, non-zero on failure.
   - You can reuse or adapt logic from `scripts/find-working-route.sh`.

4. **CI Integration (Optional but Recommended):**
   - Add a step to `.github/workflows/ci-pr.yml` (or a new workflow) that triggers a micro-graph MOTIS test on PR when GTFS-related files change.

**Deliverables:**

- Kubernetes Job manifests in `k8s/motis-testing/`.
- The orchestration script (`scripts/run-motis-k8s-test.sh` or similar).
- Updated CI workflow if applicable.
- Brief documentation on how to run the tests locally with `minikube` or `kind`.

Please begin your analysis and implement the Ephemeral K8s MOTIS Testing Grid.
