# Ephemeral MOTIS K8s Testing Grid

This directory contains Kubernetes job templates and helper scripts for Phase 6.2:

- `micro-job.template.yaml`: localized regression checks from bbox-scoped GTFS.
- `macro-job.template.yaml`: sparse macro graph checks for long-distance integrity.
- `bin/motis-job-runner.sh`: in-pod MOTIS import + server lifecycle.
- `bin/test-motis-routes.py`: in-pod query assertions.

Use the orchestration wrapper:

```bash
scripts/run-motis-k8s-test.sh --mode micro \
  --gtfs-path data/gtfs/runtime/de/2026-02-20/active-gtfs.zip \
  --tier regional \
  --bbox "48.05,11.35,48.30,11.75"
```

```bash
scripts/run-motis-k8s-test.sh --mode macro \
  --gtfs-path data/artifacts/canonical-high-speed-2026-02-20.zip \
  --tier high-speed
```

## Prerequisites

- Reachable Kubernetes cluster (`kubectl cluster-info` works).
- Local OSM file at `data/motis/osm.pbf` (or pass `--osm-path`).
- `python3` on the local machine (used for artifact prep).

## kind and minikube Notes

The generated job mounts local artifact/OSM directories using `hostPath`.
When your cluster node filesystem uses different paths, pass `--node-path-map`.

Example (`kind` with workspace mounted at `/workspace/trainscanner` inside node):

```bash
scripts/run-motis-k8s-test.sh --mode macro \
  --gtfs-path "$PWD/data/artifacts/canonical-high-speed-2026-02-20.zip" \
  --tier high-speed \
  --node-path-map "$PWD=/workspace/trainscanner"
```

Example (`minikube` after mounting repo path into VM):

```bash
minikube mount "$PWD:/workspace/trainscanner"
scripts/run-motis-k8s-test.sh --mode micro \
  --gtfs-path "$PWD/data/gtfs/runtime/de/2026-02-20/active-gtfs.zip" \
  --tier regional \
  --bbox "48.05,11.35,48.30,11.75" \
  --node-path-map "$PWD=/workspace/trainscanner"
```

## Output

Prepared scoped artifacts and generated query suites are written to:

- `data/motis-k8s/<job-name>/active-gtfs.zip`
- `data/motis-k8s/<job-name>/queries.json`
- `data/motis-k8s/<job-name>/job.yaml`
