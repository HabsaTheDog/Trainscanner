#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

MODE=""
BBOX=""
BBOX_PADDING_KM="20"
TIER=""
GTFS_PATH=""
OSM_PATH="${ROOT_DIR}/data/motis/osm.pbf"
QUERY_FILE_OVERRIDE=""
NAMESPACE="motis-testing"
JOB_NAME=""
NODE_PATH_MAP=""
MOTIS_IMAGE="${MOTIS_IMAGE:-ghcr.io/motis-project/motis:latest}"
TESTER_IMAGE="${TESTER_IMAGE:-python:3.11-slim}"
TIMEOUT_SEC="1200"
HEALTH_TIMEOUT_SEC="240"
KEEP_RESOURCES="false"

SCRIPTS_CONFIGMAP=""
MAP_LOCAL_PREFIX=""
MAP_NODE_PREFIX=""

usage() {
  cat <<USAGE
Usage:
  scripts/run-motis-k8s-test.sh --mode micro|macro --gtfs-path <path> [options]

Options:
  --mode <micro|macro>           Test mode (required)
  --gtfs-path <path>             Source GTFS ZIP path on local machine (required)
  --tier <tier>                  Tier filter: high-speed|regional|local|all
                                 default: micro=regional, macro=high-speed
  --bbox <lat1,lon1,lat2,lon2>   Required in micro mode
  --bbox-padding-km <km>         Extra bbox padding for micro mode (default: 20)
  --osm-path <path>              OSM PBF path (default: data/motis/osm.pbf)
  --queries-json <path>          Optional override query suite JSON
  --namespace <name>             K8s namespace (default: motis-testing)
  --job-name <name>              K8s job name (default: generated)
  --motis-image <image>          MOTIS image (default: ghcr.io/motis-project/motis:latest)
  --tester-image <image>         Tester image (default: python:3.11-slim)
  --timeout-sec <sec>            Overall wait timeout (default: 1200)
  --health-timeout-sec <sec>     MOTIS health wait timeout (default: 240)
  --node-path-map <local>=<node> Rebase hostPath references for kind/minikube mounts
  --keep-resources               Keep job/configmap after execution
  --help                         Show this help
USAGE
}

fail() {
  printf '[run-motis-k8s-test] ERROR: %s\n' "$*" >&2
  exit 1
}

log() {
  printf '[run-motis-k8s-test] %s\n' "$*"
}

escape_sed_replacement() {
  printf '%s' "$1" | sed -e 's/[&|]/\\&/g'
}

to_node_path() {
  local local_path="$1"
  if [[ -z "$MAP_LOCAL_PREFIX" ]]; then
    printf '%s' "$local_path"
    return 0
  fi
  if [[ "$local_path" == "$MAP_LOCAL_PREFIX"* ]]; then
    printf '%s%s' "$MAP_NODE_PREFIX" "${local_path#"$MAP_LOCAL_PREFIX"}"
    return 0
  fi
  fail "path '$local_path' is outside mapped prefix '$MAP_LOCAL_PREFIX' for --node-path-map"
}

# shellcheck disable=SC2329 # invoked via trap on EXIT
cleanup() {
  if [[ "$KEEP_RESOURCES" == "true" ]]; then
    return 0
  fi
  if [[ -n "$SCRIPTS_CONFIGMAP" ]]; then
    kubectl -n "$NAMESPACE" delete configmap "$SCRIPTS_CONFIGMAP" --ignore-not-found >/dev/null 2>&1 || true
  fi
  if [[ -n "$JOB_NAME" ]]; then
    kubectl -n "$NAMESPACE" delete job "$JOB_NAME" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi
}

trap 'cleanup' EXIT

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --gtfs-path)
      GTFS_PATH="${2:-}"
      shift 2
      ;;
    --tier)
      TIER="${2:-}"
      shift 2
      ;;
    --bbox)
      BBOX="${2:-}"
      shift 2
      ;;
    --bbox-padding-km)
      BBOX_PADDING_KM="${2:-}"
      shift 2
      ;;
    --osm-path)
      OSM_PATH="${2:-}"
      shift 2
      ;;
    --queries-json)
      QUERY_FILE_OVERRIDE="${2:-}"
      shift 2
      ;;
    --namespace)
      NAMESPACE="${2:-}"
      shift 2
      ;;
    --job-name)
      JOB_NAME="${2:-}"
      shift 2
      ;;
    --motis-image)
      MOTIS_IMAGE="${2:-}"
      shift 2
      ;;
    --tester-image)
      TESTER_IMAGE="${2:-}"
      shift 2
      ;;
    --timeout-sec)
      TIMEOUT_SEC="${2:-}"
      shift 2
      ;;
    --health-timeout-sec)
      HEALTH_TIMEOUT_SEC="${2:-}"
      shift 2
      ;;
    --node-path-map)
      NODE_PATH_MAP="${2:-}"
      shift 2
      ;;
    --keep-resources)
      KEEP_RESOURCES="true"
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      fail "unknown argument: $1"
      ;;
  esac
done

[[ "$MODE" == "micro" || "$MODE" == "macro" ]] || fail "--mode must be micro or macro"
[[ -n "$GTFS_PATH" ]] || fail "--gtfs-path is required"
[[ "$TIMEOUT_SEC" =~ ^[0-9]+$ ]] || fail "--timeout-sec must be an integer"
[[ "$HEALTH_TIMEOUT_SEC" =~ ^[0-9]+$ ]] || fail "--health-timeout-sec must be an integer"

if [[ -z "$TIER" ]]; then
  if [[ "$MODE" == "micro" ]]; then
    TIER="regional"
  else
    TIER="high-speed"
  fi
fi
case "$TIER" in
  high-speed|regional|local|all) ;;
  *) fail "--tier must be one of high-speed|regional|local|all" ;;
esac

if [[ "$MODE" == "micro" && -z "$BBOX" ]]; then
  fail "--bbox is required for micro mode"
fi

if [[ -n "$NODE_PATH_MAP" ]]; then
  if [[ "$NODE_PATH_MAP" != *=* ]]; then
    fail "--node-path-map must be in format <local>=<node>"
  fi
  MAP_LOCAL_PREFIX="$(realpath "${NODE_PATH_MAP%%=*}")"
  MAP_NODE_PREFIX="${NODE_PATH_MAP#*=}"
  [[ -n "$MAP_NODE_PREFIX" ]] || fail "--node-path-map target prefix is empty"
fi

for cmd in kubectl python3 sed awk; do
  command -v "$cmd" >/dev/null 2>&1 || fail "required command not found: $cmd"
done

kubectl version --client >/dev/null 2>&1 || fail "kubectl is not configured correctly"
kubectl cluster-info >/dev/null 2>&1 || fail "cannot reach Kubernetes cluster"

GTFS_ABS="$(realpath "$GTFS_PATH")"
OSM_ABS="$(realpath "$OSM_PATH")"
[[ -f "$GTFS_ABS" ]] || fail "GTFS file not found: $GTFS_ABS"
[[ -f "$OSM_ABS" ]] || fail "OSM file not found: $OSM_ABS"

if [[ -n "$QUERY_FILE_OVERRIDE" ]]; then
  QUERY_FILE_OVERRIDE="$(realpath "$QUERY_FILE_OVERRIDE")"
  [[ -f "$QUERY_FILE_OVERRIDE" ]] || fail "queries file not found: $QUERY_FILE_OVERRIDE"
fi

if [[ -z "$JOB_NAME" ]]; then
  JOB_NAME="motis-${MODE}-$(date +%Y%m%d%H%M%S)-$((RANDOM % 10000))"
fi
JOB_NAME="$(printf '%s' "$JOB_NAME" | tr '[:upper:]' '[:lower:]' | tr -cs 'a-z0-9-' '-')"
JOB_NAME="${JOB_NAME#-}"
JOB_NAME="${JOB_NAME%-}"
[[ -n "$JOB_NAME" ]] || fail "computed empty job name"
JOB_NAME="$(printf '%s' "$JOB_NAME" | cut -c1-63)"
JOB_NAME="${JOB_NAME#-}"
JOB_NAME="${JOB_NAME%-}"
[[ -n "$JOB_NAME" ]] || fail "computed empty job name after truncation"

SCRIPTS_CONFIGMAP="${JOB_NAME}-scripts"
if [[ "${#SCRIPTS_CONFIGMAP}" -gt 63 ]]; then
  SCRIPTS_CONFIGMAP="$(printf '%s' "$SCRIPTS_CONFIGMAP" | cut -c1-63)"
fi
SCRIPTS_CONFIGMAP="${SCRIPTS_CONFIGMAP#-}"
SCRIPTS_CONFIGMAP="${SCRIPTS_CONFIGMAP%-}"
[[ -n "$SCRIPTS_CONFIGMAP" ]] || fail "computed empty scripts configmap name"

ARTIFACT_DIR="${ROOT_DIR}/data/motis-k8s/${JOB_NAME}"
mkdir -p "$ARTIFACT_DIR"
PREPARED_GTFS="${ARTIFACT_DIR}/active-gtfs.zip"
QUERIES_JSON="${ARTIFACT_DIR}/queries.json"

PREP_ARGS=(
  "$ROOT_DIR/scripts/qa/prepare-motis-k8s-artifacts.py"
  --mode "$MODE"
  --input-zip "$GTFS_ABS"
  --output-zip "$PREPARED_GTFS"
  --queries-json "$QUERIES_JSON"
  --tier "$TIER"
)
if [[ "$MODE" == "micro" ]]; then
  PREP_ARGS+=(--bbox "$BBOX" --padding-km "$BBOX_PADDING_KM")
fi

log "Preparing scoped GTFS artifact and query suite..."
python3 "${PREP_ARGS[@]}"
if [[ -n "$QUERY_FILE_OVERRIDE" ]]; then
  cp "$QUERY_FILE_OVERRIDE" "$QUERIES_JSON"
fi

WORK_HOST_DIR_NODE="$(to_node_path "$ARTIFACT_DIR")"
OSM_HOST_DIR_NODE="$(to_node_path "$(dirname "$OSM_ABS")")"

GTFS_FILE_NAME="$(basename "$PREPARED_GTFS")"
QUERY_FILE_NAME="$(basename "$QUERIES_JSON")"
OSM_FILE_NAME="$(basename "$OSM_ABS")"
ACTIVE_DEADLINE_SECONDS="$((TIMEOUT_SEC + 120))"

log "Ensuring namespace '$NAMESPACE' exists..."
kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$NAMESPACE" >/dev/null

log "Creating ephemeral script configmap..."
kubectl -n "$NAMESPACE" create configmap "$SCRIPTS_CONFIGMAP" \
  --from-file=motis-job-runner.sh="$ROOT_DIR/k8s/motis-testing/bin/motis-job-runner.sh" \
  --from-file=test-motis-routes.py="$ROOT_DIR/k8s/motis-testing/bin/test-motis-routes.py" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

TEMPLATE_PATH="${ROOT_DIR}/k8s/motis-testing/${MODE}-job.template.yaml"
[[ -f "$TEMPLATE_PATH" ]] || fail "template missing: $TEMPLATE_PATH"
MANIFEST_PATH="${ARTIFACT_DIR}/job.yaml"

rendered="$(cat "$TEMPLATE_PATH")"
replace_token() {
  local key="$1"
  local value="$2"
  local escaped
  escaped="$(escape_sed_replacement "$value")"
  rendered="$(printf '%s' "$rendered" | sed "s|__${key}__|${escaped}|g")"
}

replace_token "JOB_NAME" "$JOB_NAME"
replace_token "NAMESPACE" "$NAMESPACE"
replace_token "SCRIPTS_CONFIGMAP" "$SCRIPTS_CONFIGMAP"
replace_token "WORK_HOST_DIR" "$WORK_HOST_DIR_NODE"
replace_token "OSM_HOST_DIR" "$OSM_HOST_DIR_NODE"
replace_token "GTFS_FILE_NAME" "$GTFS_FILE_NAME"
replace_token "QUERY_FILE_NAME" "$QUERY_FILE_NAME"
replace_token "OSM_FILE_NAME" "$OSM_FILE_NAME"
replace_token "MOTIS_IMAGE" "$MOTIS_IMAGE"
replace_token "TESTER_IMAGE" "$TESTER_IMAGE"
replace_token "TEST_TIMEOUT_SEC" "$TIMEOUT_SEC"
replace_token "HEALTH_TIMEOUT_SEC" "$HEALTH_TIMEOUT_SEC"
replace_token "ACTIVE_DEADLINE_SECONDS" "$ACTIVE_DEADLINE_SECONDS"
replace_token "TIER" "$TIER"
replace_token "BBOX" "$BBOX"

printf '%s\n' "$rendered" > "$MANIFEST_PATH"

log "Applying job manifest: $MANIFEST_PATH"
kubectl apply -f "$MANIFEST_PATH" >/dev/null

log "Waiting for job completion (timeout=${TIMEOUT_SEC}s)..."
deadline_ts="$((SECONDS + TIMEOUT_SEC))"
job_complete="false"
job_failed="false"
while true; do
  complete_status="$(kubectl -n "$NAMESPACE" get job "$JOB_NAME" -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null || true)"
  failed_status="$(kubectl -n "$NAMESPACE" get job "$JOB_NAME" -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || true)"
  if [[ "$complete_status" == "True" ]]; then
    job_complete="true"
    break
  fi
  if [[ "$failed_status" == "True" ]]; then
    job_failed="true"
    break
  fi
  if [[ "$SECONDS" -ge "$deadline_ts" ]]; then
    log "Timed out waiting for job completion."
    break
  fi
  sleep 5
done

POD_NAME="$(kubectl -n "$NAMESPACE" get pods -l "job-name=${JOB_NAME}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
if [[ -n "$POD_NAME" ]]; then
  log "Collected pod: $POD_NAME"
  for container in motis-runner tester; do
    printf '\n===== %s logs (%s) =====\n' "$container" "$POD_NAME"
    kubectl -n "$NAMESPACE" logs "$POD_NAME" -c "$container" --timestamps=true || true
  done
fi

MOTIS_EXIT="1"
TESTER_EXIT="1"
if [[ -n "$POD_NAME" ]]; then
  motis_raw="$(kubectl -n "$NAMESPACE" get pod "$POD_NAME" -o jsonpath='{.status.containerStatuses[?(@.name=="motis-runner")].state.terminated.exitCode}' 2>/dev/null || true)"
  tester_raw="$(kubectl -n "$NAMESPACE" get pod "$POD_NAME" -o jsonpath='{.status.containerStatuses[?(@.name=="tester")].state.terminated.exitCode}' 2>/dev/null || true)"
  [[ "$motis_raw" =~ ^[0-9]+$ ]] && MOTIS_EXIT="$motis_raw"
  [[ "$tester_raw" =~ ^[0-9]+$ ]] && TESTER_EXIT="$tester_raw"
fi

printf '\n[run-motis-k8s-test] Summary\n'
printf '  mode: %s\n' "$MODE"
printf '  namespace: %s\n' "$NAMESPACE"
printf '  job: %s\n' "$JOB_NAME"
printf '  artifacts: %s\n' "$ARTIFACT_DIR"
printf '  job_complete: %s\n' "$job_complete"
printf '  job_failed: %s\n' "$job_failed"
printf '  motis_exit: %s\n' "$MOTIS_EXIT"
printf '  tester_exit: %s\n' "$TESTER_EXIT"

if [[ "$job_complete" == "true" && "$MOTIS_EXIT" == "0" && "$TESTER_EXIT" == "0" ]]; then
  exit 0
fi

if [[ "$job_failed" != "true" && "$job_complete" != "true" ]]; then
  exit 124
fi

exit 1
