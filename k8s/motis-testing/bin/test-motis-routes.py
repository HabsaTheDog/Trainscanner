#!/usr/bin/env python3
"""Run MOTIS routing assertions and write a result marker for the runner container."""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

MOTIS_BASE_URL = os.environ.get("MOTIS_BASE_URL", "http://127.0.0.1:8080").rstrip("/")
QUERY_FILE = os.environ.get("QUERY_FILE", "/inputs/work/queries.json")
RESULT_FILE = os.environ.get("RESULT_FILE", "/work/test-result")
REPORT_FILE = os.environ.get("REPORT_FILE", "/work/test-report.json")
HEALTH_TIMEOUT_SEC = int(os.environ.get("HEALTH_TIMEOUT_SEC", "240"))
REQUEST_TIMEOUT_SEC = int(os.environ.get("REQUEST_TIMEOUT_SEC", "20"))
TEST_MODE = os.environ.get("MOTIS_TEST_MODE", "unknown")

PLAN_PATHS = [
    "/api/v5/plan",
    "/v5/plan",
    "/api/v4/plan",
    "/v4/plan",
    "/api/v3/plan",
    "/v3/plan",
    "/api/v2/plan",
    "/v2/plan",
    "/api/v1/plan",
    "/v1/plan",
    "/api/plan",
    "/plan",
]
QUERY_VARIANTS = (
    lambda a, b, t: {
        "fromPlace": a,
        "toPlace": b,
        "time": t,
        "detailedTransfers": "true",
    },
    lambda a, b, t: {
        "fromPlace": a,
        "toPlace": b,
        "time": t,
        "detailed_transfers": "true",
    },
    lambda a, b, t: {
        "from_place": a,
        "to_place": b,
        "time": t,
        "detailed_transfers": "true",
    },
    lambda a, b, t: {
        "from": a,
        "to": b,
        "time": t,
        "detailed_transfers": "true",
    },
    lambda a, b, t: {
        "from": a,
        "to": b,
        "departure_time": t,
        "detailed_transfers": "true",
    },
    lambda a, b, t: {
        "from": a,
        "to": b,
        "departureTime": t,
        "detailed_transfers": "true",
    },
)


def log(message: str) -> None:
    print(f"[test-motis-routes] {message}", flush=True)


def fetch_json(url: str) -> tuple[int | None, object]:
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SEC) as response:
            body = response.read().decode("utf-8")
            if not body.strip():
                return response.status, {}
            try:
                return response.status, json.loads(body)
            except json.JSONDecodeError:
                return response.status, {"raw": body}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body) if body.strip() else {}
        except json.JSONDecodeError:
            parsed = {"raw": body}
        return exc.code, parsed
    except urllib.error.URLError as exc:
        return None, {"error": f"network_error: {exc}"}


def is_non_empty_route(payload: object) -> bool:
    if isinstance(payload, list):
        return len(payload) > 0
    if not isinstance(payload, dict):
        return False

    for key in ("itineraries", "direct", "connections", "journeys", "routes"):
        value = payload.get(key)
        if isinstance(value, list) and len(value) > 0:
            return True

    for nested_key in ("route", "result", "data", "content"):
        if is_non_empty_route(payload.get(nested_key)):
            return True

    return False


def wait_for_motis() -> None:
    log(f"Waiting for MOTIS at {MOTIS_BASE_URL} (timeout={HEALTH_TIMEOUT_SEC}s)")
    started = time.time()
    while True:
        for path in ("/health", "/api/health", "/"):
            status, _ = fetch_json(f"{MOTIS_BASE_URL}{path}")
            if status is not None and status != 503:
                # 404 on /health is accepted because MOTIS endpoint variants differ by build.
                log(f"MOTIS responded on {path} with status={status}")
                return

        elapsed = time.time() - started
        if elapsed >= HEALTH_TIMEOUT_SEC:
            raise RuntimeError("timed out waiting for MOTIS health/readiness")
        time.sleep(2)


def load_queries(path: str) -> list[dict[str, object]]:
    with open(path, "r", encoding="utf-8") as fp:
        payload = json.load(fp)
    if not isinstance(payload, list) or not payload:
        raise RuntimeError("query file must be a non-empty JSON array")

    normalized: list[dict[str, object]] = []
    for index, raw in enumerate(payload):
        if not isinstance(raw, dict):
            raise RuntimeError(f"query entry at index {index} is not an object")
        origin = str(raw.get("origin", "")).strip()
        destination = str(raw.get("destination", "")).strip()
        when = str(raw.get("datetime", "")).strip()
        if not origin or not destination or not when:
            raise RuntimeError(
                f"query entry at index {index} is missing origin/destination/datetime"
            )
        normalized.append(
            {
                "name": str(raw.get("name", f"query-{index + 1}")),
                "origin": origin,
                "destination": destination,
                "datetime": when,
                "required": bool(raw.get("required", True)),
            }
        )
    return normalized


def run_single_query(query: dict[str, object]) -> dict[str, object]:
    origin = str(query["origin"])
    destination = str(query["destination"])
    when = str(query["datetime"])
    attempts: list[dict[str, object]] = []

    for path in PLAN_PATHS:
        probe_status, probe_body = fetch_json(f"{MOTIS_BASE_URL}{path}")
        attempts.append({"path": path, "probeStatus": probe_status})
        if probe_status == 404:
            continue
        if probe_status is None:
            continue

        for variant_builder in QUERY_VARIANTS:
            params = variant_builder(origin, destination, when)
            url = f"{MOTIS_BASE_URL}{path}?{urllib.parse.urlencode(params)}"
            status, body = fetch_json(url)
            attempts.append(
                {
                    "path": path,
                    "status": status,
                    "params": params,
                }
            )
            if status == 200 and is_non_empty_route(body):
                return {
                    "ok": True,
                    "name": query["name"],
                    "status": status,
                    "path": path,
                    "responseExcerpt": body if isinstance(body, dict) else {},
                    "attempts": attempts[-4:],
                }

    return {
        "ok": False,
        "name": query["name"],
        "attempts": attempts,
    }


def write_result(exit_code: int, message: str) -> None:
    os.makedirs(os.path.dirname(RESULT_FILE), exist_ok=True)
    with open(RESULT_FILE, "w", encoding="utf-8") as fp:
        fp.write(f"{exit_code} {message}\n")


def main() -> int:
    results: list[dict[str, object]] = []
    report: dict[str, object] = {
        "mode": TEST_MODE,
        "baseUrl": MOTIS_BASE_URL,
        "queryFile": QUERY_FILE,
        "results": results,
    }

    try:
        queries = load_queries(QUERY_FILE)
        wait_for_motis()
        required_failures: list[str] = []

        for query in queries:
            log(
                "Running query "
                f"name={query['name']} required={query['required']} "
                f"origin={query['origin']} destination={query['destination']}"
            )
            result = run_single_query(query)
            results.append(result)
            if not result.get("ok"):
                if query["required"]:
                    required_failures.append(str(query["name"]))
                    log(f"FAILED required query: {query['name']}")
                else:
                    log(f"FAILED optional query: {query['name']}")
            else:
                log(f"PASSED query: {query['name']}")

        report["requiredFailures"] = required_failures
        report["ok"] = len(required_failures) == 0
    except Exception as exc:  # pylint: disable=broad-except
        report["ok"] = False
        report["error"] = str(exc)

    os.makedirs(os.path.dirname(REPORT_FILE), exist_ok=True)
    with open(REPORT_FILE, "w", encoding="utf-8") as fp:
        json.dump(report, fp, indent=2, ensure_ascii=True)
        fp.write("\n")

    if report.get("ok"):
        write_result(0, "all required queries passed")
        return 0

    write_result(1, "required query failures or execution error")
    return 1


if __name__ == "__main__":
    sys.exit(main())
