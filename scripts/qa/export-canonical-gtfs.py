#!/usr/bin/env python3
import argparse
import csv
import io
import json
import os
import re
import sqlite3
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import NoReturn


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent.parent
PSQL_ON_ERROR_STOP = "ON_ERROR_STOP=1"
TRANSFERS_FILE = "transfers.txt"
VALID_TIERS = {"high-speed", "regional", "local", "all"}
VALID_QUERY_MODES = {"legacy", "optimized"}
DEFAULT_PROGRESS_INTERVAL_SEC = 20
PARALLEL_SHM_ERROR_TOKEN = "could not resize shared memory segment"
GTFS_CORE_FILES = [
    "agency.txt",
    "stops.txt",
    "routes.txt",
    "trips.txt",
    "stop_times.txt",
    "calendar.txt",
]


@dataclass(frozen=True)
class ExportBatchOptions:
    profile: str
    requested_tier: str
    as_of: str
    country: str
    source_id: str
    batch_size_trips: int
    agency_url: str
    output_zip: str
    query_mode: str
    benchmark_max_sources: int
    benchmark_max_batches: int
    benchmark_max_trips: int
    sql_profile_sample: bool
    progress_interval_sec: int


def fail(msg: str) -> NoReturn:
    print(f"[export-canonical-gtfs] ERROR: {msg}", file=sys.stderr)
    raise SystemExit(1)


def log(msg: str):
    print(f"[export-canonical-gtfs] {msg}", file=sys.stderr, flush=True)


def log_progress(progress_state: dict, message: str, *, force: bool = False):
    interval = int(progress_state.get("intervalSec") or 0)
    now = time.perf_counter()
    if not force:
        if interval <= 0:
            return
        if now - float(progress_state.get("lastLogAt") or 0.0) < float(interval):
            return
    progress_state["lastLogAt"] = now
    elapsed = max(now - float(progress_state.get("runStartedAt") or now), 0.0)
    log(f"{message}; elapsed={elapsed:.1f}s")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export deterministic GTFS from pan-European timetable and transfer facts"
    )
    parser.add_argument("--stops-csv")
    parser.add_argument("--from-db", action="store_true")
    parser.add_argument("--profile", default="pan_europe_runtime")
    parser.add_argument(
        "--as-of", default=datetime.now(timezone.utc).strftime("%Y-%m-%d")
    )
    parser.add_argument("--country")
    parser.add_argument("--source-id")
    parser.add_argument("--batch-size-trips", type=int, default=25000)
    parser.add_argument("--tier", default="all")
    parser.add_argument("--output-zip")
    parser.add_argument("--summary-json")
    parser.add_argument("--output-dir", default="data/artifacts")
    parser.add_argument("--agency-url", default="https://example.invalid/trainscanner")
    parser.add_argument("--query-mode", default="optimized")
    parser.add_argument("--benchmark-max-sources", type=int, default=0)
    parser.add_argument("--benchmark-max-batches", type=int, default=0)
    parser.add_argument("--benchmark-max-trips", type=int, default=0)
    parser.add_argument("--sql-profile-sample", action="store_true")
    parser.add_argument(
        "--progress-interval-sec", type=int, default=DEFAULT_PROGRESS_INTERVAL_SEC
    )
    return parser.parse_args()


def normalize_tier(raw: str) -> str:
    value = str(raw or "").strip().lower()
    aliases = {
        "high_speed": "high-speed",
        "highspeed": "high-speed",
    }
    value = aliases.get(value, value)
    if value not in VALID_TIERS:
        fail(
            "invalid --tier value '"
            + str(raw)
            + "' (expected one of: high-speed, regional, local, all)"
        )
    return value


def normalize_as_of(raw: str) -> str:
    try:
        dt = datetime.strptime(str(raw or "").strip(), "%Y-%m-%d")
    except Exception:
        fail(f"invalid --as-of value '{raw}' (expected YYYY-MM-DD)")
        return "1970-01-01"
    return dt.strftime("%Y-%m-%d")


def normalize_country(raw: str) -> str:
    value = str(raw or "").strip().upper()
    if not value:
        return ""
    if not re.match(r"^[A-Z]{2}$", value):
        fail("invalid --country value (expected ISO-3166 alpha-2)")
    return value


def normalize_source_id(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    if not re.match(r"^[A-Za-z0-9._:-]+$", value):
        fail("invalid --source-id value (allowed: A-Z a-z 0-9 . _ : -)")
    return value


def normalize_batch_size(raw: int) -> int:
    try:
        value = int(raw)
    except Exception:
        fail("invalid --batch-size-trips value (expected integer)")
        return 25000
    if value <= 0:
        fail("invalid --batch-size-trips value (must be > 0)")
    if value > 1000000:
        fail("invalid --batch-size-trips value (must be <= 1000000)")
    return value


def normalize_positive_limit(raw: int, arg_name: str) -> int:
    try:
        value = int(raw or 0)
    except Exception:
        fail(f"invalid {arg_name} value (expected integer >= 0)")
        return 0
    if value < 0:
        fail(f"invalid {arg_name} value (must be >= 0)")
    return value


def normalize_query_mode(raw: str) -> str:
    mode = str(raw or "").strip().lower()
    if mode not in VALID_QUERY_MODES:
        fail(
            "invalid --query-mode value '"
            + str(raw)
            + "' (expected one of: legacy, optimized)"
        )
    return mode


def csv_text(header, rows):
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(header)
    for row in rows:
        writer.writerow(row)
    return out.getvalue()


def _run_command(cmd, *, env=None, cwd=None):
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True,
        env=env,
        cwd=cwd,
    )


def _parse_psql_csv(content: str):
    reader = csv.DictReader(io.StringIO(content))
    return list(reader)


def _build_psql_cmd(
    query: str, *, csv_mode: bool, pgoptions_override: str | None = None
):
    settings = _read_db_settings()
    env = _build_psql_env(settings["password"], pgoptions_override)
    common_args = _build_psql_common_args(query, csv_mode=csv_mode)
    psql_bin = shutil.which("psql")
    if psql_bin:
        return _build_host_psql_cmd(psql_bin, settings, common_args), env
    return _build_docker_psql_cmd(settings, common_args, env), env


def _read_db_settings() -> dict:
    db_url = (
        os.environ.get("CANONICAL_DB_URL") or os.environ.get("DATABASE_URL") or ""
    ).strip()
    db_host = (
        os.environ.get("CANONICAL_DB_HOST") or os.environ.get("PGHOST") or "localhost"
    ).strip()
    db_port = (
        os.environ.get("CANONICAL_DB_PORT") or os.environ.get("PGPORT") or "5432"
    ).strip()
    db_user = (
        os.environ.get("CANONICAL_DB_USER")
        or os.environ.get("PGUSER")
        or "trainscanner"
    ).strip()
    db_name = (
        os.environ.get("CANONICAL_DB_NAME")
        or os.environ.get("PGDATABASE")
        or "trainscanner"
    ).strip()
    db_password = (
        os.environ.get("CANONICAL_DB_PASSWORD")
        or os.environ.get("PGPASSWORD")
        or "trainscanner"
    )
    return {
        "url": db_url,
        "host": db_host,
        "port": db_port,
        "user": db_user,
        "name": db_name,
        "password": db_password,
    }


def _build_psql_env(db_password: str, pgoptions_override: str | None = None) -> dict:
    env = dict(os.environ)
    env["PGPASSWORD"] = db_password
    export_pgoptions = (
        str(pgoptions_override).strip()
        if pgoptions_override is not None
        else os.environ.get("EXPORT_GTFS_PGOPTIONS", "").strip()
    )
    if export_pgoptions:
        env["PGOPTIONS"] = export_pgoptions
    elif not str(env.get("PGOPTIONS", "")).strip():
        env["PGOPTIONS"] = (
            "-c work_mem=16MB "
            "-c maintenance_work_mem=64MB "
            "-c max_parallel_workers_per_gather=2 "
            "-c max_parallel_workers=8"
        )
    return env


def _build_psql_common_args(query: str, *, csv_mode: bool) -> list[str]:
    common_args = ["-X", "-v", PSQL_ON_ERROR_STOP]
    if csv_mode:
        common_args.append("--csv")
    else:
        common_args.append("-At")
    common_args.extend(["-c", query])
    return common_args


def _build_host_psql_cmd(
    psql_bin: str, settings: dict, common_args: list[str]
) -> list[str]:
    if settings["url"]:
        return [psql_bin, settings["url"], *common_args]
    return [
        psql_bin,
        "-h",
        settings["host"],
        "-p",
        settings["port"],
        "-U",
        settings["user"],
        "-d",
        settings["name"],
        *common_args,
    ]


def _build_docker_psql_cmd(
    settings: dict, common_args: list[str], env: dict
) -> list[str]:
    cmd = [
        "docker",
        "compose",
        "--profile",
        "pan-europe-data",
        "exec",
        "-T",
    ]
    if str(env.get("PGOPTIONS", "")).strip():
        cmd.extend(["-e", f"PGOPTIONS={env['PGOPTIONS']}"])
    cmd.extend(["postgis", "psql", "-U", settings["user"], "-d", settings["name"]])
    cmd.extend(common_args)
    return cmd


def _is_parallel_shm_error(stderr_text: str) -> bool:
    text = str(stderr_text or "")
    return PARALLEL_SHM_ERROR_TOKEN in text and "No space left on device" in text


def _run_psql_with_safe_parallel_retry(query: str, *, csv_mode: bool) -> str:
    cmd, env = _build_psql_cmd(query, csv_mode=csv_mode)
    try:
        result = _run_command(cmd, env=env)
        return str(result.stdout or "")
    except subprocess.CalledProcessError as exc:
        if not _is_parallel_shm_error(exc.stderr):
            raise
        fallback_pgoptions = f"{str(env.get('PGOPTIONS') or '').strip()} -c max_parallel_workers_per_gather=0".strip()
        log(
            "Parallel worker shared-memory exhaustion detected; retrying query with max_parallel_workers_per_gather=0"
        )
        retry_cmd, retry_env = _build_psql_cmd(
            query,
            csv_mode=csv_mode,
            pgoptions_override=fallback_pgoptions,
        )
        retry_result = _run_command(retry_cmd, env=retry_env)
        return str(retry_result.stdout or "")


def run_psql_csv(query: str):
    return _parse_psql_csv(_run_psql_with_safe_parallel_retry(query, csv_mode=True))


def run_psql_text(query: str) -> str:
    return _run_psql_with_safe_parallel_retry(query, csv_mode=False)


def sql_quote(value: str) -> str:
    return str(value or "").replace("'", "''")


def build_stops_query(country: str, source_id: str):
    country_filter = ""
    if country:
        country_filter = f"AND COALESCE(gsp.country, gs.country) = '{country}'"
    source_filter = ""
    if source_id:
        source_filter = f"AND gsp.metadata ->> 'source_id' = '{sql_quote(source_id)}'"
    return f"""
SELECT
  gsp.global_stop_point_id AS stop_id,
  gsp.display_name AS stop_name,
  COALESCE(gsp.country, gs.country, '') AS country,
  COALESCE(ROUND(gsp.latitude::numeric, 6)::text, '') AS stop_lat,
  COALESCE(ROUND(gsp.longitude::numeric, 6)::text, '') AS stop_lon,
  ''::text AS location_type,
  ''::text AS parent_station
FROM global_stop_points gsp
JOIN global_stations gs
  ON gs.global_station_id = gsp.global_station_id
WHERE gsp.is_active = true
  {country_filter}
  {source_filter}
  AND gsp.latitude IS NOT NULL
  AND gsp.longitude IS NOT NULL
ORDER BY stop_id;
"""


def build_source_ids_query(as_of: str, source_id: str):
    source_filter = ""
    if source_id:
        source_filter = f"AND tt.source_id = '{sql_quote(source_id)}'"

    return f"""
SELECT DISTINCT tt.source_id
FROM timetable_trips tt
WHERE COALESCE(tt.trip_start_date, DATE '1900-01-01') <= '{as_of}'::date
  AND COALESCE(tt.trip_end_date, DATE '2999-12-31') >= '{as_of}'::date
  {source_filter}
ORDER BY tt.source_id;
"""


def build_trip_batch_query_legacy(
    as_of: str,
    country: str,
    source_id: str,
    trip_after: str,
    batch_size: int,
):
    country_filter = ""
    if country:
        country_filter = f"AND COALESCE(sp.country, gs.country) = '{country}'"
    trip_after_filter = ""
    if trip_after:
        trip_after_filter = f"AND tt.trip_fact_id > '{sql_quote(trip_after)}'"

    return f"""
WITH trip_ids AS (
  SELECT tt.trip_fact_id
  FROM timetable_trips tt
  WHERE tt.source_id = '{sql_quote(source_id)}'
    AND (
      tt.trip_start_date IS NULL
      OR tt.trip_start_date <= '{as_of}'::date
    )
    AND (
      tt.trip_end_date IS NULL
      OR tt.trip_end_date >= '{as_of}'::date
    )
    {trip_after_filter}
  ORDER BY tt.trip_fact_id
  LIMIT {int(batch_size)}
)
SELECT
  tt.trip_fact_id,
  COALESCE(NULLIF(tt.route_id, ''), 'route_' || tt.trip_fact_id) AS route_id,
  COALESCE(NULLIF(tt.route_short_name, ''), NULLIF(tt.route_id, ''), 'R') AS route_short_name,
  COALESCE(NULLIF(tt.route_long_name, ''), NULLIF(tt.trip_headsign, ''), COALESCE(NULLIF(tt.route_id, ''), tt.trip_fact_id)) AS route_long_name,
  COALESCE(NULLIF(tt.transport_mode, ''), 'rail') AS transport_mode,
  COALESCE(NULLIF(tt.service_id, ''), 'svc_' || tt.trip_fact_id) AS service_id,
  COALESCE(NULLIF(tt.trip_headsign, ''), tt.route_long_name, tt.route_id, tt.trip_fact_id) AS trip_headsign,
  tts.stop_sequence,
  COALESCE(NULLIF(tts.global_stop_point_id, ''), m.global_stop_point_id) AS stop_id,
  COALESCE(NULLIF(tts.arrival_time, ''), '') AS arrival_time,
  COALESCE(NULLIF(tts.departure_time, ''), '') AS departure_time,
  COALESCE(sp.country, gs.country, '') AS country
FROM timetable_trip_stop_times tts
JOIN trip_ids i
  ON i.trip_fact_id = tts.trip_fact_id
JOIN timetable_trips tt
  ON tt.trip_fact_id = tts.trip_fact_id
LEFT JOIN provider_global_stop_point_mappings m
  ON m.source_id = tt.source_id
 AND m.provider_stop_point_ref = COALESCE(tts.metadata ->> 'provider_stop_point_ref', '')
 AND m.is_active = true
JOIN global_stop_points sp
  ON sp.global_stop_point_id = COALESCE(NULLIF(tts.global_stop_point_id, ''), m.global_stop_point_id)
LEFT JOIN global_stations gs
  ON gs.global_station_id = sp.global_station_id
WHERE {country_filter[4:] if country_filter.startswith("AND ") else "true"}
ORDER BY tts.trip_fact_id, tts.stop_sequence;
"""


def build_trip_batch_query_optimized(
    as_of: str,
    _country: str,
    source_id: str,
    trip_after: str,
    batch_size: int,
):
    trip_after_filter = ""
    if trip_after:
        trip_after_filter = f"AND tt.trip_fact_id > '{sql_quote(trip_after)}'"

    return f"""
WITH trip_scope AS (
  SELECT
    tt.trip_fact_id,
    tt.source_id,
    COALESCE(NULLIF(tt.route_id, ''), 'route_' || tt.trip_fact_id) AS route_id,
    COALESCE(NULLIF(tt.route_short_name, ''), NULLIF(tt.route_id, ''), 'R') AS route_short_name,
    COALESCE(NULLIF(tt.route_long_name, ''), NULLIF(tt.trip_headsign, ''), COALESCE(NULLIF(tt.route_id, ''), tt.trip_fact_id)) AS route_long_name,
    COALESCE(NULLIF(tt.transport_mode, ''), 'rail') AS transport_mode,
    COALESCE(NULLIF(tt.service_id, ''), 'svc_' || tt.trip_fact_id) AS service_id,
    COALESCE(NULLIF(tt.trip_headsign, ''), tt.route_long_name, tt.route_id, tt.trip_fact_id) AS trip_headsign
  FROM timetable_trips tt
  WHERE tt.source_id = '{sql_quote(source_id)}'
    AND (
      tt.trip_start_date IS NULL
      OR tt.trip_start_date <= '{as_of}'::date
    )
    AND (
      tt.trip_end_date IS NULL
      OR tt.trip_end_date >= '{as_of}'::date
    )
    {trip_after_filter}
  ORDER BY tt.trip_fact_id
  LIMIT {int(batch_size)}
)
SELECT
  t.trip_fact_id,
  t.route_id,
  t.route_short_name,
  t.route_long_name,
  t.transport_mode,
  t.service_id,
  t.trip_headsign,
  tts.stop_sequence,
  COALESCE(NULLIF(tts.global_stop_point_id, ''), m.global_stop_point_id) AS stop_id,
  COALESCE(NULLIF(tts.arrival_time, ''), '') AS arrival_time,
  COALESCE(NULLIF(tts.departure_time, ''), '') AS departure_time
FROM trip_scope t
JOIN timetable_trip_stop_times tts
  ON tts.trip_fact_id = t.trip_fact_id
LEFT JOIN provider_global_stop_point_mappings m
  ON m.source_id = t.source_id
 AND m.provider_stop_point_ref = COALESCE(tts.metadata ->> 'provider_stop_point_ref', '')
 AND m.is_active = true
WHERE COALESCE(NULLIF(tts.global_stop_point_id, ''), m.global_stop_point_id) <> ''
ORDER BY t.trip_fact_id, tts.stop_sequence;
"""


def build_transfer_query(country: str, source_id: str):
    country_filter = ""
    if country:
        country_filter = f"AND (fsp.country = '{country}' OR tsp.country = '{country}')"
    source_filter = ""
    if source_id:
        source_filter = (
            "AND ("
            f"fsp.metadata ->> 'source_id' = '{sql_quote(source_id)}' "
            f"OR tsp.metadata ->> 'source_id' = '{sql_quote(source_id)}'"
            ")"
        )
    return f"""
SELECT DISTINCT
  te.from_global_stop_point_id AS from_stop_id,
  te.to_global_stop_point_id AS to_stop_id,
  te.transfer_type,
  te.min_transfer_seconds
FROM transfer_edges te
JOIN global_stop_points fsp
  ON fsp.global_stop_point_id = te.from_global_stop_point_id
JOIN global_stop_points tsp
  ON tsp.global_stop_point_id = te.to_global_stop_point_id
WHERE fsp.is_active = true
  AND tsp.is_active = true
  {country_filter}
  {source_filter}
ORDER BY from_stop_id, to_stop_id, min_transfer_seconds;
"""


def build_explain_query(sql: str) -> str:
    base_sql = sql.strip()
    if base_sql.endswith(";"):
        base_sql = base_sql[:-1]
    return f"EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, FORMAT JSON) {base_sql};"


def _resolve_source_ids(
    as_of: str, source_id: str, benchmark_max_sources: int
) -> list[str]:
    source_rows = run_psql_csv(build_source_ids_query(as_of, source_id))
    source_ids = [str(row.get("source_id") or "").strip() for row in source_rows]
    filtered_source_ids = [sid for sid in source_ids if sid]
    if benchmark_max_sources > 0:
        return filtered_source_ids[:benchmark_max_sources]
    return filtered_source_ids


def _build_trip_query(
    options: ExportBatchOptions,
    current_source_id: str,
    trip_after: str,
) -> str:
    if options.query_mode == "optimized":
        return build_trip_batch_query_optimized(
            as_of=options.as_of,
            _country=options.country,
            source_id=current_source_id,
            trip_after=trip_after,
            batch_size=options.batch_size_trips,
        )
    return build_trip_batch_query_legacy(
        as_of=options.as_of,
        country=options.country,
        source_id=current_source_id,
        trip_after=trip_after,
        batch_size=options.batch_size_trips,
    )


def _record_batch_profile(profiling: dict, rows, query_duration_ms: float) -> None:
    profiling["dbFetchMs"] += query_duration_ms
    profiling["batchFetchMs"].append(round(query_duration_ms, 3))
    profiling["batchRows"].append(len(rows))


def _resolve_benchmark_stop_reason(
    *,
    total_batches: int,
    total_trips: int,
    benchmark_max_batches: int,
    benchmark_max_trips: int,
) -> str:
    if benchmark_max_batches > 0 and total_batches >= benchmark_max_batches:
        return "max_batches"
    if benchmark_max_trips > 0 and total_trips >= benchmark_max_trips:
        return "max_trips"
    return ""


def iter_trip_batches(
    *,
    options: ExportBatchOptions,
    profiling: dict,
    progress_state: dict,
):
    source_ids = _resolve_source_ids(
        options.as_of,
        options.source_id,
        options.benchmark_max_sources,
    )
    log_progress(
        progress_state, f"Discovered {len(source_ids)} source(s) in scope", force=True
    )

    total_batches = 0
    total_trips = 0
    benchmark_stop_reason = ""

    for source_idx, sid in enumerate(source_ids, start=1):
        log_progress(
            progress_state,
            f"Source {source_idx}/{len(source_ids)} '{sid}' started",
            force=True,
        )
        after_trip = ""
        while True:
            query = _build_trip_query(options, sid, after_trip)
            query_started = time.perf_counter()
            rows = run_psql_csv(query)
            query_duration_ms = (time.perf_counter() - query_started) * 1000.0
            _record_batch_profile(profiling, rows, query_duration_ms)
            if not rows:
                log_progress(
                    progress_state,
                    f"Source '{sid}' finished (no more rows)",
                    force=True,
                )
                break
            total_batches += 1
            unique_trip_ids = {
                str(row.get("trip_fact_id") or "").strip()
                for row in rows
                if str(row.get("trip_fact_id") or "").strip()
            }
            total_trips += len(unique_trip_ids)
            log_progress(
                progress_state,
                "Fetched "
                + f"source='{sid}' batch={total_batches} rows={len(rows)} uniqueTrips={len(unique_trip_ids)} "
                + f"fetchMs={query_duration_ms:.1f} totalTripsSeen={total_trips}",
            )
            yield sid, rows, total_batches, total_trips
            after_trip = str(rows[-1].get("trip_fact_id") or "").strip()
            if not after_trip:
                break
            benchmark_stop_reason = _resolve_benchmark_stop_reason(
                total_batches=total_batches,
                total_trips=total_trips,
                benchmark_max_batches=options.benchmark_max_batches,
                benchmark_max_trips=options.benchmark_max_trips,
            )
            if benchmark_stop_reason:
                break
        if benchmark_stop_reason:
            log_progress(
                progress_state,
                f"Benchmark stop triggered: {benchmark_stop_reason}",
                force=True,
            )
            break

    profiling["benchmarkStopReason"] = benchmark_stop_reason or ""
    profiling["benchmarkSourceCount"] = len(source_ids)


def classify_trip_tier(trip):
    text = " ".join(
        [
            str(trip.get("transport_mode") or ""),
            str(trip.get("route_short_name") or ""),
            str(trip.get("route_long_name") or ""),
            str(trip.get("trip_headsign") or ""),
        ]
    ).lower()

    if any(
        token in text
        for token in ["high_speed", "high-speed", "ice", "tgv", "railjet", "freccia"]
    ):
        return "high-speed"
    if any(
        token in text
        for token in ["tram", "metro", "subway", "bus", "u-bahn", "s-bahn", "local"]
    ):
        return "local"
    if any(
        token in text
        for token in ["regional", "intercity", "regio", "re ", "rb ", "ir "]
    ):
        return "regional"
    return "regional"


def ensure_time(value: str, offset_seconds: int) -> str:
    text = str(value or "").strip()
    if re.match(r"^\d{2}:\d{2}:\d{2}$", text):
        return text
    hh = (offset_seconds // 3600) % 24
    mm = (offset_seconds % 3600) // 60
    ss = offset_seconds % 60
    return f"{hh:02d}:{mm:02d}:{ss:02d}"


def load_rows_from_stops_csv(path: str):
    with open(path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    required = {"stop_id", "stop_name", "stop_lat", "stop_lon"}
    if not rows:
        fail("stops CSV has no rows")
    if not required.issubset(set(rows[0].keys())):
        fail(
            "stops CSV missing required columns: stop_id, stop_name, stop_lat, stop_lon"
        )
    return rows


def load_rows_from_db_static(_as_of: str, country: str, source_id: str):
    stops = run_psql_csv(build_stops_query(country, source_id))
    transfers = run_psql_csv(build_transfer_query(country, source_id))
    return stops, transfers


def write_csv_file(path: Path, header, rows):
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)


def write_deterministic_zip_from_paths(file_paths, output_zip: str, as_of: str):
    try:
        dt = datetime.strptime(as_of, "%Y-%m-%d")
        timestamp = (dt.year, dt.month, dt.day, 0, 0, 0)
    except Exception:
        timestamp = (2024, 1, 1, 0, 0, 0)

    ordered_names = list(GTFS_CORE_FILES)
    if TRANSFERS_FILE in file_paths:
        ordered_names.append(TRANSFERS_FILE)

    with zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name in ordered_names:
            path = file_paths.get(name)
            if not path:
                continue
            info = zipfile.ZipInfo(filename=name, date_time=timestamp)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            with open(path, "rb") as handle:
                with archive.open(info, mode="w", force_zip64=True) as out_handle:
                    shutil.copyfileobj(handle, out_handle, length=1024 * 1024)


def _create_profiling_state() -> dict:
    return {
        "staticLoadMs": 0.0,
        "dbFetchMs": 0.0,
        "pythonProcessMs": 0.0,
        "sqliteWriteMs": 0.0,
        "csvWriteMs": 0.0,
        "zipMs": 0.0,
        "batchFetchMs": [],
        "batchRows": [],
        "benchmarkStopReason": "",
        "benchmarkSourceCount": 0,
    }


def _create_progress_state(run_started: float, progress_interval_sec: int) -> dict:
    return {
        "intervalSec": max(int(progress_interval_sec), 0),
        "lastLogAt": 0.0,
        "runStartedAt": run_started,
    }


def _resolve_scope_countries(stops) -> list[str]:
    countries = sorted(
        {
            str(row.get("country") or "").strip()
            for row in stops
            if str(row.get("country") or "").strip()
        }
    )
    return countries or ["EU"]


def _resolve_timezone_for_country(country: str) -> str:
    if country == "AT":
        return "Europe/Vienna"
    if country == "CH":
        return "Europe/Zurich"
    return "Europe/Berlin"


def _build_agency_rows(countries: list[str], agency_url: str) -> list[list[str]]:
    agency_rows = [
        [
            f"agency_{agency_country.lower()}",
            f"Pan-European {agency_country} Transit",
            agency_url,
            _resolve_timezone_for_country(agency_country),
            "en",
        ]
        for agency_country in countries
    ]
    agency_rows.sort(key=lambda row: row[0])
    return agency_rows


def _collect_sql_plan_summary(options: ExportBatchOptions, profiling: dict) -> dict:
    if not options.sql_profile_sample:
        return {}

    source_ids = _resolve_source_ids(
        options.as_of,
        options.source_id,
        options.benchmark_max_sources,
    )
    if not source_ids:
        return {}

    sample_source = source_ids[0]
    sample_query = _build_trip_query(options, sample_source, "")
    explain_started = time.perf_counter()
    explain_raw = run_psql_text(build_explain_query(sample_query)).strip()
    profiling["dbFetchMs"] += (time.perf_counter() - explain_started) * 1000.0
    try:
        explain_payload = json.loads(explain_raw)
        if isinstance(explain_payload, list) and explain_payload:
            root = explain_payload[0]
            plan = root.get("Plan") or {}
            return {
                "sourceId": sample_source,
                "planningTimeMs": root.get("Planning Time"),
                "executionTimeMs": root.get("Execution Time"),
                "planNodeType": plan.get("Node Type"),
                "planRows": plan.get("Plan Rows"),
                "actualRows": plan.get("Actual Rows"),
                "sharedHitBlocks": plan.get("Shared Hit Blocks"),
                "sharedReadBlocks": plan.get("Shared Read Blocks"),
                "tempReadBlocks": plan.get("Temp Read Blocks"),
                "tempWrittenBlocks": plan.get("Temp Written Blocks"),
                "ioReadTimeMs": plan.get("I/O Read Time"),
                "ioWriteTimeMs": plan.get("I/O Write Time"),
            }
    except Exception:
        return {
            "sourceId": sample_source,
            "error": "failed_to_parse_explain_json",
        }
    return {}


def _resolve_route_type(transport_mode: str) -> str:
    if any(token in transport_mode for token in ["metro", "subway", "u-bahn"]):
        return "1"
    if "tram" in transport_mode:
        return "0"
    if "bus" in transport_mode:
        return "3"
    return "2"


def _collect_valid_stop_times(
    trip_rows, stop_country_by_id: dict[str, str]
) -> tuple[list[tuple[str, str, str, int]], str]:
    valid_stop_times: list[tuple[str, str, str, int]] = []
    trip_country = str(trip_rows[0].get("country") or "").strip()
    seq = 0
    for stop in trip_rows:
        stop_id = str(stop.get("stop_id") or "").strip()
        if not stop_id or stop_id not in stop_country_by_id:
            continue
        if not trip_country:
            trip_country = stop_country_by_id.get(stop_id, "")
        seq += 1
        fallback = 6 * 3600 + (seq - 1) * 300
        arrival = ensure_time(stop.get("arrival_time") or "", fallback)
        departure = ensure_time(stop.get("departure_time") or "", fallback)
        valid_stop_times.append((arrival, departure, stop_id, seq))
    return valid_stop_times, trip_country


def _flush_trip_rows_for_batch(
    trip_rows,
    *,
    options: ExportBatchOptions,
    countries: list[str],
    stop_country_by_id: dict[str, str],
    batch_routes: dict,
    batch_services: set,
    trips_writer,
    stop_times_writer,
    counters: dict[str, int],
) -> None:
    if len(trip_rows) < 2:
        return
    template = trip_rows[0]
    trip_id = str(template.get("trip_fact_id") or "").strip()
    if not trip_id:
        return
    tier = classify_trip_tier(template)
    if options.requested_tier != "all" and tier != options.requested_tier:
        return

    route_id = str(template.get("route_id") or f"route_{trip_id}").strip()
    route_short_name = str(template.get("route_short_name") or "R").strip()
    route_long_name = str(template.get("route_long_name") or route_id).strip()
    transport_mode = str(template.get("transport_mode") or "rail").strip().lower()
    service_id = str(template.get("service_id") or f"svc_{trip_id}").strip()
    trip_headsign = str(template.get("trip_headsign") or route_long_name).strip()
    valid_stop_times, trip_country = _collect_valid_stop_times(
        trip_rows,
        stop_country_by_id,
    )
    if len(valid_stop_times) < 2:
        return

    agency_id = f"agency_{(trip_country or countries[0]).lower()}"
    batch_routes[route_id] = (
        route_id,
        agency_id,
        route_short_name,
        route_long_name,
        _resolve_route_type(transport_mode),
        f"tier:{tier};profile:{options.profile}",
    )
    batch_services.add(service_id)
    trips_writer.writerow([route_id, service_id, trip_id, trip_headsign])
    counters["trip_count"] += 1
    for arrival, departure, stop_id, seq in valid_stop_times:
        stop_times_writer.writerow([trip_id, arrival, departure, stop_id, str(seq)])
        counters["stop_time_count"] += 1


def _percentile(values, pct: float):
    if not values:
        return 0.0
    idx = int(round((len(values) - 1) * pct))
    return float(values[idx])


def _build_batch_perf(profiling: dict) -> dict:
    batch_fetch_ms = sorted(profiling["batchFetchMs"])
    batch_rows = sorted(profiling["batchRows"])
    return {
        "count": len(batch_fetch_ms),
        "fetchMsMin": round(batch_fetch_ms[0], 3) if batch_fetch_ms else 0.0,
        "fetchMsP50": round(_percentile(batch_fetch_ms, 0.50), 3),
        "fetchMsP95": round(_percentile(batch_fetch_ms, 0.95), 3),
        "fetchMsMax": round(batch_fetch_ms[-1], 3) if batch_fetch_ms else 0.0,
        "rowsMin": int(batch_rows[0]) if batch_rows else 0,
        "rowsP50": int(_percentile(batch_rows, 0.50)) if batch_rows else 0,
        "rowsP95": int(_percentile(batch_rows, 0.95)) if batch_rows else 0,
        "rowsMax": int(batch_rows[-1]) if batch_rows else 0,
    }


def export_from_db_batched(options: ExportBatchOptions):
    run_started = time.perf_counter()
    profiling = _create_profiling_state()
    progress_state = _create_progress_state(run_started, options.progress_interval_sec)
    log_progress(
        progress_state,
        "Starting export "
        + f"profile='{options.profile}' as-of='{options.as_of}' tier='{options.requested_tier}' query-mode='{options.query_mode}' "
        + f"batch-size='{options.batch_size_trips}'",
        force=True,
    )

    static_started = time.perf_counter()
    stops, transfers = load_rows_from_db_static(
        options.as_of,
        options.country,
        options.source_id,
    )
    profiling["staticLoadMs"] = (time.perf_counter() - static_started) * 1000.0
    if not stops:
        fail("export scope produced no stops")
    log_progress(
        progress_state,
        f"Loaded static scope stops={len(stops)} transfers={len(transfers)}",
        force=True,
    )

    sql_plan_summary = _collect_sql_plan_summary(options, profiling)

    countries = _resolve_scope_countries(stops)
    agency_rows = _build_agency_rows(countries, options.agency_url)
    benchmark_enabled = any(
        value > 0
        for value in [
            options.benchmark_max_sources,
            options.benchmark_max_batches,
            options.benchmark_max_trips,
        ]
    )

    with tempfile.TemporaryDirectory(prefix="export-gtfs-batch-") as tmp_dir_raw:
        tmp_dir = Path(tmp_dir_raw)
        agency_path = tmp_dir / "agency.txt"
        stops_path = tmp_dir / "stops.txt"
        routes_path = tmp_dir / "routes.txt"
        trips_path = tmp_dir / "trips.txt"
        stop_times_path = tmp_dir / "stop_times.txt"
        calendar_path = tmp_dir / "calendar.txt"
        transfers_path = tmp_dir / TRANSFERS_FILE
        sqlite_path = tmp_dir / "gtfs-build.sqlite3"

        stop_rows = []
        for row in sorted(stops, key=lambda r: str(r.get("stop_id") or "")):
            stop_id = str(row.get("stop_id") or "").strip()
            if not stop_id:
                continue
            stop_rows.append(
                [
                    stop_id,
                    str(row.get("stop_name") or stop_id).strip() or stop_id,
                    str(row.get("stop_lat") or "").strip(),
                    str(row.get("stop_lon") or "").strip(),
                    str(row.get("location_type") or "").strip(),
                    str(row.get("parent_station") or "").strip(),
                ]
            )

        csv_started = time.perf_counter()
        write_csv_file(
            agency_path,
            [
                "agency_id",
                "agency_name",
                "agency_url",
                "agency_timezone",
                "agency_lang",
            ],
            agency_rows,
        )
        write_csv_file(
            stops_path,
            [
                "stop_id",
                "stop_name",
                "stop_lat",
                "stop_lon",
                "location_type",
                "parent_station",
            ],
            stop_rows,
        )
        profiling["csvWriteMs"] += (time.perf_counter() - csv_started) * 1000.0

        transfers_rows = []
        for row in transfers:
            from_id = str(row.get("from_stop_id") or "").strip()
            to_id = str(row.get("to_stop_id") or "").strip()
            if not from_id or not to_id or from_id == to_id:
                continue
            transfers_rows.append(
                [
                    from_id,
                    to_id,
                    str(row.get("transfer_type") or "2"),
                    str(int(row.get("min_transfer_seconds") or 0)),
                ]
            )
        transfers_rows.sort(key=lambda r: (r[0], r[1], int(r[3])))
        if transfers_rows:
            transfers_started = time.perf_counter()
            write_csv_file(
                transfers_path,
                ["from_stop_id", "to_stop_id", "transfer_type", "min_transfer_time"],
                transfers_rows,
            )
            profiling["csvWriteMs"] += (
                time.perf_counter() - transfers_started
            ) * 1000.0

        route_count = 0
        batch_count = 0
        source_batches = set()
        counters = {"trip_count": 0, "stop_time_count": 0}
        stop_country_by_id = {
            str(row.get("stop_id") or "").strip(): str(row.get("country") or "").strip()
            for row in stops
            if str(row.get("stop_id") or "").strip()
        }

        with (
            sqlite3.connect(str(sqlite_path)) as db,
            trips_path.open("w", encoding="utf-8", newline="") as trips_handle,
            stop_times_path.open(
                "w", encoding="utf-8", newline=""
            ) as stop_times_handle,
        ):
            db.execute("PRAGMA journal_mode=OFF")
            db.execute("PRAGMA synchronous=OFF")
            db.execute("PRAGMA temp_store=MEMORY")
            db.execute(
                "CREATE TABLE routes (route_id TEXT PRIMARY KEY, agency_id TEXT, route_short_name TEXT, route_long_name TEXT, route_type TEXT, route_desc TEXT)"
            )
            db.execute("CREATE TABLE service_ids (service_id TEXT PRIMARY KEY)")

            trips_writer = csv.writer(trips_handle, lineterminator="\n")
            stop_times_writer = csv.writer(stop_times_handle, lineterminator="\n")
            trips_writer.writerow(
                ["route_id", "service_id", "trip_id", "trip_headsign"]
            )
            stop_times_writer.writerow(
                [
                    "trip_id",
                    "arrival_time",
                    "departure_time",
                    "stop_id",
                    "stop_sequence",
                ]
            )

            for (
                batch_source_id,
                batch_rows,
                seen_batches,
                _seen_trips,
            ) in iter_trip_batches(
                options=options,
                profiling=profiling,
                progress_state=progress_state,
            ):
                batch_count = seen_batches
                source_batches.add(batch_source_id)

                process_started = time.perf_counter()
                batch_routes = {}
                batch_services = set()
                current_trip_id = ""
                current_trip_rows = []
                for row in batch_rows:
                    trip_id = str(row.get("trip_fact_id") or "").strip()
                    if not trip_id:
                        continue
                    if current_trip_id and trip_id != current_trip_id:
                        _flush_trip_rows_for_batch(
                            current_trip_rows,
                            options=options,
                            countries=countries,
                            stop_country_by_id=stop_country_by_id,
                            batch_routes=batch_routes,
                            batch_services=batch_services,
                            trips_writer=trips_writer,
                            stop_times_writer=stop_times_writer,
                            counters=counters,
                        )
                        current_trip_rows = []
                    current_trip_id = trip_id
                    current_trip_rows.append(row)
                if current_trip_rows:
                    _flush_trip_rows_for_batch(
                        current_trip_rows,
                        options=options,
                        countries=countries,
                        stop_country_by_id=stop_country_by_id,
                        batch_routes=batch_routes,
                        batch_services=batch_services,
                        trips_writer=trips_writer,
                        stop_times_writer=stop_times_writer,
                        counters=counters,
                    )
                profiling["pythonProcessMs"] += (
                    time.perf_counter() - process_started
                ) * 1000.0

                sqlite_started = time.perf_counter()
                if batch_routes:
                    db.executemany(
                        "INSERT OR REPLACE INTO routes (route_id, agency_id, route_short_name, route_long_name, route_type, route_desc) VALUES (?, ?, ?, ?, ?, ?)",
                        [
                            batch_routes[route_id]
                            for route_id in sorted(batch_routes.keys())
                        ],
                    )
                if batch_services:
                    db.executemany(
                        "INSERT OR IGNORE INTO service_ids (service_id) VALUES (?)",
                        [(service_id,) for service_id in sorted(batch_services)],
                    )
                db.commit()
                profiling["sqliteWriteMs"] += (
                    time.perf_counter() - sqlite_started
                ) * 1000.0
                elapsed_ms = (time.perf_counter() - run_started) * 1000.0
                trips_per_minute = (
                    (counters["trip_count"] * 60000.0 / elapsed_ms)
                    if elapsed_ms > 0
                    else 0.0
                )
                log_progress(
                    progress_state,
                    "Applied "
                    + f"batch={batch_count} source='{batch_source_id}' rows={len(batch_rows)} "
                    + f"tripsWritten={counters['trip_count']} stopTimesWritten={counters['stop_time_count']} "
                    + f"tripsPerMinute={trips_per_minute:.1f}",
                )

            if counters["stop_time_count"] == 0:
                fail(
                    "export scope produced no timetable rows after stop-point resolution; "
                    "check provider_global_stop_point_mappings coverage for the selected source scope"
                )

            routes_csv_started = time.perf_counter()
            route_rows = list(
                db.execute(
                    "SELECT route_id, agency_id, route_short_name, route_long_name, route_type, route_desc FROM routes ORDER BY route_id"
                )
            )
            route_count = len(route_rows)
            write_csv_file(
                routes_path,
                [
                    "route_id",
                    "agency_id",
                    "route_short_name",
                    "route_long_name",
                    "route_type",
                    "route_desc",
                ],
                route_rows,
            )
            profiling["csvWriteMs"] += (
                time.perf_counter() - routes_csv_started
            ) * 1000.0

            calendar_started = time.perf_counter()
            calendar_rows = [
                [
                    service_id,
                    "1",
                    "1",
                    "1",
                    "1",
                    "1",
                    "1",
                    "1",
                    "20240101",
                    "20351231",
                ]
                for (service_id,) in db.execute(
                    "SELECT service_id FROM service_ids ORDER BY service_id"
                )
            ]
            write_csv_file(
                calendar_path,
                [
                    "service_id",
                    "monday",
                    "tuesday",
                    "wednesday",
                    "thursday",
                    "friday",
                    "saturday",
                    "sunday",
                    "start_date",
                    "end_date",
                ],
                calendar_rows,
            )
            profiling["csvWriteMs"] += (time.perf_counter() - calendar_started) * 1000.0

        file_paths = {
            "agency.txt": agency_path,
            "stops.txt": stops_path,
            "routes.txt": routes_path,
            "trips.txt": trips_path,
            "stop_times.txt": stop_times_path,
            "calendar.txt": calendar_path,
        }
        if transfers_rows:
            file_paths[TRANSFERS_FILE] = transfers_path

        zip_started = time.perf_counter()
        write_deterministic_zip_from_paths(
            file_paths, options.output_zip, options.as_of
        )
        profiling["zipMs"] = (time.perf_counter() - zip_started) * 1000.0

    total_runtime_ms = (time.perf_counter() - run_started) * 1000.0
    batch_perf = _build_batch_perf(profiling)

    summary = {
        "profile": options.profile,
        "tier": options.requested_tier,
        "bridgeMode": "timetable-preserving-pan-europe",
        "queryMode": options.query_mode,
        "batching": {
            "enabled": True,
            "batchSizeTrips": options.batch_size_trips,
            "sourcesProcessed": len(source_batches),
            "tripBatches": batch_count,
        },
        "counts": {
            "stops": len(stop_rows),
            "agencies": len(agency_rows),
            "routes": route_count,
            "trips": counters["trip_count"],
            "stopTimes": counters["stop_time_count"],
            "services": len(calendar_rows),
            "transfers": len(transfers_rows),
            "countries": len(countries),
        },
        "benchmark": {
            "enabled": benchmark_enabled,
            "maxSources": options.benchmark_max_sources,
            "maxBatches": options.benchmark_max_batches,
            "maxTrips": options.benchmark_max_trips,
            "truncated": bool(profiling.get("benchmarkStopReason")),
            "stopReason": profiling.get("benchmarkStopReason") or "",
            "selectedSources": int(
                profiling.get("benchmarkSourceCount") or len(source_batches)
            ),
        },
        "performance": {
            "progressIntervalSec": max(int(options.progress_interval_sec), 0),
            "totalRuntimeMs": round(total_runtime_ms, 3),
            "tripsPerMinute": round(
                (counters["trip_count"] * 60000.0 / total_runtime_ms), 3
            )
            if total_runtime_ms > 0
            else 0.0,
            "stopTimesPerMinute": round(
                (counters["stop_time_count"] * 60000.0 / total_runtime_ms), 3
            )
            if total_runtime_ms > 0
            else 0.0,
            "stagesMs": {
                "staticLoad": round(profiling["staticLoadMs"], 3),
                "dbFetch": round(profiling["dbFetchMs"], 3),
                "pythonProcess": round(profiling["pythonProcessMs"], 3),
                "sqliteWrite": round(profiling["sqliteWriteMs"], 3),
                "csvWrite": round(profiling["csvWriteMs"], 3),
                "zipWrite": round(profiling["zipMs"], 3),
            },
            "batchFetchProfile": batch_perf,
            "sqlSamplePlan": sql_plan_summary,
        },
    }
    log_progress(
        progress_state,
        f"Finished export trips={counters['trip_count']} stopTimes={counters['stop_time_count']} runtimeMs={total_runtime_ms:.1f}",
        force=True,
    )
    return summary


def _resolve_trip_route_rows(
    *,
    template,
    trip_id: str,
    countries: list[str],
    profile: str,
) -> tuple[str, str, list[str], list[str]]:
    route_id = str(template.get("route_id") or f"route_{trip_id}").strip()
    route_short_name = str(template.get("route_short_name") or "R").strip()
    route_long_name = str(template.get("route_long_name") or route_id).strip()
    transport_mode = str(template.get("transport_mode") or "rail").strip().lower()
    service_id = str(template.get("service_id") or f"svc_{trip_id}").strip()
    trip_headsign = str(template.get("trip_headsign") or route_long_name).strip()
    country = str(template.get("country") or "").strip()
    agency_id = f"agency_{(country or countries[0]).lower()}"
    tier = classify_trip_tier(template)
    route_row = [
        route_id,
        agency_id,
        route_short_name,
        route_long_name,
        _resolve_route_type(transport_mode),
        f"tier:{tier};profile:{profile}",
    ]
    trip_row = [route_id, service_id, trip_id, trip_headsign]
    return tier, service_id, route_row, trip_row


def _append_fixture_rows(
    *,
    profile: str,
    agency_id: str,
    ordered_stop_ids: list[str],
    route_defs: dict[str, list[str]],
    trip_defs: dict[str, list[str]],
    stop_time_rows: list[list[str]],
    service_ids: set[str],
) -> None:
    route_id = "route_fixture"
    trip_id = "trip_fixture"
    service_id = "svc_fixture"
    route_defs[route_id] = [
        route_id,
        agency_id,
        "FIX",
        "Fixture Route",
        "2",
        f"tier:regional;profile:{profile}",
    ]
    trip_defs[trip_id] = [route_id, service_id, trip_id, "Fixture"]
    service_ids.add(service_id)
    for idx, stop_id in enumerate(ordered_stop_ids, start=1):
        t = ensure_time("", 7 * 3600 + (idx - 1) * 300)
        stop_time_rows.append([trip_id, t, t, stop_id, str(idx)])


def _collect_unique_transfer_rows(stop_map, transfers) -> list[list[str]]:
    transfer_rows = []
    seen_transfer_keys = set()
    for row in transfers:
        from_id = str(row.get("from_stop_id") or "").strip()
        to_id = str(row.get("to_stop_id") or "").strip()
        if from_id not in stop_map or to_id not in stop_map or from_id == to_id:
            continue
        min_transfer = int(row.get("min_transfer_seconds") or 0)
        transfer_type = str(row.get("transfer_type") or "2")
        key = (from_id, to_id, transfer_type, min_transfer)
        if key in seen_transfer_keys:
            continue
        seen_transfer_keys.add(key)
        transfer_rows.append([from_id, to_id, transfer_type, str(min_transfer)])
    return transfer_rows


def _build_stop_rows(stop_map) -> list[list[str]]:
    stop_rows = []
    for stop_id, row in sorted(stop_map.items()):
        stop_rows.append(
            [
                stop_id,
                str(row.get("stop_name") or stop_id).strip() or stop_id,
                str(row.get("stop_lat") or "").strip(),
                str(row.get("stop_lon") or "").strip(),
                str(row.get("location_type") or "").strip(),
                str(row.get("parent_station") or "").strip(),
            ]
        )
    return stop_rows


def _build_calendar_rows(service_ids: set[str]) -> list[list[str]]:
    return [
        [
            service_id,
            "1",
            "1",
            "1",
            "1",
            "1",
            "1",
            "1",
            "20240101",
            "20351231",
        ]
        for service_id in sorted(service_ids)
    ]


def build_tables(
    profile: str, requested_tier: str, stops, trips, transfers, agency_url: str
):
    stop_map = {}
    for row in stops:
        stop_id = str(row.get("stop_id") or "").strip()
        if not stop_id:
            continue
        stop_map[stop_id] = row

    agency_rows = []
    countries = sorted(
        {
            str(row.get("country") or "").strip()
            for row in stops
            if str(row.get("country") or "").strip()
        }
    )
    if not countries:
        countries = ["EU"]
    for country in countries:
        tz = "Europe/Berlin"
        if country == "AT":
            tz = "Europe/Vienna"
        elif country == "CH":
            tz = "Europe/Zurich"
        agency_rows.append(
            [
                f"agency_{country.lower()}",
                f"Pan-European {country} Transit",
                agency_url,
                tz,
                "en",
            ]
        )
    agency_rows.sort(key=lambda r: r[0])

    route_defs = {}
    trip_defs = {}
    stop_time_rows = []
    service_ids = set()

    grouped = defaultdict(list)
    for row in trips:
        grouped[row["trip_fact_id"]].append(row)

    for trip_id, trip_rows in sorted(grouped.items()):
        trip_rows.sort(key=lambda row: int(row.get("stop_sequence") or 0))
        if len(trip_rows) < 2:
            continue

        template = trip_rows[0]
        tier, service_id, route_row, trip_row = _resolve_trip_route_rows(
            template=template,
            trip_id=trip_id,
            countries=countries,
            profile=profile,
        )
        if requested_tier != "all" and tier != requested_tier:
            continue
        route_id = route_row[0]
        route_defs[route_id] = route_row
        trip_defs[trip_id] = trip_row
        service_ids.add(service_id)

        for idx, stop in enumerate(trip_rows, start=1):
            stop_id = str(stop.get("stop_id") or "").strip()
            if stop_id not in stop_map:
                continue
            fallback = 6 * 3600 + (idx - 1) * 300
            arrival = ensure_time(stop.get("arrival_time") or "", fallback)
            departure = ensure_time(stop.get("departure_time") or "", fallback)
            stop_time_rows.append([trip_id, arrival, departure, stop_id, str(idx)])

    if not stop_time_rows:
        if len(stop_map) < 2:
            fail("export scope produced fewer than 2 stops and no timetable rows")
        # Fixture fallback only for explicit CSV mode without timetable facts.
        _append_fixture_rows(
            profile=profile,
            agency_id=agency_rows[0][0],
            ordered_stop_ids=sorted(stop_map.keys()),
            route_defs=route_defs,
            trip_defs=trip_defs,
            stop_time_rows=stop_time_rows,
            service_ids=service_ids,
        )

    transfer_rows = _collect_unique_transfer_rows(stop_map, transfers)
    stop_rows = _build_stop_rows(stop_map)

    route_rows = sorted(route_defs.values(), key=lambda r: r[0])
    trip_rows = sorted(trip_defs.values(), key=lambda r: r[2])
    stop_time_rows.sort(key=lambda r: (r[0], int(r[4]), r[3]))
    calendar_rows = _build_calendar_rows(service_ids)

    files = {
        "agency.txt": csv_text(
            [
                "agency_id",
                "agency_name",
                "agency_url",
                "agency_timezone",
                "agency_lang",
            ],
            agency_rows,
        ),
        "stops.txt": csv_text(
            [
                "stop_id",
                "stop_name",
                "stop_lat",
                "stop_lon",
                "location_type",
                "parent_station",
            ],
            stop_rows,
        ),
        "routes.txt": csv_text(
            [
                "route_id",
                "agency_id",
                "route_short_name",
                "route_long_name",
                "route_type",
                "route_desc",
            ],
            route_rows,
        ),
        "trips.txt": csv_text(
            ["route_id", "service_id", "trip_id", "trip_headsign"],
            trip_rows,
        ),
        "stop_times.txt": csv_text(
            ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"],
            stop_time_rows,
        ),
        "calendar.txt": csv_text(
            [
                "service_id",
                "monday",
                "tuesday",
                "wednesday",
                "thursday",
                "friday",
                "saturday",
                "sunday",
                "start_date",
                "end_date",
            ],
            calendar_rows,
        ),
    }

    if transfer_rows:
        transfer_rows.sort(key=lambda r: (r[0], r[1], int(r[3])))
        files[TRANSFERS_FILE] = csv_text(
            ["from_stop_id", "to_stop_id", "transfer_type", "min_transfer_time"],
            transfer_rows,
        )

    summary = {
        "profile": profile,
        "tier": requested_tier,
        "bridgeMode": "timetable-preserving-pan-europe",
        "counts": {
            "stops": len(stop_rows),
            "agencies": len(agency_rows),
            "routes": len(route_rows),
            "trips": len(trip_rows),
            "stopTimes": len(stop_time_rows),
            "services": len(calendar_rows),
            "transfers": len(transfer_rows),
            "countries": len(countries),
        },
    }
    return files, summary


def write_deterministic_zip(files, output_zip: str, as_of: str):
    try:
        dt = datetime.strptime(as_of, "%Y-%m-%d")
        timestamp = (dt.year, dt.month, dt.day, 0, 0, 0)
    except Exception:
        timestamp = (2024, 1, 1, 0, 0, 0)

    ordered_names = [
        "agency.txt",
        "stops.txt",
        "routes.txt",
        "trips.txt",
        "stop_times.txt",
        "calendar.txt",
    ]
    if TRANSFERS_FILE in files:
        ordered_names.append(TRANSFERS_FILE)

    with zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name in ordered_names:
            if name not in files:
                continue
            info = zipfile.ZipInfo(filename=name, date_time=timestamp)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(info, files[name])


def resolve_output_paths(args, profile: str, tier: str, as_of: str):
    slug_profile = re.sub(r"[^A-Za-z0-9._-]+", "-", profile).strip("-") or "pan-europe"
    out_dir = Path(args.output_dir)
    if not out_dir.is_absolute():
        out_dir = ROOT_DIR / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.output_zip:
        output_zip = Path(args.output_zip)
        if not output_zip.is_absolute():
            output_zip = ROOT_DIR / output_zip
    else:
        output_zip = out_dir / f"{slug_profile}-{tier}-{as_of}.zip"

    if args.summary_json:
        summary_json = Path(args.summary_json)
        if not summary_json.is_absolute():
            summary_json = ROOT_DIR / summary_json
    else:
        summary_json = out_dir / f"{slug_profile}-{tier}-{as_of}.summary.json"

    output_zip.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)
    return str(output_zip), str(summary_json)


def main():
    args = parse_args()
    tier = normalize_tier(args.tier)
    as_of = normalize_as_of(args.as_of)
    country = normalize_country(args.country)
    source_id = normalize_source_id(args.source_id)
    batch_size_trips = normalize_batch_size(args.batch_size_trips)
    query_mode = normalize_query_mode(args.query_mode)
    benchmark_max_sources = normalize_positive_limit(
        args.benchmark_max_sources, "--benchmark-max-sources"
    )
    benchmark_max_batches = normalize_positive_limit(
        args.benchmark_max_batches, "--benchmark-max-batches"
    )
    benchmark_max_trips = normalize_positive_limit(
        args.benchmark_max_trips, "--benchmark-max-trips"
    )
    progress_interval_sec = normalize_positive_limit(
        args.progress_interval_sec, "--progress-interval-sec"
    )
    profile = str(args.profile or "pan_europe_runtime").strip() or "pan_europe_runtime"

    output_zip, summary_json = resolve_output_paths(args, profile, tier, as_of)

    if args.from_db:
        summary = export_from_db_batched(
            ExportBatchOptions(
                profile=profile,
                requested_tier=tier,
                as_of=as_of,
                country=country,
                source_id=source_id,
                batch_size_trips=batch_size_trips,
                agency_url=args.agency_url,
                output_zip=output_zip,
                query_mode=query_mode,
                benchmark_max_sources=benchmark_max_sources,
                benchmark_max_batches=benchmark_max_batches,
                benchmark_max_trips=benchmark_max_trips,
                sql_profile_sample=bool(args.sql_profile_sample),
                progress_interval_sec=progress_interval_sec,
            )
        )
    elif args.stops_csv:
        stops = load_rows_from_stops_csv(args.stops_csv)
        trips = []
        transfers = []
        files, summary = build_tables(
            profile, tier, stops, trips, transfers, args.agency_url
        )
        write_deterministic_zip(files, output_zip, as_of)
    else:
        fail("either --from-db or --stops-csv is required")

    summary["asOf"] = as_of
    summary["countryScope"] = country or "ALL"
    summary["sourceScope"] = source_id or "ALL"
    summary["outputZip"] = output_zip
    summary["generatedAt"] = datetime.now(timezone.utc).isoformat()

    with open(summary_json, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")

    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
