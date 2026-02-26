#!/usr/bin/env python3
"""Prepare GTFS artifacts and query suites for ephemeral MOTIS K8s tests."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import math
import os
import re
import shutil
import sys
import zipfile
from typing import NoReturn

TIER_CHOICES = ("all", "high-speed", "regional", "local")
AGENCY_FILE = "agency.txt"
STOPS_FILE = "stops.txt"
ROUTES_FILE = "routes.txt"
TRIPS_FILE = "trips.txt"
STOP_TIMES_FILE = "stop_times.txt"
CALENDAR_FILE = "calendar.txt"
CALENDAR_DATES_FILE = "calendar_dates.txt"
TRANSFERS_FILE = "transfers.txt"
FEED_INFO_FILE = "feed_info.txt"
GTFS_TABLES = (
    AGENCY_FILE,
    STOPS_FILE,
    ROUTES_FILE,
    TRIPS_FILE,
    STOP_TIMES_FILE,
    CALENDAR_FILE,
    CALENDAR_DATES_FILE,
    TRANSFERS_FILE,
    FEED_INFO_FILE,
)
LOCAL_ROUTE_TYPES = {
    "0",
    "1",
    "5",
    "6",
    "7",
    "11",
    "12",
    "700",
    "900",
}
HIGH_SPEED_ROUTE_TYPES = {
    "101",
    "102",
}
HIGH_SPEED_TOKEN_RE = re.compile(
    r"\b(ICE|TGV|RJX|RJ|FRECCIAROSSA|FRECCIA|EUROSTAR|THALYS|AVE|AVLO|RAILJET)\b",
    re.IGNORECASE,
)
MOTIS_DATASET_TAG = "active-gtfs"


def fail(message: str, code: int = 1) -> "NoReturn":
    print(f"[prepare-motis-k8s-artifacts] ERROR: {message}", file=sys.stderr)
    raise SystemExit(code)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare GTFS ZIP + route query set for MOTIS K8s test jobs."
    )
    parser.add_argument("--mode", required=True, choices=("micro", "macro"))
    parser.add_argument("--input-zip", required=True)
    parser.add_argument("--output-zip", required=True)
    parser.add_argument("--queries-json", required=True)
    parser.add_argument("--tier", default="all", choices=TIER_CHOICES)
    parser.add_argument("--bbox", help="lat1,lon1,lat2,lon2 (required for micro)")
    parser.add_argument("--padding-km", type=float, default=20.0)
    parser.add_argument("--max-micro-queries", type=int, default=6)
    return parser.parse_args()


def parse_csv_table(
    zf: zipfile.ZipFile, name: str
) -> tuple[list[dict[str, str]], list[str]]:
    if name not in zf.namelist():
        return [], []

    with zf.open(name) as fp:
        reader = csv.DictReader(io.TextIOWrapper(fp, encoding="utf-8-sig"))
        rows = list(reader)
        return rows, list(reader.fieldnames or [])


def write_csv_bytes(fieldnames: list[str], rows: list[dict[str, str]]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=fieldnames,
        lineterminator="\n",
        extrasaction="ignore",
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buf.getvalue().encode("utf-8")


def normalize_stop_id(raw: str | None) -> str:
    return (raw or "").strip()


def parse_float(raw: str | None) -> float | None:
    text = (raw or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_bbox(raw: str) -> tuple[float, float, float, float]:
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) != 4:
        fail(f"invalid --bbox '{raw}' (expected lat1,lon1,lat2,lon2)")
    try:
        lat1, lon1, lat2, lon2 = [float(p) for p in parts]
    except ValueError as exc:
        fail(f"invalid --bbox '{raw}' ({exc})")
    return min(lat1, lat2), min(lon1, lon2), max(lat1, lat2), max(lon1, lon2)


def expand_bbox(
    bbox: tuple[float, float, float, float],
    padding_km: float,
) -> tuple[float, float, float, float]:
    min_lat, min_lon, max_lat, max_lon = bbox
    mid_lat = (min_lat + max_lat) / 2.0
    pad_lat = padding_km / 110.574
    lon_denom = max(0.01, math.cos(math.radians(mid_lat)))
    pad_lon = padding_km / (111.320 * lon_denom)
    return (
        min_lat - pad_lat,
        min_lon - pad_lon,
        max_lat + pad_lat,
        max_lon + pad_lon,
    )


def in_bbox(
    lat: float | None, lon: float | None, bbox: tuple[float, float, float, float]
) -> bool:
    if lat is None or lon is None:
        return False
    min_lat, min_lon, max_lat, max_lon = bbox
    return min_lat <= lat <= max_lat and min_lon <= lon <= max_lon


def parse_route_tier(route: dict[str, str]) -> str:
    route_desc = (route.get("route_desc") or "").strip()
    if route_desc:
        match = re.search(r"tier:(high-speed|regional|local)\b", route_desc)
        if match:
            return match.group(1)

    route_type = (route.get("route_type") or "").strip()
    if route_type in HIGH_SPEED_ROUTE_TYPES:
        return "high-speed"
    if route_type in LOCAL_ROUTE_TYPES:
        return "local"

    token_blob = " ".join(
        (
            route.get("route_short_name") or "",
            route.get("route_long_name") or "",
            route.get("route_desc") or "",
        )
    )
    if HIGH_SPEED_TOKEN_RE.search(token_blob):
        return "high-speed"

    return "regional"


def index_rows(rows: list[dict[str, str]], key: str) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        value = (row.get(key) or "").strip()
        if value:
            out[value] = row
    return out


def sort_stop_times(stop_times: list[dict[str, str]]) -> None:
    def key(row: dict[str, str]) -> tuple[int, str]:
        seq = row.get("stop_sequence") or ""
        try:
            parsed = int(seq)
        except ValueError:
            parsed = 10**9
        return parsed, (row.get("stop_id") or "")

    stop_times.sort(key=key)


def parse_service_date(raw: str | None) -> dt.date | None:
    text = (raw or "").strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        return None
    try:
        return dt.datetime.strptime(text, "%Y%m%d").date()
    except ValueError:
        return None


def active_services(
    calendar_rows: list[dict[str, str]],
    calendar_dates_rows: list[dict[str, str]],
    date_str: str,
) -> set[str]:
    date_obj = dt.datetime.strptime(date_str, "%Y%m%d").date()
    weekday_key = (
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    )[date_obj.weekday()]

    active: set[str] = set()
    for row in calendar_rows:
        start = (row.get("start_date") or "").strip()
        end = (row.get("end_date") or "").strip()
        if (
            start
            and end
            and start <= date_str <= end
            and (row.get(weekday_key) or "") == "1"
        ):
            service_id = (row.get("service_id") or "").strip()
            if service_id:
                active.add(service_id)

    for row in calendar_dates_rows:
        if (row.get("date") or "").strip() != date_str:
            continue
        service_id = (row.get("service_id") or "").strip()
        if not service_id:
            continue
        exception_type = (row.get("exception_type") or "").strip()
        if exception_type == "1":
            active.add(service_id)
        elif exception_type == "2":
            active.discard(service_id)
    return active


def choose_active_date(
    calendar_rows: list[dict[str, str]],
    calendar_dates_rows: list[dict[str, str]],
) -> str:
    today = dt.datetime.now(dt.timezone.utc).date()
    for delta in range(0, 21):
        date_obj = today + dt.timedelta(days=delta)
        date_str = date_obj.strftime("%Y%m%d")
        if active_services(calendar_rows, calendar_dates_rows, date_str):
            return date_str

    start_candidates: list[dt.date] = []
    for row in calendar_rows:
        parsed = parse_service_date(row.get("start_date"))
        if parsed:
            start_candidates.append(parsed)
    if start_candidates:
        return min(start_candidates).strftime("%Y%m%d")

    return today.strftime("%Y%m%d")


def time_to_iso(date_str: str, hhmmss: str) -> str | None:
    text = (hhmmss or "").strip()
    parts = text.split(":")
    if len(parts) != 3:
        return None
    try:
        h, m, s = [int(p) for p in parts]
    except ValueError:
        return None
    base = dt.datetime.strptime(date_str, "%Y%m%d")
    value = base + dt.timedelta(hours=h, minutes=m, seconds=s)
    return value.isoformat() + "Z"


def add_parent_and_child_stops(
    initial_ids: set[str],
    stops_rows: list[dict[str, str]],
) -> set[str]:
    rows_by_id = index_rows(stops_rows, "stop_id")
    keep = set(initial_ids)

    changed = True
    while changed:
        changed = False
        snapshot = list(keep)
        for stop_id in snapshot:
            row = rows_by_id.get(stop_id)
            if not row:
                continue
            parent = normalize_stop_id(row.get("parent_station"))
            if parent and parent not in keep:
                keep.add(parent)
                changed = True
        for row in stops_rows:
            stop_id = normalize_stop_id(row.get("stop_id"))
            parent = normalize_stop_id(row.get("parent_station"))
            if parent and parent in keep and stop_id not in keep:
                keep.add(stop_id)
                changed = True
    return keep


def filter_feed_by_routes(
    tables: dict[str, list[dict[str, str]]],
    tier: str,
) -> dict[str, list[dict[str, str]]]:
    routes_rows = tables[ROUTES_FILE]
    trips_rows = tables[TRIPS_FILE]
    stop_times_rows = tables[STOP_TIMES_FILE]
    stops_rows = tables[STOPS_FILE]
    calendar_rows = tables[CALENDAR_FILE]
    calendar_dates_rows = tables[CALENDAR_DATES_FILE]
    agency_rows = tables[AGENCY_FILE]
    transfers_rows = tables[TRANSFERS_FILE]

    route_tiers = {
        (row.get("route_id") or "").strip(): parse_route_tier(row)
        for row in routes_rows
    }
    if tier == "all":
        keep_route_ids = {rid for rid in route_tiers if rid}
    else:
        keep_route_ids = {
            route_id
            for route_id, route_tier in route_tiers.items()
            if route_tier == tier
        }
    if not keep_route_ids:
        fail(f"tier '{tier}' produced no routes")

    filtered_routes = [
        row
        for row in routes_rows
        if (row.get("route_id") or "").strip() in keep_route_ids
    ]
    filtered_trips = [
        row
        for row in trips_rows
        if (row.get("route_id") or "").strip() in keep_route_ids
    ]
    keep_trip_ids = {(row.get("trip_id") or "").strip() for row in filtered_trips}
    filtered_stop_times = [
        row
        for row in stop_times_rows
        if (row.get("trip_id") or "").strip() in keep_trip_ids
    ]
    keep_stop_ids = {(row.get("stop_id") or "").strip() for row in filtered_stop_times}
    keep_stop_ids = add_parent_and_child_stops(keep_stop_ids, stops_rows)
    filtered_stops = [
        row for row in stops_rows if (row.get("stop_id") or "").strip() in keep_stop_ids
    ]
    keep_service_ids = {(row.get("service_id") or "").strip() for row in filtered_trips}
    filtered_calendar = [
        row
        for row in calendar_rows
        if (row.get("service_id") or "").strip() in keep_service_ids
    ]
    filtered_calendar_dates = [
        row
        for row in calendar_dates_rows
        if (row.get("service_id") or "").strip() in keep_service_ids
    ]
    keep_agency_ids = {(row.get("agency_id") or "").strip() for row in filtered_routes}
    if keep_agency_ids:
        filtered_agency = [
            row
            for row in agency_rows
            if (row.get("agency_id") or "").strip() in keep_agency_ids
        ]
    else:
        filtered_agency = list(agency_rows)
    filtered_transfers = [
        row
        for row in transfers_rows
        if (row.get("from_stop_id") or "").strip() in keep_stop_ids
        and (row.get("to_stop_id") or "").strip() in keep_stop_ids
    ]

    return {
        **tables,
        ROUTES_FILE: filtered_routes,
        TRIPS_FILE: filtered_trips,
        STOP_TIMES_FILE: filtered_stop_times,
        STOPS_FILE: filtered_stops,
        CALENDAR_FILE: filtered_calendar,
        CALENDAR_DATES_FILE: filtered_calendar_dates,
        AGENCY_FILE: filtered_agency,
        TRANSFERS_FILE: filtered_transfers,
    }


def micro_scope_feed(
    tables: dict[str, list[dict[str, str]]],
    bbox: tuple[float, float, float, float],
    padding_km: float,
) -> tuple[dict[str, list[dict[str, str]]], set[str]]:
    expanded = expand_bbox(bbox, padding_km)
    stops_rows = tables[STOPS_FILE]
    trips_rows = tables[TRIPS_FILE]
    stop_times_rows = tables[STOP_TIMES_FILE]
    routes_rows = tables[ROUTES_FILE]
    calendar_rows = tables[CALENDAR_FILE]
    calendar_dates_rows = tables[CALENDAR_DATES_FILE]
    agency_rows = tables[AGENCY_FILE]
    transfers_rows = tables[TRANSFERS_FILE]

    stop_by_id = index_rows(stops_rows, "stop_id")
    bbox_stop_ids: set[str] = set()
    for row in stops_rows:
        stop_id = normalize_stop_id(row.get("stop_id"))
        if not stop_id:
            continue
        lat = parse_float(row.get("stop_lat"))
        lon = parse_float(row.get("stop_lon"))
        if in_bbox(lat, lon, expanded):
            bbox_stop_ids.add(stop_id)
    if len(bbox_stop_ids) < 2:
        fail("micro scope found fewer than 2 stops in bbox/padding window")

    stop_times_by_trip: dict[str, list[dict[str, str]]] = {}
    for row in stop_times_rows:
        trip_id = (row.get("trip_id") or "").strip()
        if trip_id:
            stop_times_by_trip.setdefault(trip_id, []).append(row)
    for rows in stop_times_by_trip.values():
        sort_stop_times(rows)

    keep_trip_ids: set[str] = set()
    for trip in trips_rows:
        trip_id = (trip.get("trip_id") or "").strip()
        if not trip_id:
            continue
        rows = stop_times_by_trip.get(trip_id, [])
        in_scope = [
            r for r in rows if normalize_stop_id(r.get("stop_id")) in bbox_stop_ids
        ]
        if len(in_scope) >= 2:
            keep_trip_ids.add(trip_id)
    if not keep_trip_ids:
        fail("micro scope found no trips crossing at least 2 scoped stops")

    filtered_trips = [
        row for row in trips_rows if (row.get("trip_id") or "").strip() in keep_trip_ids
    ]
    filtered_stop_times = [
        row
        for row in stop_times_rows
        if (row.get("trip_id") or "").strip() in keep_trip_ids
    ]
    keep_stop_ids = {(row.get("stop_id") or "").strip() for row in filtered_stop_times}
    keep_stop_ids = add_parent_and_child_stops(keep_stop_ids, stops_rows)
    filtered_stops = [
        row for row in stops_rows if (row.get("stop_id") or "").strip() in keep_stop_ids
    ]
    keep_route_ids = {(row.get("route_id") or "").strip() for row in filtered_trips}
    filtered_routes = [
        row
        for row in routes_rows
        if (row.get("route_id") or "").strip() in keep_route_ids
    ]
    keep_service_ids = {(row.get("service_id") or "").strip() for row in filtered_trips}
    filtered_calendar = [
        row
        for row in calendar_rows
        if (row.get("service_id") or "").strip() in keep_service_ids
    ]
    filtered_calendar_dates = [
        row
        for row in calendar_dates_rows
        if (row.get("service_id") or "").strip() in keep_service_ids
    ]
    keep_agency_ids = {(row.get("agency_id") or "").strip() for row in filtered_routes}
    if keep_agency_ids:
        filtered_agency = [
            row
            for row in agency_rows
            if (row.get("agency_id") or "").strip() in keep_agency_ids
        ]
    else:
        filtered_agency = list(agency_rows)
    filtered_transfers = [
        row
        for row in transfers_rows
        if (row.get("from_stop_id") or "").strip() in keep_stop_ids
        and (row.get("to_stop_id") or "").strip() in keep_stop_ids
    ]

    unresolved_bbox_stops = {sid for sid in bbox_stop_ids if sid in stop_by_id}
    return (
        {
            **tables,
            ROUTES_FILE: filtered_routes,
            TRIPS_FILE: filtered_trips,
            STOP_TIMES_FILE: filtered_stop_times,
            STOPS_FILE: filtered_stops,
            CALENDAR_FILE: filtered_calendar,
            CALENDAR_DATES_FILE: filtered_calendar_dates,
            AGENCY_FILE: filtered_agency,
            TRANSFERS_FILE: filtered_transfers,
        },
        unresolved_bbox_stops,
    )


def build_micro_queries(
    tables: dict[str, list[dict[str, str]]],
    bbox_stop_ids: set[str],
    max_queries: int,
) -> list[dict[str, object]]:
    stops_rows = tables[STOPS_FILE]
    trips_rows = tables[TRIPS_FILE]
    stop_times_rows = tables[STOP_TIMES_FILE]
    calendar_rows = tables[CALENDAR_FILE]
    calendar_dates_rows = tables[CALENDAR_DATES_FILE]
    target_date = choose_active_date(calendar_rows, calendar_dates_rows)

    service_by_trip = {
        (row.get("trip_id") or "").strip(): (row.get("service_id") or "").strip()
        for row in trips_rows
    }
    active_service_ids = active_services(
        calendar_rows, calendar_dates_rows, target_date
    )

    stops_by_id = index_rows(stops_rows, "stop_id")
    stop_times_by_trip: dict[str, list[dict[str, str]]] = {}
    for row in stop_times_rows:
        trip_id = (row.get("trip_id") or "").strip()
        if not trip_id:
            continue
        if service_by_trip.get(trip_id, "") not in active_service_ids:
            continue
        stop_times_by_trip.setdefault(trip_id, []).append(row)
    for rows in stop_times_by_trip.values():
        sort_stop_times(rows)

    queries: list[dict[str, object]] = []
    seen: set[tuple[str, str, str]] = set()
    for trip_id in sorted(stop_times_by_trip.keys()):
        rows = stop_times_by_trip[trip_id]
        scoped_rows = [
            r for r in rows if normalize_stop_id(r.get("stop_id")) in bbox_stop_ids
        ]
        if len(scoped_rows) < 2:
            continue
        origin_row = scoped_rows[0]
        destination_row = scoped_rows[-1]
        origin_stop_id = normalize_stop_id(origin_row.get("stop_id"))
        destination_stop_id = normalize_stop_id(destination_row.get("stop_id"))
        dep_time = (
            origin_row.get("departure_time") or origin_row.get("arrival_time") or ""
        ).strip()
        dt_iso = time_to_iso(target_date, dep_time)
        if not dt_iso:
            continue
        origin_stop = stops_by_id.get(origin_stop_id, {})
        destination_stop = stops_by_id.get(destination_stop_id, {})
        o_lat = parse_float(origin_stop.get("stop_lat"))
        o_lon = parse_float(origin_stop.get("stop_lon"))
        d_lat = parse_float(destination_stop.get("stop_lat"))
        d_lon = parse_float(destination_stop.get("stop_lon"))
        if None in (o_lat, o_lon, d_lat, d_lon):
            continue
        if origin_stop_id and destination_stop_id:
            # Prefer stable station IDs to avoid depending on street/geocoder lookup.
            origin = f"{MOTIS_DATASET_TAG}_{origin_stop_id}"
            destination = f"{MOTIS_DATASET_TAG}_{destination_stop_id}"
        else:
            origin = f"{o_lat},{o_lon}"
            destination = f"{d_lat},{d_lon}"
        key = (origin, destination, dt_iso)
        if key in seen:
            continue
        seen.add(key)
        queries.append(
            {
                "name": f"micro-{trip_id}",
                "required": True,
                "origin": origin,
                "destination": destination,
                "datetime": dt_iso,
            }
        )
        if len(queries) >= max_queries:
            break
    if not queries:
        fail("micro mode produced no testable scoped routes from filtered GTFS")
    return queries


def build_macro_queries(
    tables: dict[str, list[dict[str, str]]],
) -> list[dict[str, object]]:
    target_date = choose_active_date(
        tables[CALENDAR_FILE], tables[CALENDAR_DATES_FILE]
    )
    date_obj = dt.datetime.strptime(target_date, "%Y%m%d")
    q1 = (date_obj + dt.timedelta(hours=8)).isoformat() + "Z"
    q2 = (date_obj + dt.timedelta(hours=10)).isoformat() + "Z"
    return [
        {
            "name": "berlin-to-wien",
            "required": True,
            "origin": "Berlin Hbf",
            "destination": "Wien Hbf",
            "datetime": q1,
        },
        {
            "name": "zurich-to-munich",
            "required": True,
            "origin": "Z\u00fcrich HB",
            "destination": "M\u00fcnchen Hbf",
            "datetime": q2,
        },
    ]


def write_filtered_zip(
    input_zip: str,
    output_zip: str,
    tables: dict[str, list[dict[str, str]]],
    fieldnames_by_table: dict[str, list[str]],
) -> None:
    os.makedirs(os.path.dirname(output_zip), exist_ok=True)
    with (
        zipfile.ZipFile(input_zip) as in_zip,
        zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED) as out_zip,
    ):
        seen_table_names: set[str] = set()
        for name in in_zip.namelist():
            base = os.path.basename(name)
            if base in tables and base in fieldnames_by_table:
                seen_table_names.add(base)
                payload = write_csv_bytes(fieldnames_by_table[base], tables[base])
                info = zipfile.ZipInfo(filename=name, date_time=(2024, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                out_zip.writestr(info, payload)
            else:
                info = in_zip.getinfo(name)
                raw = in_zip.read(name)
                out_zip.writestr(info, raw)

        for table in GTFS_TABLES:
            if table in seen_table_names:
                continue
            fieldnames = fieldnames_by_table.get(table)
            if not fieldnames:
                continue
            payload = write_csv_bytes(fieldnames, tables.get(table, []))
            info = zipfile.ZipInfo(filename=table, date_time=(2024, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            out_zip.writestr(info, payload)


def load_tables(
    input_zip: str,
) -> tuple[dict[str, list[dict[str, str]]], dict[str, list[str]]]:
    tables: dict[str, list[dict[str, str]]] = {}
    fieldnames: dict[str, list[str]] = {}
    with zipfile.ZipFile(input_zip) as zf:
        for table in GTFS_TABLES:
            rows, names = parse_csv_table(zf, table)
            tables[table] = rows
            fieldnames[table] = names
    required = (STOPS_FILE, ROUTES_FILE, TRIPS_FILE, STOP_TIMES_FILE)
    for table in required:
        if not fieldnames.get(table):
            fail(f"input GTFS missing required table or header: {table}")
    return tables, fieldnames


def main() -> None:
    args = parse_args()
    if args.mode == "micro" and not args.bbox:
        fail("--bbox is required in --mode micro")
    if args.padding_km < 0:
        fail("--padding-km must be >= 0")

    input_zip = os.path.abspath(args.input_zip)
    output_zip = os.path.abspath(args.output_zip)
    queries_path = os.path.abspath(args.queries_json)
    if not os.path.isfile(input_zip):
        fail(f"input zip not found: {input_zip}")

    tables, fieldnames = load_tables(input_zip)
    scoped = filter_feed_by_routes(tables, args.tier)
    if args.mode == "micro":
        parsed_bbox = parse_bbox(args.bbox)
        scoped, bbox_stop_ids = micro_scope_feed(scoped, parsed_bbox, args.padding_km)
        queries = build_micro_queries(scoped, bbox_stop_ids, args.max_micro_queries)
    else:
        queries = build_macro_queries(scoped)

    if len(scoped[STOPS_FILE]) < 2:
        fail("scoped feed has fewer than 2 stops")
    if len(scoped[TRIPS_FILE]) < 1:
        fail("scoped feed has no trips")
    if len(scoped[STOP_TIMES_FILE]) < 2:
        fail("scoped feed has fewer than 2 stop_times rows")

    if os.path.normpath(input_zip) != os.path.normpath(output_zip):
        write_filtered_zip(input_zip, output_zip, scoped, fieldnames)
    else:
        # Keep behavior deterministic when output path equals input path.
        tmp_output = output_zip + ".tmp"
        write_filtered_zip(input_zip, tmp_output, scoped, fieldnames)
        shutil.move(tmp_output, output_zip)

    os.makedirs(os.path.dirname(queries_path), exist_ok=True)
    with open(queries_path, "w", encoding="utf-8") as fp:
        json.dump(queries, fp, indent=2, ensure_ascii=True)
        fp.write("\n")

    summary = {
        "mode": args.mode,
        "tier": args.tier,
        "inputZip": input_zip,
        "outputZip": output_zip,
        "queriesJson": queries_path,
        "counts": {
            "stops": len(scoped[STOPS_FILE]),
            "routes": len(scoped[ROUTES_FILE]),
            "trips": len(scoped[TRIPS_FILE]),
            "stop_times": len(scoped[STOP_TIMES_FILE]),
            "queries": len(queries),
        },
    }
    print(json.dumps(summary, ensure_ascii=True))


if __name__ == "__main__":
    main()
