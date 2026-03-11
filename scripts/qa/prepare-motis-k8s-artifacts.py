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
from dataclasses import dataclass
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


@dataclass
class MacroTripState:
    trip_id: str
    is_active: bool
    first_stop_id: str = ""
    last_stop_id: str = ""
    departure_time: str = ""
    stop_count: int = 0


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


def count_csv_rows(zf: zipfile.ZipFile, name: str) -> int:
    if name not in zf.namelist():
        return 0
    with zf.open(name) as fp:
        wrapper = io.TextIOWrapper(fp, encoding="utf-8-sig")
        header_seen = False
        count = 0
        for _line in wrapper:
            if not header_seen:
                header_seen = True
                continue
            count += 1
    return count


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


def _row_value(row: dict[str, str], key: str) -> str:
    return (row.get(key) or "").strip()


def _calendar_row_is_active(
    row: dict[str, str],
    date_str: str,
    weekday_key: str,
) -> bool:
    start = _row_value(row, "start_date")
    end = _row_value(row, "end_date")
    return bool(
        start
        and end
        and start <= date_str <= end
        and _row_value(row, weekday_key) == "1"
    )


def _apply_calendar_exception(
    active: set[str],
    row: dict[str, str],
    date_str: str,
) -> None:
    if _row_value(row, "date") != date_str:
        return
    service_id = _row_value(row, "service_id")
    if not service_id:
        return
    exception_type = _row_value(row, "exception_type")
    if exception_type == "1":
        active.add(service_id)
    elif exception_type == "2":
        active.discard(service_id)


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
        if _calendar_row_is_active(row, date_str, weekday_key):
            service_id = _row_value(row, "service_id")
            if service_id:
                active.add(service_id)

    for row in calendar_dates_rows:
        _apply_calendar_exception(active, row, date_str)
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


def _add_missing_parents(
    keep: set[str],
    rows_by_id: dict[str, dict[str, str]],
) -> bool:
    changed = False
    for stop_id in keep.copy():
        row = rows_by_id.get(stop_id)
        if not row:
            continue
        parent = normalize_stop_id(row.get("parent_station"))
        if parent and parent not in keep:
            keep.add(parent)
            changed = True
    return changed


def _add_missing_children(keep: set[str], stops_rows: list[dict[str, str]]) -> bool:
    changed = False
    for row in stops_rows:
        stop_id = normalize_stop_id(row.get("stop_id"))
        parent = normalize_stop_id(row.get("parent_station"))
        if parent and parent in keep and stop_id not in keep:
            keep.add(stop_id)
            changed = True
    return changed


def add_parent_and_child_stops(
    initial_ids: set[str],
    stops_rows: list[dict[str, str]],
) -> set[str]:
    rows_by_id = index_rows(stops_rows, "stop_id")
    keep = set(initial_ids)

    while True:
        changed = _add_missing_parents(keep, rows_by_id)
        changed = _add_missing_children(keep, stops_rows) or changed
        if not changed:
            break
    return keep


def _collect_ids(rows: list[dict[str, str]], key: str) -> set[str]:
    return {value for row in rows if (value := _row_value(row, key))}


def _filter_rows_by_ids(
    rows: list[dict[str, str]],
    key: str,
    keep_ids: set[str],
) -> list[dict[str, str]]:
    return [row for row in rows if _row_value(row, key) in keep_ids]


def _filter_agency_rows(
    agency_rows: list[dict[str, str]],
    keep_agency_ids: set[str],
) -> list[dict[str, str]]:
    if not keep_agency_ids:
        return list(agency_rows)
    return _filter_rows_by_ids(agency_rows, "agency_id", keep_agency_ids)


def _filter_transfer_rows(
    transfer_rows: list[dict[str, str]],
    keep_stop_ids: set[str],
) -> list[dict[str, str]]:
    return [
        row
        for row in transfer_rows
        if _row_value(row, "from_stop_id") in keep_stop_ids
        and _row_value(row, "to_stop_id") in keep_stop_ids
    ]


def _scope_tables_from_trips(
    tables: dict[str, list[dict[str, str]]],
    filtered_trips: list[dict[str, str]],
    filtered_stop_times: list[dict[str, str]],
) -> tuple[dict[str, list[dict[str, str]]], set[str]]:
    stops_rows = tables[STOPS_FILE]
    keep_stop_ids = add_parent_and_child_stops(
        _collect_ids(filtered_stop_times, "stop_id"),
        stops_rows,
    )
    filtered_stops = _filter_rows_by_ids(stops_rows, "stop_id", keep_stop_ids)
    filtered_routes = _filter_rows_by_ids(
        tables[ROUTES_FILE],
        "route_id",
        _collect_ids(filtered_trips, "route_id"),
    )
    keep_service_ids = _collect_ids(filtered_trips, "service_id")
    filtered_calendar = _filter_rows_by_ids(
        tables[CALENDAR_FILE], "service_id", keep_service_ids
    )
    filtered_calendar_dates = _filter_rows_by_ids(
        tables[CALENDAR_DATES_FILE],
        "service_id",
        keep_service_ids,
    )
    filtered_agency = _filter_agency_rows(
        tables[AGENCY_FILE],
        _collect_ids(filtered_routes, "agency_id"),
    )
    filtered_transfers = _filter_transfer_rows(tables[TRANSFERS_FILE], keep_stop_ids)

    scoped = {
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
    return scoped, keep_stop_ids


def filter_feed_by_routes(
    tables: dict[str, list[dict[str, str]]],
    tier: str,
) -> dict[str, list[dict[str, str]]]:
    routes_rows = tables[ROUTES_FILE]
    trips_rows = tables[TRIPS_FILE]
    stop_times_rows = tables[STOP_TIMES_FILE]
    route_tiers = {
        route_id: parse_route_tier(row)
        for row in routes_rows
        if (route_id := _row_value(row, "route_id"))
    }
    if tier == "all":
        keep_route_ids = set(route_tiers)
    else:
        keep_route_ids = {
            route_id
            for route_id, route_tier in route_tiers.items()
            if route_tier == tier
        }
    if not keep_route_ids:
        fail(f"tier '{tier}' produced no routes")

    filtered_trips = _filter_rows_by_ids(trips_rows, "route_id", keep_route_ids)
    filtered_stop_times = _filter_rows_by_ids(
        stop_times_rows,
        "trip_id",
        _collect_ids(filtered_trips, "trip_id"),
    )
    scoped, _ = _scope_tables_from_trips(tables, filtered_trips, filtered_stop_times)
    return scoped


def _collect_bbox_stop_ids(
    stops_rows: list[dict[str, str]],
    bbox: tuple[float, float, float, float],
) -> set[str]:
    in_scope: set[str] = set()
    for row in stops_rows:
        stop_id = normalize_stop_id(row.get("stop_id"))
        if not stop_id:
            continue
        if in_bbox(
            parse_float(row.get("stop_lat")), parse_float(row.get("stop_lon")), bbox
        ):
            in_scope.add(stop_id)
    return in_scope


def _group_stop_times_by_trip(
    stop_times_rows: list[dict[str, str]],
) -> dict[str, list[dict[str, str]]]:
    by_trip: dict[str, list[dict[str, str]]] = {}
    for row in stop_times_rows:
        trip_id = _row_value(row, "trip_id")
        if trip_id:
            by_trip.setdefault(trip_id, []).append(row)
    for rows in by_trip.values():
        sort_stop_times(rows)
    return by_trip


def _trip_ids_crossing_scoped_stops(
    trips_rows: list[dict[str, str]],
    stop_times_by_trip: dict[str, list[dict[str, str]]],
    bbox_stop_ids: set[str],
) -> set[str]:
    keep_trip_ids: set[str] = set()
    for trip in trips_rows:
        trip_id = _row_value(trip, "trip_id")
        if not trip_id:
            continue
        scoped_rows = [
            row
            for row in stop_times_by_trip.get(trip_id, [])
            if normalize_stop_id(row.get("stop_id")) in bbox_stop_ids
        ]
        if len(scoped_rows) >= 2:
            keep_trip_ids.add(trip_id)
    return keep_trip_ids


def micro_scope_feed(
    tables: dict[str, list[dict[str, str]]],
    bbox: tuple[float, float, float, float],
    padding_km: float,
) -> tuple[dict[str, list[dict[str, str]]], set[str]]:
    expanded = expand_bbox(bbox, padding_km)
    stops_rows = tables[STOPS_FILE]
    trips_rows = tables[TRIPS_FILE]
    stop_times_rows = tables[STOP_TIMES_FILE]

    stop_by_id = index_rows(stops_rows, "stop_id")
    bbox_stop_ids = _collect_bbox_stop_ids(stops_rows, expanded)
    if len(bbox_stop_ids) < 2:
        fail("micro scope found fewer than 2 stops in bbox/padding window")

    stop_times_by_trip = _group_stop_times_by_trip(stop_times_rows)
    keep_trip_ids = _trip_ids_crossing_scoped_stops(
        trips_rows,
        stop_times_by_trip,
        bbox_stop_ids,
    )
    if not keep_trip_ids:
        fail("micro scope found no trips crossing at least 2 scoped stops")

    filtered_trips = _filter_rows_by_ids(trips_rows, "trip_id", keep_trip_ids)
    filtered_stop_times = _filter_rows_by_ids(stop_times_rows, "trip_id", keep_trip_ids)
    scoped, _ = _scope_tables_from_trips(tables, filtered_trips, filtered_stop_times)

    unresolved_bbox_stops = {sid for sid in bbox_stop_ids if sid in stop_by_id}
    return scoped, unresolved_bbox_stops


def _group_active_stop_times_by_trip(
    stop_times_rows: list[dict[str, str]],
    service_by_trip: dict[str, str],
    active_service_ids: set[str],
) -> dict[str, list[dict[str, str]]]:
    by_trip: dict[str, list[dict[str, str]]] = {}
    for row in stop_times_rows:
        trip_id = _row_value(row, "trip_id")
        if not trip_id:
            continue
        if service_by_trip.get(trip_id, "") not in active_service_ids:
            continue
        by_trip.setdefault(trip_id, []).append(row)
    for rows in by_trip.values():
        sort_stop_times(rows)
    return by_trip


def _trip_query_candidate(
    trip_id: str,
    rows: list[dict[str, str]],
    bbox_stop_ids: set[str],
    target_date: str,
    stops_by_id: dict[str, dict[str, str]],
) -> tuple[dict[str, object], tuple[str, str, str]] | None:
    scoped_rows = [
        row for row in rows if normalize_stop_id(row.get("stop_id")) in bbox_stop_ids
    ]
    if len(scoped_rows) < 2:
        return None

    origin_row = scoped_rows[0]
    destination_row = scoped_rows[-1]
    origin_stop_id = normalize_stop_id(origin_row.get("stop_id"))
    destination_stop_id = normalize_stop_id(destination_row.get("stop_id"))
    dep_time = _row_value(origin_row, "departure_time") or _row_value(
        origin_row, "arrival_time"
    )
    dt_iso = time_to_iso(target_date, dep_time)
    if not dt_iso:
        return None

    origin_stop = stops_by_id.get(origin_stop_id, {})
    destination_stop = stops_by_id.get(destination_stop_id, {})
    o_lat = parse_float(origin_stop.get("stop_lat"))
    o_lon = parse_float(origin_stop.get("stop_lon"))
    d_lat = parse_float(destination_stop.get("stop_lat"))
    d_lon = parse_float(destination_stop.get("stop_lon"))
    if None in (o_lat, o_lon, d_lat, d_lon):
        return None

    if origin_stop_id and destination_stop_id:
        origin = f"{MOTIS_DATASET_TAG}_{origin_stop_id}"
        destination = f"{MOTIS_DATASET_TAG}_{destination_stop_id}"
    else:
        origin = f"{o_lat},{o_lon}"
        destination = f"{d_lat},{d_lon}"

    query = {
        "name": f"micro-{trip_id}",
        "required": True,
        "origin": origin,
        "destination": destination,
        "datetime": dt_iso,
    }
    return query, (origin, destination, dt_iso)


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
        _row_value(row, "trip_id"): _row_value(row, "service_id")
        for row in trips_rows
        if _row_value(row, "trip_id")
    }
    active_service_ids = active_services(
        calendar_rows, calendar_dates_rows, target_date
    )

    stops_by_id = index_rows(stops_rows, "stop_id")
    stop_times_by_trip = _group_active_stop_times_by_trip(
        stop_times_rows,
        service_by_trip,
        active_service_ids,
    )

    queries: list[dict[str, object]] = []
    seen: set[tuple[str, str, str]] = set()
    for trip_id, rows in sorted(stop_times_by_trip.items()):
        candidate = _trip_query_candidate(
            trip_id,
            rows,
            bbox_stop_ids,
            target_date,
            stops_by_id,
        )
        if not candidate:
            continue
        query, key = candidate
        if key in seen:
            continue
        seen.add(key)
        queries.append(query)
        if len(queries) >= max_queries:
            break
    if not queries:
        fail("micro mode produced no testable scoped routes from filtered GTFS")
    return queries


def build_macro_queries(
    tables: dict[str, list[dict[str, str]]],
) -> list[dict[str, object]]:
    target_date = choose_active_date(tables[CALENDAR_FILE], tables[CALENDAR_DATES_FILE])
    service_by_trip = {
        _row_value(row, "trip_id"): _row_value(row, "service_id")
        for row in tables[TRIPS_FILE]
        if _row_value(row, "trip_id")
    }
    active_service_ids = active_services(
        tables[CALENDAR_FILE],
        tables[CALENDAR_DATES_FILE],
        target_date,
    )
    stop_times_by_trip = _group_active_stop_times_by_trip(
        tables[STOP_TIMES_FILE],
        service_by_trip,
        active_service_ids,
    )

    queries: list[dict[str, object]] = []
    seen_pairs: set[tuple[str, str]] = set()
    for trip_id, rows in sorted(stop_times_by_trip.items()):
        if len(rows) < 2:
            continue
        origin_stop_id = normalize_stop_id(rows[0].get("stop_id"))
        destination_stop_id = normalize_stop_id(rows[-1].get("stop_id"))
        if (
            not origin_stop_id
            or not destination_stop_id
            or origin_stop_id == destination_stop_id
        ):
            continue
        dep_time = _row_value(rows[0], "departure_time") or _row_value(
            rows[0], "arrival_time"
        )
        dt_iso = time_to_iso(target_date, dep_time)
        if not dt_iso:
            continue
        dedupe_key = (origin_stop_id, destination_stop_id)
        if dedupe_key in seen_pairs:
            continue
        seen_pairs.add(dedupe_key)
        queries.append(
            {
                "name": f"macro-{trip_id}",
                "required": True,
                "origin": f"{MOTIS_DATASET_TAG}_{origin_stop_id}",
                "destination": f"{MOTIS_DATASET_TAG}_{destination_stop_id}",
                "datetime": dt_iso,
            }
        )
        if len(queries) >= 2:
            break

    if len(queries) < 2:
        fail("macro mode produced fewer than 2 testable in-feed routes")
    return queries


def _collect_bbox_stop_scope(
    zf: zipfile.ZipFile,
    expanded_bbox: tuple[float, float, float, float],
) -> tuple[set[str], dict[str, dict[str, str]]]:
    bbox_stop_ids: set[str] = set()
    bbox_stop_rows: dict[str, dict[str, str]] = {}
    with zf.open(STOPS_FILE) as fp:
        reader = csv.DictReader(io.TextIOWrapper(fp, encoding="utf-8-sig"))
        for row in reader:
            stop_id = normalize_stop_id(row.get("stop_id"))
            if not stop_id:
                continue
            if in_bbox(
                parse_float(row.get("stop_lat")),
                parse_float(row.get("stop_lon")),
                expanded_bbox,
            ):
                bbox_stop_ids.add(stop_id)
                bbox_stop_rows[stop_id] = row
    return bbox_stop_ids, bbox_stop_rows


def _store_micro_trip_candidate(
    trip_candidates: dict[str, tuple[str, str, str]],
    current_trip: str,
    first_scoped_stop: str,
    last_scoped_stop: str,
    departure_time: str,
    scoped_count: int,
) -> None:
    if not current_trip or scoped_count < 2:
        return
    if not first_scoped_stop or not last_scoped_stop:
        return
    if first_scoped_stop == last_scoped_stop:
        return
    trip_candidates[current_trip] = (
        first_scoped_stop,
        last_scoped_stop,
        departure_time,
    )


def _collect_micro_trip_candidates(
    zf: zipfile.ZipFile,
    bbox_stop_ids: set[str],
) -> dict[str, tuple[str, str, str]]:
    trip_candidates: dict[str, tuple[str, str, str]] = {}
    current_trip = ""
    first_scoped_stop = ""
    last_scoped_stop = ""
    departure_time = ""
    scoped_count = 0

    with zf.open(STOP_TIMES_FILE) as fp:
        reader = csv.DictReader(io.TextIOWrapper(fp, encoding="utf-8-sig"))
        for row in reader:
            trip_id = _row_value(row, "trip_id")
            if not trip_id:
                continue
            if trip_id != current_trip:
                _store_micro_trip_candidate(
                    trip_candidates,
                    current_trip,
                    first_scoped_stop,
                    last_scoped_stop,
                    departure_time,
                    scoped_count,
                )
                current_trip = trip_id
                first_scoped_stop = ""
                last_scoped_stop = ""
                departure_time = ""
                scoped_count = 0

            stop_id = normalize_stop_id(row.get("stop_id"))
            if stop_id not in bbox_stop_ids:
                continue
            if scoped_count == 0:
                first_scoped_stop = stop_id
                departure_time = _row_value(row, "departure_time") or _row_value(
                    row, "arrival_time"
                )
            last_scoped_stop = stop_id
            scoped_count += 1

    _store_micro_trip_candidate(
        trip_candidates,
        current_trip,
        first_scoped_stop,
        last_scoped_stop,
        departure_time,
        scoped_count,
    )
    return trip_candidates


def _append_micro_query(
    queries: list[dict[str, object]],
    seen: set[tuple[str, str, str]],
    trip_id: str,
    target_date: str,
    dep_time: str,
    origin_stop_id: str,
    destination_stop_id: str,
    bbox_stop_rows: dict[str, dict[str, str]],
) -> bool:
    dt_iso = time_to_iso(target_date, dep_time)
    if not dt_iso:
        return False

    origin_stop = bbox_stop_rows.get(origin_stop_id, {})
    destination_stop = bbox_stop_rows.get(destination_stop_id, {})
    o_lat = parse_float(origin_stop.get("stop_lat"))
    o_lon = parse_float(origin_stop.get("stop_lon"))
    d_lat = parse_float(destination_stop.get("stop_lat"))
    d_lon = parse_float(destination_stop.get("stop_lon"))
    if None in (o_lat, o_lon, d_lat, d_lon):
        return False

    origin = f"{MOTIS_DATASET_TAG}_{origin_stop_id}"
    destination = f"{MOTIS_DATASET_TAG}_{destination_stop_id}"
    dedupe_key = (origin, destination, dt_iso)
    if dedupe_key in seen:
        return False
    seen.add(dedupe_key)
    queries.append(
        {
            "name": f"micro-{trip_id}",
            "required": True,
            "origin": origin,
            "destination": destination,
            "datetime": dt_iso,
        }
    )
    return True


def _build_micro_queries_from_candidates(
    zf: zipfile.ZipFile,
    trip_candidates: dict[str, tuple[str, str, str]],
    bbox_stop_rows: dict[str, dict[str, str]],
    calendar_rows: list[dict[str, str]],
    calendar_dates_rows: list[dict[str, str]],
    max_queries: int,
) -> list[dict[str, object]]:
    target_date = choose_active_date(calendar_rows, calendar_dates_rows)
    active_service_ids = active_services(
        calendar_rows,
        calendar_dates_rows,
        target_date,
    )

    queries: list[dict[str, object]] = []
    seen: set[tuple[str, str, str]] = set()
    with zf.open(TRIPS_FILE) as fp:
        reader = csv.DictReader(io.TextIOWrapper(fp, encoding="utf-8-sig"))
        for row in reader:
            trip_id = _row_value(row, "trip_id")
            if not trip_id or trip_id not in trip_candidates:
                continue
            if _row_value(row, "service_id") not in active_service_ids:
                continue
            origin_stop_id, destination_stop_id, dep_time = trip_candidates[trip_id]
            appended = _append_micro_query(
                queries,
                seen,
                trip_id,
                target_date,
                dep_time,
                origin_stop_id,
                destination_stop_id,
                bbox_stop_rows,
            )
            if appended and len(queries) >= max_queries:
                break
    return queries


def build_micro_queries_from_zip(
    input_zip: str,
    bbox: tuple[float, float, float, float],
    padding_km: float,
    max_queries: int,
) -> tuple[list[dict[str, object]], dict[str, int]]:
    expanded = expand_bbox(bbox, padding_km)
    with zipfile.ZipFile(input_zip) as zf:
        calendar_rows, _ = parse_csv_table(zf, CALENDAR_FILE)
        calendar_dates_rows, _ = parse_csv_table(zf, CALENDAR_DATES_FILE)
        bbox_stop_ids, bbox_stop_rows = _collect_bbox_stop_scope(zf, expanded)
        if len(bbox_stop_ids) < 2:
            fail("micro scope found fewer than 2 stops in bbox/padding window")
        trip_candidates = _collect_micro_trip_candidates(zf, bbox_stop_ids)
        if not trip_candidates:
            fail("micro scope found no trips crossing at least 2 scoped stops")
        queries = _build_micro_queries_from_candidates(
            zf,
            trip_candidates,
            bbox_stop_rows,
            calendar_rows,
            calendar_dates_rows,
            max_queries,
        )
        if not queries:
            fail("micro mode produced no testable scoped routes from filtered GTFS")
        counts = {
            "stops": len(bbox_stop_ids),
            "routes": count_csv_rows(zf, ROUTES_FILE),
            "trips": count_csv_rows(zf, TRIPS_FILE),
            "stop_times": count_csv_rows(zf, STOP_TIMES_FILE),
            "queries": len(queries),
        }
    return queries, counts


def _collect_active_trip_ids(
    zf: zipfile.ZipFile,
    active_service_ids: set[str],
) -> set[str]:
    active_trip_ids: set[str] = set()
    with zf.open(TRIPS_FILE) as fp:
        reader = csv.DictReader(io.TextIOWrapper(fp, encoding="utf-8-sig"))
        for row in reader:
            trip_id = _row_value(row, "trip_id")
            if not trip_id:
                continue
            if _row_value(row, "service_id") in active_service_ids:
                active_trip_ids.add(trip_id)
    return active_trip_ids


def _append_macro_query(
    queries: list[dict[str, object]],
    seen_pairs: set[tuple[str, str]],
    current_trip: str,
    target_date: str,
    first_stop_id: str,
    last_stop_id: str,
    departure_time: str,
    stop_count: int,
    current_is_active: bool,
) -> bool:
    if not current_is_active or stop_count < 2:
        return False
    if not first_stop_id or not last_stop_id or first_stop_id == last_stop_id:
        return False
    dt_iso = time_to_iso(target_date, departure_time)
    if not dt_iso:
        return False
    pair_key = (first_stop_id, last_stop_id)
    if pair_key in seen_pairs:
        return False
    seen_pairs.add(pair_key)
    queries.append(
        {
            "name": f"macro-{current_trip}",
            "required": True,
            "origin": f"{MOTIS_DATASET_TAG}_{first_stop_id}",
            "destination": f"{MOTIS_DATASET_TAG}_{last_stop_id}",
            "datetime": dt_iso,
        }
    )
    return True


def _create_macro_trip_state(trip_id: str, active_trip_ids: set[str]) -> MacroTripState:
    return MacroTripState(trip_id=trip_id, is_active=trip_id in active_trip_ids)


def _append_macro_query_from_state(
    queries: list[dict[str, object]],
    seen_pairs: set[tuple[str, str]],
    target_date: str,
    state: MacroTripState,
) -> bool:
    return _append_macro_query(
        queries,
        seen_pairs,
        state.trip_id,
        target_date,
        state.first_stop_id,
        state.last_stop_id,
        state.departure_time,
        state.stop_count,
        state.is_active,
    )


def _consume_macro_stop_time_row(state: MacroTripState, row: dict[str, str]) -> None:
    stop_id = normalize_stop_id(row.get("stop_id"))
    if not stop_id:
        return
    if state.stop_count == 0:
        state.first_stop_id = stop_id
        state.departure_time = _row_value(row, "departure_time") or _row_value(
            row, "arrival_time"
        )
    state.last_stop_id = stop_id
    state.stop_count += 1


def _build_macro_queries_from_streams(
    zf: zipfile.ZipFile,
    calendar_rows: list[dict[str, str]],
    calendar_dates_rows: list[dict[str, str]],
) -> list[dict[str, object]]:
    target_date = choose_active_date(calendar_rows, calendar_dates_rows)
    active_service_ids = active_services(
        calendar_rows,
        calendar_dates_rows,
        target_date,
    )
    if not active_service_ids:
        fail("macro mode found no active services in feed calendar")

    active_trip_ids = _collect_active_trip_ids(zf, active_service_ids)
    if not active_trip_ids:
        fail("macro mode found no active trips for selected date")

    queries: list[dict[str, object]] = []
    seen_pairs: set[tuple[str, str]] = set()
    current_state: MacroTripState | None = None

    with zf.open(STOP_TIMES_FILE) as fp:
        reader = csv.DictReader(io.TextIOWrapper(fp, encoding="utf-8-sig"))
        for row in reader:
            trip_id = _row_value(row, "trip_id")
            if not trip_id:
                continue
            if current_state is None or trip_id != current_state.trip_id:
                if current_state is not None:
                    appended = _append_macro_query_from_state(
                        queries, seen_pairs, target_date, current_state
                    )
                    if appended and len(queries) >= 2:
                        break
                current_state = _create_macro_trip_state(trip_id, active_trip_ids)
            if not current_state.is_active:
                continue
            _consume_macro_stop_time_row(current_state, row)

    if len(queries) < 2 and current_state is not None:
        _append_macro_query_from_state(queries, seen_pairs, target_date, current_state)
    if len(queries) < 2:
        fail("macro mode produced fewer than 2 active in-feed route queries")
    return queries[:2]


def build_macro_queries_from_zip(
    input_zip: str,
) -> tuple[list[dict[str, object]], dict[str, int]]:
    with zipfile.ZipFile(input_zip) as zf:
        calendar_rows, _ = parse_csv_table(zf, CALENDAR_FILE)
        calendar_dates_rows, _ = parse_csv_table(zf, CALENDAR_DATES_FILE)
        queries = _build_macro_queries_from_streams(
            zf, calendar_rows, calendar_dates_rows
        )
        counts = {
            "stops": count_csv_rows(zf, STOPS_FILE),
            "routes": count_csv_rows(zf, ROUTES_FILE),
            "trips": count_csv_rows(zf, TRIPS_FILE),
            "stop_times": count_csv_rows(zf, STOP_TIMES_FILE),
            "queries": len(queries),
        }
    return queries, counts


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


def _ensure_parent_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def _copy_input_zip_if_needed(input_zip: str, output_zip: str) -> None:
    _ensure_parent_dir(output_zip)
    if os.path.normpath(input_zip) != os.path.normpath(output_zip):
        shutil.copyfile(input_zip, output_zip)


def _run_all_tier_mode(
    args: argparse.Namespace,
    input_zip: str,
    output_zip: str,
) -> tuple[list[dict[str, object]], dict[str, int]]:
    if args.mode == "micro":
        parsed_bbox = parse_bbox(args.bbox)
        queries, counts = build_micro_queries_from_zip(
            input_zip,
            parsed_bbox,
            args.padding_km,
            args.max_micro_queries,
        )
    else:
        queries, counts = build_macro_queries_from_zip(input_zip)
    _copy_input_zip_if_needed(input_zip, output_zip)
    return queries, counts


def _run_scoped_mode(
    args: argparse.Namespace,
    input_zip: str,
    output_zip: str,
) -> tuple[list[dict[str, object]], dict[str, int]]:
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
        tmp_output = output_zip + ".tmp"
        write_filtered_zip(input_zip, tmp_output, scoped, fieldnames)
        shutil.move(tmp_output, output_zip)
    counts = {
        "stops": len(scoped[STOPS_FILE]),
        "routes": len(scoped[ROUTES_FILE]),
        "trips": len(scoped[TRIPS_FILE]),
        "stop_times": len(scoped[STOP_TIMES_FILE]),
        "queries": len(queries),
    }
    return queries, counts


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

    # All-tier jobs on large pan-EU artifacts cannot afford full-table materialization.
    # Use streaming query derivation and copy the original ZIP unchanged.
    if args.tier == "all":
        queries, counts = _run_all_tier_mode(args, input_zip, output_zip)
    else:
        queries, counts = _run_scoped_mode(args, input_zip, output_zip)

    _ensure_parent_dir(queries_path)
    with open(queries_path, "w", encoding="utf-8") as fp:
        json.dump(queries, fp, indent=2, ensure_ascii=True)
        fp.write("\n")

    summary = {
        "mode": args.mode,
        "tier": args.tier,
        "inputZip": input_zip,
        "outputZip": output_zip,
        "queriesJson": queries_path,
        "counts": counts,
    }
    print(json.dumps(summary, ensure_ascii=True))


if __name__ == "__main__":
    main()
