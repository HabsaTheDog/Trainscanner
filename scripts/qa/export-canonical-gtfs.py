#!/usr/bin/env python3
import argparse
import csv
import io
import json
import sys
import zipfile
from collections import defaultdict
from datetime import datetime


REQUIRED_COLUMNS = {
    "stop_id",
    "stop_name",
    "country",
    "stop_lat",
    "stop_lon",
}


def fail(msg: str) -> None:
    print(f"[export-canonical-gtfs] ERROR: {msg}", file=sys.stderr)
    raise SystemExit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate deterministic GTFS zip from canonical stop rows")
    parser.add_argument("--stops-csv", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--output-zip", required=True)
    parser.add_argument("--summary-json", required=True)
    parser.add_argument("--agency-url", default="https://example.invalid/trainscanner")
    return parser.parse_args()


def parse_bool(value: str, default: bool = True) -> bool:
    clean = (value or "").strip().lower()
    if not clean:
        return default
    if clean in {"1", "true", "t", "yes", "y"}:
        return True
    if clean in {"0", "false", "f", "no", "n"}:
        return False
    return default


def parse_walk_links(raw: str):
    text = (raw or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []

    links = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        to_stop_id = str(item.get("to_stop_id") or item.get("toStopId") or "").strip()
        if not to_stop_id:
            continue
        minutes_raw = item.get("min_walk_minutes")
        try:
            minutes = int(minutes_raw)
            if minutes < 0:
                minutes = 0
        except Exception:
            minutes = 0
        links.append({"to_stop_id": to_stop_id, "min_walk_minutes": minutes})

    links.sort(key=lambda l: (l["to_stop_id"], l["min_walk_minutes"]))
    return links


def load_stops(path: str):
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            fail("stops CSV has no header")
        missing = REQUIRED_COLUMNS - set(reader.fieldnames)
        if missing:
            fail(f"stops CSV missing required columns: {', '.join(sorted(missing))}")

        stops = []
        missing_coords = []
        for row in reader:
            stop_id = (row.get("stop_id") or "").strip()
            stop_name = (row.get("stop_name") or "").strip()
            country = (row.get("country") or "").strip()
            lat_raw = (row.get("stop_lat") or "").strip()
            lon_raw = (row.get("stop_lon") or "").strip()

            if not stop_id or not stop_name or country not in {"DE", "AT", "CH"}:
                fail(f"invalid stop row values for stop_id='{stop_id}' country='{country}'")

            try:
                lat = float(lat_raw)
                lon = float(lon_raw)
            except Exception:
                missing_coords.append(stop_id)
                continue

            if abs(lat) > 90 or abs(lon) > 180:
                fail(f"invalid coordinates for stop '{stop_id}': lat={lat_raw} lon={lon_raw}")

            location_type = (row.get("location_type") or "").strip()
            parent_station = (row.get("parent_station") or "").strip()
            section_type = (row.get("section_type") or "").strip()

            stops.append(
                {
                    "stop_id": stop_id,
                    "stop_name": stop_name,
                    "country": country,
                    "stop_lat": lat,
                    "stop_lon": lon,
                    "location_type": location_type,
                    "parent_station": parent_station,
                    "is_user_facing": parse_bool(row.get("is_user_facing"), default=True),
                    "walk_links": parse_walk_links(row.get("walk_links_json") or ""),
                    "section_type": section_type,
                }
            )

    if missing_coords:
        sample = ", ".join(missing_coords[:10])
        fail(
            "canonical export requires coordinates for all stops; missing/invalid for "
            f"{len(missing_coords)} stop(s), e.g. {sample}"
        )

    if len(stops) < 2:
        fail("export scope produced fewer than 2 stops; cannot build journey bridge")

    stops.sort(key=lambda s: (s["country"], s["stop_id"]))

    by_country_user_facing = defaultdict(list)
    for stop in stops:
        if stop["is_user_facing"]:
            by_country_user_facing[stop["country"]].append(stop)

    too_small = [country for country, rows in by_country_user_facing.items() if len(rows) < 2]
    if too_small:
        fail(
            "cannot build journey bridge because some countries have fewer than 2 user-facing stops: "
            + ", ".join(sorted(too_small))
        )

    return stops, by_country_user_facing


def csv_text(header, rows):
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(header)
    for row in rows:
        writer.writerow(row)
    return out.getvalue()


def fmt_time(total_seconds: int) -> str:
    hh = total_seconds // 3600
    mm = (total_seconds % 3600) // 60
    ss = total_seconds % 60
    return f"{hh:02d}:{mm:02d}:{ss:02d}"


def build_transfers(stops):
    known_stop_ids = {stop["stop_id"] for stop in stops}
    rows = []
    seen = set()

    for stop in stops:
        from_stop_id = stop["stop_id"]
        for link in stop.get("walk_links", []):
            to_stop_id = str(link.get("to_stop_id") or "").strip()
            if not to_stop_id or to_stop_id not in known_stop_ids or to_stop_id == from_stop_id:
                continue
            min_walk_minutes = int(link.get("min_walk_minutes") or 0)
            if min_walk_minutes < 0:
                min_walk_minutes = 0
            transfer_seconds = min_walk_minutes * 60
            key = (from_stop_id, to_stop_id, transfer_seconds)
            if key in seen:
                continue
            seen.add(key)
            rows.append([from_stop_id, to_stop_id, "2", str(transfer_seconds)])

    rows.sort(key=lambda r: (r[0], r[1], int(r[3])))
    return rows


def build_tables(profile: str, stops, by_country_user_facing, agency_url: str):
    tz_map = {
        "DE": "Europe/Berlin",
        "AT": "Europe/Vienna",
        "CH": "Europe/Zurich",
    }

    agency_rows = []
    route_rows = []
    trip_rows = []
    stop_time_rows = []
    calendar_rows = []

    for country in sorted(by_country_user_facing.keys()):
        rows = sorted(by_country_user_facing[country], key=lambda r: r["stop_id"])
        agency_id = f"agency_{country.lower()}"
        route_id = f"route_{country.lower()}_main"
        service_id = f"svc_{country.lower()}"

        agency_rows.append(
            [
                agency_id,
                f"Canonical {country} Transit",
                agency_url,
                tz_map.get(country, "Europe/Berlin"),
                "de",
            ]
        )

        route_rows.append(
            [
                route_id,
                agency_id,
                f"{country} Canonical",
                f"{country} Canonical Bridge",
                "2",
            ]
        )

        first_stop = rows[0]["stop_name"]
        last_stop = rows[-1]["stop_name"]
        trip_out_id = f"trip_{country.lower()}_outbound"
        trip_in_id = f"trip_{country.lower()}_inbound"

        trip_rows.append([route_id, service_id, trip_out_id, f"{first_stop} -> {last_stop}"])
        trip_rows.append([route_id, service_id, trip_in_id, f"{last_stop} -> {first_stop}"])

        calendar_rows.append(
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
        )

        base_outbound = 6 * 3600
        for idx, stop in enumerate(rows, start=1):
            t = fmt_time(base_outbound + (idx - 1) * 420)
            stop_time_rows.append([trip_out_id, t, t, stop["stop_id"], str(idx)])

        base_inbound = 18 * 3600
        for idx, stop in enumerate(reversed(rows), start=1):
            t = fmt_time(base_inbound + (idx - 1) * 420)
            stop_time_rows.append([trip_in_id, t, t, stop["stop_id"], str(idx)])

    stops_rows = [
        [
            stop["stop_id"],
            stop["stop_name"],
            f"{stop['stop_lat']:.6f}",
            f"{stop['stop_lon']:.6f}",
            stop["location_type"],
            stop["parent_station"],
        ]
        for stop in stops
    ]

    transfer_rows = build_transfers(stops)

    stop_time_rows.sort(key=lambda r: (r[0], int(r[4]), r[3]))
    trip_rows.sort(key=lambda r: r[2])
    route_rows.sort(key=lambda r: r[0])
    agency_rows.sort(key=lambda r: r[0])
    calendar_rows.sort(key=lambda r: r[0])

    files = {
        "agency.txt": csv_text(
            ["agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang"],
            agency_rows,
        ),
        "stops.txt": csv_text(
            ["stop_id", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station"],
            stops_rows,
        ),
        "routes.txt": csv_text(
            ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"],
            route_rows,
        ),
        "trips.txt": csv_text(["route_id", "service_id", "trip_id", "trip_headsign"], trip_rows),
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
        files["transfers.txt"] = csv_text(
            ["from_stop_id", "to_stop_id", "transfer_type", "min_transfer_time"],
            transfer_rows,
        )

    summary = {
        "profile": profile,
        "bridgeMode": "group-aware-synthetic-journeys-from-canonical-stops",
        "counts": {
            "stops": len(stops_rows),
            "userFacingStops": sum(1 for stop in stops if stop["is_user_facing"]),
            "sectionStops": sum(1 for stop in stops if stop["parent_station"]),
            "agencies": len(agency_rows),
            "routes": len(route_rows),
            "trips": len(trip_rows),
            "stopTimes": len(stop_time_rows),
            "services": len(calendar_rows),
            "transfers": len(transfer_rows),
            "countries": len(by_country_user_facing),
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
    if "transfers.txt" in files:
        ordered_names.append("transfers.txt")

    with zipfile.ZipFile(output_zip, "w") as zf:
        for name in ordered_names:
            payload = files[name].encode("utf-8")
            info = zipfile.ZipInfo(name)
            info.date_time = timestamp
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            zf.writestr(info, payload)


def main() -> None:
    args = parse_args()
    stops, by_country_user_facing = load_stops(args.stops_csv)
    files, summary = build_tables(args.profile, stops, by_country_user_facing, args.agency_url)
    write_deterministic_zip(files, args.output_zip, args.as_of)

    with open(args.summary_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
        f.write("\n")


if __name__ == "__main__":
    main()
