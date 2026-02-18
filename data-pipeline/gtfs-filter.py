#!/usr/bin/env python3
"""
GTFS Filter — Keeps only rail-related route types to minimize RAM usage.

Filters:
  KEEP: route_type 2 (Rail), 1 (Metro/Subway), 100-109 (Regional Rail)
  DROP: Everything else (Bus, Tram, Ferry, etc.)

Usage:
  python gtfs-filter.py input.zip output.zip
"""

import sys
import os
import csv
import io
import zipfile
from pathlib import Path

# Route types to keep (rail-only)
KEEP_ROUTE_TYPES = {1, 2} | set(range(100, 110))


def filter_gtfs(input_path: str, output_path: str) -> dict:
    """Filter a GTFS ZIP to only rail-related routes."""
    stats = {"routes_kept": 0, "routes_dropped": 0, "trips_kept": 0, "trips_dropped": 0}

    with zipfile.ZipFile(input_path, "r") as zin:
        filenames = zin.namelist()

        # Step 1: Find routes to keep
        kept_route_ids = set()
        if "routes.txt" in filenames:
            with zin.open("routes.txt") as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
                for row in reader:
                    route_type = int(row.get("route_type", -1))
                    if route_type in KEEP_ROUTE_TYPES:
                        kept_route_ids.add(row["route_id"])
                        stats["routes_kept"] += 1
                    else:
                        stats["routes_dropped"] += 1

        print(f"Routes: kept {stats['routes_kept']}, dropped {stats['routes_dropped']}")

        # Step 2: Find trips belonging to kept routes
        kept_trip_ids = set()
        if "trips.txt" in filenames:
            with zin.open("trips.txt") as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
                for row in reader:
                    if row["route_id"] in kept_route_ids:
                        kept_trip_ids.add(row["trip_id"])
                        stats["trips_kept"] += 1
                    else:
                        stats["trips_dropped"] += 1

        print(f"Trips: kept {stats['trips_kept']}, dropped {stats['trips_dropped']}")

        # Step 3: Find stops used by kept trips
        kept_stop_ids = set()
        if "stop_times.txt" in filenames:
            with zin.open("stop_times.txt") as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
                for row in reader:
                    if row["trip_id"] in kept_trip_ids:
                        kept_stop_ids.add(row["stop_id"])

        # Also keep parent stations
        parent_stop_ids = set()
        if "stops.txt" in filenames:
            with zin.open("stops.txt") as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
                for row in reader:
                    if row["stop_id"] in kept_stop_ids:
                        parent = row.get("parent_station", "")
                        if parent:
                            parent_stop_ids.add(parent)
        kept_stop_ids |= parent_stop_ids

        # Step 4: Find services used by kept trips
        kept_service_ids = set()
        if "trips.txt" in filenames:
            with zin.open("trips.txt") as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
                for row in reader:
                    if row["trip_id"] in kept_trip_ids:
                        kept_service_ids.add(row["service_id"])

        # Step 5: Write filtered GTFS
        with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zout:
            for filename in filenames:
                with zin.open(filename) as f:
                    reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
                    fieldnames = reader.fieldnames
                    if not fieldnames:
                        continue

                    rows = []
                    for row in reader:
                        keep = True

                        if filename == "routes.txt":
                            keep = row["route_id"] in kept_route_ids
                        elif filename == "trips.txt":
                            keep = row["trip_id"] in kept_trip_ids
                        elif filename == "stop_times.txt":
                            keep = row["trip_id"] in kept_trip_ids
                        elif filename == "stops.txt":
                            keep = row["stop_id"] in kept_stop_ids
                        elif filename == "calendar.txt":
                            keep = row.get("service_id", "") in kept_service_ids
                        elif filename == "calendar_dates.txt":
                            keep = row.get("service_id", "") in kept_service_ids
                        # Keep all rows for other files (agency.txt, feed_info.txt, etc.)

                        if keep:
                            rows.append(row)

                    # Write filtered file
                    output = io.StringIO()
                    writer = csv.DictWriter(output, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)
                    zout.writestr(filename, output.getvalue())

                    print(f"  {filename}: {len(rows)} rows")

    return stats


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python gtfs-filter.py <input.zip> <output.zip>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    if not os.path.exists(input_path):
        print(f"Error: Input file not found: {input_path}")
        sys.exit(1)

    print(f"Filtering GTFS: {input_path} → {output_path}")
    stats = filter_gtfs(input_path, output_path)
    print(f"\nDone! Routes kept: {stats['routes_kept']}, Trips kept: {stats['trips_kept']}")
