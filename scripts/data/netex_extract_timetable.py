#!/usr/bin/env python3
"""Extract timetable facts from zipped NeTEx into CSV files."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import lxml.etree as etree

MAX_XML_ENTRIES_TO_SCAN = 120_000
MAX_XML_ENTRY_BYTES = 1_000_000_000  # 1GB uncompressed guardrail


def local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def clean_text(value: str | None) -> str:
    return (value or "").strip()


def child_text(elem: etree._Element, name: str) -> str:
    for child in elem:
        if local_name(child.tag) == name:
            return clean_text(child.text)
    return ""


def child_ref(elem: etree._Element, name: str) -> str:
    for child in elem:
        if local_name(child.tag) == name:
            return clean_text(child.attrib.get("ref"))
    return ""


def nested_child_text(elem: etree._Element, path: list[str]) -> str:
    current = elem
    for name in path:
        next_elem = None
        for child in current:
            if local_name(child.tag) == name:
                next_elem = child
                break
        if next_elem is None:
            return ""
        current = next_elem
    return clean_text(current.text)


def parse_int(value: str, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def stable_id(prefix: str, value: str) -> str:
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:24]
    return f"{prefix}_{digest}"


def clear_element(elem: etree._Element) -> None:
    elem.clear()
    while elem.getprevious() is not None:
        del elem.getparent()[0]


@dataclass
class Summary:
    xml_entries_scanned: int = 0
    service_journeys_found: int = 0
    trips_written: int = 0
    trip_stop_times_written: int = 0
    duplicate_trips_skipped: int = 0
    duplicate_stop_times_skipped: int = 0
    journeys_missing_pattern: int = 0
    stop_times_missing_stop_point: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "xmlEntriesScanned": self.xml_entries_scanned,
            "serviceJourneysFound": self.service_journeys_found,
            "tripsWritten": self.trips_written,
            "tripStopTimesWritten": self.trip_stop_times_written,
            "duplicateTripsSkipped": self.duplicate_trips_skipped,
            "duplicateStopTimesSkipped": self.duplicate_stop_times_skipped,
            "journeysMissingPattern": self.journeys_missing_pattern,
            "stopTimesMissingStopPoint": self.stop_times_missing_stop_point,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract timetable facts from NeTEx ZIP"
    )
    parser.add_argument("--zip-path", required=True)
    parser.add_argument("--output-trips-csv", required=True)
    parser.add_argument("--output-stop-times-csv", required=True)
    parser.add_argument("--summary-json", required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--country", required=True)
    parser.add_argument("--provider-slug", required=True)
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--manifest-sha256", default="")
    parser.add_argument("--import-run-id", required=True)
    return parser.parse_args()


def candidate_xml_entries(archive: zipfile.ZipFile, zip_path: Path) -> list[str]:
    xml_entries = sorted(
        name for name in archive.namelist() if name.lower().endswith(".xml")
    )
    if not xml_entries:
        raise RuntimeError(f"No XML entries found in archive: {zip_path}")

    preferred = [
        name
        for name in xml_entries
        if any(
            token in Path(name).name.lower()
            for token in ("timetable", "line", "service", "journey")
        )
    ]
    selected = preferred or xml_entries

    if len(selected) > MAX_XML_ENTRIES_TO_SCAN:
        raise RuntimeError(
            f"Too many XML entries to scan ({len(selected)}). "
            f"Limit is {MAX_XML_ENTRIES_TO_SCAN}."
        )

    return selected


def parse_line_meta(line_elem: etree._Element) -> tuple[str, dict[str, str]] | None:
    line_id = clean_text(line_elem.attrib.get("id"))
    if not line_id:
        return None

    name = child_text(line_elem, "Name")
    short_name = child_text(line_elem, "ShortName")
    public_code = child_text(line_elem, "PublicCode")
    transport_mode = child_text(line_elem, "TransportMode")

    return (
        line_id,
        {
            "name": name,
            "short_name": short_name,
            "public_code": public_code,
            "transport_mode": transport_mode,
        },
    )


def parse_passenger_stop_assignment(
    assignment_elem: etree._Element,
) -> tuple[str, str] | None:
    scheduled_ref = child_ref(assignment_elem, "ScheduledStopPointRef")
    quay_ref = child_ref(assignment_elem, "QuayRef")
    stop_point_ref = child_ref(assignment_elem, "StopPointRef")
    stop_place_ref = child_ref(assignment_elem, "StopPlaceRef")
    if not scheduled_ref:
        return None
    return scheduled_ref, quay_ref or stop_point_ref or stop_place_ref


def parse_journey_pattern(
    pattern_elem: etree._Element,
) -> tuple[str, dict[str, object]] | None:
    pattern_id = clean_text(pattern_elem.attrib.get("id"))
    if not pattern_id:
        return None

    line_ref = ""
    route_view = pattern_elem.find(".//{*}RouteView")
    if route_view is not None:
        line_ref = child_ref(route_view, "LineRef")

    points: dict[str, dict[str, object]] = {}
    for point in pattern_elem.iterfind(".//{*}StopPointInJourneyPattern"):
        point_id = clean_text(point.attrib.get("id"))
        if not point_id:
            continue
        order = parse_int(clean_text(point.attrib.get("order")), 0)
        scheduled_ref = child_ref(point, "ScheduledStopPointRef")
        points[point_id] = {
            "order": order,
            "scheduled_stop_ref": scheduled_ref,
        }

    return (
        pattern_id,
        {
            "line_ref": line_ref,
            "points": points,
        },
    )


def parse_service_journey_day_types(journey_elem: etree._Element) -> list[str]:
    refs: list[str] = []
    day_types = journey_elem.find("./{*}dayTypes")
    if day_types is None:
        return refs
    for ref in day_types:
        if local_name(ref.tag) != "DayTypeRef":
            continue
        value = clean_text(ref.attrib.get("ref"))
        if value:
            refs.append(value)
    return refs


def parse_service_journey_availability_refs(journey_elem: etree._Element) -> list[str]:
    refs: list[str] = []
    validity_conditions = journey_elem.find("./{*}validityConditions")
    if validity_conditions is None:
        return refs
    for child in validity_conditions:
        if local_name(child.tag) != "AvailabilityConditionRef":
            continue
        value = clean_text(child.attrib.get("ref"))
        if value:
            refs.append(value)
    return refs


def extract_call_stop_times(
    *,
    journey_elem: etree._Element,
    entry: str,
    stop_point_to_stop_place: dict[str, str],
    summary: Summary,
) -> list[dict[str, object]]:
    raw_stop_times: list[dict[str, object]] = []
    fallback_order = 0
    for call in journey_elem.iterfind(".//{*}Call"):
        fallback_order += 1
        order = parse_int(clean_text(call.attrib.get("order")), fallback_order)
        if order <= 0:
            order = fallback_order

        scheduled_stop_ref = child_ref(call, "ScheduledStopPointRef")
        if not scheduled_stop_ref:
            scheduled_stop_ref = child_ref(call, "StopPointRef")
        if not scheduled_stop_ref:
            scheduled_stop_ref = child_ref(call, "StopPlaceRef")
        provider_stop_place_ref = stop_point_to_stop_place.get(scheduled_stop_ref, "")
        provider_stop_point_ref = scheduled_stop_ref
        if not provider_stop_point_ref:
            summary.stop_times_missing_stop_point += 1
            continue

        arrival_time = nested_child_text(call, ["Arrival", "Time"]) or child_text(
            call, "ArrivalTime"
        )
        departure_time = nested_child_text(call, ["Departure", "Time"]) or child_text(
            call, "DepartureTime"
        )
        if not arrival_time and departure_time:
            arrival_time = departure_time
        if not departure_time and arrival_time:
            departure_time = arrival_time
        if not arrival_time and not departure_time:
            continue

        metadata = {
            "provider_stop_point_ref": provider_stop_point_ref,
            "provider_stop_place_ref": provider_stop_place_ref,
            "scheduled_stop_point_ref": scheduled_stop_ref,
            "source_file": entry,
            "stop_time_source": "call",
        }
        raw_stop_times.append(
            {
                "order": order,
                "fallback_order": fallback_order,
                "arrival_time": arrival_time,
                "departure_time": departure_time,
                "metadata": metadata,
            }
        )
    return raw_stop_times


def append_trip_rows(
    *,
    journey_elem: etree._Element,
    entry: str,
    args: argparse.Namespace,
    summary: Summary,
    trip_writer: csv.DictWriter,
    stop_time_writer: csv.DictWriter,
    seen_trip_ids: set[str],
    seen_trip_stop_times: set[tuple[str, int]],
    line_meta: dict[str, dict[str, str]],
    stop_point_to_stop_place: dict[str, str],
    pattern_map: dict[str, dict[str, object]],
) -> None:
    provider_trip_ref = clean_text(journey_elem.attrib.get("id"))
    if not provider_trip_ref:
        return

    day_type_refs = parse_service_journey_day_types(journey_elem)
    availability_refs = parse_service_journey_availability_refs(journey_elem)
    service_id = (
        day_type_refs[0]
        if day_type_refs
        else (availability_refs[0] if availability_refs else "")
    )

    pattern_ref = child_ref(journey_elem, "ServiceJourneyPatternRef")
    pattern_info = pattern_map.get(pattern_ref) if pattern_ref else None
    if pattern_ref and pattern_info is None:
        summary.journeys_missing_pattern += 1

    line_ref = ""
    if pattern_info is not None:
        line_ref = str(pattern_info.get("line_ref") or "")
    if not line_ref:
        line_ref = child_ref(journey_elem, "LineRef")
    line_info = line_meta.get(line_ref, {})

    route_id = line_ref
    route_short_name = (
        line_info.get("short_name") or line_info.get("public_code") or route_id
    )
    route_long_name = line_info.get("name") or route_short_name
    transport_mode = child_text(journey_elem, "TransportMode") or line_info.get(
        "transport_mode", ""
    )
    trip_headsign = route_long_name or route_short_name or provider_trip_ref

    trip_fact_id = stable_id(
        "ttf", f"{args.dataset_id}|{args.source_id}|{provider_trip_ref}"
    )

    if trip_fact_id in seen_trip_ids:
        summary.duplicate_trips_skipped += 1
    else:
        payload = {
            "source_file": entry,
            "service_journey_pattern_ref": pattern_ref,
            "line_ref": line_ref,
            "day_type_refs": day_type_refs,
            "availability_condition_refs": availability_refs,
            "import_run_id": args.import_run_id,
            "provider_slug": args.provider_slug,
            "country": args.country,
            "snapshot_date": args.snapshot_date,
            "manifest_sha256": args.manifest_sha256,
        }
        trip_writer.writerow(
            {
                "trip_fact_id": trip_fact_id,
                "dataset_id": args.dataset_id,
                "source_id": args.source_id,
                "provider_trip_ref": provider_trip_ref,
                "service_id": service_id,
                "route_id": route_id,
                "route_short_name": route_short_name,
                "route_long_name": route_long_name,
                "trip_headsign": trip_headsign,
                "transport_mode": transport_mode,
                "trip_start_date": "",
                "trip_end_date": "",
                "raw_payload": json.dumps(
                    payload, ensure_ascii=True, separators=(",", ":")
                ),
            }
        )
        seen_trip_ids.add(trip_fact_id)
        summary.trips_written += 1

    raw_stop_times: list[dict[str, Any]] = []
    if pattern_info is not None:
        pattern_points_raw = pattern_info.get("points")
        pattern_points = (
            cast(dict[str, dict[str, object]], pattern_points_raw)
            if isinstance(pattern_points_raw, dict)
            else {}
        )
        fallback_order = 0
        for passing in journey_elem.iterfind(".//{*}TimetabledPassingTime"):
            fallback_order += 1
            point_ref = child_ref(passing, "StopPointInJourneyPatternRef")
            point_info = pattern_points.get(point_ref) or {}
            scheduled_stop_ref = str(point_info.get("scheduled_stop_ref") or "")
            order_value = point_info.get("order")
            order = parse_int(
                str(order_value) if order_value is not None else "",
                fallback_order,
            )
            if order <= 0:
                order = fallback_order

            provider_stop_place_ref = stop_point_to_stop_place.get(
                scheduled_stop_ref, ""
            )
            provider_stop_point_ref = scheduled_stop_ref
            if not provider_stop_point_ref:
                summary.stop_times_missing_stop_point += 1
                continue

            arrival_time = child_text(passing, "ArrivalTime")
            departure_time = child_text(passing, "DepartureTime")
            if not arrival_time and departure_time:
                arrival_time = departure_time
            if not departure_time and arrival_time:
                departure_time = arrival_time
            if not arrival_time and not departure_time:
                continue

            metadata = {
                "provider_stop_point_ref": provider_stop_point_ref,
                "provider_stop_place_ref": provider_stop_place_ref,
                "scheduled_stop_point_ref": scheduled_stop_ref,
                "stop_point_in_journey_pattern_ref": point_ref,
                "source_file": entry,
                "stop_time_source": "timetabled_passing_time",
            }
            raw_stop_times.append(
                {
                    "order": order,
                    "fallback_order": fallback_order,
                    "arrival_time": arrival_time,
                    "departure_time": departure_time,
                    "metadata": metadata,
                }
            )

    if not raw_stop_times:
        raw_stop_times = extract_call_stop_times(
            journey_elem=journey_elem,
            entry=entry,
            stop_point_to_stop_place=stop_point_to_stop_place,
            summary=summary,
        )

    if not raw_stop_times:
        return

    raw_stop_times.sort(
        key=lambda item: (
            cast(int, item["order"]),
            cast(int, item["fallback_order"]),
        )
    )
    for seq, stop_time in enumerate(raw_stop_times, start=1):
        key = (trip_fact_id, seq)
        if key in seen_trip_stop_times:
            summary.duplicate_stop_times_skipped += 1
            continue

        stop_time_writer.writerow(
            {
                "trip_fact_id": trip_fact_id,
                "stop_sequence": seq,
                "global_stop_point_id": "",
                "arrival_time": stop_time["arrival_time"],
                "departure_time": stop_time["departure_time"],
                "pickup_type": 0,
                "drop_off_type": 0,
                "metadata": json.dumps(
                    stop_time["metadata"], ensure_ascii=True, separators=(",", ":")
                ),
            }
        )
        seen_trip_stop_times.add(key)
        summary.trip_stop_times_written += 1


def scan_entry(
    *,
    archive: zipfile.ZipFile,
    entry: str,
    args: argparse.Namespace,
    summary: Summary,
    trip_writer: csv.DictWriter,
    stop_time_writer: csv.DictWriter,
    seen_trip_ids: set[str],
    seen_trip_stop_times: set[tuple[str, int]],
) -> None:
    entry_info = archive.getinfo(entry)
    if entry_info.file_size > MAX_XML_ENTRY_BYTES:
        raise RuntimeError(
            f"XML entry '{entry}' exceeds safety limit ({entry_info.file_size} bytes)"
        )

    line_meta: dict[str, dict[str, str]] = {}
    stop_point_to_stop_place: dict[str, str] = {}
    pattern_map: dict[str, dict[str, object]] = {}

    with archive.open(entry) as handle:
        context = etree.iterparse(
            handle,
            events=("end",),
            tag=(
                "{*}Line",
                "{*}PassengerStopAssignment",
                "{*}ServiceJourneyPattern",
                "{*}ServiceJourney",
            ),
            recover=False,
            huge_tree=True,
            resolve_entities=False,
            load_dtd=False,
            no_network=True,
        )

        for _, elem in context:
            tag = local_name(elem.tag)
            if tag == "Line":
                parsed = parse_line_meta(elem)
                if parsed:
                    line_id, payload = parsed
                    line_meta[line_id] = payload
            elif tag == "PassengerStopAssignment":
                parsed = parse_passenger_stop_assignment(elem)
                if parsed:
                    scheduled_ref, stop_place_ref = parsed
                    if scheduled_ref:
                        stop_point_to_stop_place[scheduled_ref] = stop_place_ref
            elif tag == "ServiceJourneyPattern":
                parsed = parse_journey_pattern(elem)
                if parsed:
                    pattern_id, payload = parsed
                    pattern_map[pattern_id] = payload
            elif tag == "ServiceJourney":
                summary.service_journeys_found += 1
                append_trip_rows(
                    journey_elem=elem,
                    entry=entry,
                    args=args,
                    summary=summary,
                    trip_writer=trip_writer,
                    stop_time_writer=stop_time_writer,
                    seen_trip_ids=seen_trip_ids,
                    seen_trip_stop_times=seen_trip_stop_times,
                    line_meta=line_meta,
                    stop_point_to_stop_place=stop_point_to_stop_place,
                    pattern_map=pattern_map,
                )

            clear_element(elem)


def extract(
    *,
    zip_path: Path,
    args: argparse.Namespace,
    trip_writer: csv.DictWriter,
    stop_time_writer: csv.DictWriter,
) -> Summary:
    summary = Summary()
    seen_trip_ids: set[str] = set()
    seen_trip_stop_times: set[tuple[str, int]] = set()

    with zipfile.ZipFile(zip_path) as archive:
        entries = candidate_xml_entries(archive, zip_path)
        for idx, entry in enumerate(entries):
            summary.xml_entries_scanned += 1
            if idx == 0 or (idx + 1) % 200 == 0:
                print(
                    f"[netex-extract-timetable] INFO: Scanning entry {idx + 1}/{len(entries)}: {Path(entry).name}",
                    file=sys.stderr,
                )
            try:
                scan_entry(
                    archive=archive,
                    entry=entry,
                    args=args,
                    summary=summary,
                    trip_writer=trip_writer,
                    stop_time_writer=stop_time_writer,
                    seen_trip_ids=seen_trip_ids,
                    seen_trip_stop_times=seen_trip_stop_times,
                )
            except etree.XMLSyntaxError as exc:
                raise RuntimeError(
                    f"NeTEx XML parse error in entry '{entry}': {exc}"
                ) from exc

    return summary


def main() -> int:
    args = parse_args()

    zip_path = Path(args.zip_path)
    output_trips_csv = Path(args.output_trips_csv)
    output_stop_times_csv = Path(args.output_stop_times_csv)
    summary_json = Path(args.summary_json)

    if not zip_path.is_file():
        raise RuntimeError(f"ZIP not found: {zip_path}")

    output_trips_csv.parent.mkdir(parents=True, exist_ok=True)
    output_stop_times_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)

    with (
        output_trips_csv.open("w", newline="", encoding="utf-8") as trips_handle,
        output_stop_times_csv.open(
            "w", newline="", encoding="utf-8"
        ) as stop_times_handle,
    ):
        trip_writer = csv.DictWriter(
            trips_handle,
            fieldnames=[
                "trip_fact_id",
                "dataset_id",
                "source_id",
                "provider_trip_ref",
                "service_id",
                "route_id",
                "route_short_name",
                "route_long_name",
                "trip_headsign",
                "transport_mode",
                "trip_start_date",
                "trip_end_date",
                "raw_payload",
            ],
        )
        stop_time_writer = csv.DictWriter(
            stop_times_handle,
            fieldnames=[
                "trip_fact_id",
                "stop_sequence",
                "global_stop_point_id",
                "arrival_time",
                "departure_time",
                "pickup_type",
                "drop_off_type",
                "metadata",
            ],
        )
        trip_writer.writeheader()
        stop_time_writer.writeheader()

        summary = extract(
            zip_path=zip_path,
            args=args,
            trip_writer=trip_writer,
            stop_time_writer=stop_time_writer,
        )

    summary_payload = summary.as_dict()
    summary_json.write_text(
        json.dumps(summary_payload, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary_payload, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[netex-extract-timetable] ERROR: {exc}", file=sys.stderr)
        raise
