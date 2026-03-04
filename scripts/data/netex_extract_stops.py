#!/usr/bin/env python3
"""Extract StopPlace-level station rows from zipped NeTEx into CSV."""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path

import lxml.etree as etree

MAX_XML_ENTRIES_TO_SCAN = 50_000
MAX_XML_ENTRY_BYTES = 1_000_000_000  # 1GB uncompressed guardrail
PEEK_BYTES = 8192  # bytes to read for content-based filtering
CONTENT_PEEK_THRESHOLD = 500  # do content-based peek when entries exceed this


def local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped else None


def compute_grid_id(country: str, lat: float | None, lon: float | None) -> str:
    if lat is not None and lon is not None and -90 <= lat <= 90 and -180 <= lon <= 180:
        lat_bucket = math.floor(lat + 90)
        lon_bucket = math.floor(lon + 180)
        return f"g{lat_bucket:03d}_{lon_bucket:03d}"
    return f"zzz{country.strip().lower()}"


def first_direct_child_text(elem: etree._Element, names: set[str]) -> str | None:
    for child in elem:
        if local_name(child.tag) in names:
            text = clean_text(child.text)
            if text:
                return text
    return None


def first_descriptor_name(elem: etree._Element) -> str | None:
    for child in elem:
        if local_name(child.tag) != "Descriptor":
            continue
        for descriptor_child in child:
            if local_name(descriptor_child.tag) == "Name":
                text = clean_text(descriptor_child.text)
                if text:
                    return text
    return None


def _parse_coordinate_value(raw: str | None) -> float | None:
    text = clean_text(raw)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _valid_coordinates(lat: float | None, lon: float | None) -> bool:
    return (
        lat is not None and lon is not None and -90 <= lat <= 90 and -180 <= lon <= 180
    )


def _coords_from_location(
    location: etree._Element,
) -> tuple[float | None, float | None]:
    lat: float | None = None
    lon: float | None = None
    for child in location:
        tag = local_name(child.tag)
        if tag == "Latitude":
            lat = _parse_coordinate_value(child.text)
        elif tag == "Longitude":
            lon = _parse_coordinate_value(child.text)
    return lat, lon


def first_location_coords(elem: etree._Element) -> tuple[float | None, float | None]:
    for location in elem.iterfind(".//{*}Location"):
        lat, lon = _coords_from_location(location)
        if _valid_coordinates(lat, lon):
            return lat, lon
    return None, None


def parent_site_ref(elem: etree._Element) -> str | None:
    for child in elem:
        if local_name(child.tag) == "ParentSiteRef":
            ref = clean_text(child.attrib.get("ref"))
            if ref:
                return ref
    return None


def key_values(elem: etree._Element) -> dict[str, str]:
    result: dict[str, str] = {}
    for kv in elem.iterfind(".//{*}KeyValue"):
        key_text: str | None = None
        value_text: str | None = None
        for child in kv:
            tag = local_name(child.tag)
            if tag == "Key":
                key_text = clean_text(child.text)
            elif tag == "Value":
                value_text = clean_text(child.text)
        if key_text and value_text:
            result[key_text.strip().lower()] = value_text
    return result


@dataclass
class Summary:
    xml_entries_scanned: int = 0
    stop_places_found: int = 0
    stop_places_written: int = 0
    duplicates_skipped: int = 0
    missing_id_skipped: int = 0
    missing_name_skipped: int = 0
    with_coordinates: int = 0
    without_coordinates: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "xmlEntriesScanned": self.xml_entries_scanned,
            "stopPlacesFound": self.stop_places_found,
            "stopPlacesWritten": self.stop_places_written,
            "duplicatesSkipped": self.duplicates_skipped,
            "missingIdSkipped": self.missing_id_skipped,
            "missingNameSkipped": self.missing_name_skipped,
            "withCoordinates": self.with_coordinates,
            "withoutCoordinates": self.without_coordinates,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract NeTEx stops from ZIP")
    parser.add_argument("--zip-path", required=True)
    parser.add_argument("--output-csv", required=True)
    parser.add_argument("--summary-json", required=True)
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--country", required=True)
    parser.add_argument("--provider-slug", required=True)
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--manifest-sha256", default="")
    parser.add_argument("--import-run-id", required=True)
    return parser.parse_args()


def _clear_element(elem: etree._Element) -> None:
    elem.clear()
    while elem.getprevious() is not None:
        del elem.getparent()[0]


def _entry_contains_stop_place(archive: zipfile.ZipFile, entry: str) -> bool:
    """Quick peek into the first PEEK_BYTES of a zip entry using Python zipfile."""
    try:
        with archive.open(entry) as handle:
            head = handle.read(PEEK_BYTES)
            return b"StopPlace" in head or b"SiteFrame" in head
    except Exception:
        return False


def _candidate_xml_entries(archive: zipfile.ZipFile, zip_path: Path) -> list[str]:
    """Determine which XML entries to scan."""
    xml_entries = sorted(
        name for name in archive.namelist() if name.lower().endswith(".xml")
    )
    if not xml_entries:
        raise RuntimeError(f"No XML entries found in archive: {zip_path}")

    print(
        f"[netex-extract] INFO: Archive contains {len(xml_entries)} XML entries.",
        file=sys.stderr,
    )

    # Step 1: Try filename-based filtering (fast)
    preferred_entries = [
        name
        for name in xml_entries
        if any(
            token in Path(name).name.lower() for token in ("site", "stop", "station")
        )
    ]
    if preferred_entries:
        print(
            f"[netex-extract] INFO: Found {len(preferred_entries)} entries with site/stop/station in name.",
            file=sys.stderr,
        )
        if len(preferred_entries) > MAX_XML_ENTRIES_TO_SCAN:
            raise RuntimeError(
                f"Too many XML entries to scan ({len(preferred_entries)}). "
                f"Limit is {MAX_XML_ENTRIES_TO_SCAN}."
            )
        return preferred_entries

    # Step 2: No filename matches — if manageable count, scan all
    if len(xml_entries) <= CONTENT_PEEK_THRESHOLD:
        print(
            f"[netex-extract] WARN: No site-like XML entry name; "
            f"falling back to all {len(xml_entries)} XML entries.",
            file=sys.stderr,
        )
        return xml_entries

    # Step 3: Too many entries — do content-based peek filtering
    print(
        f"[netex-extract] INFO: {len(xml_entries)} XML entries without site-like names; "
        "peeking into each to find StopPlace/SiteFrame content...",
        file=sys.stderr,
    )
    stop_entries = []
    for i, name in enumerate(xml_entries):
        if _entry_contains_stop_place(archive, name):
            stop_entries.append(name)
        if (i + 1) % 5000 == 0:
            print(
                f"[netex-extract] INFO: Peeked {i + 1}/{len(xml_entries)} entries, "
                f"found {len(stop_entries)} with StopPlace data so far...",
                file=sys.stderr,
            )
    if stop_entries:
        print(
            f"[netex-extract] INFO: Content peek found {len(stop_entries)} entries "
            f"containing StopPlace/SiteFrame data (out of {len(xml_entries)} total).",
            file=sys.stderr,
        )
        if len(stop_entries) > MAX_XML_ENTRIES_TO_SCAN:
            raise RuntimeError(
                f"Too many StopPlace entries after peek ({len(stop_entries)}). "
                f"Limit is {MAX_XML_ENTRIES_TO_SCAN}."
            )
        return stop_entries

    # Step 4: Content peek found nothing — fall back to all
    print(
        f"[netex-extract] WARN: Content peek found no StopPlace entries; "
        f"falling back to all {len(xml_entries)} XML entries.",
        file=sys.stderr,
    )
    if len(xml_entries) > MAX_XML_ENTRIES_TO_SCAN:
        raise RuntimeError(
            f"Too many XML entries to scan ({len(xml_entries)}). "
            f"Limit is {MAX_XML_ENTRIES_TO_SCAN}."
        )
    return xml_entries


def _pick_hard_id(
    public_code: str | None, private_code: str | None, kv: dict[str, str]
) -> str | None:
    hard_id = public_code or private_code
    if hard_id:
        return hard_id
    for key in ("uic", "ifopt", "eva", "stopplaceref", "globalid"):
        if key in kv:
            return kv[key]
    return None


def _write_stop_row(
    writer: csv.DictWriter,
    args: argparse.Namespace,
    entry: str,
    elem: etree._Element,
    source_stop_id: str,
    stop_name: str,
    lat: float | None,
    lon: float | None,
    public_code: str | None,
    private_code: str | None,
    hard_id: str | None,
) -> None:
    payload = {
        "xmlEntry": entry,
        "stopPlaceType": first_direct_child_text(elem, {"StopPlaceType"}),
        "transportMode": first_direct_child_text(elem, {"TransportMode"}),
    }
    writer.writerow(
        {
            "import_run_id": args.import_run_id,
            "source_id": args.source_id,
            "country": args.country,
            "provider_slug": args.provider_slug,
            "snapshot_date": args.snapshot_date,
            "manifest_sha256": args.manifest_sha256,
            "source_stop_id": source_stop_id,
            "source_parent_stop_id": parent_site_ref(elem) or "",
            "stop_name": stop_name,
            "latitude": "" if lat is None else f"{lat:.8f}",
            "longitude": "" if lon is None else f"{lon:.8f}",
            "grid_id": compute_grid_id(args.country, lat, lon),
            "public_code": public_code or "",
            "private_code": private_code or "",
            "hard_id": hard_id or "",
            "source_file": entry,
            "raw_payload": json.dumps(
                payload, ensure_ascii=True, separators=(",", ":")
            ),
        }
    )


def _process_stop_place(
    elem: etree._Element,
    entry: str,
    writer: csv.DictWriter,
    args: argparse.Namespace,
    seen_stop_ids: set[str],
    summary: Summary,
) -> None:
    summary.stop_places_found += 1
    source_stop_id = clean_text(elem.attrib.get("id"))
    if not source_stop_id:
        summary.missing_id_skipped += 1
        return
    if source_stop_id in seen_stop_ids:
        summary.duplicates_skipped += 1
        return

    stop_name = first_direct_child_text(elem, {"Name"}) or first_descriptor_name(elem)
    if not stop_name:
        summary.missing_name_skipped += 1
        return

    lat, lon = first_location_coords(elem)
    if lat is None or lon is None:
        summary.without_coordinates += 1
    else:
        summary.with_coordinates += 1

    public_code = first_direct_child_text(elem, {"PublicCode"})
    private_code = first_direct_child_text(elem, {"PrivateCode"})
    hard_id = _pick_hard_id(public_code, private_code, key_values(elem))
    _write_stop_row(
        writer,
        args,
        entry,
        elem,
        source_stop_id,
        stop_name,
        lat,
        lon,
        public_code,
        private_code,
        hard_id,
    )
    seen_stop_ids.add(source_stop_id)
    summary.stop_places_written += 1


def _scan_xml_entry(
    archive: zipfile.ZipFile,
    entry: str,
    writer: csv.DictWriter,
    args: argparse.Namespace,
    seen_stop_ids: set[str],
    summary: Summary,
) -> None:
    """Stream a single zip entry and parse with lxml iterparse, avoiding memory leaks."""

    # These bulky NeTEx objects are the ones that accumulate in RAM if not explicitly matched and cleared.
    tags_to_clear_and_ignore = {
        "{*}VehicleJourney",
        "{*}ServiceJourney",
        "{*}TimetableFrame",
        "{*}ServiceFrame",
        "{*}JourneyPattern",
        "{*}Line",
        "{*}Route",
        "{*}SiteFrame",  # Clear this at the very end to free up its children
    }

    events_tags = ("{*}StopPlace",) + tuple(tags_to_clear_and_ignore)

    entry_info = archive.getinfo(entry)
    if entry_info.file_size > MAX_XML_ENTRY_BYTES:
        raise RuntimeError(
            f"XML entry '{entry}' exceeds safety limit ({entry_info.file_size} bytes)"
        )

    with archive.open(entry) as handle:
        context = etree.iterparse(
            handle,
            events=("end",),
            tag=events_tags,
            recover=False,
            huge_tree=True,
            resolve_entities=False,
            load_dtd=False,
            no_network=True,
        )
        for _, elem in context:
            if local_name(elem.tag) == "StopPlace":
                _process_stop_place(elem, entry, writer, args, seen_stop_ids, summary)

            # By calling _clear_element on both StopPlace and the massive non-StopPlace nodes,
            # we ensure lxml can actually garbage collect them and we don't hit OOM.
            _clear_element(elem)


def extract(
    zip_path: Path, writer: csv.DictWriter, args: argparse.Namespace
) -> Summary:
    summary = Summary()
    seen_stop_ids: set[str] = set()

    with zipfile.ZipFile(zip_path) as archive:
        entries = _candidate_xml_entries(archive, zip_path)
        for i, entry in enumerate(entries):
            summary.xml_entries_scanned += 1
            if (i + 1) % 100 == 0 or i == 0:
                print(
                    f"[netex-extract] INFO: Scanning entry {i + 1}/{len(entries)}: {Path(entry).name}",
                    file=sys.stderr,
                )
            try:
                _scan_xml_entry(archive, entry, writer, args, seen_stop_ids, summary)
            except etree.XMLSyntaxError as exc:
                raise RuntimeError(
                    f"NeTEx XML parse error in entry '{entry}': {exc}"
                ) from exc

    return summary


def main() -> int:
    args = parse_args()

    zip_path = Path(args.zip_path)
    output_csv = Path(args.output_csv)
    summary_json = Path(args.summary_json)

    if not zip_path.is_file():
        raise RuntimeError(f"ZIP not found: {zip_path}")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)

    with output_csv.open("w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(
            out,
            fieldnames=[
                "import_run_id",
                "source_id",
                "country",
                "provider_slug",
                "snapshot_date",
                "manifest_sha256",
                "source_stop_id",
                "source_parent_stop_id",
                "stop_name",
                "latitude",
                "longitude",
                "grid_id",
                "public_code",
                "private_code",
                "hard_id",
                "source_file",
                "raw_payload",
            ],
        )
        writer.writeheader()
        summary = extract(zip_path, writer, args)

    summary_payload = summary.as_dict()
    summary_json.write_text(
        json.dumps(summary_payload, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )

    if summary.stop_places_written == 0:
        raise RuntimeError(
            "No StopPlace rows extracted from NeTEx archive; failing hard per policy"
        )

    print(json.dumps(summary_payload, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[netex-extract] ERROR: {exc}", file=sys.stderr)
        raise
