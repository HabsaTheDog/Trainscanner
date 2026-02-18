#!/usr/bin/env python3
"""
GTFS station dedup helper.

Purpose:
1. Generate station-match suggestions from GTFS stops -> station_map.json
2. Support interactive manual review
3. Apply accepted decisions back into station_map.json

This script focuses on the station mapping workflow from plan.md:
"name + coordinate proximity" semi-automatic matching, then manual curation.

Usage examples:
  # 1) Build a review CSV (non-interactive)
  python3 data-pipeline/gtfs-dedup.py suggest \
    data/gtfs_raw/de_fv.zip data/gtfs_raw/de_rv.zip data/gtfs_raw/ch_full.zip \
    --out-csv data/station_dedup_review.csv

  # 2) Run interactive review
  python3 data-pipeline/gtfs-dedup.py review data/gtfs_raw/*.zip \
    --out-csv data/station_dedup_review.csv --accept-auto

  # 3) Apply reviewed decisions into station_map.json
  python3 data-pipeline/gtfs-dedup.py apply data/station_dedup_review.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import shutil
import sys
import time
import unicodedata
import zipfile
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


REVIEW_COLUMNS = [
    "feed",
    "source_path",
    "country",
    "stop_id",
    "stop_name",
    "stop_lat",
    "stop_lon",
    "stop_code",
    "parent_station",
    "location_type",
    "suggested_uic",
    "suggested_station",
    "suggested_country",
    "name_similarity",
    "distance_m",
    "confidence",
    "recommendation",
    "decision",
    "decision_station_uic",
    "note",
]

NAME_STOPWORDS = {
    "bahnhof",
    "hauptbahnhof",
    "hb",
    "hbf",
    "bf",
    "station",
    "stazione",
    "gare",
}

COUNTRY_HINTS = {
    "de": "de",
    "germany": "de",
    "deutschland": "de",
    "ch": "ch",
    "switzerland": "ch",
    "schweiz": "ch",
    "at": "at",
    "austria": "at",
    "oebb": "at",
}


@dataclass
class StopRecord:
    feed: str
    source_path: str
    country: str
    stop_id: str
    stop_name: str
    lat: float
    lon: float
    stop_code: str
    parent_station: str
    location_type: str


def normalize_name(value: str) -> str:
    text = (value or "").strip().lower()
    text = (
        text.replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
    )
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    tokens = [tok for tok in text.split() if tok and tok not in NAME_STOPWORDS]
    return " ".join(tokens)


def name_similarity(left: str, right: str) -> float:
    a = normalize_name(left)
    b = normalize_name(right)
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def haversine_distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6_371_000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def parse_country_hint(token: str) -> Optional[str]:
    lowered = token.lower()
    for key, value in COUNTRY_HINTS.items():
        if key in lowered:
            return value
    return None


def parse_input_token(token: str) -> Tuple[Optional[str], str]:
    match = re.match(r"^([A-Za-z]{2}):(.*)$", token)
    if not match:
        return None, token
    country = match.group(1).lower()
    path_part = match.group(2)
    if path_part:
        return country, path_part
    return None, token


def find_stops_member(zf: zipfile.ZipFile) -> Optional[str]:
    names = zf.namelist()
    if "stops.txt" in names:
        return "stops.txt"
    for name in names:
        if name.lower().endswith("/stops.txt"):
            return name
    return None


def parse_float(value: str) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def read_stops_from_zip(path: Path, country_hint: Optional[str]) -> List[StopRecord]:
    stops: List[StopRecord] = []
    with zipfile.ZipFile(path, "r") as zf:
        member = find_stops_member(zf)
        if not member:
            return stops
        inferred_country = country_hint or parse_country_hint(path.name) or "xx"
        with zf.open(member) as raw:
            reader = csv.DictReader((line.decode("utf-8-sig") for line in raw))
            for row in reader:
                lat = parse_float(row.get("stop_lat", ""))
                lon = parse_float(row.get("stop_lon", ""))
                if lat is None or lon is None:
                    continue
                location_type = (row.get("location_type", "") or "").strip()
                if location_type and location_type not in {"0", "1"}:
                    # Skip entrances/boarding areas etc. to keep manual review focused.
                    continue
                stops.append(
                    StopRecord(
                        feed=path.stem,
                        source_path=str(path),
                        country=inferred_country,
                        stop_id=(row.get("stop_id", "") or "").strip(),
                        stop_name=(row.get("stop_name", "") or "").strip(),
                        lat=lat,
                        lon=lon,
                        stop_code=(row.get("stop_code", "") or "").strip(),
                        parent_station=(row.get("parent_station", "") or "").strip(),
                        location_type=location_type,
                    )
                )
    return stops


def read_stops_from_txt(path: Path, country_hint: Optional[str]) -> List[StopRecord]:
    stops: List[StopRecord] = []
    inferred_country = country_hint or parse_country_hint(path.name) or parse_country_hint(str(path.parent)) or "xx"
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            lat = parse_float(row.get("stop_lat", ""))
            lon = parse_float(row.get("stop_lon", ""))
            if lat is None or lon is None:
                continue
            location_type = (row.get("location_type", "") or "").strip()
            if location_type and location_type not in {"0", "1"}:
                continue
            stops.append(
                StopRecord(
                    feed=path.parent.name if path.name == "stops.txt" else path.stem,
                    source_path=str(path),
                    country=inferred_country,
                    stop_id=(row.get("stop_id", "") or "").strip(),
                    stop_name=(row.get("stop_name", "") or "").strip(),
                    lat=lat,
                    lon=lon,
                    stop_code=(row.get("stop_code", "") or "").strip(),
                    parent_station=(row.get("parent_station", "") or "").strip(),
                    location_type=location_type,
                )
            )
    return stops


def collect_input_files(tokens: Sequence[str]) -> List[Tuple[Path, Optional[str]]]:
    pairs: List[Tuple[Path, Optional[str]]] = []
    for token in tokens:
        explicit_country, raw_path = parse_input_token(token)
        path = Path(raw_path)
        if path.is_file():
            pairs.append((path, explicit_country))
            continue
        if path.is_dir():
            for zip_file in sorted(path.glob("*.zip")):
                pairs.append((zip_file, explicit_country))
            direct_stops = path / "stops.txt"
            if direct_stops.exists():
                pairs.append((direct_stops, explicit_country))
            for nested_stops in sorted(path.glob("*/stops.txt")):
                pairs.append((nested_stops, explicit_country))
            continue
        raise FileNotFoundError(f"Input path not found: {token}")

    # De-duplicate by resolved path.
    seen = set()
    deduped: List[Tuple[Path, Optional[str]]] = []
    for p, c in pairs:
        key = str(p.resolve())
        if key in seen:
            continue
        seen.add(key)
        deduped.append((p, c))
    return deduped


def load_station_map(path: Path) -> List[dict]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise ValueError(f"Station map must be a JSON list: {path}")
    return data


def score_station_match(
    stop: StopRecord,
    station: dict,
    max_distance_m: float,
    prefer_same_country: bool,
) -> Optional[Tuple[float, float, float]]:
    coords = station.get("coords")
    if not isinstance(coords, list) or len(coords) != 2:
        return None
    station_lat = parse_float(str(coords[0]))
    station_lon = parse_float(str(coords[1]))
    if station_lat is None or station_lon is None:
        return None

    rough_max_lat_delta = max_distance_m / 111_000.0
    if abs(stop.lat - station_lat) > rough_max_lat_delta:
        return None

    distance_m = haversine_distance_m(stop.lat, stop.lon, station_lat, station_lon)
    if distance_m > max_distance_m:
        return None

    similarity = name_similarity(stop.stop_name, station.get("name", ""))
    if similarity <= 0.0:
        return None

    distance_score = max(0.0, 1.0 - (distance_m / max_distance_m))
    score = (similarity * 0.7) + (distance_score * 0.3)

    station_country = (station.get("country", "") or "").lower()
    if prefer_same_country and station_country and station_country == stop.country:
        score += 0.05
    return score, similarity, distance_m


def recommendation_for(similarity: float, distance_m: float) -> str:
    if similarity >= 0.93 and distance_m <= 400:
        return "auto_link"
    if similarity >= 0.84 and distance_m <= 1_500:
        return "review_link"
    if similarity >= 0.70 and distance_m <= 2_500:
        return "weak_link"
    return "unmatched"


def build_review_rows(
    stops: Sequence[StopRecord],
    station_map: Sequence[dict],
    min_similarity: float,
    max_distance_m: float,
    prefer_same_country: bool,
    include_unmatched: bool,
    accept_auto: bool,
    limit: Optional[int],
) -> List[dict]:
    rows: List[dict] = []
    total = len(stops) if limit is None else min(len(stops), limit)

    for idx, stop in enumerate(stops[:total], start=1):
        best_station = None
        best_score = -1.0
        best_similarity = 0.0
        best_distance = 0.0

        for station in station_map:
            scored = score_station_match(
                stop=stop,
                station=station,
                max_distance_m=max_distance_m,
                prefer_same_country=prefer_same_country,
            )
            if scored is None:
                continue
            score, similarity, distance_m = scored
            if similarity < min_similarity:
                continue
            if score > best_score:
                best_score = score
                best_station = station
                best_similarity = similarity
                best_distance = distance_m

        if best_station is None:
            recommendation = "unmatched"
            if not include_unmatched:
                continue
            row = {
                "feed": stop.feed,
                "source_path": stop.source_path,
                "country": stop.country,
                "stop_id": stop.stop_id,
                "stop_name": stop.stop_name,
                "stop_lat": f"{stop.lat:.6f}",
                "stop_lon": f"{stop.lon:.6f}",
                "stop_code": stop.stop_code,
                "parent_station": stop.parent_station,
                "location_type": stop.location_type,
                "suggested_uic": "",
                "suggested_station": "",
                "suggested_country": "",
                "name_similarity": "",
                "distance_m": "",
                "confidence": "",
                "recommendation": recommendation,
                "decision": "",
                "decision_station_uic": "",
                "note": "",
            }
            rows.append(row)
            continue

        recommendation = recommendation_for(best_similarity, best_distance)
        conflict_note = ""
        country_gtfs_ids = best_station.get("gtfs_ids", {}) if isinstance(best_station.get("gtfs_ids", {}), dict) else {}
        existing_country_gtfs_id = str(country_gtfs_ids.get(stop.country, "")).strip()
        has_country_conflict = bool(
            existing_country_gtfs_id and existing_country_gtfs_id != stop.stop_id
        )
        if has_country_conflict:
            conflict_note = (
                f"existing_gtfs_ids[{stop.country}]={existing_country_gtfs_id}; "
                f"candidate_stop_id={stop.stop_id}"
            )
            if recommendation == "auto_link":
                recommendation = "review_link"

        decision = ""
        decision_uic = ""
        if accept_auto and recommendation == "auto_link":
            decision = "link"
            decision_uic = str(best_station.get("uic", ""))

        row = {
            "feed": stop.feed,
            "source_path": stop.source_path,
            "country": stop.country,
            "stop_id": stop.stop_id,
            "stop_name": stop.stop_name,
            "stop_lat": f"{stop.lat:.6f}",
            "stop_lon": f"{stop.lon:.6f}",
            "stop_code": stop.stop_code,
            "parent_station": stop.parent_station,
            "location_type": stop.location_type,
            "suggested_uic": str(best_station.get("uic", "")),
            "suggested_station": str(best_station.get("name", "")),
            "suggested_country": str(best_station.get("country", "")).lower(),
            "name_similarity": f"{best_similarity:.3f}",
            "distance_m": f"{best_distance:.1f}",
            "confidence": f"{best_score:.3f}",
            "recommendation": recommendation,
            "decision": decision,
            "decision_station_uic": decision_uic,
            "note": conflict_note,
        }
        rows.append(row)

        if idx % 10_000 == 0:
            print(f"Processed {idx:,} / {total:,} stops...", file=sys.stderr)

    return rows


def write_review_csv(path: Path, rows: Sequence[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=REVIEW_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in REVIEW_COLUMNS})


def read_review_csv(path: Path) -> List[dict]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = []
        for row in reader:
            normalized = {key: row.get(key, "") for key in REVIEW_COLUMNS}
            rows.append(normalized)
    return rows


def summarize_rows(rows: Sequence[dict]) -> Dict[str, int]:
    summary: Dict[str, int] = {}
    for row in rows:
        recommendation = (row.get("recommendation", "") or "unknown").strip() or "unknown"
        summary[recommendation] = summary.get(recommendation, 0) + 1
    return summary


def print_summary(rows: Sequence[dict]) -> None:
    print("")
    print("Review summary:")
    summary = summarize_rows(rows)
    for key in sorted(summary.keys()):
        print(f"  {key:>11}: {summary[key]:,}")
    auto_decisions = sum(1 for row in rows if (row.get("decision", "").strip().lower() == "link"))
    print(f"  {'decisions':>11}: {auto_decisions:,} pre-filled")
    print(f"  {'total':>11}: {len(rows):,}")


def station_lookup_by_uic(station_map: Sequence[dict]) -> Dict[str, dict]:
    lookup = {}
    for station in station_map:
        uic = str(station.get("uic", "")).strip()
        if uic:
            lookup[uic] = station
    return lookup


def pick_manual_station(station_map: Sequence[dict], query: str) -> Optional[str]:
    q = query.strip().lower()
    if not q:
        return None
    matches = [s for s in station_map if q in str(s.get("name", "")).lower()]
    if not matches:
        ranked = sorted(
            station_map,
            key=lambda s: name_similarity(query, str(s.get("name", ""))),
            reverse=True,
        )[:8]
        matches = [s for s in ranked if name_similarity(query, str(s.get("name", ""))) >= 0.45]

    if not matches:
        print("  No station match found for this query.")
        return None

    print("  Choose station:")
    for idx, station in enumerate(matches[:12], start=1):
        print(f"    {idx:2d}) {station.get('name','')} [{station.get('uic','')}] ({station.get('country','')})")
    selection = input("  Number (blank to cancel): ").strip()
    if not selection:
        return None
    if not selection.isdigit():
        print("  Invalid number.")
        return None
    number = int(selection)
    if number < 1 or number > min(len(matches), 12):
        print("  Out of range.")
        return None
    return str(matches[number - 1].get("uic", "")).strip() or None


def interactive_review(rows: List[dict], station_map: Sequence[dict]) -> None:
    pending = [row for row in rows if (row.get("recommendation") or "") != "auto_link"]
    if not pending:
        print("No pending rows for interactive review.")
        return

    print("")
    print("Interactive mode:")
    print("  y = link suggested station")
    print("  m = manually choose station")
    print("  c = create new station entry")
    print("  s = skip for now")
    print("  q = quit review early")

    for idx, row in enumerate(pending, start=1):
        print("")
        print(f"[{idx}/{len(pending)}] {row.get('feed')} | {row.get('stop_id')} | {row.get('stop_name')}")
        print(f"  Country: {row.get('country')}  Coords: {row.get('stop_lat')},{row.get('stop_lon')}")
        recommendation = row.get("recommendation", "unmatched")
        if row.get("suggested_uic"):
            print(
                "  Suggested: "
                f"{row.get('suggested_station')} [{row.get('suggested_uic')}] "
                f"(sim={row.get('name_similarity')}, dist={row.get('distance_m')}m, {recommendation})"
            )
        else:
            print(f"  Suggested: none ({recommendation})")

        while True:
            action = input("  Action [y/m/c/s/q]: ").strip().lower()
            if action == "y":
                suggested_uic = (row.get("suggested_uic") or "").strip()
                if not suggested_uic:
                    print("  No suggested station for this row.")
                    continue
                row["decision"] = "link"
                row["decision_station_uic"] = suggested_uic
                break
            if action == "m":
                query = input("  Search station (blank = stop name): ").strip() or row.get("stop_name", "")
                chosen_uic = pick_manual_station(station_map, query)
                if not chosen_uic:
                    continue
                row["decision"] = "link"
                row["decision_station_uic"] = chosen_uic
                break
            if action == "c":
                default_uic = f"TEMP-{(row.get('country') or 'xx').upper()}-{row.get('stop_id')}"
                entered_uic = input(f"  UIC (blank = {default_uic}): ").strip() or default_uic
                row["decision"] = "create"
                row["decision_station_uic"] = entered_uic
                break
            if action == "s":
                row["decision"] = ""
                row["decision_station_uic"] = ""
                break
            if action == "q":
                print("Stopped interactive review early.")
                return
            print("  Invalid action.")


def generate_temp_uic(country: str, stop_id: str, existing_uics: set[str]) -> str:
    base = f"TEMP-{country.upper()}-{stop_id}"
    if base not in existing_uics:
        return base
    i = 2
    while True:
        candidate = f"{base}-{i}"
        if candidate not in existing_uics:
            return candidate
        i += 1


def apply_decisions(
    review_rows: Sequence[dict],
    station_map: List[dict],
    overwrite_gtfs_id: bool,
    default_transfer_minutes: int,
) -> Dict[str, int]:
    stats = {
        "linked": 0,
        "created": 0,
        "skipped": 0,
        "conflicts": 0,
        "errors": 0,
    }

    by_uic = station_lookup_by_uic(station_map)
    existing_uics = set(by_uic.keys())

    for row in review_rows:
        decision = (row.get("decision", "") or "").strip().lower()
        if decision in {"", "skip"}:
            stats["skipped"] += 1
            continue

        country = (row.get("country", "") or "").strip().lower()
        stop_id = (row.get("stop_id", "") or "").strip()
        stop_name = (row.get("stop_name", "") or "").strip()
        if not country or not stop_id:
            stats["errors"] += 1
            continue

        if decision == "link":
            uic = (row.get("decision_station_uic") or row.get("suggested_uic") or "").strip()
            if not uic:
                stats["errors"] += 1
                continue
            station = by_uic.get(uic)
            if not station:
                stats["errors"] += 1
                continue
            gtfs_ids = station.setdefault("gtfs_ids", {})
            current = gtfs_ids.get(country)
            if current and current != stop_id and not overwrite_gtfs_id:
                stats["conflicts"] += 1
                continue
            gtfs_ids[country] = stop_id
            stats["linked"] += 1
            continue

        if decision == "create":
            chosen_uic = (row.get("decision_station_uic", "") or "").strip()
            if chosen_uic and chosen_uic in by_uic:
                stats["errors"] += 1
                continue
            uic = chosen_uic or generate_temp_uic(country, stop_id, existing_uics)
            existing_uics.add(uic)

            lat = parse_float(row.get("stop_lat", ""))
            lon = parse_float(row.get("stop_lon", ""))
            if lat is None or lon is None:
                stats["errors"] += 1
                continue

            station = {
                "name": stop_name or f"Station {stop_id}",
                "uic": uic,
                "country": country.upper(),
                "gtfs_ids": {country: stop_id},
                "ojp_ref": "",
                "coords": [round(lat, 6), round(lon, 6)],
                "min_transfer_minutes": default_transfer_minutes,
                "type": "stop",
            }
            station_map.append(station)
            by_uic[uic] = station
            stats["created"] += 1
            continue

        stats["errors"] += 1

    return stats


def save_station_map(path: Path, station_map: Sequence[dict]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(list(station_map), handle, indent=2, ensure_ascii=False)
        handle.write("\n")


def sort_station_map_by_name(station_map: List[dict]) -> None:
    station_map.sort(key=lambda item: normalize_name(str(item.get("name", ""))))


def load_stops(inputs: Sequence[Tuple[Path, Optional[str]]]) -> List[StopRecord]:
    all_stops: List[StopRecord] = []
    for path, country_hint in inputs:
        if path.suffix.lower() == ".zip":
            stops = read_stops_from_zip(path, country_hint)
        else:
            stops = read_stops_from_txt(path, country_hint)
        print(f"Loaded {len(stops):,} stops from {path}")
        all_stops.extend(stops)
    return all_stops


def run_suggest(args: argparse.Namespace) -> int:
    inputs = collect_input_files(args.inputs)
    if not inputs:
        print("No GTFS inputs found.", file=sys.stderr)
        return 1
    station_map = load_station_map(Path(args.station_map))
    stops = load_stops(inputs)
    rows = build_review_rows(
        stops=stops,
        station_map=station_map,
        min_similarity=args.min_similarity,
        max_distance_m=args.max_distance_m,
        prefer_same_country=not args.no_country_preference,
        include_unmatched=args.include_unmatched,
        accept_auto=args.accept_auto,
        limit=args.limit,
    )
    out_path = Path(args.out_csv)
    write_review_csv(out_path, rows)
    print_summary(rows)
    print(f"\nWrote review CSV: {out_path}")
    return 0


def run_review(args: argparse.Namespace) -> int:
    inputs = collect_input_files(args.inputs)
    if not inputs:
        print("No GTFS inputs found.", file=sys.stderr)
        return 1
    station_map = load_station_map(Path(args.station_map))
    stops = load_stops(inputs)
    rows = build_review_rows(
        stops=stops,
        station_map=station_map,
        min_similarity=args.min_similarity,
        max_distance_m=args.max_distance_m,
        prefer_same_country=not args.no_country_preference,
        include_unmatched=args.include_unmatched,
        accept_auto=args.accept_auto,
        limit=args.limit,
    )
    interactive_review(rows, station_map)
    out_path = Path(args.out_csv)
    write_review_csv(out_path, rows)
    print_summary(rows)
    print(f"\nWrote reviewed CSV: {out_path}")
    return 0


def run_apply(args: argparse.Namespace) -> int:
    station_map_path = Path(args.station_map)
    review_path = Path(args.review_csv)
    if not station_map_path.exists():
        print(f"station_map not found: {station_map_path}", file=sys.stderr)
        return 1
    if not review_path.exists():
        print(f"review csv not found: {review_path}", file=sys.stderr)
        return 1

    station_map = load_station_map(station_map_path)
    review_rows = read_review_csv(review_path)
    stats = apply_decisions(
        review_rows=review_rows,
        station_map=station_map,
        overwrite_gtfs_id=args.overwrite_gtfs_id,
        default_transfer_minutes=args.default_transfer_minutes,
    )

    if args.sort_by_name:
        sort_station_map_by_name(station_map)

    if args.dry_run:
        print("Dry-run mode: no files changed.")
    else:
        if args.backup:
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            backup_path = station_map_path.with_suffix(station_map_path.suffix + f".bak.{timestamp}")
            shutil.copy2(station_map_path, backup_path)
            print(f"Backup created: {backup_path}")
        save_station_map(station_map_path, station_map)
        print(f"Updated station map: {station_map_path}")

    print("")
    print("Apply summary:")
    for key in ("linked", "created", "conflicts", "errors", "skipped"):
        print(f"  {key:>9}: {stats[key]:,}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="GTFS station dedup helper for manual station_map curation."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def add_match_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("inputs", nargs="+", help="GTFS .zip, stops.txt, or a directory containing those files. Optional country hint syntax: de:/path/feed.zip")
        p.add_argument("--station-map", default="data/station_map.json", help="Path to station_map.json")
        p.add_argument("--out-csv", default="data/station_dedup_review.csv", help="Output CSV path")
        p.add_argument("--min-similarity", type=float, default=0.72, help="Minimum name similarity [0..1] for suggestions")
        p.add_argument("--max-distance-m", type=float, default=2500.0, help="Maximum distance for candidate station matching")
        p.add_argument("--include-unmatched", action="store_true", help="Include stops without a suggestion in review CSV")
        p.add_argument("--accept-auto", action="store_true", help="Pre-fill decision=link for high confidence auto_link suggestions")
        p.add_argument("--limit", type=int, default=None, help="Only process first N stops")
        p.add_argument("--no-country-preference", action="store_true", help="Do not boost same-country candidates")

    suggest = sub.add_parser("suggest", help="Generate suggestion CSV (non-interactive)")
    add_match_args(suggest)
    suggest.set_defaults(handler=run_suggest)

    review = sub.add_parser("review", help="Generate suggestions and review interactively")
    add_match_args(review)
    review.set_defaults(handler=run_review)

    apply = sub.add_parser("apply", help="Apply reviewed decisions to station_map.json")
    apply.add_argument("review_csv", help="Review CSV path generated by suggest/review")
    apply.add_argument("--station-map", default="data/station_map.json", help="Path to station_map.json")
    apply.add_argument("--dry-run", action="store_true", help="Compute changes but do not write files")
    apply.add_argument("--backup", action="store_true", help="Create station_map.json backup before writing")
    apply.add_argument("--overwrite-gtfs-id", action="store_true", help="Allow replacing an existing gtfs_ids[country] mapping")
    apply.add_argument("--default-transfer-minutes", type=int, default=5, help="Default min_transfer_minutes for newly created stations")
    apply.add_argument("--sort-by-name", action="store_true", help="Sort station_map entries by station name before writing")
    apply.set_defaults(handler=run_apply)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return int(args.handler(args))
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted by user.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
