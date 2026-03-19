#!/usr/bin/env python3
"""Extract compact QA route/adjacency context from zipped NeTEx into CSV files."""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import hashlib
import json
import math
import os
import sqlite3
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict, cast

import lxml.etree as ET

MAX_XML_ENTRIES_TO_SCAN = 120_000
MAX_XML_ENTRY_BYTES = 1_000_000_000  # 1GB uncompressed guardrail
PEEK_BYTES = 32768
CONTENT_PEEK_THRESHOLD = 500
FLUSH_PATTERN_BATCH_LIMIT = 10_000
DEFAULT_EXTRACT_MAX_WORKERS = 2
MIN_ENTRIES_FOR_PARALLEL_EXTRACT = 1_000
MIN_SYSTEM_MEMORY_RESERVE_BYTES = 8 * 1024 * 1024 * 1024
ESTIMATED_EXTRACT_WORKER_MEMORY_BYTES = 2 * 1024 * 1024 * 1024
MIN_SWAP_FREE_BYTES_FOR_PARALLEL = 2 * 1024 * 1024 * 1024


def local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def clean_text(value: str | None) -> str:
    return (value or "").strip()


def child_text(elem: ET.Element, name: str) -> str:
    for child in elem:
        if local_name(child.tag) == name:
            return clean_text(child.text)
    return ""


def child_ref(elem: ET.Element, name: str) -> str:
    for child in elem:
        if local_name(child.tag) == name:
            return clean_text(child.attrib.get("ref"))
    return ""


def parse_int(value: str, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def iter_relevant_elements(handle, target_tags: set[str]):
    context = ET.iterparse(
        handle,
        events=("end",),
        tag=tuple(f"{{*}}{tag}" for tag in target_tags),
        huge_tree=True,
        recover=True,
    )

    for _event, elem in context:
        yield local_name(elem.tag), elem
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]


def candidate_xml_entries(archive: zipfile.ZipFile, zip_path: Path) -> list[str]:
    xml_entries = sorted(
        name for name in archive.namelist() if name.lower().endswith(".xml")
    )
    if not xml_entries:
        raise RuntimeError(f"No XML entries found in archive: {zip_path}")

    print(
        f"[netex-extract-qa-network] INFO: Archive contains {len(xml_entries)} XML entries.",
        file=sys.stderr,
    )

    def is_relevant(name: str) -> bool:
        lowered = Path(name).name.lower()
        if any(
            token in lowered
            for token in ("servicecalendar", "_common_", "_resource_", "_site_")
        ):
            return False
        if (
            "timetable" in lowered
            or "journey" in lowered
            or "offer" in lowered
            or "_line_" in lowered
            or lowered.endswith("_line.xml")
        ):
            return True
        return "_service_" in lowered or lowered.endswith("_service.xml")

    preferred = [name for name in xml_entries if is_relevant(name)]
    if preferred:
        print(
            (
                "[netex-extract-qa-network] INFO: "
                f"Found {len(preferred)} candidate entries by filename."
            ),
            file=sys.stderr,
        )
        selected = preferred
    else:
        selected = xml_entries
        if len(selected) > CONTENT_PEEK_THRESHOLD:
            print(
                (
                    "[netex-extract-qa-network] INFO: "
                    f"Peeking into {len(selected)} XML entries to find QA context tags..."
                ),
                file=sys.stderr,
            )
            qa_entries: list[str] = []
            for index, name in enumerate(selected):
                if entry_contains_qa_context(archive, name):
                    qa_entries.append(name)
                if (index + 1) % 5000 == 0:
                    print(
                        (
                            "[netex-extract-qa-network] INFO: "
                            f"Peeked {index + 1}/{len(selected)} entries, "
                            f"found {len(qa_entries)} QA candidates so far..."
                        ),
                        file=sys.stderr,
                    )

            if qa_entries:
                print(
                    (
                        "[netex-extract-qa-network] INFO: "
                        f"Content peek found {len(qa_entries)} QA candidate entries."
                    ),
                    file=sys.stderr,
                )
                selected = qa_entries
            else:
                print(
                    (
                        "[netex-extract-qa-network] WARN: "
                        "Content peek found no QA candidate entries; falling back to filename filter."
                    ),
                    file=sys.stderr,
                )

    if len(selected) > MAX_XML_ENTRIES_TO_SCAN:
        raise RuntimeError(
            f"Too many XML entries to scan ({len(selected)}). "
            f"Limit is {MAX_XML_ENTRIES_TO_SCAN}."
        )

    return selected


def entry_contains_qa_context(archive: zipfile.ZipFile, entry: str) -> bool:
    try:
        with archive.open(entry) as handle:
            head = handle.read(PEEK_BYTES)
    except Exception:
        return False

    return any(
        token in head
        for token in (
            b"ServiceJourneyPattern",
            b"JourneyPattern",
            b"TemplateServiceJourney",
            b"ServiceJourney",
            b"PassengerStopAssignment",
            b"ScheduledStopPointRef",
            b"<Line",
        )
    )


def parse_line_meta(line_elem: ET.Element) -> tuple[str, dict[str, str]] | None:
    line_id = clean_text(line_elem.attrib.get("id"))
    if not line_id:
        return None

    return (
        line_id,
        {
            "name": child_text(line_elem, "Name"),
            "short_name": child_text(line_elem, "ShortName"),
            "public_code": child_text(line_elem, "PublicCode"),
            "transport_mode": child_text(line_elem, "TransportMode"),
        },
    )


def parse_passenger_stop_assignment(
    assignment_elem: ET.Element,
) -> tuple[str, str] | None:
    scheduled_ref = child_ref(assignment_elem, "ScheduledStopPointRef")
    quay_ref = child_ref(assignment_elem, "QuayRef")
    stop_point_ref = child_ref(assignment_elem, "StopPointRef")
    stop_place_ref = child_ref(assignment_elem, "StopPlaceRef")
    if not scheduled_ref:
        return None
    return scheduled_ref, quay_ref or stop_point_ref or stop_place_ref


def parse_journey_pattern(
    pattern_elem: ET.Element,
) -> tuple[str, dict[str, object]] | None:
    pattern_id = clean_text(pattern_elem.attrib.get("id"))
    if not pattern_id:
        return None

    tag = local_name(pattern_elem.tag)
    line_ref = ""
    route_view = None
    for candidate in pattern_elem.iter():
        if local_name(candidate.tag) == "RouteView":
            route_view = candidate
            break
    if route_view is not None:
        line_ref = child_ref(route_view, "LineRef")
    if not line_ref:
        line_ref = child_ref(pattern_elem, "LineRef")
    if not line_ref:
        line_ref = child_ref(pattern_elem, "RouteRef")

    points: list[tuple[int, str]] = []
    for point in pattern_elem.iter():
        point_tag = local_name(point.tag)
        if tag in {"ServiceJourneyPattern", "JourneyPattern"}:
            if point_tag != "StopPointInJourneyPattern":
                continue
        elif tag in {"ServiceJourney", "TemplateServiceJourney"}:
            if point_tag != "Call":
                continue
        else:
            continue

        order = parse_int(clean_text(point.attrib.get("order")), 0)
        if order <= 0:
            continue
        scheduled_ref = child_ref(point, "ScheduledStopPointRef")
        if not scheduled_ref:
            scheduled_ref = child_ref(point, "StopPointRef")
        if not scheduled_ref:
            scheduled_ref = child_ref(point, "StopPlaceRef")
        if not scheduled_ref:
            continue
        points.append((order, scheduled_ref))

    if not points:
        return None

    points.sort(key=lambda item: item[0])
    scheduled_stop_refs = [scheduled_ref for _, scheduled_ref in points]
    return (
        pattern_id,
        {
            "tag": tag,
            "line_ref": line_ref,
            "scheduled_stop_refs": scheduled_stop_refs,
            "transport_mode": child_text(pattern_elem, "TransportMode"),
            "signature": json.dumps(
                {
                    "tag": tag,
                    "line_ref": line_ref,
                    "scheduled_stop_refs": scheduled_stop_refs,
                },
                ensure_ascii=True,
                separators=(",", ":"),
            ),
        },
    )


def resolve_transport_mode(
    pattern_transport_mode: str, line_info: dict[str, str]
) -> str:
    return (pattern_transport_mode or line_info.get("transport_mode", "") or "").strip()


def resolve_route_label(pattern_id: str, line_info: dict[str, str]) -> str:
    return (
        line_info.get("short_name")
        or line_info.get("public_code")
        or line_info.get("name")
        or pattern_id
        or "unlabeled"
    ).strip() or "unlabeled"


def resolve_stop_place_ref(
    scheduled_stop_ref: str, stop_place_ref: str
) -> tuple[str, bool]:
    stop_place_ref = clean_text(stop_place_ref)
    if stop_place_ref:
        return stop_place_ref, False
    scheduled_stop_ref = clean_text(scheduled_stop_ref)
    if not scheduled_stop_ref:
        return "", False
    return f"synthetic:{scheduled_stop_ref}", True


@dataclass
class Summary:
    xml_entries_scanned: int = 0
    patterns_found: int = 0
    route_rows_written: int = 0
    adjacency_rows_written: int = 0
    patterns_missing_points: int = 0
    synthetic_stop_place_resolutions: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "xmlEntriesScanned": self.xml_entries_scanned,
            "patternsFound": self.patterns_found,
            "routeRowsWritten": self.route_rows_written,
            "adjacencyRowsWritten": self.adjacency_rows_written,
            "patternsMissingPoints": self.patterns_missing_points,
            "syntheticStopPlaceResolutions": self.synthetic_stop_place_resolutions,
        }


class WorkerChunkResult(TypedDict):
    db_path: str
    xml_entries_scanned: int
    parse_fail_missing_points: int
    first_entry_index: int
    last_entry_index: int


def signature_hash(signature: str) -> str:
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()


def read_meminfo_bytes(field_name: str) -> int | None:
    meminfo_path = Path("/proc/meminfo")
    if not meminfo_path.is_file():
        return None

    try:
        for line in meminfo_path.read_text(encoding="utf-8").splitlines():
            if not line.startswith(f"{field_name}:"):
                continue
            parts = line.split()
            if len(parts) < 2:
                return None
            return int(parts[1]) * 1024
    except Exception:
        return None

    return None


def read_mem_available_bytes() -> int | None:
    return read_meminfo_bytes("MemAvailable")


def read_swap_free_bytes() -> int | None:
    return read_meminfo_bytes("SwapFree")


def unsafe_parallelism_allowed() -> bool:
    return str(
        os.environ.get("QA_NETWORK_EXTRACT_ALLOW_UNSAFE_PARALLELISM", "")
    ).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


class PatternFactStore:
    """Spill per-pattern facts to a temporary SQLite database."""

    def __init__(self, *, dedupe_locally: bool) -> None:
        tmpdir = os.environ.get("TMPDIR") or None
        handle = tempfile.NamedTemporaryFile(
            prefix="netex-qa-patterns-",
            suffix=".sqlite3",
            dir=tmpdir,
            delete=False,
        )
        self._db_path = Path(handle.name)
        handle.close()
        self._conn = sqlite3.connect(str(self._db_path))
        self._dedupe_locally = dedupe_locally
        self._seen_signatures: set[str] = set()
        self._pattern_rows: list[tuple[str, int, int, int]] = []
        self._route_rows: list[tuple[str, int, str, str, str]] = []
        self._adjacency_rows: list[tuple[str, int, str, str]] = []
        self._configure()
        self._create_tables()

    def _configure(self) -> None:
        self._conn.execute("PRAGMA journal_mode=OFF")
        self._conn.execute("PRAGMA synchronous=OFF")
        self._conn.execute("PRAGMA temp_store=FILE")
        self._conn.execute("PRAGMA cache_size=-20000")

    def _create_tables(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS pattern_signatures (
              signature_hash TEXT NOT NULL,
              first_entry_index INTEGER NOT NULL,
              missing_points INTEGER NOT NULL DEFAULT 0,
              synthetic_stop_place_resolutions INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY (signature_hash, first_entry_index)
            );

            CREATE TABLE IF NOT EXISTS route_pattern_hits (
              signature_hash TEXT NOT NULL,
              first_entry_index INTEGER NOT NULL,
              provider_stop_place_ref TEXT NOT NULL,
              route_label TEXT NOT NULL,
              transport_mode TEXT NOT NULL,
              PRIMARY KEY (
                signature_hash,
                first_entry_index,
                provider_stop_place_ref,
                route_label,
                transport_mode
              )
            );

            CREATE TABLE IF NOT EXISTS adjacency_pattern_hits (
              signature_hash TEXT NOT NULL,
              first_entry_index INTEGER NOT NULL,
              from_provider_stop_place_ref TEXT NOT NULL,
              to_provider_stop_place_ref TEXT NOT NULL,
              PRIMARY KEY (
                signature_hash,
                first_entry_index,
                from_provider_stop_place_ref,
                to_provider_stop_place_ref
              )
            );
            """
        )
        self._conn.commit()

    def _flush_patterns(self) -> None:
        if not self._pattern_rows:
            return
        with self._conn:
            self._conn.executemany(
                """
                INSERT OR IGNORE INTO pattern_signatures (
                  signature_hash,
                  first_entry_index,
                  missing_points,
                  synthetic_stop_place_resolutions
                ) VALUES (?, ?, ?, ?)
                """,
                self._pattern_rows,
            )
        self._pattern_rows.clear()

    def _flush_routes(self) -> None:
        if not self._route_rows:
            return
        with self._conn:
            self._conn.executemany(
                """
                INSERT OR IGNORE INTO route_pattern_hits (
                  signature_hash,
                  first_entry_index,
                  provider_stop_place_ref,
                  route_label,
                  transport_mode
                ) VALUES (?, ?, ?, ?, ?)
                """,
                self._route_rows,
            )
        self._route_rows.clear()

    def _flush_adjacencies(self) -> None:
        if not self._adjacency_rows:
            return
        with self._conn:
            self._conn.executemany(
                """
                INSERT OR IGNORE INTO adjacency_pattern_hits (
                  signature_hash,
                  first_entry_index,
                  from_provider_stop_place_ref,
                  to_provider_stop_place_ref
                ) VALUES (?, ?, ?, ?)
                """,
                self._adjacency_rows,
            )
        self._adjacency_rows.clear()

    def _maybe_flush(self) -> None:
        if (
            len(self._pattern_rows) + len(self._route_rows) + len(self._adjacency_rows)
            >= FLUSH_PATTERN_BATCH_LIMIT
        ):
            self.flush_all()

    def flush_all(self) -> None:
        self._flush_patterns()
        self._flush_routes()
        self._flush_adjacencies()

    def record_pattern(
        self,
        *,
        signature_hash_value: str,
        first_entry_index: int,
        missing_points: bool,
        synthetic_stop_place_resolutions: int,
        route_signals: set[tuple[str, str, str]],
        adjacency_signals: set[tuple[str, str]],
    ) -> bool:
        if self._dedupe_locally and signature_hash_value in self._seen_signatures:
            return False

        if self._dedupe_locally:
            self._seen_signatures.add(signature_hash_value)

        self._pattern_rows.append(
            (
                signature_hash_value,
                first_entry_index,
                1 if missing_points else 0,
                synthetic_stop_place_resolutions,
            )
        )
        self._route_rows.extend(
            (
                signature_hash_value,
                first_entry_index,
                stop_place_ref,
                route_label,
                transport_mode,
            )
            for stop_place_ref, route_label, transport_mode in route_signals
        )
        self._adjacency_rows.extend(
            (
                signature_hash_value,
                first_entry_index,
                from_ref,
                to_ref,
            )
            for from_ref, to_ref in adjacency_signals
        )
        self._maybe_flush()
        return True

    def merge_from(self, other_db_path: Path) -> None:
        self.flush_all()
        attach_name = f"merge_src_{abs(hash(str(other_db_path))) % 1_000_000_000}"
        self._conn.execute(f"ATTACH DATABASE ? AS {attach_name}", (str(other_db_path),))
        try:
            with self._conn:
                self._conn.execute(
                    f"""
                    INSERT OR IGNORE INTO pattern_signatures (
                      signature_hash,
                      first_entry_index,
                      missing_points,
                      synthetic_stop_place_resolutions
                    )
                    SELECT
                      signature_hash,
                      first_entry_index,
                      missing_points,
                      synthetic_stop_place_resolutions
                    FROM {attach_name}.pattern_signatures
                    """
                )
                self._conn.execute(
                    f"""
                    INSERT OR IGNORE INTO route_pattern_hits (
                      signature_hash,
                      first_entry_index,
                      provider_stop_place_ref,
                      route_label,
                      transport_mode
                    )
                    SELECT
                      signature_hash,
                      first_entry_index,
                      provider_stop_place_ref,
                      route_label,
                      transport_mode
                    FROM {attach_name}.route_pattern_hits
                    """
                )
                self._conn.execute(
                    f"""
                    INSERT OR IGNORE INTO adjacency_pattern_hits (
                      signature_hash,
                      first_entry_index,
                      from_provider_stop_place_ref,
                      to_provider_stop_place_ref
                    )
                    SELECT
                      signature_hash,
                      first_entry_index,
                      from_provider_stop_place_ref,
                      to_provider_stop_place_ref
                    FROM {attach_name}.adjacency_pattern_hits
                    """
                )
        finally:
            self._conn.execute(f"DETACH DATABASE {attach_name}")

    def iter_routes(self) -> sqlite3.Cursor:
        self.flush_all()
        return self._conn.execute(
            """
            WITH canonical_patterns AS (
              SELECT
                signature_hash,
                MIN(first_entry_index) AS first_entry_index
              FROM pattern_signatures
              WHERE missing_points = 0
              GROUP BY signature_hash
            )
            SELECT
              route.provider_stop_place_ref,
              route.route_label,
              route.transport_mode,
              COUNT(*) AS pattern_hits
            FROM route_pattern_hits route
            JOIN canonical_patterns canonical
              ON canonical.signature_hash = route.signature_hash
             AND canonical.first_entry_index = route.first_entry_index
            GROUP BY
              route.provider_stop_place_ref,
              route.route_label,
              route.transport_mode
            ORDER BY provider_stop_place_ref, route_label, transport_mode
            """
        )

    def iter_adjacencies(self) -> sqlite3.Cursor:
        self.flush_all()
        return self._conn.execute(
            """
            WITH canonical_patterns AS (
              SELECT
                signature_hash,
                MIN(first_entry_index) AS first_entry_index
              FROM pattern_signatures
              WHERE missing_points = 0
              GROUP BY signature_hash
            )
            SELECT
              adjacency.from_provider_stop_place_ref,
              adjacency.to_provider_stop_place_ref,
              COUNT(*) AS pattern_hits
            FROM adjacency_pattern_hits adjacency
            JOIN canonical_patterns canonical
              ON canonical.signature_hash = adjacency.signature_hash
             AND canonical.first_entry_index = adjacency.first_entry_index
            GROUP BY
              adjacency.from_provider_stop_place_ref,
              adjacency.to_provider_stop_place_ref
            ORDER BY from_provider_stop_place_ref, to_provider_stop_place_ref
            """
        )

    def build_summary(
        self, *, xml_entries_scanned: int, parse_fail_missing_points: int
    ) -> Summary:
        self.flush_all()
        row = self._conn.execute(
            """
            WITH canonical_patterns AS (
              SELECT
                signature_hash,
                MIN(first_entry_index) AS first_entry_index
              FROM pattern_signatures
              GROUP BY signature_hash
            )
            SELECT
              COUNT(*) FILTER (WHERE pattern.missing_points = 0) AS patterns_found,
              COUNT(*) FILTER (WHERE pattern.missing_points = 1) AS deduped_missing_points,
              COALESCE(SUM(
                CASE
                  WHEN pattern.missing_points = 0
                    THEN pattern.synthetic_stop_place_resolutions
                  ELSE 0
                END
              ), 0) AS synthetic_stop_place_resolutions
            FROM canonical_patterns canonical
            JOIN pattern_signatures pattern
              ON pattern.signature_hash = canonical.signature_hash
             AND pattern.first_entry_index = canonical.first_entry_index
            """
        ).fetchone()
        return Summary(
            xml_entries_scanned=xml_entries_scanned,
            patterns_found=int(row[0] or 0),
            patterns_missing_points=parse_fail_missing_points + int(row[1] or 0),
            synthetic_stop_place_resolutions=int(row[2] or 0),
        )

    def close(self) -> None:
        try:
            self._conn.close()
        finally:
            try:
                self._db_path.unlink(missing_ok=True)
            except OSError:
                pass

    @property
    def db_path(self) -> Path:
        return self._db_path


def scan_entry(
    *,
    archive: zipfile.ZipFile,
    entry: str,
    entry_index: int,
    parse_fail_missing_points: list[int],
    pattern_store: PatternFactStore,
) -> None:
    line_meta_by_id: dict[str, dict[str, str]] = {}
    stop_place_by_scheduled_ref: dict[str, str] = {}
    pending_patterns: list[tuple[str, dict[str, object]]] = []

    with archive.open(entry) as handle:
        for tag, elem in iter_relevant_elements(
            handle,
            {
                "Line",
                "PassengerStopAssignment",
                "ServiceJourneyPattern",
                "JourneyPattern",
                "ServiceJourney",
                "TemplateServiceJourney",
            },
        ):
            if tag == "Line":
                parsed = parse_line_meta(elem)
                if parsed:
                    line_id, payload = parsed
                    line_meta_by_id[line_id] = payload
                continue

            if tag == "PassengerStopAssignment":
                parsed = parse_passenger_stop_assignment(elem)
                if parsed:
                    scheduled_ref, stop_place_ref = parsed
                    if scheduled_ref:
                        stop_place_by_scheduled_ref[scheduled_ref] = stop_place_ref
                continue

            parsed = parse_journey_pattern(elem)
            if not parsed:
                parse_fail_missing_points[0] += 1
                continue
            pending_patterns.append(parsed)

    for pattern_id, pattern_info in pending_patterns:
        pattern_signature = str(pattern_info.get("signature") or "")
        pattern_signature_hash = signature_hash(pattern_signature)

        line_ref = str(pattern_info.get("line_ref") or "")
        line_info = line_meta_by_id.get(line_ref, {})
        route_label = resolve_route_label(pattern_id, line_info)
        transport_mode = resolve_transport_mode(
            str(pattern_info.get("transport_mode") or ""),
            line_info,
        )

        resolved_stop_places: list[str] = []
        route_signals: set[tuple[str, str, str]] = set()
        adjacency_signals: set[tuple[str, str]] = set()
        synthetic_stop_place_resolutions = 0
        scheduled_stop_refs = cast(
            list[str], pattern_info.get("scheduled_stop_refs", [])
        )
        for scheduled_stop_ref in scheduled_stop_refs:
            resolved_stop_place_ref = stop_place_by_scheduled_ref.get(
                str(scheduled_stop_ref),
                "",
            )
            stop_place_ref, used_synthetic = resolve_stop_place_ref(
                str(scheduled_stop_ref),
                resolved_stop_place_ref,
            )
            if not stop_place_ref:
                continue
            if used_synthetic:
                synthetic_stop_place_resolutions += 1
            route_signals.add((stop_place_ref, route_label, transport_mode))
            if not resolved_stop_places or resolved_stop_places[-1] != stop_place_ref:
                resolved_stop_places.append(stop_place_ref)

        if not resolved_stop_places:
            pattern_store.record_pattern(
                signature_hash_value=pattern_signature_hash,
                first_entry_index=entry_index,
                missing_points=True,
                synthetic_stop_place_resolutions=synthetic_stop_place_resolutions,
                route_signals=set(),
                adjacency_signals=set(),
            )
            continue

        for from_ref, to_ref in zip(
            resolved_stop_places,
            resolved_stop_places[1:],
            strict=False,
        ):
            if from_ref and to_ref and from_ref != to_ref:
                adjacency_signals.add((from_ref, to_ref))

        pattern_store.record_pattern(
            signature_hash_value=pattern_signature_hash,
            first_entry_index=entry_index,
            missing_points=False,
            synthetic_stop_place_resolutions=synthetic_stop_place_resolutions,
            route_signals=route_signals,
            adjacency_signals=adjacency_signals,
        )


def resolve_worker_count(requested_workers: int, entry_count: int) -> int:
    if requested_workers > 0:
        desired_workers = requested_workers
    else:
        if entry_count < MIN_ENTRIES_FOR_PARALLEL_EXTRACT:
            return 1
        cpu_count = os.cpu_count() or 1
        desired_workers = min(DEFAULT_EXTRACT_MAX_WORKERS, cpu_count)

    capped_workers = min(max(1, desired_workers), max(1, entry_count))
    if capped_workers <= 1:
        return 1

    if unsafe_parallelism_allowed():
        return capped_workers

    mem_available_bytes = read_mem_available_bytes()
    if mem_available_bytes is None:
        mem_safe_workers = capped_workers
    else:
        mem_safe_workers = max(
            1,
            min(
                capped_workers,
                int(
                    1
                    + max(
                        0,
                        (mem_available_bytes - MIN_SYSTEM_MEMORY_RESERVE_BYTES)
                        // ESTIMATED_EXTRACT_WORKER_MEMORY_BYTES,
                    )
                ),
            ),
        )

    swap_free_bytes = read_swap_free_bytes()
    if (
        swap_free_bytes is not None
        and swap_free_bytes < MIN_SWAP_FREE_BYTES_FOR_PARALLEL
    ):
        return 1

    return mem_safe_workers


def build_entry_chunks(
    entries: list[str], worker_count: int
) -> list[list[tuple[int, str]]]:
    if worker_count <= 1 or len(entries) <= 1:
        return [[(index, entry) for index, entry in enumerate(entries)]]

    chunk_size = max(1, math.ceil(len(entries) / worker_count))
    chunks: list[list[tuple[int, str]]] = []
    for start_index in range(0, len(entries), chunk_size):
        chunk_entries = [
            (entry_index, entries[entry_index])
            for entry_index in range(
                start_index, min(len(entries), start_index + chunk_size)
            )
        ]
        if chunk_entries:
            chunks.append(chunk_entries)
    return chunks


def extract_entry_chunk(
    *,
    zip_path: str,
    chunk_entries: list[tuple[int, str]],
) -> WorkerChunkResult:
    parse_fail_missing_points = [0]
    pattern_store = PatternFactStore(dedupe_locally=True)

    try:
        with zipfile.ZipFile(zip_path) as archive:
            for entry_index, entry in chunk_entries:
                entry_info = archive.getinfo(entry)
                if entry_info.file_size > MAX_XML_ENTRY_BYTES:
                    raise RuntimeError(
                        f"XML entry '{entry}' exceeds safety limit ({entry_info.file_size} bytes)"
                    )
                try:
                    scan_entry(
                        archive=archive,
                        entry=entry,
                        entry_index=entry_index,
                        parse_fail_missing_points=parse_fail_missing_points,
                        pattern_store=pattern_store,
                    )
                except ET.ParseError as exc:
                    raise RuntimeError(
                        f"NeTEx XML parse error in entry '{entry}': {exc}"
                    ) from exc

        pattern_store.flush_all()
        return {
            "db_path": str(pattern_store.db_path),
            "xml_entries_scanned": len(chunk_entries),
            "parse_fail_missing_points": parse_fail_missing_points[0],
            "first_entry_index": chunk_entries[0][0] if chunk_entries else 0,
            "last_entry_index": chunk_entries[-1][0] if chunk_entries else 0,
        }
    except Exception:
        pattern_store.close()
        raise


def extract_entries_parallel(
    *,
    zip_path: Path,
    entries: list[str],
    worker_count: int,
) -> list[WorkerChunkResult]:
    chunk_entries = build_entry_chunks(entries, worker_count)
    if len(chunk_entries) == 1:
        only_chunk = chunk_entries[0]
        for entry_index, entry in only_chunk:
            if entry_index == 0 or (entry_index + 1) % 100 == 0:
                print(
                    f"[netex-extract-qa-network] INFO: Scanning entry {entry_index + 1}/{len(entries)}: {Path(entry).name}",
                    file=sys.stderr,
                )
        return [
            extract_entry_chunk(
                zip_path=str(zip_path),
                chunk_entries=only_chunk,
            )
        ]

    print(
        (
            "[netex-extract-qa-network] INFO: "
            f"Parallel extract enabled with {len(chunk_entries)} workers."
        ),
        file=sys.stderr,
    )

    results: list[WorkerChunkResult] = []
    with concurrent.futures.ProcessPoolExecutor(
        max_workers=len(chunk_entries)
    ) as executor:
        future_map = {
            executor.submit(
                extract_entry_chunk,
                zip_path=str(zip_path),
                chunk_entries=chunk,
            ): chunk
            for chunk in chunk_entries
        }
        for future in concurrent.futures.as_completed(future_map):
            result = future.result()
            results.append(result)
            print(
                (
                    "[netex-extract-qa-network] INFO: "
                    f"Completed entries {int(result['first_entry_index']) + 1}"
                    f"-{int(result['last_entry_index']) + 1}/{len(entries)}"
                ),
                file=sys.stderr,
            )

    results.sort(key=lambda item: int(item["first_entry_index"]))
    return results


def extract(
    *,
    zip_path: Path,
    args: argparse.Namespace,
    route_writer: csv.DictWriter,
    adjacency_writer: csv.DictWriter,
) -> Summary:
    metadata = json.dumps(
        {
            "import_run_id": args.import_run_id,
            "provider_slug": args.provider_slug,
            "country": args.country,
            "snapshot_date": args.snapshot_date,
            "manifest_sha256": args.manifest_sha256,
        },
        ensure_ascii=True,
        separators=(",", ":"),
    )

    pattern_store = PatternFactStore(dedupe_locally=False)
    try:
        with zipfile.ZipFile(zip_path) as archive:
            entries = candidate_xml_entries(archive, zip_path)
        worker_count = resolve_worker_count(args.workers, len(entries))
        swap_free_bytes = read_swap_free_bytes()
        if args.workers > 0 and worker_count < args.workers:
            print(
                (
                    "[netex-extract-qa-network] WARN: "
                    f"Clamped requested worker count from {args.workers} to {worker_count} "
                    "to keep the extract within desktop safety limits."
                ),
                file=sys.stderr,
            )
        elif (
            worker_count == 1
            and len(entries) >= MIN_ENTRIES_FOR_PARALLEL_EXTRACT
            and swap_free_bytes is not None
            and swap_free_bytes < MIN_SWAP_FREE_BYTES_FOR_PARALLEL
        ):
            print(
                (
                    "[netex-extract-qa-network] WARN: "
                    "Falling back to a single worker because swap headroom is too low "
                    "for safe parallel extraction."
                ),
                file=sys.stderr,
            )
        worker_results = extract_entries_parallel(
            zip_path=zip_path,
            entries=entries,
            worker_count=worker_count,
        )

        for worker_result in worker_results:
            pattern_store.merge_from(Path(worker_result["db_path"]))

        summary = pattern_store.build_summary(
            xml_entries_scanned=sum(
                int(worker_result["xml_entries_scanned"])
                for worker_result in worker_results
            ),
            parse_fail_missing_points=sum(
                int(worker_result["parse_fail_missing_points"])
                for worker_result in worker_results
            ),
        )

        for (
            stop_place_ref,
            route_label,
            transport_mode,
            pattern_hits,
        ) in pattern_store.iter_routes():
            route_writer.writerow(
                {
                    "source_country": args.country,
                    "source_id": args.source_id,
                    "dataset_id": args.dataset_id,
                    "provider_stop_place_ref": stop_place_ref,
                    "route_label": route_label,
                    "transport_mode": transport_mode,
                    "pattern_hits": pattern_hits,
                    "metadata": metadata,
                }
            )
            summary.route_rows_written += 1

        for from_ref, to_ref, pattern_hits in pattern_store.iter_adjacencies():
            adjacency_writer.writerow(
                {
                    "source_country": args.country,
                    "source_id": args.source_id,
                    "dataset_id": args.dataset_id,
                    "from_provider_stop_place_ref": from_ref,
                    "to_provider_stop_place_ref": to_ref,
                    "pattern_hits": pattern_hits,
                    "metadata": metadata,
                }
            )
            summary.adjacency_rows_written += 1
    finally:
        pattern_store.close()
        for worker_result in locals().get("worker_results", []):
            try:
                Path(worker_result["db_path"]).unlink(missing_ok=True)
            except OSError:
                pass

    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract compact QA route and adjacency context from NeTEx ZIP"
    )
    parser.add_argument("--zip-path", required=True)
    parser.add_argument("--output-routes-csv", required=True)
    parser.add_argument("--output-adjacencies-csv", required=True)
    parser.add_argument("--summary-json", required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--country", required=True)
    parser.add_argument("--provider-slug", required=True)
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--manifest-sha256", default="")
    parser.add_argument("--import-run-id", required=True)
    parser.add_argument(
        "--workers",
        type=int,
        default=int(os.environ.get("QA_NETWORK_EXTRACT_WORKERS", "0") or "0"),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    zip_path = Path(args.zip_path)
    output_routes_csv = Path(args.output_routes_csv)
    output_adjacencies_csv = Path(args.output_adjacencies_csv)
    summary_json = Path(args.summary_json)

    if not zip_path.is_file():
        raise RuntimeError(f"ZIP not found: {zip_path}")

    output_routes_csv.parent.mkdir(parents=True, exist_ok=True)
    output_adjacencies_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)

    with (
        output_routes_csv.open("w", newline="", encoding="utf-8") as routes_handle,
        output_adjacencies_csv.open(
            "w", newline="", encoding="utf-8"
        ) as adjacencies_handle,
    ):
        route_writer = csv.DictWriter(
            routes_handle,
            fieldnames=[
                "source_country",
                "source_id",
                "dataset_id",
                "provider_stop_place_ref",
                "route_label",
                "transport_mode",
                "pattern_hits",
                "metadata",
            ],
        )
        adjacency_writer = csv.DictWriter(
            adjacencies_handle,
            fieldnames=[
                "source_country",
                "source_id",
                "dataset_id",
                "from_provider_stop_place_ref",
                "to_provider_stop_place_ref",
                "pattern_hits",
                "metadata",
            ],
        )
        route_writer.writeheader()
        adjacency_writer.writeheader()

        summary = extract(
            zip_path=zip_path,
            args=args,
            route_writer=route_writer,
            adjacency_writer=adjacency_writer,
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
        print(f"[netex-extract-qa-network] ERROR: {exc}", file=sys.stderr)
        raise
