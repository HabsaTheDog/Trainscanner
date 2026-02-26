#!/usr/bin/env python3
import argparse
import csv
import io
import json
import os
import re
import shutil
import subprocess
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import NoReturn


REQUIRED_COLUMNS = {
    "stop_id",
    "stop_name",
    "country",
    "stop_lat",
    "stop_lon",
}

VALID_COUNTRIES = {"DE", "AT", "CH"}
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent.parent
PSQL_ON_ERROR_STOP = "ON_ERROR_STOP=1"
TRANSFERS_FILE = "transfers.txt"
TIER_SEQUENCE = ["high-speed", "regional", "local"]
TIER_LABELS = {
    "high-speed": "High-Speed / Long-Distance",
    "regional": "Regional / Intercity",
    "local": "Local / S-Bahn / Metro",
}
TIER_SHORT = {
    "high-speed": "HS",
    "regional": "REG",
    "local": "LOC",
}
TIER_ALIASES = {
    "high-speed": "high-speed",
    "high_speed": "high-speed",
    "highspeed": "high-speed",
    "regional": "regional",
    "intercity": "regional",
    "local": "local",
    "all": "all",
}

HIGH_SPEED_PATTERNS = [
    re.compile(r"\bice\b"),
    re.compile(r"\btgv\b"),
    re.compile(r"\brailjet\b"),
    re.compile(r"\bfrecciarossa\b"),
    re.compile(r"\beurostar\b"),
    re.compile(r"\bthalys\b"),
    re.compile(r"\bouigo\b"),
    re.compile(r"\bintercity\s+express\b"),
]
REGIONAL_PATTERNS = [
    re.compile(r"\bregional\b"),
    re.compile(r"\bregio\b"),
    re.compile(r"\bintercity\b"),
    re.compile(r"\bic\b"),
    re.compile(r"\bir\b"),
    re.compile(r"\bre\b"),
    re.compile(r"\brb\b"),
    re.compile(r"\brex\b"),
]
LOCAL_PATTERNS = [
    re.compile(r"\bs[ -]?bahn\b"),
    re.compile(r"\bu[ -]?bahn\b"),
    re.compile(r"\bmetro\b"),
    re.compile(r"\bsubway\b"),
    re.compile(r"\btram\b"),
    re.compile(r"\bstreetcar\b"),
    re.compile(r"\bbus\b"),
]


def fail(msg: str) -> NoReturn:
    print(f"[export-canonical-gtfs] ERROR: {msg}", file=sys.stderr)
    raise SystemExit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate deterministic GTFS zip from canonical stop rows"
    )
    parser.add_argument("--stops-csv")
    parser.add_argument("--from-db", action="store_true")
    parser.add_argument("--profile", default="canonical_runtime")
    parser.add_argument(
        "--as-of", default=datetime.now(timezone.utc).strftime("%Y-%m-%d")
    )
    parser.add_argument("--country", choices=sorted(VALID_COUNTRIES))
    parser.add_argument("--tier", default="all")
    parser.add_argument("--output-zip")
    parser.add_argument("--summary-json")
    parser.add_argument("--output-dir", default="data/artifacts")
    parser.add_argument("--agency-url", default="https://example.invalid/trainscanner")
    return parser.parse_args()


def normalize_tier(raw: str) -> str:
    key = str(raw or "").strip().lower()
    normalized = TIER_ALIASES.get(key)
    if normalized is None:
        fail(
            "invalid --tier value '"
            + str(raw)
            + "' (expected one of: high-speed, regional, local, all)"
        )
    return normalized


def normalize_as_of(raw: str) -> str:
    try:
        dt = datetime.strptime(str(raw or "").strip(), "%Y-%m-%d")
    except Exception:
        fail(f"invalid --as-of value '{raw}' (expected YYYY-MM-DD)")
        return "1970-01-01"
    return dt.strftime("%Y-%m-%d")


def parse_bool(value: str, default: bool = True) -> bool:
    clean = (value or "").strip().lower()
    if not clean:
        return default
    if clean in {"1", "true", "t", "yes", "y"}:
        return True
    if clean in {"0", "false", "f", "no", "n"}:
        return False
    return default


def parse_json_string_list(raw: str):
    text = (raw or "").strip()
    if not text:
        return []

    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            values = [str(v).strip() for v in parsed]
            return sorted({v for v in values if v})
    except Exception:
        pass

    if text.startswith("{") and text.endswith("}") and "," in text:
        items = [part.strip().strip('"') for part in text[1:-1].split(",")]
        return sorted({v for v in items if v})

    parts = [part.strip() for part in re.split(r"[,;|]", text)]
    return sorted({part for part in parts if part})


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
            minutes = int(minutes_raw or 0)
            if minutes < 0:
                minutes = 0
        except Exception:
            minutes = 0
        links.append({"to_stop_id": to_stop_id, "min_walk_minutes": minutes})

    links.sort(
        key=lambda link_item: (
            link_item["to_stop_id"],
            link_item["min_walk_minutes"],
        )
    )
    return links


def parse_route_type_hint(value):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(text)
    except Exception:
        return None


def load_rows_from_csv(path: str):
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        if not fieldnames:
            fail("stops CSV has no header")
        missing = REQUIRED_COLUMNS - set(fieldnames)
        if missing:
            fail(f"stops CSV missing required columns: {', '.join(sorted(missing))}")
        return list(reader)


def run_psql_csv(query: str):
    def _run_command(cmd, *, env=None, cwd=None):
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            env=env,
            cwd=cwd,
        )

    def _format_error(err):
        if isinstance(err, FileNotFoundError):
            return str(err)
        stderr = (getattr(err, "stderr", "") or "").strip()
        stdout = (getattr(err, "stdout", "") or "").strip()
        return stderr or stdout or str(err)

    def _run_direct_psql():
        if shutil.which("psql") is None:
            raise FileNotFoundError("psql")

        db_url = (
            os.environ.get("CANONICAL_DB_URL") or os.environ.get("DATABASE_URL") or ""
        ).strip()
        db_host = (
            os.environ.get("CANONICAL_DB_HOST")
            or os.environ.get("PGHOST")
            or "localhost"
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

        if db_url:
            cmd = ["psql", db_url, "-X", "-v", PSQL_ON_ERROR_STOP, "--csv", "-c", query]
        else:
            cmd = [
                "psql",
                "-X",
                "-h",
                db_host,
                "-p",
                db_port,
                "-U",
                db_user,
                "-d",
                db_name,
                "-v",
                PSQL_ON_ERROR_STOP,
                "--csv",
                "-c",
                query,
            ]

        env = dict(os.environ)
        env["PGPASSWORD"] = db_password
        return _run_command(cmd, env=env)

    def _run_docker_psql():
        if shutil.which("docker") is None:
            raise FileNotFoundError("docker")

        docker_profile = (
            os.environ.get("CANONICAL_DB_DOCKER_PROFILE") or "dach-data"
        ).strip()
        docker_service = (
            os.environ.get("CANONICAL_DB_DOCKER_SERVICE") or "postgis"
        ).strip()
        db_user = (os.environ.get("CANONICAL_DB_USER") or "trainscanner").strip()
        db_name = (os.environ.get("CANONICAL_DB_NAME") or "trainscanner").strip()

        up_cmd = [
            "docker",
            "compose",
            "--profile",
            docker_profile,
            "up",
            "-d",
            docker_service,
        ]
        _run_command(up_cmd, cwd=str(ROOT_DIR))

        cmd = [
            "docker",
            "compose",
            "--profile",
            docker_profile,
            "exec",
            "-T",
            docker_service,
            "psql",
            "-v",
            PSQL_ON_ERROR_STOP,
            "-U",
            db_user,
            "-d",
            db_name,
            "--csv",
            "-c",
            query,
        ]
        return _run_command(cmd, cwd=str(ROOT_DIR))

    mode = (os.environ.get("CANONICAL_DB_MODE") or "auto").strip().lower()
    if mode not in {"auto", "direct", "docker-compose"}:
        fail("invalid CANONICAL_DB_MODE (expected auto, direct, docker-compose)")

    errors = []
    if mode in {"auto", "direct"}:
        try:
            result = _run_direct_psql()
            content = result.stdout or ""
            reader = csv.DictReader(io.StringIO(content))
            fieldnames = reader.fieldnames
            if not fieldnames:
                fail("database export query did not return CSV header")
            missing = REQUIRED_COLUMNS - set(fieldnames)
            if missing:
                fail(
                    "database export query missing required columns: "
                    + ", ".join(sorted(missing))
                )
            return list(reader)
        except (FileNotFoundError, subprocess.CalledProcessError) as err:
            errors.append(f"direct mode: {_format_error(err)}")
            if mode == "direct":
                fail("database export query failed: " + errors[-1])

    if mode in {"auto", "docker-compose"}:
        try:
            result = _run_docker_psql()
            content = result.stdout or ""
            reader = csv.DictReader(io.StringIO(content))
            fieldnames = reader.fieldnames
            if not fieldnames:
                fail("database export query did not return CSV header")
            missing = REQUIRED_COLUMNS - set(fieldnames)
            if missing:
                fail(
                    "database export query missing required columns: "
                    + ", ".join(sorted(missing))
                )
            return list(reader)
        except (FileNotFoundError, subprocess.CalledProcessError) as err:
            errors.append(f"docker-compose mode: {_format_error(err)}")
            fail("database export query failed: " + " | ".join(errors))

    fail("database export query failed due to unresolved mode selection")


def build_primary_db_query(as_of: str, country: str):
    country_filter = ""
    if country:
        country_filter = f"AND rs.country = '{country}'::char(2)"

    return f"""
WITH selected_snapshots AS (
  SELECT rs.source_id, rs.country, MAX(rs.snapshot_date) AS snapshot_date
  FROM raw_snapshots rs
  WHERE rs.format = 'netex'
    AND rs.snapshot_date <= '{as_of}'::date
    {country_filter}
  GROUP BY rs.source_id, rs.country
),
selected_mappings AS (
  SELECT
    css.canonical_station_id,
    css.country,
    css.snapshot_date,
    css.source_id,
    css.source_stop_id
  FROM canonical_station_sources css
  JOIN selected_snapshots ss
    ON ss.source_id = css.source_id
   AND ss.snapshot_date = css.snapshot_date
  WHERE css.country IN ('DE', 'AT', 'CH')
),
source_rows AS (
  SELECT
    sm.canonical_station_id,
    sm.country,
    s.provider_slug,
    COALESCE(s.raw_payload, '{{}}'::jsonb) AS raw_payload
  FROM selected_mappings sm
  LEFT JOIN netex_stops_staging s
    ON s.source_id = sm.source_id
   AND s.source_stop_id = sm.source_stop_id
   AND s.snapshot_date = sm.snapshot_date
),
all_tokens AS (
  SELECT canonical_station_id, country, LOWER(BTRIM(provider_slug)) AS token
  FROM source_rows
  WHERE provider_slug IS NOT NULL AND BTRIM(provider_slug) <> ''

  UNION ALL
  SELECT canonical_station_id, country, LOWER(BTRIM(v.value)) AS token
  FROM source_rows sr
  JOIN LATERAL (
    VALUES
      (sr.raw_payload ->> 'line'),
      (sr.raw_payload ->> 'route'),
      (sr.raw_payload ->> 'service'),
      (sr.raw_payload ->> 'trip'),
      (sr.raw_payload ->> 'line_code'),
      (sr.raw_payload ->> 'route_id'),
      (sr.raw_payload ->> 'service_id')
  ) AS v(value) ON true
  WHERE v.value IS NOT NULL AND BTRIM(v.value) <> ''

  UNION ALL
  SELECT canonical_station_id, country, LOWER(BTRIM(item)) AS token
  FROM source_rows sr
  JOIN LATERAL jsonb_array_elements_text(
    CASE
      WHEN jsonb_typeof(sr.raw_payload -> 'lines') = 'array' THEN sr.raw_payload -> 'lines'
      ELSE '[]'::jsonb
    END
  ) AS arr(item) ON true

  UNION ALL
  SELECT canonical_station_id, country, LOWER(BTRIM(item)) AS token
  FROM source_rows sr
  JOIN LATERAL jsonb_array_elements_text(
    CASE
      WHEN jsonb_typeof(sr.raw_payload -> 'routes') = 'array' THEN sr.raw_payload -> 'routes'
      ELSE '[]'::jsonb
    END
  ) AS arr(item) ON true

  UNION ALL
  SELECT canonical_station_id, country, LOWER(BTRIM(item)) AS token
  FROM source_rows sr
  JOIN LATERAL jsonb_array_elements_text(
    CASE
      WHEN jsonb_typeof(sr.raw_payload -> 'services') = 'array' THEN sr.raw_payload -> 'services'
      ELSE '[]'::jsonb
    END
  ) AS arr(item) ON true

  UNION ALL
  SELECT canonical_station_id, country, LOWER(BTRIM(item)) AS token
  FROM source_rows sr
  JOIN LATERAL jsonb_array_elements_text(
    CASE
      WHEN jsonb_typeof(sr.raw_payload -> 'service_context' -> 'lines') = 'array'
        THEN sr.raw_payload -> 'service_context' -> 'lines'
      WHEN jsonb_typeof(sr.raw_payload -> 'serviceContext' -> 'lines') = 'array'
        THEN sr.raw_payload -> 'serviceContext' -> 'lines'
      ELSE '[]'::jsonb
    END
  ) AS arr(item) ON true
),
aggregated_tokens AS (
  SELECT
    canonical_station_id,
    country,
    json_agg(DISTINCT token ORDER BY token) AS service_labels_json
  FROM all_tokens
  WHERE token IS NOT NULL AND token <> ''
  GROUP BY canonical_station_id, country
),
scope_stations AS (
  SELECT
    sm.canonical_station_id,
    sm.country
  FROM selected_mappings sm
  GROUP BY sm.canonical_station_id, sm.country
)
SELECT
  ss.canonical_station_id AS stop_id,
  COALESCE(NULLIF(cs.canonical_name, ''), ss.canonical_station_id) AS stop_name,
  ss.country,
  COALESCE(ROUND(cs.latitude::numeric, 6)::text, '') AS stop_lat,
  COALESCE(ROUND(cs.longitude::numeric, 6)::text, '') AS stop_lon,
  ''::text AS location_type,
  ''::text AS parent_station,
  'true'::text AS is_user_facing,
  '[]'::text AS walk_links_json,
  ''::text AS section_type,
  COALESCE(at.service_labels_json::text, '[]') AS service_labels_json
FROM scope_stations ss
JOIN canonical_stations cs
  ON cs.canonical_station_id = ss.canonical_station_id
 AND cs.country = ss.country
LEFT JOIN aggregated_tokens at
  ON at.canonical_station_id = ss.canonical_station_id
 AND at.country = ss.country
WHERE cs.country IN ('DE', 'AT', 'CH')
  AND cs.is_deleted = false
  AND cs.latitude IS NOT NULL
  AND cs.longitude IS NOT NULL
ORDER BY ss.country, ss.canonical_station_id;
"""


def build_fallback_db_query(country: str):
    country_filter = ""
    if country:
        country_filter = f"AND cs.country = '{country}'::char(2)"

    return f"""
SELECT
  cs.canonical_station_id AS stop_id,
  COALESCE(NULLIF(cs.canonical_name, ''), cs.canonical_station_id) AS stop_name,
  cs.country,
  COALESCE(ROUND(cs.latitude::numeric, 6)::text, '') AS stop_lat,
  COALESCE(ROUND(cs.longitude::numeric, 6)::text, '') AS stop_lon,
  ''::text AS location_type,
  ''::text AS parent_station,
  'true'::text AS is_user_facing,
  '[]'::text AS walk_links_json,
  ''::text AS section_type,
  '[]'::text AS service_labels_json
FROM canonical_stations cs
WHERE cs.country IN ('DE', 'AT', 'CH')
  {country_filter}
  AND cs.is_deleted = false
  AND cs.latitude IS NOT NULL
  AND cs.longitude IS NOT NULL
ORDER BY cs.country, cs.canonical_station_id;
"""


def load_rows_from_db(as_of: str, country: str):
    rows = run_psql_csv(build_primary_db_query(as_of, country))
    if rows:
        return rows
    return run_psql_csv(build_fallback_db_query(country))


def compile_classification_text(stop):
    values = [
        stop.get("stop_name", ""),
        stop.get("section_type", ""),
        " ".join(stop.get("service_labels", [])),
        " ".join(stop.get("provider_tags", [])),
    ]
    return " ".join(v for v in values if v).strip().lower()


def parse_explicit_tiers(raw_hint: str):
    tiers = set()
    for token in parse_json_string_list(raw_hint):
        normalized = TIER_ALIASES.get(token.lower())
        if normalized and normalized != "all":
            tiers.add(normalized)
    return tiers


def classify_stop_tiers(stop):
    tiers = set()

    tiers.update(parse_explicit_tiers(stop.get("tier_hint", "")))

    section_type = str(stop.get("section_type") or "").strip().lower()
    if section_type in {"subway", "tram", "bus"}:
        tiers.add("local")
    elif section_type in {"main", "secondary"}:
        tiers.add("regional")

    route_type_hint = stop.get("route_type_hint")
    if route_type_hint is not None:
        if route_type_hint in {0, 1, 3, 4, 5, 6, 7, 11, 12}:
            tiers.add("local")
        elif route_type_hint in {2, 100, 101, 102, 103, 106, 107}:
            tiers.add("regional")

    text = compile_classification_text(stop)
    if text:
        for pattern in HIGH_SPEED_PATTERNS:
            if pattern.search(text):
                tiers.add("high-speed")
                break

        for pattern in REGIONAL_PATTERNS:
            if pattern.search(text):
                tiers.add("regional")
                break

        for pattern in LOCAL_PATTERNS:
            if pattern.search(text):
                tiers.add("local")
                break

    if not tiers:
        tiers.add("regional")

    return sorted(tiers)


def load_stops_from_rows(rows):
    if not rows:
        fail("no stop rows found for export scope")

    stops = []
    missing_coords = []

    for row in rows:
        stop_id = (row.get("stop_id") or "").strip()
        stop_name = (row.get("stop_name") or "").strip()
        country = (row.get("country") or "").strip()
        lat_raw = (row.get("stop_lat") or "").strip()
        lon_raw = (row.get("stop_lon") or "").strip()

        if not stop_id or not stop_name or country not in VALID_COUNTRIES:
            fail(f"invalid stop row values for stop_id='{stop_id}' country='{country}'")

        try:
            lat = float(lat_raw)
            lon = float(lon_raw)
        except Exception:
            missing_coords.append(stop_id)
            continue

        if abs(lat) > 90 or abs(lon) > 180:
            fail(
                f"invalid coordinates for stop '{stop_id}': lat={lat_raw} lon={lon_raw}"
            )

        location_type = (row.get("location_type") or "").strip()
        parent_station = (row.get("parent_station") or "").strip()
        section_type = (row.get("section_type") or "").strip()

        service_labels = parse_json_string_list(
            (row.get("service_labels_json") or row.get("service_labels") or "")
        )
        provider_tags = parse_json_string_list(
            (row.get("provider_tags_json") or row.get("provider_tags") or "")
        )

        stop = {
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
            "service_labels": service_labels,
            "provider_tags": provider_tags,
            "tier_hint": (row.get("tier_hint") or "").strip(),
            "route_type_hint": parse_route_type_hint(row.get("route_type_hint")),
        }

        stop["tiers"] = classify_stop_tiers(stop)
        stops.append(stop)

    if missing_coords:
        sample = ", ".join(missing_coords[:10])
        fail(
            "canonical export requires coordinates for all stops; missing/invalid for "
            f"{len(missing_coords)} stop(s), e.g. {sample}"
        )

    if len(stops) < 2:
        fail("export scope produced fewer than 2 stops; cannot build journey bridge")

    stops.sort(key=lambda s: (s["country"], s["stop_id"]))
    return stops


def count_tier_distribution(stops):
    counts = dict.fromkeys(TIER_SEQUENCE, 0)
    for stop in stops:
        for tier in stop.get("tiers", []):
            if tier in counts:
                counts[tier] += 1
    return counts


def filter_stops_by_tier(stops, tier: str):
    if tier == "all":
        return list(stops)
    return [stop for stop in stops if tier in stop.get("tiers", [])]


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
            if (
                not to_stop_id
                or to_stop_id not in known_stop_ids
                or to_stop_id == from_stop_id
            ):
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


def infer_route_type(stops, tier: str) -> str:
    if tier == "high-speed":
        return "2"
    if tier == "regional":
        return "2"

    text = " ".join(
        [
            stop.get("stop_name", "")
            + " "
            + stop.get("section_type", "")
            + " "
            + " ".join(stop.get("service_labels", []))
            for stop in stops
        ]
    ).lower()

    if "metro" in text or "subway" in text or "u-bahn" in text or "ubahn" in text:
        return "1"
    if "bus" in text:
        return "3"
    if "s-bahn" in text or "sbahn" in text:
        return "2"
    return "0"


def build_tables(profile: str, requested_tier: str, stops, agency_url: str):
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

    agency_seen = set()
    countries = sorted({stop["country"] for stop in stops})
    tiers_to_emit = TIER_SEQUENCE if requested_tier == "all" else [requested_tier]

    for country in countries:
        agency_id = f"agency_{country.lower()}"
        if agency_id not in agency_seen:
            agency_rows.append(
                [
                    agency_id,
                    f"Canonical {country} Transit",
                    agency_url,
                    tz_map.get(country, "Europe/Berlin"),
                    "de",
                ]
            )
            agency_seen.add(agency_id)

        for tier in tiers_to_emit:
            country_tier_rows = [
                stop
                for stop in stops
                if stop["country"] == country and tier in stop.get("tiers", [])
            ]

            route_stops = sorted(
                [stop for stop in country_tier_rows if stop["is_user_facing"]],
                key=lambda r: r["stop_id"],
            )

            if len(route_stops) < 2:
                route_stops = sorted(country_tier_rows, key=lambda r: r["stop_id"])

            if len(route_stops) < 2:
                continue

            tier_slug = tier.replace("-", "_")
            route_id = f"route_{country.lower()}_{tier_slug}"
            service_id = f"svc_{country.lower()}_{tier_slug}"

            route_rows.append(
                [
                    route_id,
                    agency_id,
                    f"{country} {TIER_SHORT[tier]}",
                    f"{country} {TIER_LABELS[tier]} Canonical Bridge",
                    infer_route_type(route_stops, tier),
                    f"tier:{tier};profile:{profile}",
                ]
            )

            first_stop = route_stops[0]["stop_name"]
            last_stop = route_stops[-1]["stop_name"]
            trip_out_id = f"trip_{country.lower()}_{tier_slug}_outbound"
            trip_in_id = f"trip_{country.lower()}_{tier_slug}_inbound"

            trip_rows.append(
                [route_id, service_id, trip_out_id, f"{first_stop} -> {last_stop}"]
            )
            trip_rows.append(
                [route_id, service_id, trip_in_id, f"{last_stop} -> {first_stop}"]
            )

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

            tier_slot = TIER_SEQUENCE.index(tier)
            base_outbound = (6 + tier_slot * 2) * 3600
            for idx, stop in enumerate(route_stops, start=1):
                t = fmt_time(base_outbound + (idx - 1) * 420)
                stop_time_rows.append([trip_out_id, t, t, stop["stop_id"], str(idx)])

            base_inbound = (18 + tier_slot * 2) * 3600
            for idx, stop in enumerate(reversed(route_stops), start=1):
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
            stops_rows,
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
            ["route_id", "service_id", "trip_id", "trip_headsign"], trip_rows
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
        files[TRANSFERS_FILE] = csv_text(
            ["from_stop_id", "to_stop_id", "transfer_type", "min_transfer_time"],
            transfer_rows,
        )

    summary = {
        "profile": profile,
        "tier": requested_tier,
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

    output_path = Path(output_zip)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(output_path, "w") as zf:
        for name in ordered_names:
            payload = files[name].encode("utf-8")
            info = zipfile.ZipInfo(name)
            info.date_time = timestamp
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            zf.writestr(info, payload)


def resolve_output_paths(args, profile: str, tier: str, as_of: str):
    slug_profile = re.sub(r"[^A-Za-z0-9._-]+", "-", profile).strip("-") or "canonical"

    output_zip = args.output_zip
    summary_json = args.summary_json

    if not output_zip or not summary_json:
        base_dir = Path(args.output_dir)
        base_dir.mkdir(parents=True, exist_ok=True)
        stem = f"{slug_profile}-{tier}-{as_of}"
        if not output_zip:
            output_zip = str(base_dir / f"{stem}.zip")
        if not summary_json:
            summary_json = str(base_dir / f"{stem}.summary.json")

    return output_zip, summary_json


def main() -> None:
    args = parse_args()

    tier = normalize_tier(args.tier)
    as_of = normalize_as_of(args.as_of)
    profile = str(args.profile or "canonical_runtime").strip() or "canonical_runtime"

    if args.from_db and args.stops_csv:
        fail("--from-db and --stops-csv are mutually exclusive")

    use_db_source = args.from_db or not args.stops_csv

    output_zip, summary_json = resolve_output_paths(args, profile, tier, as_of)

    if use_db_source:
        rows = load_rows_from_db(as_of, args.country)
    else:
        rows = load_rows_from_csv(args.stops_csv)

    all_stops = load_stops_from_rows(rows)
    tier_distribution = count_tier_distribution(all_stops)

    scoped_stops = filter_stops_by_tier(all_stops, tier)
    if len(scoped_stops) < 2:
        fail(
            f"tier '{tier}' produced fewer than 2 scoped stops "
            f"({len(scoped_stops)} stop(s)); cannot build artifact"
        )

    files, summary = build_tables(profile, tier, scoped_stops, args.agency_url)
    if summary["counts"]["routes"] < 1:
        fail(
            f"tier '{tier}' produced no routable country/tier groups with at least 2 stops"
        )

    write_deterministic_zip(files, output_zip, as_of)

    summary["asOf"] = as_of
    summary["sourceMode"] = "db" if use_db_source else "csv"
    summary["tierDistributionBeforeFilter"] = tier_distribution
    summary["outputZip"] = str(output_zip)

    summary_path = Path(summary_json)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
        f.write("\n")


if __name__ == "__main__":
    main()
