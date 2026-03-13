#!/usr/bin/env python3

import argparse
import json
import pathlib
import sys
import urllib.parse
import urllib.request


ALLOWED_PRIMARY_CATEGORIES = {
    "airport",
    "airport_terminal",
    "bus_station",
    "ferry_service",
    "light_rail_and_subway_stations",
    "metro_station",
    "public_transportation",
    "subway_station",
    "train_station",
    "transport_interchange",
    "tram_stop",
}
ALLOWED_BASIC_CATEGORIES = {
    "airport",
    "airport_terminal",
    "bus_station",
    "train_station",
    "transport_interchange",
}

DEFAULT_STAC_CATALOG_URL = "https://stac.overturemaps.org/catalog.json"
DEFAULT_RELEASE = "latest"
COUNTRY_BBOXES = {
    "AT": (9.4, 46.3, 17.3, 49.1),
    "BE": (2.4, 49.4, 6.5, 51.6),
    "CH": (5.8, 45.7, 10.7, 47.9),
    "CZ": (12.0, 48.5, 18.9, 51.1),
    "DE": (5.5, 47.0, 15.7, 55.2),
    "FR": (-5.5, 41.2, 9.8, 51.3),
    "IT": (6.0, 36.4, 18.7, 47.2),
    "NL": (3.0, 50.7, 7.4, 53.7),
    "PL": (14.0, 49.0, 24.3, 54.9),
}


def normalize_station_name(value: str) -> str:
    output = []
    previous_space = False
    for char in (value or "").lower():
        if char.isalnum():
            output.append(char)
            previous_space = False
            continue
        if not previous_space:
            output.append(" ")
            previous_space = True
    return "".join(output).strip()


def log(message: str):
    sys.stderr.write(f"[overture] {message}\n")
    sys.stderr.flush()


def normalize_category(value: str) -> str:
    return str(value or "").strip().lower()


def is_transit_category(category: str, subtype: str, basic_category: str) -> bool:
    normalized_category = normalize_category(category)
    normalized_subtype = normalize_category(subtype)
    normalized_basic = normalize_category(basic_category)
    return (
        normalized_subtype in ALLOWED_PRIMARY_CATEGORIES
        or normalized_category in ALLOWED_PRIMARY_CATEGORIES
        or normalized_basic in ALLOWED_BASIC_CATEGORIES
    )


def fetch_json(url: str):
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "Trainscanner external reference importer",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def resolve_latest_release(stac_catalog_url: str) -> str:
    catalog = fetch_json(stac_catalog_url)
    latest = str(catalog.get("latest") or "").strip()
    if not latest:
        raise RuntimeError("Overture STAC catalog did not expose a latest release")
    return latest


def intersects_bbox(left, right) -> bool:
    left_min_lon, left_min_lat, left_max_lon, left_max_lat = left
    right_min_lon, right_min_lat, right_max_lon, right_max_lat = right
    return not (
        left_max_lon < right_min_lon
        or left_min_lon > right_max_lon
        or left_max_lat < right_min_lat
        or left_min_lat > right_max_lat
    )


def resolve_overture_auto_inputs(country: str, release: str, stac_catalog_url: str):
    clean_country = str(country or "").strip().upper()
    if not clean_country:
        raise RuntimeError("Automatic Overture imports require --country")
    country_bbox = COUNTRY_BBOXES.get(clean_country)
    if not country_bbox:
        raise RuntimeError(
            f"No Overture country bbox mapping is configured for {clean_country}"
        )

    resolved_release = (
        resolve_latest_release(stac_catalog_url)
        if not release or release == DEFAULT_RELEASE
        else release
    )
    log(f"resolved_release={resolved_release} country={clean_country}")
    collection_url = (
        f"https://stac.overturemaps.org/{resolved_release}/places/place/collection.json"
    )
    collection = fetch_json(collection_url)
    urls = []
    for link in collection.get("links", []):
        if link.get("rel") != "item":
            continue
        item_url = urllib.parse.urljoin(collection_url, str(link.get("href") or ""))
        item = fetch_json(item_url)
        bbox = item.get("bbox")
        if not bbox or not intersects_bbox(tuple(bbox), country_bbox):
            continue
        assets = item.get("assets") or {}
        href = (
            (assets.get("aws") or {}).get("href")
            or (assets.get("azure") or {}).get("href")
            or ""
        )
        if href:
            urls.append(str(href).strip())

    if not urls:
        raise RuntimeError(
            f"No Overture place parquet assets intersected the configured bbox for {clean_country}"
        )
    log(f"selected_assets={len(urls)} country={clean_country}")
    return urls, resolved_release, country_bbox


def extract_from_json(path: pathlib.Path, country: str):
    if path.suffix.lower() == ".json":
        rows = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(rows, list):
            raise ValueError("Expected JSON array for overture fixture input")
        for row in rows:
            yield row
        return

    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def emit_json_rows(rows, country: str):
    emitted = []
    for row in rows:
        category = str(row.get("category") or "").strip()
        subtype = str(
            row.get("subtype")
            or row.get("primary_category")
            or row.get("secondary_category")
            or ""
        ).strip()
        basic_category = str(row.get("basic_category") or category or "").strip()
        if not is_transit_category(category, subtype, basic_category):
            continue
        row_country = str(row.get("country") or country or "").strip().upper()
        if country and row_country and row_country != country:
            continue
        display_name = str(
            row.get("display_name") or row.get("name") or row.get("primary_name") or ""
        ).strip()
        external_id = str(row.get("external_id") or row.get("id") or "").strip()
        if not display_name or not external_id:
            continue
        emitted.append(
            {
                "external_id": external_id,
                "display_name": display_name,
                "normalized_name": normalize_station_name(display_name),
                "country": row_country,
                "latitude": row.get("latitude"),
                "longitude": row.get("longitude"),
                "category": category or basic_category,
                "subtype": subtype,
                "source_url": str(row.get("source_url") or "").strip(),
                "metadata": row.get("metadata")
                if isinstance(row.get("metadata"), dict)
                else {},
            }
        )
    sys.stdout.write(json.dumps(emitted))


def sql_quote(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def extract_with_duckdb(inputs, country: str, country_bbox=None):
    try:
        import duckdb  # type: ignore
    except ImportError as error:
        raise RuntimeError(
            "duckdb is required for Overture imports; install it in the importer environment"
        ) from error

    paths = [str(value).strip() for value in inputs if str(value).strip()]
    if not paths:
        sys.stdout.write("[]")
        return

    log(f"duckdb_inputs={len(paths)} country={country or 'ALL'}")
    path_list_literal = ", ".join(sql_quote(path) for path in paths)
    allowed_primary_literal = ", ".join(
        sql_quote(value) for value in sorted(ALLOWED_PRIMARY_CATEGORIES)
    )
    allowed_basic_literal = ", ".join(
        sql_quote(value) for value in sorted(ALLOWED_BASIC_CATEGORIES)
    )
    query = f"""
    SELECT
      CAST(id AS VARCHAR) AS external_id,
      COALESCE(
        CAST(names.primary AS VARCHAR),
        CAST(id AS VARCHAR)
      ) AS display_name,
      LOWER(COALESCE(CAST(basic_category AS VARCHAR), CAST(categories.primary AS VARCHAR), '')) AS category,
      LOWER(COALESCE(CAST(categories.primary AS VARCHAR), '')) AS subtype,
      (bbox.ymin + bbox.ymax) / 2.0 AS latitude,
      (bbox.xmin + bbox.xmax) / 2.0 AS longitude,
      LOWER(COALESCE(CAST(basic_category AS VARCHAR), '')) AS basic_category
    FROM read_parquet([{path_list_literal}], union_by_name=true)
    WHERE id IS NOT NULL
      AND (
        LOWER(COALESCE(CAST(categories.primary AS VARCHAR), '')) IN ({allowed_primary_literal})
        OR LOWER(COALESCE(CAST(basic_category AS VARCHAR), CAST(categories.primary AS VARCHAR), '')) IN ({allowed_primary_literal})
        OR LOWER(COALESCE(CAST(basic_category AS VARCHAR), '')) IN ({allowed_basic_literal})
      )
    """

    parameters = []
    if country_bbox:
        min_lon, min_lat, max_lon, max_lat = country_bbox
        query += """
      AND longitude BETWEEN ? AND ?
      AND latitude BETWEEN ? AND ?
    """
        parameters.extend([min_lon, max_lon, min_lat, max_lat])
    if country:
        query += """
      AND addresses IS NOT NULL
      AND list_contains(
        list_transform(addresses, x -> UPPER(COALESCE(x.country, ''))),
        ?
      )
    """
        parameters.append(str(country or "").strip().upper())

    connection = duckdb.connect()
    try:
        connection.execute("PRAGMA disable_progress_bar;")
        connection.execute("INSTALL httpfs; LOAD httpfs;")
        result = connection.execute(query, parameters).fetchall()
    finally:
        connection.close()

    log(f"matched_rows={len(result)} country={country or 'ALL'}")
    rows = []
    for (
        external_id,
        display_name,
        category,
        subtype,
        latitude,
        longitude,
        basic_category,
    ) in result:
        rows.append(
            {
                "external_id": external_id,
                "display_name": display_name,
                "normalized_name": normalize_station_name(display_name),
                "country": str(country or "").strip().upper(),
                "latitude": latitude,
                "longitude": longitude,
                "category": category or basic_category,
                "subtype": subtype,
                "source_url": "",
                "metadata": {},
            }
        )
    sys.stdout.write(json.dumps(rows))


def main():
    parser = argparse.ArgumentParser(
        description="Normalize Overture Places station-like rows for external reference imports"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="GeoParquet or JSON/JSONL input path, or 'auto' for STAC discovery",
    )
    parser.add_argument("--country", default="", help="ISO2 country scope")
    parser.add_argument(
        "--release",
        default=DEFAULT_RELEASE,
        help="Overture release label when --input auto is used",
    )
    parser.add_argument(
        "--stac-catalog-url",
        default=DEFAULT_STAC_CATALOG_URL,
        help="Overture STAC catalog URL",
    )
    args = parser.parse_args()

    input_value = str(args.input or "").strip()
    country = str(args.country or "").strip().upper()

    if input_value == "auto":
        urls, resolved_release, country_bbox = resolve_overture_auto_inputs(
            country,
            str(args.release or "").strip(),
            str(args.stac_catalog_url or "").strip(),
        )
        extract_with_duckdb(urls, country, country_bbox)
        return

    input_path = pathlib.Path(input_value).expanduser().resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Overture input path does not exist: {input_path}")

    if input_path.suffix.lower() in {".json", ".jsonl", ".ndjson"}:
        emit_json_rows(extract_from_json(input_path, country), country)
        return

    extract_with_duckdb([str(input_path)], country, COUNTRY_BBOXES.get(country))


if __name__ == "__main__":
    main()
