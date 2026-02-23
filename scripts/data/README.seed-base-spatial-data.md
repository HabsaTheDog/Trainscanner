# Base Spatial Seeding (Task 1.2)

This seeder pre-populates `canonical_stations` with OSM/UIC topology to reduce V2 cold-start novelty spikes.

## What It Does

- Downloads OSM rail-station topology from Overpass (`DE`, `AT`, `CH` by default).
- Optionally ingests UIC station codes from CSV or JSON (`--uic-file` and/or `--uic-url`).
- Builds deterministic `canonical_station_id` rows:
  - `match_method=hard_id` when a UIC code is linked.
  - `match_method=name_geo` otherwise.
- Computes partition routing using DB-side `compute_geo_grid_id(...)`.
- Runs idempotent upsert into `canonical_stations`.

## Legal Isolation

This seeder only writes ODbL-derived topology into `canonical_stations` and does not read or mutate schedule staging payloads (`netex_stops_staging`, raw operator payloads). Keep this separation to avoid license contamination across data domains.

## Prerequisites

- Migration `012_v2_bounding_box_partitioning.sql` applied (requires `compute_geo_grid_id(...)`).
- Reachable PostGIS (same env vars already used by orchestrator scripts).
- Network access for Overpass unless `--offline` is used.

## Usage

Dry-run (no DB writes):

```bash
scripts/data/seed-base-spatial-data.sh --dry-run
```

Seed all default countries with a local UIC file:

```bash
scripts/data/seed-base-spatial-data.sh \
  --uic-file /path/to/uic-stations.csv
```

Seed one country from UIC URL:

```bash
scripts/data/seed-base-spatial-data.sh \
  --country DE \
  --uic-url https://example.org/uic-stations.json
```

Offline replay from cached OSM payloads in `data/raw/base-spatial-seed/`:

```bash
scripts/data/seed-base-spatial-data.sh --offline --dry-run
```

## Supported UIC Input Shapes

CSV headers (case-insensitive):

- `country` (or `country_code`, `iso2`)
- `uic` (or `uic_code`, `station_code`, `code`, `uic_ref`)
- Optional: `name`, `lat`/`latitude`, `lon`/`lng`/`longitude`

JSON:

- Array of row objects, or object containing one of `stations`, `records`, `data`, `items` arrays.
- Field aliases are the same as CSV.

## Outputs

Run artifacts are written to `data/raw/base-spatial-seed/`:

- `seed-summary-<timestamp>.json`
- `seed-manifest-<timestamp>.json` (includes source refs and multilingual name variants)
- `seed-rows-<timestamp>.json` (DB payload rows)

The CLI also prints a final JSON summary to stdout.
