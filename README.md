# European Rail Meta-Router (DACH MVP)

Cross-border rail route planner for Germany, Switzerland, and Austria.  
Stack: React + Vite frontend, NestJS orchestrator, MOTIS routing, Redis cache, PostGIS.

## Architecture

```
User -> Frontend (Vite+React, :5173) -> Orchestrator (NestJS, :3000)
                                              |- MOTIS (GTFS backbone, :8080)
                                              |- OJP APIs (mock/live)
                                              |- Redis (cache/rate limit, :6379)
                                              `- PostGIS (stations, :5432)
```

## Services and Ports

| Service | Port | Purpose |
|:---|:---|:---|
| Frontend | 5173 | Search UI + map |
| Orchestrator | 3000 | API (`/health`, `/api/routes`, `/api/stations`) |
| MOTIS | 8080 | GTFS routing engine |
| Redis | 6379 | Cache + rate limiting |
| PostGIS | 5432 | Station DB (seeded via `config/init-db.sql`) |

## Prerequisites

### Docker path
- Docker Engine + Docker Compose plugin

### Local development path
- Node.js 20+
- npm
- Python 3 (for pipeline scripts)
- `curl`, `unzip`
- Optional for OSM extraction: `osmium` (`osmium-tool`)

## Quick Start (Docker, full stack)

```bash
cp .env.example .env
mkdir -p data/gtfs_raw data/gtfs_filtered data/osm
docker compose up --build -d
```

Open:
- Frontend: `http://localhost:5173`
- Health: `http://localhost:3000/health`

Quick checks:

```bash
curl http://localhost:3000/health
curl "http://localhost:3000/api/stations?q=Zurich"
```

## Local Development (without Docker)

You can run frontend + orchestrator locally without Redis/MOTIS; the backend has fallback behavior:
- Redis fallback: in-memory cache
- MOTIS fallback: mock backbone segment

Terminal 1 (backend):

```bash
cd orchestrator
npm install
npm run start:dev
```

Terminal 2 (frontend):

```bash
cd frontend
npm install
npm run dev
```

Open:
- Frontend: `http://localhost:5173`
- API proxied by Vite to `http://localhost:3000`

Optional: run infrastructure only via Docker while coding locally:

```bash
docker compose up -d redis db motis
```

## API Usage

### Health

```bash
curl http://localhost:3000/health
```

### Station autocomplete

```bash
curl "http://localhost:3000/api/stations?q=Munchen"
```

### Route search

```bash
curl -X POST http://localhost:3000/api/routes \
  -H "Content-Type: application/json" \
  -d '{
    "origin": "Munchen Hbf",
    "destination": "Wien Hbf",
    "departure": "2026-02-18T18:00:00.000Z",
    "max_results": 5
  }'
```

## Data Pipeline

The pipeline is under `data-pipeline/`.

### 1. Download GTFS feeds

Default output folder is `data/gtfs_raw`.

```bash
# Germany only (automatic)
bash data-pipeline/download-gtfs.sh --de

# All sources (DE automatic, CH/AT with manual browser steps)
bash data-pipeline/download-gtfs.sh --all

# Custom output folder (optional)
bash data-pipeline/download-gtfs.sh ./data/gtfs_raw --all
```

Notes:
- CH feed (`ch_full.zip`) and AT feed (`at_oebb.zip`) must be downloaded manually due provider restrictions.
- Script validates ZIP integrity and GTFS core files.

### 2. Run the GTFS visualizer (interactive filter)

Recommended (serve over HTTP):

```bash
python3 -m http.server 8090 --directory data-pipeline/gtfs-explorer
```

Then open `http://localhost:8090`.

Alternative:

```bash
xdg-open data-pipeline/gtfs-explorer/index.html
```

Visualizer workflow:
1. Drag a GTFS `.zip` (or select `.txt/.csv` files).
2. Filter by route types and operators.
3. Click `Export Filtered` to download a filtered GTFS zip.

### 3. Filter GTFS via CLI (zip -> zip)

```bash
python3 data-pipeline/gtfs-filter.py data/gtfs_raw/de_fv.zip data/gtfs_filtered/de_fv.zip
python3 data-pipeline/gtfs-filter.py data/gtfs_raw/de_rv.zip data/gtfs_filtered/de_rv.zip
python3 data-pipeline/gtfs-filter.py data/gtfs_raw/de_nv.zip data/gtfs_filtered/de_nv.zip
```

### 4. Station dedup helper (semi-auto + manual review)

The dedup helper matches GTFS `stops.txt` entries to `data/station_map.json` by:
- name similarity
- coordinate proximity

It is designed for the plan workflow: auto-suggest first, then manual curation.

Generate suggestions (CSV):

```bash
python3 data-pipeline/gtfs-dedup.py suggest \
  data/gtfs_raw/de_fv.zip data/gtfs_raw/de_rv.zip data/gtfs_raw/de_nv.zip \
  --out-csv data/station_dedup_review.csv \
  --include-unmatched --accept-auto
```

Interactive manual review:

```bash
python3 data-pipeline/gtfs-dedup.py review data/gtfs_raw \
  --out-csv data/station_dedup_review.csv \
  --include-unmatched --accept-auto
```

Apply reviewed decisions back to station map:

```bash
python3 data-pipeline/gtfs-dedup.py apply data/station_dedup_review.csv --backup
```

Notes:
- Decision types in CSV: `link`, `create`, empty (skip).
- If a station already has `gtfs_ids[country]`, additional IDs for the same country are flagged as conflicts (manual decision required).

### 5. Detect station changes between updates

```bash
bash data-pipeline/gtfs-diff.sh old/stops.txt new/stops.txt
```

### 6. Extract OSM station islands (optional)

Prerequisite: `osmium` and `europe-latest.osm.pbf`.

```bash
bash data-pipeline/osm-island-extract.sh \
  /path/to/stops.txt \
  /path/to/europe-latest.osm.pbf \
  data/osm/europe_islands.pbf
```

## Running the Project After Data Prep

When `data/gtfs_filtered` and `data/osm` are ready:

```bash
docker compose up --build -d
```

MOTIS gets:
- GTFS from `./data/gtfs_filtered` -> `/input/gtfs`
- OSM from `./data/osm` -> `/input/osm`

## Useful Commands

```bash
# Start/restart all services
docker compose up --build -d

# Follow logs
docker compose logs -f orchestrator frontend motis

# Stop everything
docker compose down

# Stop and remove volumes (DB/Redis data)
docker compose down -v
```

## Troubleshooting

- `Frontend loads but API calls fail`:
  - Check `http://localhost:3000/health`
  - Ensure orchestrator is running and port `3000` is free

- `No real MOTIS routes`:
  - If MOTIS is unavailable, orchestrator returns mock backbone segments by design
  - Verify `http://localhost:8080/` and that GTFS files exist in `data/gtfs_filtered`

- `Visualizer does not load correctly`:
  - Prefer HTTP mode (`python3 -m http.server ...`) instead of `file://`
  - Ensure browser can access CDN assets (`unpkg.com`, `fonts.googleapis.com`)

## Repo Layout

```
Trainscanner/
|- orchestrator/      # NestJS backend
|- frontend/          # React + Vite frontend
|- data-pipeline/     # GTFS and OSM helper scripts
|- data/              # station_map.json, GTFS inputs/outputs
|- config/            # MOTIS and DB config
`- docker-compose.yml
```
