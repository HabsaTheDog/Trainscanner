# European Rail Meta-Router — AGENTS.md

## Purpose
Cross-border rail route planner for DACH region (Germany, Switzerland, Austria).
Stitches together routes from multiple national operators (DB, SBB, ÖBB) into unified
multi-hop journeys using MOTIS for GTFS routing and OJP for cross-border segments.

## Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌────────┐
│  React+Vite │────▶│  NestJS          │────▶│ MOTIS  │
│  Frontend   │     │  Orchestrator    │     │ (GTFS) │
│  :5173      │     │  :3000           │     │ :8080  │
└─────────────┘     │  ├─ StationsService      └────────┘
                    │  ├─ StitchingEngine │
                    │  ├─ OJP Client (mock)│──▶ OJP API
                    │  └─ Cache (Redis)   │
                    └──────────────────┘
                            │
                    ┌───────┴───────┐
                    │   PostGIS     │
                    │   :5432       │
                    └───────────────┘
```

## Project Structure

```
Trainscanner/
├── docker-compose.yml          # All 5 services
├── .env.example                # Environment template
│
├── orchestrator/               # NestJS backend (:3000)
│   ├── src/
│   │   ├── routing/            # /api/routes, /api/stations endpoints
│   │   ├── motis/              # MOTIS client (GTFS routing)
│   │   ├── ojp/                # OJP client (mock mode default)
│   │   ├── stations/           # Station resolver (station_map.json)
│   │   ├── stitching/          # Route combiner + ranking
│   │   ├── cache/              # Redis with in-memory fallback
│   │   ├── rate-limiter/       # Token bucket rate limiter
│   │   └── health/             # /health endpoint
│   ├── Dockerfile
│   └── package.json
│
├── frontend/                   # Vite+React (:5173)
│   ├── src/
│   │   ├── components/         # SearchForm, MapView, RouteCard, etc.
│   │   ├── pages/              # SearchPage, PrivacyPage
│   │   ├── services/           # API client
│   │   └── types/              # Shared TypeScript types
│   ├── Dockerfile
│   └── package.json
│
├── data/
│   ├── station_map.json        # 20 DACH hub stations
│   └── gtfs_raw/               # Downloaded GTFS feeds
│       ├── de_fv.zip           # Germany long-distance (ICE/IC)
│       ├── de_rv.zip           # Germany regional (RE/RB/S-Bahn)
│       └── de_nv.zip           # Germany local transit
│
├── data-pipeline/
│   ├── download-gtfs.sh        # GTFS feed downloader
│   ├── gtfs-filter.py          # Rail-only filter script
│   ├── gtfs-explorer/          # Interactive browser-based filter tool
│   │   └── index.html
│   ├── osm-island-extract.sh
│   └── gtfs-diff.sh
│
└── config/
    ├── motis-config.ini        # MOTIS v2 config
    └── init-db.sql             # PostGIS schema + seed data
```

## Key Technologies
- **Backend:** NestJS, TypeScript, Redis, PostGIS
- **Frontend:** React 18, Vite, MapLibre GL JS, TypeScript
- **Routing Engine:** MOTIS v2 (GTFS-based)
- **Cross-border:** OJP API (mock mode for MVP)
- **Infrastructure:** Docker Compose, Node 20 Alpine
- **GTFS Sources:** gtfs.de (DE, CC-BY-4.0), opentransportdata.swiss (CH), data.oebb.at (AT)

## Running the Project

### Quick Start (Docker)
```bash
cp .env.example .env
docker compose up --build
# Frontend: http://localhost:5173
# API:      http://localhost:3000/health
```

### Development (without Docker)
```bash
# Backend
cd orchestrator && npm install && npm run start:dev

# Frontend (separate terminal)
cd frontend && npm install && npm run dev
```

### GTFS Explorer (standalone)
```bash
xdg-open data-pipeline/gtfs-explorer/index.html
# Drag a GTFS .zip onto the page to visualize
```

## Environment Variables
See `.env.example`. Key ones:
- `OJP_MODE=mock` — Use mock OJP data (default, no API key needed)
- `REDIS_URL` — Redis connection (falls back to in-memory)
- `MOTIS_URL` — MOTIS endpoint

## Data Pipeline
1. `bash data-pipeline/download-gtfs.sh` — Download GTFS feeds
2. Open `data-pipeline/gtfs-explorer/index.html` — Visually filter
3. `python3 data-pipeline/gtfs-filter.py <in> <out>` — Filter to rail-only

## Current Status
- [x] Project scaffolding & Docker setup
- [x] NestJS orchestrator with all modules
- [x] React frontend with dark theme & MapLibre
- [x] GTFS download pipeline (DE complete, CH/AT manual)
- [x] Interactive GTFS explorer tool
- [ ] MOTIS integration with real GTFS data
- [ ] OJP live API integration (needs API key)
- [ ] Production deployment
