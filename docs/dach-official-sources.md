# DACH Official Data Sources (DE/AT/CH)

This layer covers official-source discovery and raw retrieval only.
No canonical ETL transformation is implemented here.

## Policy

- Countries in scope: `DE`, `AT`, `CH`
- Official sources only (operator/government/NAP portals)
- NeTEx is preferred
- GTFS is used only with explicit `fallbackReason`
- Runtime must not auto-fallback from NeTEx to GTFS for the same source
- Raw files are stored locally under `data/raw/<country>/<provider>/<format>/<YYYY-MM-DD>/`

## Selected Sources

| id | Country | Provider | Format | Official portal | Download strategy | Last verified (UTC) |
|---|---|---|---|---|---|---|
| `de_delfi_sollfahrplandaten_netex` | DE | DELFI e.V. | NeTEx | https://www.opendata-oepnv.de/ht/de/organisation/delfi/startseite | authenticated detail page resolution | `2026-02-19T14:42:41Z` |
| `at_oebb_mmtis_netex` | AT | OeBB-Infrastruktur AG | NeTEx | https://data.oebb.at/de/datensaetze~datenbereitstellung_delegierte_verordnung_eu_2024-490~ | resolve `data-download` NetEx ZIP from official dataset page | `2026-02-19T14:42:41Z` |
| `ch_opentransportdata_timetable_netex` | CH | Open Data Mobility Switzerland | NeTEx | https://data.opentransportdata.swiss/en/dataset/timetablenetex_2026 | resolve latest resource `/download/...zip` from official CKAN dataset page | `2026-02-19T14:42:41Z` |

## GTFS fallback justification

- No GTFS fallback is currently configured in `config/dach-data-sources.json`.
- DE now uses authenticated DELFI NeTEx access as the primary source.

## License and attribution summary

- `de_delfi_sollfahrplandaten_netex`:
  - License: CC BY (current version)
  - Attribution: `Datenquelle: DELFI e.V. via OpenData OePNV Deutschland.`
- `at_oebb_mmtis_netex`:
  - License: CC BY 3.0 AT
  - Attribution: `Datenquelle: OeBB-Infrastruktur AG` (with link to `http://infrastruktur.oebb.at/de/`)
- `ch_opentransportdata_timetable_netex`:
  - Terms/license: opentransportdata.swiss terms of use
  - Attribution: cite `opentransportdata.swiss` as source URL for raw data

## Commands

Validate config + policy + reachability:

```bash
scripts/data/verify-dach-sources.sh
```

Quick one-by-one verification:

```bash
scripts/data/verify-dach-sources.sh --source-id de_delfi_sollfahrplandaten_netex
scripts/data/verify-dach-sources.sh --source-id at_oebb_mmtis_netex
scripts/data/verify-dach-sources.sh --source-id ch_opentransportdata_timetable_netex
```

Fetch latest from all configured sources:

```bash
scripts/data/fetch-dach-sources.sh
```

Quick one-by-one fetch:

```bash
scripts/data/fetch-dach-sources.sh --source-id de_delfi_sollfahrplandaten_netex
scripts/data/fetch-dach-sources.sh --source-id at_oebb_mmtis_netex
scripts/data/fetch-dach-sources.sh --source-id ch_opentransportdata_timetable_netex
```

Fetch deterministic snapshot as-of date:

```bash
scripts/data/fetch-dach-sources.sh --as-of 2026-02-01
```

Fetch only Austria:

```bash
scripts/data/fetch-dach-sources.sh --country AT
```

Fetch one source:

```bash
scripts/data/fetch-dach-sources.sh --source-id ch_opentransportdata_timetable_netex
```

Verify one source with as-of resolution:

```bash
scripts/data/verify-dach-sources.sh --source-id at_oebb_mmtis_netex --as-of 2026-02-01
```

Example DE DELFI automatic login setup:

```bash
cat >> .env <<'EOF'
DE_DELFI_SOLLFAHRPLANDATEN_NETEX_USERNAME='your_username'
DE_DELFI_SOLLFAHRPLANDATEN_NETEX_PASSWORD='your_password'
EOF
```

Fallback cookie-based setup (if you do not want to store credentials):

```bash
cat >> .env <<'EOF'
DE_DELFI_SOLLFAHRPLANDATEN_NETEX_COOKIE='fe_typo_user=...; cookieconsent_status=dismiss'
EOF
```

## Secrets

Place secrets in `.env` only (not in Git). Supported env names for non-public sources:

- `<SOURCE_ID_UPPER>_API_KEY` or `DACH_API_KEY`
- `<SOURCE_ID_UPPER>_TOKEN` or `DACH_TOKEN`
- `<SOURCE_ID_UPPER>_COOKIE` or `DACH_COOKIE`
- `<SOURCE_ID_UPPER>_COOKIE_FILE` or `DACH_COOKIE_FILE`
- `<SOURCE_ID_UPPER>_HEADER` or `DACH_HEADER`
- `<SOURCE_ID_UPPER>_USERNAME` / `<SOURCE_ID_UPPER>_PASSWORD`
- `<SOURCE_ID_UPPER>_LOGIN_URL` (optional)

For DE DELFI NeTEx, provide either login credentials or cookie/header auth.

Verification note:

- `de_delfi_sollfahrplandaten_netex` may emit a warning that the resolved URL filename does not explicitly contain `netex`. This is expected for DELFI's current naming (`...fahrplaene_gesamtdeutschland.zip`) and does not indicate non-NeTEx format.
