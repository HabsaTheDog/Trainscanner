#!/bin/bash
# ============================================
# GTFS Feed Downloader — DACH Region
# ============================================
# Downloads free GTFS feeds for Germany, Switzerland, and Austria.
# All feeds are CC-BY-4.0 licensed.
#
# Usage:
#   ./download-gtfs.sh [--all | --de | --ch | --at]
#   ./download-gtfs.sh [DATA_DIR] [--all | --de | --ch | --at]

set -euo pipefail

DEFAULT_DATA_DIR="$(dirname "$0")/../data/gtfs_raw"
DATA_DIR="$DEFAULT_DATA_DIR"
MODE="--all"

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  echo "Usage:"
  echo "  $0 [--all | --de | --ch | --at]"
  echo "  $0 [DATA_DIR] [--all | --de | --ch | --at]"
  exit 0
fi

if [[ -n "${1:-}" ]]; then
  if [[ "$1" == --* ]]; then
    MODE="$1"
  else
    DATA_DIR="$1"
    MODE="${2:---all}"
  fi
fi

mkdir -p "$DATA_DIR"

echo "📁 Download directory: $DATA_DIR"
echo ""

# ── Germany (gtfs.de — DELFI e.V., CC-BY-4.0, 7-day rolling) ──

download_de() {
  echo "🇩🇪 Germany — Long Distance Rail (ICE/IC/EC/EN)"
  curl -L -o "$DATA_DIR/de_fv.zip" \
    "https://download.gtfs.de/germany/fv_free/latest.zip"
  echo "   ✅ de_fv.zip ($(du -h "$DATA_DIR/de_fv.zip" | cut -f1))"

  echo "🇩🇪 Germany — Regional Rail (RE/RB/IRE/S-Bahn)"
  curl -L -o "$DATA_DIR/de_rv.zip" \
    "https://download.gtfs.de/germany/rv_free/latest.zip"
  echo "   ✅ de_rv.zip ($(du -h "$DATA_DIR/de_rv.zip" | cut -f1))"

  echo "🇩🇪 Germany — Local Transit (U-Bahn/Tram/Bus/Ferry)"
  curl -L -o "$DATA_DIR/de_nv.zip" \
    "https://download.gtfs.de/germany/nv_free/latest.zip"
  echo "   ✅ de_nv.zip ($(du -h "$DATA_DIR/de_nv.zip" | cut -f1))"
}

# ── Switzerland (opentransportdata.swiss — requires browser-based registration) ──

download_ch() {
  echo "🇨🇭 Switzerland — Full GTFS (opentransportdata.swiss)"
  echo ""
  echo "   ⚠️  MANUAL DOWNLOAD REQUIRED — the Swiss feed requires registration."
  echo ""
  echo "   Steps:"
  echo "   1. Go to: https://opentransportdata.swiss"
  echo "   2. Search for 'timetable GTFS' or 'Fahrplan GTFS'"
  echo "   3. Register / log in if required"
  echo "   4. Download the GTFS ZIP file"
  echo "   5. Save it as: $DATA_DIR/ch_full.zip"
  echo ""
  if [ -f "$DATA_DIR/ch_full.zip" ]; then
    if unzip -tq "$DATA_DIR/ch_full.zip" >/dev/null 2>&1; then
      echo "   ✅ ch_full.zip exists and is a valid ZIP ($(du -h "$DATA_DIR/ch_full.zip" | cut -f1))"
    else
      echo "   ❌ ch_full.zip exists but is NOT a valid ZIP (likely an HTML error page). Please re-download."
      rm -f "$DATA_DIR/ch_full.zip"
    fi
  else
    echo "   ⏳ Not downloaded yet."
  fi
}

# ── Austria (data.oebb.at — requires browser-based terms acceptance, CC-BY-4.0) ──

download_at() {
  echo "🇦🇹 Austria — ÖBB GTFS (data.oebb.at)"
  echo ""
  echo "   ⚠️  MANUAL DOWNLOAD REQUIRED — ÖBB requires terms acceptance in browser."
  echo ""
  echo "   Steps:"
  echo "   1. Go to: https://data.oebb.at/oebb?dataset=fahrplan"
  echo "   2. Accept the Terms of Service (CC-BY-4.0)"
  echo "   3. Download the GTFS ZIP file"
  echo "   4. Save it as: $DATA_DIR/at_oebb.zip"
  echo ""
  if [ -f "$DATA_DIR/at_oebb.zip" ]; then
    if unzip -tq "$DATA_DIR/at_oebb.zip" >/dev/null 2>&1; then
      echo "   ✅ at_oebb.zip exists and is a valid ZIP ($(du -h "$DATA_DIR/at_oebb.zip" | cut -f1))"
    else
      echo "   ❌ at_oebb.zip exists but is NOT a valid ZIP (likely an HTML error page). Please re-download."
      rm -f "$DATA_DIR/at_oebb.zip"
    fi
  else
    echo "   ⏳ Not downloaded yet."
  fi
}

# ── Validation ──

validate_downloads() {
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "🔍 Validating downloads..."
  echo ""
  for f in "$DATA_DIR"/*.zip; do
    [ -f "$f" ] || continue
    basename=$(basename "$f")
    if unzip -tq "$f" >/dev/null 2>&1; then
      # Check if it contains GTFS files
      if unzip -l "$f" 2>/dev/null | grep -q "routes.txt\|stops.txt"; then
        echo "   ✅ $basename — valid GTFS ($(du -h "$f" | cut -f1))"
      else
        echo "   ⚠️  $basename — valid ZIP but missing GTFS files (routes.txt/stops.txt)"
      fi
    else
      echo "   ❌ $basename — NOT a valid ZIP file! Removing..."
      rm -f "$f"
    fi
  done
}

# ── Main ──

case "$MODE" in
  --de) download_de ;;
  --ch) download_ch ;;
  --at) download_at ;;
  --all|*)
    download_de
    echo ""
    download_ch
    echo ""
    download_at
    ;;
esac

validate_downloads

echo ""
echo "📦 Final state of $DATA_DIR:"
ls -lh "$DATA_DIR"/*.zip 2>/dev/null || echo "   No zip files found."
echo ""
echo "Next steps:"
echo "  1. Unzip and inspect: cd $DATA_DIR && for f in *.zip; do mkdir -p \${f%.zip} && unzip -o \$f -d \${f%.zip}; done"
echo "  2. Run the interactive filter: xdg-open data-pipeline/gtfs-explorer/index.html"
echo "  3. CLI filter (zip -> zip): python3 data-pipeline/gtfs-filter.py <input.zip> <output.zip>"
