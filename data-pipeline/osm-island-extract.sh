#!/bin/bash
# ============================================
# OSM Island Extraction
# ============================================
# Extracts 2km bounding boxes around all GTFS stations
# from a full europe-latest.osm.pbf file.
#
# Prerequisites:
#   - osmium-tool (apt install osmium-tool)
#   - python3 with csv module (stdlib)
#
# Usage:
#   ./osm-island-extract.sh <stops.txt> <europe.osm.pbf> <output.pbf>
# ============================================

set -euo pipefail

STOPS_FILE="${1:?Usage: $0 <stops.txt> <europe.osm.pbf> <output.pbf>}"
INPUT_PBF="${2:?Usage: $0 <stops.txt> <europe.osm.pbf> <output.pbf>}"
OUTPUT_PBF="${3:?Usage: $0 <stops.txt> <europe.osm.pbf> <output.pbf>}"

RADIUS_KM=2
# ~0.018 degrees latitude per km
DEG_PER_KM=0.018

BBOX_FILE=$(mktemp /tmp/osm_bboxes_XXXXXX.txt)

echo "📍 Extracting station coordinates from $STOPS_FILE..."

# Extract bounding boxes from stops.txt
python3 -c "
import csv, sys

radius = $RADIUS_KM * $DEG_PER_KM
with open('$STOPS_FILE', 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    bboxes = []
    for row in reader:
        lat = float(row.get('stop_lat', 0))
        lon = float(row.get('stop_lon', 0))
        if lat == 0 and lon == 0:
            continue
        # bbox format for osmium: left,bottom,right,top
        bbox = f'{lon-radius},{lat-radius},{lon+radius},{lat+radius}'
        bboxes.append(bbox)

    print(f'Found {len(bboxes)} stations', file=sys.stderr)
    for b in bboxes:
        print(b)
" > "$BBOX_FILE"

STATION_COUNT=$(wc -l < "$BBOX_FILE")
echo "📦 Processing $STATION_COUNT station bounding boxes..."

# Extract all islands in one pass using osmium
# Build the --bbox arguments
BBOX_ARGS=""
while IFS= read -r bbox; do
    BBOX_ARGS="$BBOX_ARGS --bbox $bbox"
done < "$BBOX_FILE"

echo "✂️  Cutting OSM islands from $INPUT_PBF..."
# Use osmium extract with a config file approach for many bboxes
CONFIG_FILE=$(mktemp /tmp/osm_config_XXXXXX.json)

python3 -c "
import json

bboxes = []
with open('$BBOX_FILE') as f:
    for i, line in enumerate(f):
        parts = line.strip().split(',')
        if len(parts) == 4:
            bboxes.append({
                'output': f'/tmp/island_{i}.pbf',
                'bbox': [float(x) for x in parts]
            })

config = {
    'directory': '/tmp/',
    'extracts': bboxes[:500]  # Limit to prevent memory issues
}

with open('$CONFIG_FILE', 'w') as f:
    json.dump(config, f)

print(f'Configured {len(bboxes[:500])} extracts')
"

# For large numbers of stations, merge iteratively
if [ "$STATION_COUNT" -gt 500 ]; then
    echo "⚠️  Many stations ($STATION_COUNT). Using simplified single-bbox approach..."
    # Calculate overall bounding box
    OVERALL_BBOX=$(python3 -c "
with open('$BBOX_FILE') as f:
    min_lon, min_lat, max_lon, max_lat = 180, 90, -180, -90
    for line in f:
        parts = line.strip().split(',')
        if len(parts) == 4:
            l, b, r, t = [float(x) for x in parts]
            min_lon = min(min_lon, l)
            min_lat = min(min_lat, b)
            max_lon = max(max_lon, r)
            max_lat = max(max_lat, t)
    print(f'{min_lon},{min_lat},{max_lon},{max_lat}')
")
    osmium extract --bbox "$OVERALL_BBOX" -o "$OUTPUT_PBF" --overwrite "$INPUT_PBF"
else
    osmium extract --config "$CONFIG_FILE" "$INPUT_PBF" 2>/dev/null || true
    # Merge all island files
    ISLAND_FILES=$(find /tmp -name "island_*.pbf" 2>/dev/null | sort)
    if [ -n "$ISLAND_FILES" ]; then
        osmium merge $ISLAND_FILES -o "$OUTPUT_PBF" --overwrite
        rm -f /tmp/island_*.pbf
    fi
fi

# Cleanup
rm -f "$BBOX_FILE" "$CONFIG_FILE"

OUTPUT_SIZE=$(du -h "$OUTPUT_PBF" | cut -f1)
echo "✅ Done! Output: $OUTPUT_PBF ($OUTPUT_SIZE)"
