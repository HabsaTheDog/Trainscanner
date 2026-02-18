#!/bin/bash
# ============================================
# GTFS Diff — Station Change Detection
# ============================================
# Compares old and new versions of stops.txt
# to detect NEW, REMOVED, and MODIFIED stations.
#
# Usage:
#   ./gtfs-diff.sh <old_stops.txt> <new_stops.txt>
# ============================================

set -euo pipefail

OLD_STOPS="${1:?Usage: $0 <old_stops.txt> <new_stops.txt>}"
NEW_STOPS="${2:?Usage: $0 <old_stops.txt> <new_stops.txt>}"

echo "=========================================="
echo "  GTFS Station Diff Report"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="
echo ""

python3 -c "
import csv
import sys
from datetime import datetime

def load_stops(filepath):
    stops = {}
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            stop_id = row['stop_id']
            stops[stop_id] = {
                'name': row.get('stop_name', ''),
                'lat': row.get('stop_lat', ''),
                'lon': row.get('stop_lon', ''),
            }
    return stops

old_stops = load_stops('$OLD_STOPS')
new_stops = load_stops('$NEW_STOPS')

old_ids = set(old_stops.keys())
new_ids = set(new_stops.keys())

added = new_ids - old_ids
removed = old_ids - new_ids
common = old_ids & new_ids

modified = []
for sid in common:
    old = old_stops[sid]
    new = new_stops[sid]
    changes = []
    if old['name'] != new['name']:
        changes.append(f\"name: '{old['name']}' → '{new['name']}'\")
    if old['lat'] != new['lat'] or old['lon'] != new['lon']:
        changes.append(f\"coords: ({old['lat']},{old['lon']}) → ({new['lat']},{new['lon']})\")
    if changes:
        modified.append((sid, changes))

# Report
print(f'Old stops: {len(old_ids)}')
print(f'New stops: {len(new_ids)}')
print()

if added:
    print(f'🟢 NEW stations ({len(added)}) — needs mapping:')
    for sid in sorted(added):
        s = new_stops[sid]
        print(f\"  [{sid}] {s['name']} ({s['lat']}, {s['lon']})\")
    print()

if removed:
    print(f'🔴 REMOVED stations ({len(removed)}) — check mapping:')
    for sid in sorted(removed):
        s = old_stops[sid]
        print(f\"  [{sid}] {s['name']}\")
    print()

if modified:
    print(f'🟡 MODIFIED stations ({len(modified)}) — verify:')
    for sid, changes in sorted(modified):
        print(f'  [{sid}] {\"  |  \".join(changes)}')
    print()

if not added and not removed and not modified:
    print('✅ No changes detected.')

total_changes = len(added) + len(removed) + len(modified)
print(f'Total changes requiring review: {total_changes}')
"
