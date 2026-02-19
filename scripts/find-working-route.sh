#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
API_URL="${API_URL:-http://localhost:3000}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-200}"
TARGET_DATE="${TARGET_DATE:-}"
MOTIS_DATASET_TAG="${MOTIS_DATASET_TAG:-active-gtfs}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --api-url)
      API_URL="${2:-}"
      shift 2
      ;;
    --max-attempts)
      MAX_ATTEMPTS="${2:-}"
      shift 2
      ;;
    --target-date)
      TARGET_DATE="${2:-}"
      shift 2
      ;;
    --help|-h)
      cat <<USAGE
Usage:
  scripts/find-working-route.sh [--api-url <url>] [--max-attempts <n>] [--target-date YYYY-MM-DD]

Finds a working /api/routes combination from active GTFS by trying real scheduled trip pairs.
USAGE
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

export API_URL MAX_ATTEMPTS TARGET_DATE ROOT_DIR MOTIS_DATASET_TAG
python3 - <<'PY'
import csv
import datetime as dt
import io
import json
import os
import sys
import urllib.request
import urllib.error
import zipfile

ROOT_DIR = os.environ['ROOT_DIR']
API_URL = os.environ.get('API_URL', 'http://localhost:3000').rstrip('/')
MAX_ATTEMPTS = int(os.environ.get('MAX_ATTEMPTS', '200'))
TARGET_DATE = os.environ.get('TARGET_DATE', '').strip()
MOTIS_DATASET_TAG = os.environ.get('MOTIS_DATASET_TAG', 'active-gtfs').strip()


def read_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def parse_time_to_iso(date_obj, hhmmss):
    if not hhmmss:
        return None
    parts = hhmmss.split(':')
    if len(parts) != 3:
        return None
    h, m, s = map(int, parts)
    base = dt.datetime(date_obj.year, date_obj.month, date_obj.day)
    value = base + dt.timedelta(hours=h, minutes=m, seconds=s)
    return value.isoformat() + 'Z'


def active_services(calendar_rows, cal_dates_rows, date_str):
    date_obj = dt.datetime.strptime(date_str, '%Y%m%d').date()
    weekday_key = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'][date_obj.weekday()]
    active = set()
    for row in calendar_rows:
        if row['start_date'] <= date_str <= row['end_date'] and row.get(weekday_key) == '1':
            active.add(row['service_id'])
    for row in cal_dates_rows:
        if row.get('date') != date_str:
            continue
        if row.get('exception_type') == '1':
            active.add(row['service_id'])
        elif row.get('exception_type') == '2':
            active.discard(row['service_id'])
    return active


def choose_date(calendar_rows, cal_dates_rows):
    if TARGET_DATE:
        return TARGET_DATE.replace('-', '')

    today = dt.datetime.now(dt.timezone.utc).date()
    for delta in range(0, 21):
        d = today + dt.timedelta(days=delta)
        ds = d.strftime('%Y%m%d')
        if active_services(calendar_rows, cal_dates_rows, ds):
            return ds

    starts = [row['start_date'] for row in calendar_rows if row.get('start_date')]
    if starts:
        return min(starts)

    return today.strftime('%Y%m%d')


active_path = os.path.join(ROOT_DIR, 'state', 'active-gtfs.json')
if not os.path.isfile(active_path):
    active_path = os.path.join(ROOT_DIR, 'config', 'active-gtfs.json')
active = read_json(active_path)
profiles_raw = read_json(os.path.join(ROOT_DIR, 'config', 'gtfs-profiles.json'))
profiles = profiles_raw.get('profiles', profiles_raw)
active_profile = active.get('activeProfile')
if not active_profile or active_profile not in profiles:
    print('ERROR: active profile missing or unknown', file=sys.stderr)
    sys.exit(2)

def resolve_runtime_zip(entry_obj, default_profile):
    runtime = entry_obj.get('runtime') if isinstance(entry_obj, dict) else None
    if not isinstance(runtime, dict):
        return None

    mode = (runtime.get('mode') or runtime.get('source') or 'canonical-export').strip()
    if mode != 'canonical-export':
        print(f"ERROR: unsupported runtime mode '{mode}' in profile '{default_profile}'", file=sys.stderr)
        sys.exit(2)

    artifact_path = (runtime.get('artifactPath') or '').strip()
    if artifact_path:
        return artifact_path

    runtime_profile = (runtime.get('profile') or default_profile).strip()
    requested_as_of = (runtime.get('asOf') or 'latest').strip()
    runtime_root = os.path.join(ROOT_DIR, 'data', 'gtfs', 'runtime', runtime_profile)

    if requested_as_of == 'latest':
        if not os.path.isdir(runtime_root):
            return None
        date_dirs = []
        for name in os.listdir(runtime_root):
            full = os.path.join(runtime_root, name)
            if not os.path.isdir(full):
                continue
            if len(name) == 10 and name[4] == '-' and name[7] == '-' and name.replace('-', '').isdigit():
                zip_candidate = os.path.join(full, 'active-gtfs.zip')
                if os.path.isfile(zip_candidate):
                    date_dirs.append(name)
        if not date_dirs:
            return None
        date_dirs.sort()
        return os.path.join('data', 'gtfs', 'runtime', runtime_profile, date_dirs[-1], 'active-gtfs.zip')

    return os.path.join('data', 'gtfs', 'runtime', runtime_profile, requested_as_of, 'active-gtfs.zip')

zip_rel = (active.get('zipPath') or '').strip()
if zip_rel:
    zip_path_candidate = zip_rel if os.path.isabs(zip_rel) else os.path.join(ROOT_DIR, zip_rel)
    if not os.path.isfile(zip_path_candidate):
        zip_rel = ''

if not zip_rel:
    entry = profiles[active_profile]
    if isinstance(entry, str):
        zip_rel = entry
    elif isinstance(entry, dict):
        zip_rel = (entry.get('zipPath') or entry.get('zip') or '').strip()
        if not zip_rel:
            zip_rel = resolve_runtime_zip(entry, active_profile) or ''
    else:
        zip_rel = ''

if not zip_rel:
    print('ERROR: active profile has no zip path', file=sys.stderr)
    sys.exit(2)

zip_path = zip_rel if os.path.isabs(zip_rel) else os.path.join(ROOT_DIR, zip_rel)
if not os.path.isfile(zip_path):
    print(f'ERROR: GTFS zip not found: {zip_path}', file=sys.stderr)
    sys.exit(2)

with zipfile.ZipFile(zip_path) as zf:
    def read_csv(name):
        if name not in zf.namelist():
            return []
        with zf.open(name) as fp:
            return list(csv.DictReader(io.TextIOWrapper(fp, encoding='utf-8-sig')))

    stops_rows = read_csv('stops.txt')
    trips_rows = read_csv('trips.txt')
    stop_times_rows = read_csv('stop_times.txt')
    calendar_rows = read_csv('calendar.txt')
    cal_dates_rows = read_csv('calendar_dates.txt')

def stop_token(row):
    lat = (row.get('stop_lat') or '').strip()
    lon = (row.get('stop_lon') or '').strip()
    if not lat or not lon:
        return None
    return f'{lat},{lon}'

stops = {r['stop_id']: r.get('stop_name', '').strip() for r in stops_rows}
stop_rows_by_id = {r['stop_id']: r for r in stops_rows}
trip_service = {r['trip_id']: r['service_id'] for r in trips_rows}

target_date = choose_date(calendar_rows, cal_dates_rows)
active_svc = active_services(calendar_rows, cal_dates_rows, target_date)
if not active_svc:
    print(f'ERROR: no active services on {target_date}', file=sys.stderr)
    sys.exit(3)

active_trips = {tid for tid, sid in trip_service.items() if sid in active_svc}

trip_stops = {}
for row in stop_times_rows:
    tid = row['trip_id']
    if tid not in active_trips:
        continue
    trip_stops.setdefault(tid, []).append(row)

candidates = []
for tid, rows in trip_stops.items():
    rows.sort(key=lambda r: int(r.get('stop_sequence') or 0))
    if len(rows) < 2:
        continue

    # sample a few segments per trip: first->last, first->mid, mid->last
    picks = [(0, len(rows) - 1), (0, len(rows) // 2), (len(rows) // 2, len(rows) - 1)]
    used = set()
    for i, j in picks:
        if i >= j:
            continue
        key = (i, j)
        if key in used:
            continue
        used.add(key)

        a = rows[i]
        b = rows[j]
        dep = (a.get('departure_time') or a.get('arrival_time') or '').strip()
        if not dep:
            continue

        origin_id = a['stop_id']
        dest_id = b['stop_id']
        origin_name = stops.get(origin_id, origin_id)
        dest_name = stops.get(dest_id, dest_id)
        origin_row = stop_rows_by_id.get(origin_id, {})
        dest_row = stop_rows_by_id.get(dest_id, {})
        origin_token = stop_token(origin_row) if origin_row else None
        destination_token = stop_token(dest_row) if dest_row else None
        dt_iso = parse_time_to_iso(dt.datetime.strptime(target_date, '%Y%m%d').date(), dep)
        if not dt_iso:
            continue

        candidates.append({
            'trip_id': tid,
            'origin': f'{origin_name} [{origin_id}]',
            'destination': f'{dest_name} [{dest_id}]',
            'origin_token': origin_token,
            'destination_token': destination_token,
            'origin_tagged_id': f'{MOTIS_DATASET_TAG}_{origin_id}',
            'destination_tagged_id': f'{MOTIS_DATASET_TAG}_{dest_id}',
            'datetime': dt_iso
        })

# prioritize deterministic order
candidates.sort(key=lambda c: (c['datetime'], c['origin'], c['destination']))

if not candidates:
    print('ERROR: no candidate pairs generated from GTFS', file=sys.stderr)
    sys.exit(4)


def post_json(url, payload):
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, method='POST')
    req.add_header('Content-Type', 'application/json')
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode('utf-8')
            return resp.status, json.loads(body)
    except urllib.error.URLError as e:
        return None, {'error': f'network_error: {e}'}
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8')
        try:
            parsed = json.loads(body)
        except Exception:
            parsed = {'raw': body}
        return e.code, parsed

attempted = 0
last = None
for cand in candidates:
    if attempted >= MAX_ATTEMPTS:
        break
    attempted += 1
    status, resp = post_json(API_URL + '/api/routes', {
        'origin': cand.get('origin_tagged_id') or cand.get('origin_token') or cand['origin'],
        'destination': cand.get('destination_tagged_id') or cand.get('destination_token') or cand['destination'],
        'datetime': cand['datetime']
    })
    last = (cand, status, resp)

    if status is None and isinstance(resp, dict) and str(resp.get('error', '')).startswith('network_error:'):
        print(json.dumps({
            'found': False,
            'attempts': attempted,
            'target_date': target_date,
            'network_error': resp['error'],
            'hint': f'Cannot reach {API_URL}. Ensure stack is running and script is executed on the same host as Docker services.'
        }, indent=2, ensure_ascii=False))
        sys.exit(6)

    if status == 200:
        route = resp.get('route') or {}
        itineraries = route.get('itineraries') or []
        direct = route.get('direct') or []
        if itineraries or direct:
            print(json.dumps({
                'found': True,
                'attempts': attempted,
                'target_date': target_date,
                'candidate': cand,
                'status': status,
                'summary': {
                    'itineraries': len(itineraries),
                    'direct': len(direct)
                },
                'curl': (
                    "curl -s -X POST " + API_URL + "/api/routes "
                    "-H 'Content-Type: application/json' "
                    "-d '" + json.dumps({
                        'origin': cand.get('origin_tagged_id') or cand.get('origin_token') or cand['origin'],
                        'destination': cand.get('destination_tagged_id') or cand.get('destination_token') or cand['destination'],
                        'datetime': cand['datetime']
                    }).replace("'", "'\\''") + "'"
                )
            }, indent=2, ensure_ascii=False))
            sys.exit(0)

print(json.dumps({
    'found': False,
    'attempts': attempted,
    'target_date': target_date,
    'last': {
        'candidate': last[0] if last else None,
        'status': last[1] if last else None,
        'response': last[2] if last else None
    },
    'hint': 'No non-empty itinerary found in tested candidates. Increase --max-attempts or inspect last.response.'
}, indent=2, ensure_ascii=False))
sys.exit(5)
PY
