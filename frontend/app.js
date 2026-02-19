const profileSelect = document.getElementById('profileSelect');
const activateBtn = document.getElementById('activateBtn');
const statusBadge = document.getElementById('statusBadge');
const statusMessage = document.getElementById('statusMessage');
const routeForm = document.getElementById('routeForm');
const routeBtn = document.getElementById('routeBtn');
const routeResult = document.getElementById('routeResult');
const routeSummary = document.getElementById('routeSummary');
const routeMapStatus = document.getElementById('routeMapStatus');
const routeMapElement = document.getElementById('routeMap');
const datetimeInput = document.getElementById('datetime');
const originInput = document.getElementById('origin');
const destinationInput = document.getElementById('destination');
const stationSuggestions = document.getElementById('stationSuggestions');

let lastStatusState = null;
let stationSuggestionTimer = null;
let stationFetchCounter = 0;
const stationTokenByValue = new Map();
const stationTokenById = new Map();

let routeMap = null;
let routeMapReady = false;
let routeLineMarkers = [];
let pendingRouteGeoJson = null;
let pendingRouteBounds = null;

const ROUTE_SOURCE_ID = 'route-lines';
const ROUTE_LAYER_ID = 'route-lines';

const MODE_COLORS = {
  WALK: '#64748b',
  BIKE: '#7c3aed',
  CAR: '#334155',
  BUS: '#dc2626',
  TRAM: '#ea580c',
  SUBWAY: '#2563eb',
  FERRY: '#0d9488',
  REGIONAL_RAIL: '#0e7490',
  REGIONAL_FAST_RAIL: '#0e7490',
  LONG_DISTANCE_RAIL: '#1d4ed8',
  LONG_DISTANCE_HIGH_SPEED_RAIL: '#1d4ed8'
};

function pretty(payload) {
  return JSON.stringify(payload, null, 2);
}

function clearElement(node) {
  while (node.firstChild) {
    node.removeChild(node.firstChild);
  }
}

function formatTime(value) {
  if (!value) {
    return '--:--';
  }
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) {
    return String(value);
  }
  return dt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

function formatDateTime(value) {
  if (!value) {
    return '-';
  }
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) {
    return String(value);
  }
  return dt.toLocaleString();
}

function durationToText(seconds) {
  const total = Number(seconds || 0);
  if (!Number.isFinite(total) || total <= 0) {
    return '0m';
  }
  const hours = Math.floor(total / 3600);
  const minutes = Math.round((total % 3600) / 60);
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  return `${minutes}m`;
}

function setStatus(status) {
  const state = status.state || 'idle';
  statusBadge.textContent = state;
  statusBadge.className = `badge ${state}`;

  const profile = status.activeProfile || status.requestedProfile || 'none';
  const details = {
    state,
    activeProfile: profile,
    message: status.message || '',
    updatedAt: status.updatedAt || null,
    error: status.error || null
  };
  statusMessage.textContent = pretty(details);

  const motisReady = state === 'ready';
  routeBtn.disabled = !motisReady;
  routeForm.querySelectorAll('input').forEach((input) => {
    input.disabled = !motisReady;
  });
}

function parseBracketId(value) {
  const input = String(value || '').trim();
  const match = input.match(/\[(.+?)\]\s*$/);
  return match ? match[1].trim() : '';
}

function resolveStationToken(rawValue) {
  const input = String(rawValue || '').trim();
  if (!input) {
    return '';
  }

  if (/^-?\d+(?:\.\d+)?\s*,\s*-?\d+(?:\.\d+)?$/.test(input)) {
    return input.replace(/\s+/g, '');
  }

  const direct = stationTokenByValue.get(input);
  if (direct) {
    return direct;
  }

  const bracketId = parseBracketId(input);
  if (bracketId && stationTokenById.has(bracketId)) {
    return stationTokenById.get(bracketId);
  }

  return input;
}

function renderStationSuggestions(stations) {
  if (!stationSuggestions) {
    return;
  }
  stationSuggestions.innerHTML = '';
  stations.forEach((station) => {
    if (station && station.value && station.token) {
      stationTokenByValue.set(station.value, station.token);
    }
    if (station && station.id && station.token) {
      stationTokenById.set(station.id, station.token);
    }
    const option = document.createElement('option');
    option.value = station.value;
    option.textContent = station.value;
    stationSuggestions.appendChild(option);
  });
}

async function loadStationSuggestions(query) {
  const requestId = ++stationFetchCounter;
  const q = (query || '').trim();
  const url = `/api/gtfs/stations?limit=80&q=${encodeURIComponent(q)}`;

  try {
    const payload = await fetchJson(url);
    if (requestId !== stationFetchCounter) {
      return;
    }
    renderStationSuggestions(payload.stations || []);
  } catch (err) {
    if (requestId !== stationFetchCounter) {
      return;
    }
    renderStationSuggestions([]);
  }
}

function scheduleStationSuggestions(query) {
  if (stationSuggestionTimer) {
    clearTimeout(stationSuggestionTimer);
  }
  stationSuggestionTimer = setTimeout(() => {
    loadStationSuggestions(query);
  }, 120);
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    const err = new Error(data.error || `Request failed (${response.status})`);
    err.payload = data;
    err.status = response.status;
    throw err;
  }
  return data;
}

async function loadProfiles() {
  const payload = await fetchJson('/api/gtfs/profiles');
  profileSelect.innerHTML = '';

  if (!payload.profiles || payload.profiles.length === 0) {
    const option = document.createElement('option');
    option.value = '';
    option.textContent = 'No profiles configured';
    profileSelect.appendChild(option);
    activateBtn.disabled = true;
    return;
  }

  payload.profiles.forEach((profile) => {
    const option = document.createElement('option');
    option.value = profile.name;
    option.textContent = `${profile.name}${profile.exists ? '' : ' (missing file)'}`;
    if (payload.activeProfile && payload.activeProfile === profile.name) {
      option.selected = true;
    }
    profileSelect.appendChild(option);
  });

  activateBtn.disabled = false;
}

async function loadStatus() {
  try {
    const status = await fetchJson('/api/gtfs/status');
    setStatus(status);

    if (status.state !== lastStatusState) {
      lastStatusState = status.state;
      if (status.state === 'ready') {
        stationTokenByValue.clear();
        stationTokenById.clear();
        await loadProfiles();
        scheduleStationSuggestions('');
      }
    }
  } catch (err) {
    setStatus({
      state: 'failed',
      message: err.message,
      error: err.message,
      updatedAt: new Date().toISOString()
    });
  }
}

function setRawResult(payload) {
  routeResult.textContent = pretty(payload);
}

function protomapsStyleUrl() {
  const key = String(window.PROTOMAPS_API_KEY || '').trim();
  if (!key) {
    return null;
  }
  return `https://api.protomaps.com/styles/v4/light/en.json?key=${encodeURIComponent(key)}`;
}

function mapStyleUrl() {
  const explicit = String(window.MAP_STYLE_URL || '').trim();
  if (explicit) {
    return explicit;
  }

  const proto = protomapsStyleUrl();
  if (proto) {
    return proto;
  }

  // Fallback when no Protomaps key is configured.
  return 'https://tiles.openfreemap.org/styles/liberty';
}

function clearMapMarkers() {
  routeLineMarkers.forEach((marker) => marker.remove());
  routeLineMarkers = [];
}

function ensureRouteSource() {
  if (!routeMapReady || !routeMap) {
    return;
  }

  if (!routeMap.getSource(ROUTE_SOURCE_ID)) {
    routeMap.addSource(ROUTE_SOURCE_ID, {
      type: 'geojson',
      data: {
        type: 'FeatureCollection',
        features: []
      }
    });
  }

  if (!routeMap.getLayer(ROUTE_LAYER_ID)) {
    routeMap.addLayer({
      id: ROUTE_LAYER_ID,
      type: 'line',
      source: ROUTE_SOURCE_ID,
      paint: {
        'line-color': ['coalesce', ['get', 'color'], '#2563eb'],
        'line-width': 4,
        'line-opacity': 0.9
      }
    });
  }
}

function applyPendingRouteData() {
  if (!routeMapReady || !routeMap) {
    return;
  }

  ensureRouteSource();
  const source = routeMap.getSource(ROUTE_SOURCE_ID);
  if (source && pendingRouteGeoJson) {
    source.setData(pendingRouteGeoJson);
  }

  if (pendingRouteBounds && pendingRouteBounds.count >= 2) {
    routeMap.fitBounds(
      [
        [pendingRouteBounds.minLon, pendingRouteBounds.minLat],
        [pendingRouteBounds.maxLon, pendingRouteBounds.maxLat]
      ],
      { padding: 40, maxZoom: 13 }
    );
  } else if (pendingRouteBounds && pendingRouteBounds.count === 1) {
    routeMap.easeTo({
      center: [pendingRouteBounds.minLon, pendingRouteBounds.minLat],
      zoom: 11
    });
  }
}

function ensureMap() {
  if (routeMap) {
    return true;
  }

  if (typeof window.maplibregl === 'undefined' || !routeMapElement) {
    routeMapStatus.textContent = 'Map library unavailable.';
    return false;
  }

  routeMap = new window.maplibregl.Map({
    container: routeMapElement,
    style: mapStyleUrl(),
    center: [10, 51],
    zoom: 5
  });

  routeMap.addControl(new window.maplibregl.NavigationControl({ showCompass: false }), 'top-right');

  routeMap.on('load', () => {
    routeMapReady = true;
    ensureRouteSource();
    applyPendingRouteData();

    if (String(window.PROTOMAPS_API_KEY || '').trim()) {
      routeMapStatus.textContent = 'Map ready (MapLibre + Protomaps style).';
    } else {
      routeMapStatus.textContent = 'Map ready (MapLibre; fallback style, no Protomaps key set).';
    }
  });

  routeMap.on('error', (event) => {
    const message = event && event.error && event.error.message ? event.error.message : 'Unknown map error';
    routeMapStatus.textContent = `Map error: ${message}`;
  });

  routeMapStatus.textContent = 'Map initializing...';
  return true;
}

function clearMap() {
  pendingRouteGeoJson = {
    type: 'FeatureCollection',
    features: []
  };
  pendingRouteBounds = null;
  clearMapMarkers();

  if (!routeMap || !routeMapReady) {
    return;
  }

  ensureRouteSource();
  const source = routeMap.getSource(ROUTE_SOURCE_ID);
  if (source) {
    source.setData(pendingRouteGeoJson);
  }
}

function decodePolyline(encoded, precision) {
  const coords = [];
  if (!encoded || typeof encoded !== 'string') {
    return coords;
  }

  const factor = Math.pow(10, Number.isFinite(precision) ? precision : 5);
  let index = 0;
  let lat = 0;
  let lon = 0;

  while (index < encoded.length) {
    let result = 0;
    let shift = 0;
    let byte = 0;

    do {
      byte = encoded.charCodeAt(index++) - 63;
      result |= (byte & 0x1f) << shift;
      shift += 5;
    } while (byte >= 0x20 && index < encoded.length + 1);

    lat += result & 1 ? ~(result >> 1) : result >> 1;

    result = 0;
    shift = 0;
    do {
      byte = encoded.charCodeAt(index++) - 63;
      result |= (byte & 0x1f) << shift;
      shift += 5;
    } while (byte >= 0x20 && index < encoded.length + 1);

    lon += result & 1 ? ~(result >> 1) : result >> 1;

    coords.push([lon / factor, lat / factor]);
  }

  return coords;
}

function legLineCoordinates(leg) {
  if (leg && leg.legGeometry && leg.legGeometry.points) {
    try {
      const precision = Number(leg.legGeometry.precision);
      const decoded = decodePolyline(leg.legGeometry.points, precision);
      if (decoded.length >= 2) {
        return decoded;
      }
    } catch {
      // fallback below
    }
  }

  if (
    leg &&
    leg.from &&
    leg.to &&
    Number.isFinite(leg.from.lat) &&
    Number.isFinite(leg.from.lon) &&
    Number.isFinite(leg.to.lat) &&
    Number.isFinite(leg.to.lon)
  ) {
    return [
      [leg.from.lon, leg.from.lat],
      [leg.to.lon, leg.to.lat]
    ];
  }

  return [];
}

function updateBounds(bounds, lon, lat) {
  if (!Number.isFinite(lon) || !Number.isFinite(lat)) {
    return;
  }
  bounds.minLat = Math.min(bounds.minLat, lat);
  bounds.maxLat = Math.max(bounds.maxLat, lat);
  bounds.minLon = Math.min(bounds.minLon, lon);
  bounds.maxLon = Math.max(bounds.maxLon, lon);
  bounds.count += 1;
}

function drawRouteMap(payload) {
  if (!ensureMap()) {
    return;
  }

  clearMap();

  const route = (payload && payload.route) || {};
  const itineraries = Array.isArray(route.itineraries) ? route.itineraries : [];
  const direct = Array.isArray(route.direct) ? route.direct : [];
  const selected = itineraries.length > 0 ? itineraries[0] : direct.length > 0 ? direct[0] : null;

  if (!selected || !Array.isArray(selected.legs) || selected.legs.length === 0) {
    routeMapStatus.textContent = 'No drawable itinerary found for this query.';
    return;
  }

  const features = [];
  const bounds = {
    minLat: Infinity,
    maxLat: -Infinity,
    minLon: Infinity,
    maxLon: -Infinity,
    count: 0
  };

  selected.legs.forEach((leg, idx) => {
    const mode = String(leg.mode || '').toUpperCase();
    const color = MODE_COLORS[mode] || '#2563eb';
    const coordinates = legLineCoordinates(leg);

    if (coordinates.length >= 2) {
      coordinates.forEach((point) => updateBounds(bounds, point[0], point[1]));
      features.push({
        type: 'Feature',
        properties: {
          color,
          mode,
          idx
        },
        geometry: {
          type: 'LineString',
          coordinates
        }
      });
    }
  });

  const from = route.from;
  const to = route.to;

  clearMapMarkers();
  if (from && Number.isFinite(from.lon) && Number.isFinite(from.lat)) {
    updateBounds(bounds, from.lon, from.lat);
    const marker = new window.maplibregl.Marker({ color: '#16a34a' })
      .setLngLat([from.lon, from.lat])
      .setPopup(new window.maplibregl.Popup({ closeButton: false, offset: 24 }).setText(`From: ${from.name || 'Origin'}`));
    marker.addTo(routeMap);
    routeLineMarkers.push(marker);
  }

  if (to && Number.isFinite(to.lon) && Number.isFinite(to.lat)) {
    updateBounds(bounds, to.lon, to.lat);
    const marker = new window.maplibregl.Marker({ color: '#dc2626' })
      .setLngLat([to.lon, to.lat])
      .setPopup(new window.maplibregl.Popup({ closeButton: false, offset: 24 }).setText(`To: ${to.name || 'Destination'}`));
    marker.addTo(routeMap);
    routeLineMarkers.push(marker);
  }

  pendingRouteGeoJson = {
    type: 'FeatureCollection',
    features
  };
  pendingRouteBounds = bounds;

  applyPendingRouteData();
  routeMapStatus.textContent = 'Showing first itinerary on map.';
}

function renderRouteError(payload) {
  clearElement(routeSummary);
  const title = document.createElement('p');
  title.className = 'summary-title';
  title.textContent = 'Route query failed.';
  routeSummary.appendChild(title);

  const detail = document.createElement('p');
  detail.className = 'muted';
  detail.textContent = (payload && payload.error) || 'Unknown error';
  routeSummary.appendChild(detail);

  routeMapStatus.textContent = 'Map not updated due to query error.';
  clearMap();
}

function renderRouteSummary(payload) {
  clearElement(routeSummary);

  if (!payload || !payload.ok || !payload.route) {
    const fallback = document.createElement('p');
    fallback.className = 'muted';
    fallback.textContent = 'No route data available.';
    routeSummary.appendChild(fallback);
    return;
  }

  const route = payload.route;
  const itineraries = Array.isArray(route.itineraries) ? route.itineraries : [];
  const direct = Array.isArray(route.direct) ? route.direct : [];

  const fromName =
    (route.from && route.from.name) ||
    (payload.routeRequestResolved && payload.routeRequestResolved.origin && payload.routeRequestResolved.origin.input) ||
    'Origin';
  const toName =
    (route.to && route.to.name) ||
    (payload.routeRequestResolved && payload.routeRequestResolved.destination && payload.routeRequestResolved.destination.input) ||
    'Destination';

  const title = document.createElement('p');
  title.className = 'summary-title';
  title.textContent = `${fromName} -> ${toName}`;
  routeSummary.appendChild(title);

  const stats = document.createElement('p');
  stats.className = 'muted';
  stats.textContent = `${itineraries.length} itineraries, ${direct.length} direct options`;
  routeSummary.appendChild(stats);

  if (itineraries.length === 0) {
    const none = document.createElement('p');
    none.className = 'muted';
    none.textContent = 'No itineraries found for this query window.';
    routeSummary.appendChild(none);
    return;
  }

  const list = document.createElement('ol');
  list.className = 'summary-list';
  itineraries.slice(0, 5).forEach((itinerary) => {
    const item = document.createElement('li');
    const transfers = Number(itinerary.transfers || 0);
    const transferText = `${transfers} transfer${transfers === 1 ? '' : 's'}`;
    item.textContent = `${formatDateTime(itinerary.startTime)} - ${formatDateTime(itinerary.endTime)} | ${durationToText(
      itinerary.duration
    )} | ${transferText}`;
    list.appendChild(item);
  });
  routeSummary.appendChild(list);

  const first = itineraries[0];
  if (Array.isArray(first.legs) && first.legs.length > 0) {
    const legsTitle = document.createElement('p');
    legsTitle.className = 'summary-title';
    legsTitle.textContent = 'First itinerary legs';
    routeSummary.appendChild(legsTitle);

    const legs = document.createElement('ul');
    legs.className = 'summary-legs';
    first.legs.forEach((leg) => {
      const item = document.createElement('li');
      const label = leg.displayName || leg.routeShortName || leg.headsign || '';
      const mode = leg.mode || 'UNKNOWN';
      const suffix = label ? ` (${label})` : '';
      item.textContent = `${mode}${suffix}: ${formatTime(
        leg.from && leg.from.departure ? leg.from.departure : leg.startTime
      )} -> ${formatTime(leg.to && leg.to.arrival ? leg.to.arrival : leg.endTime)}`;
      legs.appendChild(item);
    });
    routeSummary.appendChild(legs);
  }
}

activateBtn.addEventListener('click', async () => {
  const profile = profileSelect.value;
  if (!profile) {
    return;
  }

  activateBtn.disabled = true;
  try {
    const payload = await fetchJson('/api/gtfs/activate', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ profile })
    });

    setRawResult(payload);
    clearElement(routeSummary);
    const msg = document.createElement('p');
    msg.className = 'muted';
    msg.textContent = payload.message || 'Profile switch request accepted.';
    routeSummary.appendChild(msg);

    await loadStatus();
  } catch (err) {
    const payload = err.payload || { error: err.message };
    setRawResult(payload);
    renderRouteError(payload);
  } finally {
    activateBtn.disabled = false;
  }
});

routeForm.addEventListener('submit', async (event) => {
  event.preventDefault();

  const formData = new FormData(routeForm);
  const datetimeLocal = String(formData.get('datetime') || '');
  const datetime = datetimeLocal ? new Date(datetimeLocal).toISOString() : '';

  const payload = {
    origin: resolveStationToken(formData.get('origin')),
    destination: resolveStationToken(formData.get('destination')),
    datetime
  };

  try {
    routeBtn.disabled = true;
    const data = await fetchJson('/api/routes', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload)
    });

    setRawResult(data);
    renderRouteSummary(data);
    drawRouteMap(data);
  } catch (err) {
    const payloadErr = err.payload || { error: err.message };
    setRawResult(payloadErr);
    renderRouteError(payloadErr);
  } finally {
    routeBtn.disabled = false;
  }
});

originInput.addEventListener('input', () => {
  scheduleStationSuggestions(originInput.value);
});

destinationInput.addEventListener('input', () => {
  scheduleStationSuggestions(destinationInput.value);
});

originInput.addEventListener('focus', () => {
  scheduleStationSuggestions(originInput.value);
});

destinationInput.addEventListener('focus', () => {
  scheduleStationSuggestions(destinationInput.value);
});

function setDefaultDatetimeIfEmpty() {
  if (!datetimeInput || datetimeInput.value) {
    return;
  }
  const now = new Date();
  now.setMinutes(now.getMinutes() + 60);
  now.setSeconds(0);
  now.setMilliseconds(0);
  const local = new Date(now.getTime() - now.getTimezoneOffset() * 60000)
    .toISOString()
    .slice(0, 16);
  datetimeInput.value = local;
}

(function init() {
  setDefaultDatetimeIfEmpty();
  ensureMap();
  loadProfiles()
    .then(loadStatus)
    .then(() => {
      scheduleStationSuggestions('');
      setInterval(loadStatus, 2000);
    })
    .catch((err) => {
      setRawResult({ error: err.message || 'Initialization failed' });
      renderRouteError({ error: err.message || 'Initialization failed' });
    });
})();
