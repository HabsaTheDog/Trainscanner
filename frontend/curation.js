let map = null;
let currentMarkers = [];
let queueItems = [];
let activeItem = null;
let currentRenameAction = null;

async function initMap() {
    const container = document.getElementById('curationMap');

    let style = 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json';
    if (window.MAP_STYLE_URL) {
        style = window.MAP_STYLE_URL;
    } else if (window.PROTOMAPS_API_KEY) {
        style = `https://api.protomaps.com/styles/v2/light.json?key=${window.PROTOMAPS_API_KEY}`;
    }

    map = new maplibregl.Map({
        container,
        style,
        center: [10.4515, 51.1657], // Center of Germany approx
        zoom: 5
    });

    map.addControl(new maplibregl.NavigationControl(), 'top-right');
}

function clearMarkers() {
    for (const marker of currentMarkers) {
        marker.remove();
    }
    currentMarkers = [];
}

function focusLocations(locations) {
    clearMarkers();
    if (!locations || locations.length === 0) {
        document.getElementById('curationMapStatus').textContent = 'No geometrical data available for this issue.';
        return;
    }

    const bounds = new maplibregl.LngLatBounds();
    let hasValidCoordinates = false;

    for (const loc of locations) {
        if (loc.longitude && loc.latitude) {
            hasValidCoordinates = true;
            const marker = new maplibregl.Marker({ color: loc.color || '#0e7490' })
                .setLngLat([loc.longitude, loc.latitude])
                .setPopup(new maplibregl.Popup().setHTML(`
          <strong>${loc.name || 'Unknown'}</strong><br/>
          ID: ${loc.id}<br/>
          Country: ${loc.country || ''}
        `))
                .addTo(map);
            currentMarkers.push(marker);
            bounds.extend([loc.longitude, loc.latitude]);
        }
    }

    if (hasValidCoordinates) {
        document.getElementById('curationMapStatus').textContent = 'Locations plotted.';
        map.fitBounds(bounds, { padding: 50, maxZoom: 14 });
    } else {
        document.getElementById('curationMapStatus').textContent = 'No valid coordinates found for members.';
    }
}

async function fetchQueue() {
    const listEl = document.getElementById('queueList');
    listEl.innerHTML = 'Loading...';

    const country = document.getElementById('countryFilter').value;
    const url = country ? `/api/qa/queue?country=${encodeURIComponent(country)}` : '/api/qa/queue';

    try {
        const res = await fetch(url);
        if (!res.ok) throw new Error(`HTTP error ${res.status}`);
        queueItems = await res.json();
        renderQueue();
    } catch (err) {
        listEl.innerHTML = `<span class="badge failed">Failed to load: ${err.message}</span>`;
    }
}

function renderQueue() {
    const listEl = document.getElementById('queueList');
    listEl.innerHTML = '';

    if (queueItems.length === 0) {
        listEl.innerHTML = '<p class="muted">No items in the review queue.</p>';
        map.flyTo({ center: [10.4515, 51.1657], zoom: 5 });
        clearMarkers();
        return;
    }

    queueItems.forEach(item => {
        const div = document.createElement('div');
        div.className = `issue-item ${activeItem === item ? 'active' : ''}`;
        div.onclick = (e) => {
            if (e.target.tagName.toLowerCase() === 'button') return;
            selectItem(item);
        };

        let title = `${item.issue_type} - ${item.country}`;
        if (item.canonical_station_id) {
            title += ` (${item.canonical_station_id})`;
        }

        div.innerHTML = `
      <h3>${title}</h3>
      <p class="muted" style="font-size: 0.9em; margin-bottom: 4px;">Severity: <strong style="text-transform: capitalize;">${item.severity}</strong></p>
      <p class="muted" style="font-size: 0.85em; margin-bottom: 0;">Desc: ${JSON.stringify(item.details)}</p>
      <div class="issue-actions">
        <button onclick="handleAction(${item.review_item_id}, 'merge')">Merge</button>
        <button class="btn-secondary" onclick="handleAction(${item.review_item_id}, 'keep_separate')">Keep Separate</button>
        <button class="btn-secondary" onclick="promptRename(${item.review_item_id})">Rename</button>
      </div>
    `;

        listEl.appendChild(div);
    });
}

function selectItem(item) {
    activeItem = item;
    renderQueue();

    const locations = [];

    if (item.members && item.members.length > 0) {
        item.members.forEach(m => {
            locations.push({
                latitude: m.latitude,
                longitude: m.longitude,
                name: m.stop_name,
                id: m.source_stop_id,
                color: '#0e7490'
            });
        });
    }

    if (item.related_stations && item.related_stations.length > 0) {
        item.related_stations.forEach(cs => {
            locations.push({
                latitude: cs.latitude,
                longitude: cs.longitude,
                name: cs.canonical_name,
                id: cs.canonical_station_id,
                color: '#b91c1c'
            });
        });
    }

    focusLocations(locations);
}

async function handleAction(review_item_id, operation, new_canonical_name = null) {
    try {
        const item = queueItems.find(x => x.review_item_id === review_item_id);
        let operation_payload = {};
        if (operation === 'merge' && item.details && item.details.canonicalStationIds) {
            const ids = item.details.canonicalStationIds;
            if (ids.length >= 2) {
                operation_payload = {
                    target_canonical_station_id: ids[0],
                    source_canonical_station_id: ids[1]
                };
            }
        }

        const res = await fetch('/api/qa/overrides', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ review_item_id, operation, new_canonical_name, operation_payload })
        });

        if (!res.ok) {
            const error = await res.json();
            throw new Error(error.message || 'Unknown error');
        }

        if (activeItem && activeItem.review_item_id === review_item_id) {
            activeItem = null;
        }
        await fetchQueue();
    } catch (err) {
        alert(`Error applying fix: ${err.message}`);
    }
}

function promptRename(review_item_id) {
    currentRenameAction = review_item_id;
    document.getElementById('newNameInput').value = '';
    document.getElementById('renameDialog').showModal();
}

document.getElementById('cancelRenameBtn').addEventListener('click', () => {
    document.getElementById('renameDialog').close();
    currentRenameAction = null;
});

document.getElementById('renameForm').addEventListener('submit', (e) => {
    e.preventDefault();
    const newName = document.getElementById('newNameInput').value.trim();
    if (newName && currentRenameAction) {
        handleAction(currentRenameAction, 'rename', newName);
    }
    document.getElementById('renameDialog').close();
    currentRenameAction = null;
});

document.getElementById('refreshBtn').addEventListener('click', fetchQueue);
document.getElementById('countryFilter').addEventListener('change', fetchQueue);

document.addEventListener('DOMContentLoaded', () => {
    initMap();
    fetchQueue();
});
