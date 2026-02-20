let map = null;
let currentMarkers = [];
let queueItems = [];
let activeItem = null;
let currentRenameAction = null;
let pipelinePollHandle = null;

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

function setPipelineStatus(message, tone = 'idle') {
    const statusEl = document.getElementById('pipelineStatus');
    statusEl.textContent = message;
    if (tone === 'running') {
        statusEl.style.color = 'var(--warn)';
    } else if (tone === 'success') {
        statusEl.style.color = 'var(--ok)';
    } else if (tone === 'error') {
        statusEl.style.color = 'var(--danger)';
    } else {
        statusEl.style.color = 'var(--muted)';
    }
}

function formatBytes(bytes) {
    const value = Number(bytes);
    if (!Number.isFinite(value) || value <= 0) {
        return '0 B';
    }

    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let size = value;
    let unitIndex = 0;
    while (size >= 1024 && unitIndex < units.length - 1) {
        size /= 1024;
        unitIndex += 1;
    }

    const decimals = size >= 10 || unitIndex === 0 ? 0 : 1;
    return `${size.toFixed(decimals)} ${units[unitIndex]}`;
}

function clampPercent(value) {
    const num = Number(value);
    if (!Number.isFinite(num)) {
        return 0;
    }
    if (num < 0) return 0;
    if (num > 100) return 100;
    return num;
}

function setProgressFill(element, percent, indeterminate = false) {
    if (!element) return;
    if (indeterminate) {
        element.classList.add('indeterminate');
        return;
    }
    element.classList.remove('indeterminate');
    element.style.width = `${clampPercent(percent)}%`;
}

function hidePipelineProgress() {
    const container = document.getElementById('pipelineProgress');
    if (container) {
        container.hidden = true;
    }
}

function renderPipelineProgress(payload) {
    const container = document.getElementById('pipelineProgress');
    const stepBar = document.getElementById('pipelineStepBar');
    const stepLabel = document.getElementById('pipelineStepLabel');
    const stepValue = document.getElementById('pipelineStepValue');
    const downloadRow = document.getElementById('pipelineDownloadRow');
    const downloadLabel = document.getElementById('pipelineDownloadLabel');
    const downloadValue = document.getElementById('pipelineDownloadValue');
    const downloadBar = document.getElementById('pipelineDownloadBar');
    if (!container || !stepBar || !stepLabel || !stepValue || !downloadRow || !downloadLabel || !downloadValue || !downloadBar) {
        return;
    }

    const completed = Number(payload?.progress?.completed_steps || 0);
    const total = Number(payload?.progress?.total_steps || 4);
    const status = payload?.status || 'running';
    const step = payload?.step || '';

    container.hidden = false;
    stepLabel.textContent = payload?.step_label || 'Pipeline progress';
    stepValue.textContent = `${completed} / ${total}`;

    let stepPercent = total > 0 ? (completed / total) * 100 : 0;
    const downloadProgress = payload?.download_progress || payload?.checkpoint?.downloadProgress || null;

    if (status === 'completed') {
        stepPercent = 100;
    } else if (status === 'running' && step === 'fetching_sources' && downloadProgress?.total_sources > 0) {
        const sourceIndex = Number(downloadProgress.source_index || 0);
        const totalSources = Number(downloadProgress.total_sources || 0);
        const totalBytes = Number(downloadProgress.total_bytes || 0);
        const downloadedBytes = Number(downloadProgress.downloaded_bytes || 0);

        let sourceProgress = Math.max(0, sourceIndex - 1);
        if (downloadProgress.stage === 'source_completed') {
            sourceProgress = Math.max(sourceProgress, sourceIndex);
        } else if (downloadProgress.stage === 'downloading' && totalBytes > 0) {
            sourceProgress += Math.min(downloadedBytes / totalBytes, 1);
        }

        const stepFraction = Math.min(Math.max(sourceProgress / totalSources, 0), 1);
        stepPercent = total > 0 ? ((completed + stepFraction) / total) * 100 : stepPercent;
    }

    setProgressFill(stepBar, stepPercent, status === 'running' && stepPercent === 0);

    const showDownload = status === 'running' && step === 'fetching_sources';
    downloadRow.hidden = !showDownload;
    if (!showDownload) {
        return;
    }

    const sourceId = downloadProgress?.source_id || 'source';
    const sourceIndex = Number(downloadProgress?.source_index || 0);
    const totalSources = Number(downloadProgress?.total_sources || 0);
    const downloadedBytes = Number(downloadProgress?.downloaded_bytes || 0);
    const totalBytes = Number(downloadProgress?.total_bytes || 0);

    downloadLabel.textContent = `Download ${sourceIndex > 0 ? `${sourceIndex}/${totalSources || '?'}` : ''} ${sourceId}`.trim();
    if (totalBytes > 0) {
        const downloadPercent = clampPercent((downloadedBytes / totalBytes) * 100);
        downloadValue.textContent = `${formatBytes(downloadedBytes)} / ${formatBytes(totalBytes)} (${downloadPercent.toFixed(0)}%)`;
        setProgressFill(downloadBar, downloadPercent, false);
    } else {
        downloadValue.textContent = `${formatBytes(downloadedBytes)} downloaded`;
        setProgressFill(downloadBar, 0, true);
    }
}

function stopPipelinePolling() {
    if (pipelinePollHandle) {
        clearInterval(pipelinePollHandle);
        pipelinePollHandle = null;
    }
}

function setPipelineButtonState(isRunning) {
    const button = document.getElementById('runPipelineBtn');
    button.disabled = isRunning;
    button.textContent = isRunning ? 'Running...' : 'Run Pipeline';
}

async function pollPipelineJob(jobId) {
    try {
        const res = await fetch(`/api/qa/jobs/${encodeURIComponent(jobId)}`);
        let payload = null;
        try {
            payload = await res.json();
        } catch {
            payload = null;
        }

        if (!res.ok) {
            const message = payload?.error || `HTTP error ${res.status}`;
            throw new Error(message);
        }

        const stepLabel = payload.step_label || payload.step || 'Running';
        const completed = payload.progress?.completed_steps || 0;
        const total = payload.progress?.total_steps || 4;
        renderPipelineProgress(payload);

        if (payload.raw_status === 'queued') {
            setPipelineStatus('Pipeline queued. Waiting for active run to finish...', 'running');
            return;
        }

        if (payload.status === 'completed') {
            stopPipelinePolling();
            setPipelineButtonState(false);
            setPipelineStatus('Pipeline finished successfully. Refreshing queue...', 'success');
            await fetchQueue();
            setPipelineStatus('Pipeline finished successfully.', 'success');
            return;
        }

        if (payload.status === 'failed') {
            stopPipelinePolling();
            setPipelineButtonState(false);
            const errorMessage = payload.error_message || 'Pipeline failed';
            setPipelineStatus(`Pipeline failed: ${errorMessage}`, 'error');
            alert(`Pipeline failed: ${errorMessage}`);
            return;
        }

        setPipelineStatus(`Pipeline running: ${stepLabel} (${completed}/${total})`, 'running');
    } catch (err) {
        stopPipelinePolling();
        setPipelineButtonState(false);
        setPipelineStatus(`Pipeline status check failed: ${err.message}`, 'error');
        hidePipelineProgress();
    }
}

function startPipelinePolling(jobId) {
    stopPipelinePolling();
    pollPipelineJob(jobId);
    pipelinePollHandle = setInterval(() => {
        pollPipelineJob(jobId);
    }, 3000);
}

async function runPipelineRefresh() {
    setPipelineButtonState(true);
    setPipelineStatus('Starting pipeline...', 'running');
    renderPipelineProgress({
        status: 'running',
        step: 'starting',
        step_label: 'Starting pipeline',
        progress: { completed_steps: 0, total_steps: 4 }
    });

    const country = document.getElementById('countryFilter').value;
    const payload = country ? { country } : {};

    try {
        const res = await fetch('/api/qa/jobs/refresh', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        let data = null;
        try {
            data = await res.json();
        } catch {
            data = null;
        }

        if (!res.ok) {
            const message = data?.error || `HTTP error ${res.status}`;
            throw new Error(message);
        }

        const jobId = data?.job_id || data?.job?.job_id;
        if (!jobId) {
            throw new Error('Missing job_id in response');
        }

        if (data.reused) {
            setPipelineStatus('A refresh job is already running. Monitoring active job...', 'running');
        } else {
            setPipelineStatus('Pipeline started. Monitoring progress...', 'running');
        }

        startPipelinePolling(jobId);
    } catch (err) {
        setPipelineButtonState(false);
        setPipelineStatus(`Pipeline start failed: ${err.message}`, 'error');
        hidePipelineProgress();
        alert(`Pipeline start failed: ${err.message}`);
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
document.getElementById('runPipelineBtn').addEventListener('click', runPipelineRefresh);
document.getElementById('countryFilter').addEventListener('change', fetchQueue);
window.addEventListener('beforeunload', stopPipelinePolling);

document.addEventListener('DOMContentLoaded', () => {
    hidePipelineProgress();
    initMap();
    fetchQueue();
});
