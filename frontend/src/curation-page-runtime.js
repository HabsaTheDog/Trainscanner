import maplibregl from "maplibre-gl";

async function graphqlQuery(query, variables = {}) {
  const res = await fetch("/api/graphql", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables }),
  });
  const data = await res.json();
  if (data.errors) {
    throw new Error(data.errors.map((e) => e.message).join(", "));
  }
  return data.data;
}

function getClusterDetailElements() {
  return {
    headerEl: document.getElementById("clusterHeader"),
    metaEl: document.getElementById("clusterMeta"),
    candidatesEl: document.getElementById("candidateList"),
    evidenceEl: document.getElementById("evidenceList"),
    decisionsEl: document.getElementById("decisionHistoryList"),
  };
}

export function initCurationApp() {
  const MAP_MODE_SESSION_KEY = "qa.curation.mapMode";

  let map = null;
  let currentMarkers = [];
  let clusterItems = [];
  let activeClusterSummary = null;
  let activeClusterDetail = null;
  let activeCuratedProjectionItems = [];
  let selectedStationIds = new Set();
  const selectedDraftMergeIds = new Set();
  let activeTool = "merge";
  let pendingFocusCandidates = null;
  let activeMarkerPopup = null;
  let lastFocusFingerprint = "";

  let draftState;
  let renameEditorState = {
    refKey: "",
    value: "",
  };

  const createEmptyDraftState = () => {
    return {
      mergeItems: [],
      groups: [],
      pairWalkMinutesByKey: {},
      renameByRef: {},
      note: "",
    };
  };
  draftState = createEmptyDraftState();

  const createDraftId = (prefix) => {
    return `${prefix}_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`;
  };

  const escapeHtml = (value) => {
    return String(value || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  };

  const showNotice = (message, tone = "info", sticky = false) => {
    const box = document.getElementById("uiNotice");
    if (!box) {
      return;
    }

    box.hidden = false;
    box.textContent = String(message || "");
    box.className = `ui-notice ui-notice-${tone}`;

    if (!sticky) {
      clearTimeout(showNotice._timer);
      showNotice._timer = setTimeout(() => {
        box.hidden = true;
      }, 4500);
    }
  };

  const normalizeText = (value) => {
    return String(value || "")
      .trim()
      .toLowerCase();
  };

  const toCandidateRef = (stationId) => {
    return `candidate:${String(stationId || "").trim()}`;
  };

  const toMergeRef = (mergeId) => {
    return `merge:${String(mergeId || "").trim()}`;
  };

  const toGroupRef = (groupId) => {
    return `group:${String(groupId || "").trim()}`;
  };

  const parseRef = (refKey) => {
    const raw = String(refKey || "").trim();
    const index = raw.indexOf(":");
    if (index <= 0) {
      return { type: "", id: "" };
    }
    return {
      type: raw.slice(0, index),
      id: raw.slice(index + 1),
    };
  };

  function resolveCandidateLabel(candidate) {
    if (!candidate) {
      return "Unknown candidate";
    }

    const stationId = String(candidate.canonical_station_id || "").trim();
    const baseName = String(candidate.display_name || "").trim();
    const renamed = draftState.renameByRef[toCandidateRef(stationId)] || "";
    const displayName = renamed || baseName;

    if (displayName && stationId) {
      return `${displayName} (${stationId})`;
    }

    return displayName || stationId || "Unknown candidate";
  }

  function inferCandidateCategory(candidate) {
    const segment = candidate?.segment_context || {};
    const text = normalizeText(
      [
        candidate?.display_name,
        segment.segment_name,
        segment.segment_type,
        ...(Array.isArray(candidate?.aliases) ? candidate.aliases : []),
      ].join(" "),
    );

    if (!text) {
      return "other";
    }

    if (text.includes("bus") || text.includes("zob")) {
      return "bus";
    }
    if (text.includes("tram") || text.includes("streetcar")) {
      return "tram";
    }
    if (
      text.includes("subway") ||
      text.includes("u-bahn") ||
      text.includes("ubahn") ||
      text.includes("metro")
    ) {
      return "subway";
    }
    if (
      text.includes("north") ||
      text.includes("south") ||
      text.includes("east") ||
      text.includes("west") ||
      text.includes("secondary")
    ) {
      return "secondary";
    }
    if (
      text.includes("main") ||
      text.includes("hbf") ||
      text.includes("hauptbahnhof") ||
      text.includes("rail") ||
      text.includes("platform")
    ) {
      return "main";
    }
    return "other";
  }

  function getCandidateByStationId(stationId) {
    if (!Array.isArray(activeClusterDetail?.candidates)) {
      return null;
    }
    return (
      activeClusterDetail.candidates.find(
        (candidate) => candidate.canonical_station_id === stationId,
      ) || null
    );
  }

  const compareCandidateRank = (a, b) => {
    const rankA = Number.parseInt(String(a?.candidate_rank ?? ""), 10);
    const rankB = Number.parseInt(String(b?.candidate_rank ?? ""), 10);
    const safeRankA =
      Number.isFinite(rankA) && rankA > 0 ? rankA : Number.MAX_SAFE_INTEGER;
    const safeRankB =
      Number.isFinite(rankB) && rankB > 0 ? rankB : Number.MAX_SAFE_INTEGER;
    if (safeRankA !== safeRankB) {
      return safeRankA - safeRankB;
    }
    return String(a?.canonical_station_id || "").localeCompare(
      String(b?.canonical_station_id || ""),
    );
  };

  function sortStationIdsByCandidateRank(stationIds) {
    const input = Array.isArray(stationIds) ? stationIds : [];
    const lookup = new Map(
      (Array.isArray(activeClusterDetail?.candidates)
        ? activeClusterDetail.candidates
        : []
      ).map((candidate) => [candidate.canonical_station_id, candidate]),
    );

    return input.slice().sort((a, b) => {
      const candidateA = lookup.get(a) || {
        canonical_station_id: a,
        candidate_rank: Number.MAX_SAFE_INTEGER,
      };
      const candidateB = lookup.get(b) || {
        canonical_station_id: b,
        candidate_rank: Number.MAX_SAFE_INTEGER,
      };
      return compareCandidateRank(candidateA, candidateB);
    });
  }

  function uniqueStrings(values) {
    const out = [];
    const seen = new Set();
    for (const value of Array.isArray(values) ? values : []) {
      const clean = String(value || "").trim();
      if (!clean || seen.has(clean)) {
        continue;
      }
      seen.add(clean);
      out.push(clean);
    }
    return out;
  }

  function getDraftMergeById(mergeId) {
    return (
      draftState.mergeItems.find((item) => item.merge_id === mergeId) || null
    );
  }

  function resolveMergeNameAssumption(stationIds = []) {
    const sortedIds = sortStationIdsByCandidateRank(stationIds);
    const firstId = sortedIds[0] || "";
    const firstCandidate = firstId ? getCandidateByStationId(firstId) : null;
    const clusterName = String(activeClusterDetail?.display_name || "").trim();
    const candidateName = String(firstCandidate?.display_name || "").trim();
    return clusterName || candidateName || "";
  }

  function updateMergeNameAssumption() {
    const inputEl = document.getElementById("editMergeRenameInput");
    if (!inputEl) {
      return;
    }

    if (String(inputEl.value || "").trim()) {
      return;
    }

    const stationIds =
      selectedStationIds.size > 0
        ? Array.from(selectedStationIds.values())
        : getVisibleStandaloneCandidates().map(
            (candidate) => candidate.canonical_station_id,
          );
    const assumption = resolveMergeNameAssumption(stationIds);
    if (assumption) {
      inputEl.value = assumption;
    }
  }

  function resolveDraftMergeDisplayName(mergeItem) {
    if (!mergeItem) {
      return "Merged candidate";
    }
    const refKey = toMergeRef(mergeItem.merge_id);
    return (
      String(
        draftState.renameByRef[refKey] ||
          mergeItem.display_name ||
          resolveMergeNameAssumption(mergeItem.member_station_ids) ||
          "Merged candidate",
      ).trim() || "Merged candidate"
    );
  }

  function getRefMemberStationIds(refKey) {
    const parsed = parseRef(refKey);
    if (parsed.type === "candidate") {
      return parsed.id ? [parsed.id] : [];
    }

    if (parsed.type === "merge") {
      const mergeItem = getDraftMergeById(parsed.id);
      return uniqueStrings(mergeItem?.member_station_ids || []);
    }

    return [];
  }

  function getSelectedRefKeys() {
    const refs = [];
    for (const stationId of selectedStationIds.values()) {
      refs.push(toCandidateRef(stationId));
    }
    for (const mergeId of selectedDraftMergeIds.values()) {
      refs.push(toMergeRef(mergeId));
    }
    return refs;
  }

  function collectMergeMemberStationIdsFromCurated(stationIds, items) {
    if (!Array.isArray(items)) {
      return;
    }
    for (const item of items) {
      if (normalizeText(item?.derived_operation) !== "merge") {
        continue;
      }
      for (const member of Array.isArray(item?.members) ? item.members : []) {
        const stationId = String(member?.canonical_station_id || "").trim();
        if (stationId) {
          stationIds.add(stationId);
        }
      }
    }
  }

  function collectMergeMemberStationIdsFromDraft(stationIds, mergeItems) {
    for (const mergeItem of mergeItems) {
      for (const stationId of uniqueStrings(mergeItem.member_station_ids)) {
        stationIds.add(stationId);
      }
    }
  }

  function getMergeDerivedMemberStationIds() {
    const stationIds = new Set();
    collectMergeMemberStationIdsFromCurated(
      stationIds,
      activeCuratedProjectionItems,
    );
    collectMergeMemberStationIdsFromDraft(stationIds, draftState.mergeItems);
    return stationIds;
  }

  function getVisibleStandaloneCandidates() {
    const candidates = Array.isArray(activeClusterDetail?.candidates)
      ? activeClusterDetail.candidates
      : [];
    const hiddenMemberIds = getMergeDerivedMemberStationIds();
    if (hiddenMemberIds.size === 0) {
      return candidates.slice();
    }
    return candidates.filter(
      (candidate) =>
        !hiddenMemberIds.has(
          String(candidate?.canonical_station_id || "").trim(),
        ),
    );
  }

  function pruneHiddenSelections() {
    let changed = false;

    if (selectedStationIds.size > 0) {
      const hiddenMemberIds = getMergeDerivedMemberStationIds();
      for (const stationId of Array.from(selectedStationIds.values())) {
        if (hiddenMemberIds.has(stationId)) {
          selectedStationIds.delete(stationId);
          changed = true;
        }
      }
    }

    if (selectedDraftMergeIds.size > 0) {
      const validMergeIds = new Set(
        draftState.mergeItems.map((item) => item.merge_id),
      );
      for (const mergeId of Array.from(selectedDraftMergeIds.values())) {
        if (!validMergeIds.has(mergeId)) {
          selectedDraftMergeIds.delete(mergeId);
          changed = true;
        }
      }
    }

    return changed;
  }

  function pruneGroupReferences() {
    const validMergeIds = new Set(
      draftState.mergeItems.map((item) => item.merge_id),
    );
    for (const group of draftState.groups) {
      group.member_refs = (
        Array.isArray(group.member_refs) ? group.member_refs : []
      ).filter((refKey) => {
        const parsed = parseRef(refKey);
        if (parsed.type === "candidate") {
          return Boolean(parsed.id && getCandidateByStationId(parsed.id));
        }
        if (parsed.type === "merge") {
          return Boolean(parsed.id && validMergeIds.has(parsed.id));
        }
        return false;
      });
      group.member_refs = uniqueStrings(group.member_refs);
    }
    draftState.groups = draftState.groups.filter(
      (group) => group.member_refs.length > 0,
    );
  }

  const resolveDefaultMapStyle = () => {
    if (globalThis.MAP_STYLE_URL) {
      return globalThis.MAP_STYLE_URL;
    }
    if (globalThis.PROTOMAPS_API_KEY) {
      return `https://api.protomaps.com/styles/v2/light.json?key=${globalThis.PROTOMAPS_API_KEY}`;
    }
    return "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json";
  };

  const resolveSatelliteMapStyle = () => {
    if (globalThis.SATELLITE_MAP_STYLE_URL) {
      return globalThis.SATELLITE_MAP_STYLE_URL;
    }

    return {
      version: 8,
      sources: {
        satellite: {
          type: "raster",
          tiles: [
            "https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
          ],
          tileSize: 256,
          attribution: "Tiles © Esri",
        },
      },
      layers: [
        {
          id: "satellite",
          type: "raster",
          source: "satellite",
        },
      ],
    };
  };

  function getMapMode() {
    const saved = sessionStorage.getItem(MAP_MODE_SESSION_KEY);
    return saved === "satellite" ? "satellite" : "default";
  }

  function setMapMode(mode) {
    const nextMode = mode === "satellite" ? "satellite" : "default";
    sessionStorage.setItem(MAP_MODE_SESSION_KEY, nextMode);

    const defaultBtn = document.getElementById("mapModeDefaultBtn");
    const satelliteBtn = document.getElementById("mapModeSatelliteBtn");
    if (defaultBtn && satelliteBtn) {
      defaultBtn.classList.toggle("btn-active", nextMode === "default");
      satelliteBtn.classList.toggle("btn-active", nextMode === "satellite");
    }

    if (!map) {
      return;
    }

    const style =
      nextMode === "satellite"
        ? resolveSatelliteMapStyle()
        : resolveDefaultMapStyle();
    map.setStyle(style);
  }

  async function initMap() {
    if (map) {
      return;
    }

    if (!maplibregl?.Map) {
      throw new Error(
        "MapLibre did not load in the browser (check network/cache and reload).",
      );
    }

    map = new maplibregl.Map({
      container: document.getElementById("curationMap"),
      style: resolveDefaultMapStyle(),
      center: [10.4515, 51.1657],
      zoom: 5,
    });

    map.once("load", () => {
      if (!Array.isArray(pendingFocusCandidates)) {
        return;
      }

      const queued = pendingFocusCandidates;
      pendingFocusCandidates = null;
      focusLocations(queued);
    });

    map.addControl(new maplibregl.NavigationControl(), "top-right");
    map.on("zoom", applyMarkerZoomScale);

    document
      .getElementById("mapModeDefaultBtn")
      .addEventListener("click", () => setMapMode("default"));
    document
      .getElementById("mapModeSatelliteBtn")
      .addEventListener("click", () => setMapMode("satellite"));
    setMapMode(getMapMode());
  }

  function clearMarkers() {
    if (activeMarkerPopup) {
      activeMarkerPopup.remove();
      activeMarkerPopup = null;
    }

    for (const marker of currentMarkers) {
      marker.remove();
    }
    currentMarkers = [];
  }

  const getCandidateStationId = (candidate) => {
    return String(candidate?.canonical_station_id || "").trim();
  };

  function sortCandidatesForMarkerRings(candidates) {
    return candidates.slice().sort((left, right) => {
      const leftRank = Number(left?.candidate_rank);
      const rightRank = Number(right?.candidate_rank);
      if (
        Number.isFinite(leftRank) &&
        Number.isFinite(rightRank) &&
        leftRank !== rightRank
      ) {
        return leftRank - rightRank;
      }
      return getCandidateStationId(left).localeCompare(
        getCandidateStationId(right),
      );
    });
  }

  const getGroupedCandidatesByLocation = (candidates) => {
    const groups = new Map();

    for (const candidate of candidates || []) {
      const longitude = Number(candidate?.longitude);
      const latitude = Number(candidate?.latitude);
      if (!Number.isFinite(longitude) || !Number.isFinite(latitude)) {
        continue;
      }

      const key = `${longitude.toFixed(6)}|${latitude.toFixed(6)}`;
      if (!groups.has(key)) {
        groups.set(key, {
          longitude,
          latitude,
          candidates: [],
        });
      }
      groups.get(key).candidates.push(candidate);
    }

    return Array.from(groups.values());
  };

  function showCandidatePopup(candidate, longitude, latitude) {
    if (!map) {
      return;
    }

    if (activeMarkerPopup) {
      activeMarkerPopup.remove();
      activeMarkerPopup = null;
    }

    activeMarkerPopup = new maplibregl.Popup({
      closeButton: false,
      closeOnMove: false,
      offset: 10,
    })
      .setLngLat([longitude, latitude])
      .setHTML(
        `<strong>${escapeHtml(candidate.display_name || candidate.canonical_station_id)}</strong><br/>` +
          `Station ID: ${escapeHtml(candidate.canonical_station_id)}`,
      )
      .addTo(map);
  }

  function createConcentricMarkerElement(group) {
    const ordered = sortCandidatesForMarkerRings(group.candidates || []);
    const ringCount = Math.max(1, ordered.length);
    const baseRadius = 5;
    const maxOuterRadius = 28;
    const ringGap =
      ringCount <= 1
        ? 0
        : Math.max(
            1.7,
            Math.min(4, (maxOuterRadius - baseRadius) / (ringCount - 1)),
          );
    const outerRadius = baseRadius + (ringCount - 1) * ringGap;
    const padding = 5;
    const size = (outerRadius + padding + 2) * 2;
    const center = size / 2;

    const root = document.createElement("div");
    root.className = "candidate-marker-group";
    root.style.setProperty("--marker-scale", "1");
    root.title =
      ringCount > 1
        ? `${ringCount} overlapping candidates (concentric rings)`
        : resolveCandidateLabel(ordered[0]);
    root.setAttribute("role", "group");
    root.setAttribute(
      "aria-label",
      ringCount > 1
        ? `${ringCount} overlapping station candidates`
        : `Station candidate ${resolveCandidateLabel(ordered[0])}`,
    );

    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("class", "candidate-marker-stack");
    svg.setAttribute("viewBox", `0 0 ${size} ${size}`);
    svg.setAttribute("width", String(size));
    svg.setAttribute("height", String(size));
    svg.setAttribute("role", "group");

    for (let index = 0; index < ringCount; index += 1) {
      const candidate = ordered[index];
      const stationId = getCandidateStationId(candidate);
      const selected = selectedStationIds.has(stationId);
      const radius = outerRadius - index * ringGap;

      const circle = document.createElementNS(
        "http://www.w3.org/2000/svg",
        "circle",
      );
      circle.setAttribute("cx", String(center));
      circle.setAttribute("cy", String(center));
      circle.setAttribute("r", String(Math.max(2, radius)));
      circle.setAttribute(
        "class",
        `candidate-ring ${selected ? "selected" : ""}`,
      );
      circle.setAttribute("pointer-events", "stroke");
      circle.setAttribute("tabindex", "0");
      circle.setAttribute("role", "button");
      circle.setAttribute(
        "aria-label",
        `Candidate ${index + 1} of ${ringCount}: ${resolveCandidateLabel(candidate)}`,
      );

      circle.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        toggleCandidateSelection(stationId, !selectedStationIds.has(stationId));
        showCandidatePopup(candidate, group.longitude, group.latitude);
      });

      circle.addEventListener("keydown", (event) => {
        if (event.key !== "Enter" && event.key !== " ") {
          return;
        }
        event.preventDefault();
        toggleCandidateSelection(stationId, !selectedStationIds.has(stationId));
        showCandidatePopup(candidate, group.longitude, group.latitude);
      });

      circle.addEventListener("mouseenter", () => {
        root.title = resolveCandidateLabel(candidate);
      });

      svg.appendChild(circle);
    }

    root.appendChild(svg);
    return root;
  }

  function computeMarkerScaleFromZoom() {
    if (!map) {
      return 1;
    }
    const zoom = Number(map.getZoom());
    if (!Number.isFinite(zoom)) {
      return 1;
    }
    return Math.max(0.62, Math.min(1.9, 0.74 + (zoom - 5) * 0.1));
  }

  function applyMarkerZoomScale() {
    const scale = computeMarkerScaleFromZoom();
    for (const marker of currentMarkers) {
      const markerEl = marker.getElement();
      if (markerEl) {
        markerEl.style.setProperty("--marker-scale", scale.toFixed(3));
      }
    }
  }

  function buildFocusFingerprint(candidates) {
    return (candidates || [])
      .filter(
        (candidate) =>
          Number.isFinite(candidate?.longitude) &&
          Number.isFinite(candidate?.latitude),
      )
      .map(
        (candidate) =>
          `${getCandidateStationId(candidate)}:${Number(candidate.longitude).toFixed(6)}:${Number(candidate.latitude).toFixed(6)}`,
      )
      .sort()
      .join("|");
  }

  function focusLocations(candidates) {
    if (!map) {
      pendingFocusCandidates = Array.isArray(candidates)
        ? candidates.slice()
        : [];
      document.getElementById("curationMapStatus").textContent =
        "Map is still initializing. Retrying candidate rendering...";
      return;
    }

    clearMarkers();

    if (!Array.isArray(candidates) || candidates.length === 0) {
      lastFocusFingerprint = "";
      document.getElementById("curationMapStatus").textContent =
        "No candidate coordinates available for this cluster.";
      return;
    }

    const focusFingerprint = buildFocusFingerprint(candidates);
    const shouldFitBounds = focusFingerprint !== lastFocusFingerprint;

    const bounds = new maplibregl.LngLatBounds();
    const grouped = getGroupedCandidatesByLocation(candidates);
    let hasValidCoordinates = false;
    let overlapGroupCount = 0;

    for (const group of grouped) {
      hasValidCoordinates = true;
      if ((group.candidates || []).length > 1) {
        overlapGroupCount += 1;
      }

      let marker = null;
      try {
        marker = new maplibregl.Marker({
          element: createConcentricMarkerElement(group),
          anchor: "center",
        })
          .setLngLat([group.longitude, group.latitude])
          .addTo(map);
      } catch (err) {
        console.error("Failed to render candidate marker", err);
        continue;
      }

      currentMarkers.push(marker);
      bounds.extend([group.longitude, group.latitude]);
    }

    if (!hasValidCoordinates) {
      lastFocusFingerprint = "";
      document.getElementById("curationMapStatus").textContent =
        "No valid candidate coordinates found.";
      return;
    }

    applyMarkerZoomScale();
    const overlapSummary =
      overlapGroupCount > 0
        ? ` ${overlapGroupCount} shared-location group(s) are shown as concentric rings.`
        : "";
    document.getElementById("curationMapStatus").textContent =
      `Candidate locations plotted.${overlapSummary}`;
    if (shouldFitBounds) {
      map.fitBounds(bounds, { padding: 50, maxZoom: 15 });
    }
    lastFocusFingerprint = focusFingerprint;
  }

  function renderClusterListMeta() {
    const metaEl = document.getElementById("clusterListMeta");
    if (!metaEl) {
      return;
    }

    const country = document.getElementById("countryFilter").value || "ALL";
    const status = document.getElementById("statusFilter").value || "ALL";
    const scopeTag =
      document.getElementById("scopeTagFilter").value || "latest";

    const totals = {
      open: 0,
      in_review: 0,
      resolved: 0,
      dismissed: 0,
    };

    for (const cluster of clusterItems) {
      const key = String(cluster?.status || "").toLowerCase();
      if (Object.hasOwn(totals, key)) {
        totals[key] += 1;
      }
    }

    metaEl.textContent = `Showing ${clusterItems.length} clusters (country=${country}, status=${status}, scope=${scopeTag}) • open ${totals.open}, in_review ${totals.in_review}, resolved ${totals.resolved}, dismissed ${totals.dismissed}`;
  }

  async function fetchClusters() {
    const listEl = document.getElementById("clusterList");
    listEl.innerHTML = "Loading clusters...";

    try {
      const country = document.getElementById("countryFilter").value || null;
      const data = await graphqlQuery(
        `
        query GetClusters($country: String) {
          clusters(country: $country) {
            cluster_id
            country
            status
            member_count
          }
        }
      `,
        { country },
      );

      clusterItems = Array.isArray(data.clusters) ? data.clusters : [];
      renderClusterList();
      renderClusterListMeta();

      if (activeClusterSummary) {
        const refreshed = clusterItems.find(
          (item) => item.cluster_id === activeClusterSummary.cluster_id,
        );
        if (refreshed) {
          activeClusterSummary = refreshed;
        } else {
          activeClusterSummary = null;
          activeClusterDetail = null;
          activeCuratedProjectionItems = [];
          clearSelection();
          resetDraftState();
        }
      }

      if (!activeClusterSummary && clusterItems.length > 0) {
        await loadClusterDetail(clusterItems[0].cluster_id, true);
        return;
      }

      if (clusterItems.length === 0) {
        activeClusterSummary = null;
        activeClusterDetail = null;
        activeCuratedProjectionItems = [];
        renderClusterDetail();
      }
    } catch (err) {
      listEl.innerHTML = `<span class="badge failed">Failed to load clusters: ${escapeHtml(err.message)}</span>`;
      const metaEl = document.getElementById("clusterListMeta");
      if (metaEl) {
        metaEl.textContent = "Cluster summary unavailable.";
      }
      showNotice(`Failed to load clusters: ${err.message}`, "error", true);
    }
  }

  function renderClusterList() {
    const listEl = document.getElementById("clusterList");
    listEl.innerHTML = "";

    if (!Array.isArray(clusterItems) || clusterItems.length === 0) {
      listEl.innerHTML =
        '<p class="muted">No clusters found for this filter.</p>';
      return;
    }

    for (const cluster of clusterItems) {
      const isActive =
        activeClusterSummary &&
        activeClusterSummary.cluster_id === cluster.cluster_id;

      const div = document.createElement("div");
      div.className = `cluster-item ${isActive ? "active" : ""}`;
      div.onclick = () => loadClusterDetail(cluster.cluster_id, true);

      div.innerHTML = `
        <h3>${escapeHtml(cluster.display_name || cluster.cluster_id)}</h3>
        <p class="muted tiny">${escapeHtml(cluster.country)} • ${escapeHtml(cluster.severity)} • ${escapeHtml(cluster.status)}</p>
        <p class="muted tiny">${escapeHtml(cluster.candidate_count)} candidates • ${escapeHtml(cluster.issue_count)} issues • scope ${escapeHtml(cluster.scope_tag)}</p>
      `;

      listEl.appendChild(div);
    }
  }

  function resetDraftState() {
    draftState = createEmptyDraftState();
    renameEditorState = { refKey: "", value: "" };
    const noteEl = document.getElementById("editNoteInput");
    if (noteEl) {
      noteEl.value = "";
    }
    const mergeNameEl = document.getElementById("editMergeRenameInput");
    if (mergeNameEl) {
      mergeNameEl.value = "";
    }
  }

  function clearSelection() {
    selectedStationIds.clear();
    selectedDraftMergeIds.clear();
  }

  function selectAllCandidates() {
    selectedStationIds = new Set(
      getVisibleStandaloneCandidates().map(
        (candidate) => candidate.canonical_station_id,
      ),
    );
    selectedDraftMergeIds.clear();
    renderClusterDetail();
  }

  function clearCandidateSelection() {
    clearSelection();
    renderClusterDetail();
  }

  function toggleCandidateSelection(stationId, checked) {
    if (!stationId) {
      return;
    }
    if (checked) {
      selectedStationIds.add(stationId);
    } else {
      selectedStationIds.delete(stationId);
    }
    renderClusterDetail();
  }

  function toggleDraftMergeSelection(mergeId, checked) {
    if (!mergeId) {
      return;
    }
    if (checked) {
      selectedDraftMergeIds.add(mergeId);
    } else {
      selectedDraftMergeIds.delete(mergeId);
    }
    renderClusterDetail();
  }

  function updateSelectionSummary() {
    const summaryEl = document.getElementById("selectionSummary");
    if (!summaryEl) {
      return;
    }

    pruneHiddenSelections();

    const candidateCount = selectedStationIds.size;
    const mergedCount = selectedDraftMergeIds.size;
    if (candidateCount === 0 && mergedCount === 0) {
      summaryEl.textContent = "No candidates selected.";
      return;
    }

    const segments = [];
    if (candidateCount > 0) {
      segments.push(`${candidateCount} candidate(s)`);
    }
    if (mergedCount > 0) {
      segments.push(`${mergedCount} merged item(s)`);
    }
    summaryEl.textContent = `Selected: ${segments.join(", ")}.`;
  }

  function renderSelectedServiceContext() {
    const incomingEl = document.getElementById("selectedServiceIncoming");
    const outgoingEl = document.getElementById("selectedServiceOutgoing");

    if (!incomingEl || !outgoingEl) {
      return;
    }

    const selected = Array.from(selectedStationIds.values())
      .map((stationId) => getCandidateByStationId(stationId))
      .filter(Boolean);

    const incoming = new Set();
    const outgoing = new Set();
    for (const candidate of selected) {
      for (const value of candidate?.service_context?.incoming || []) {
        if (String(value || "").trim()) {
          incoming.add(String(value).trim());
        }
      }
      for (const value of candidate?.service_context?.outgoing || []) {
        if (String(value || "").trim()) {
          outgoing.add(String(value).trim());
        }
      }
    }

    const incomingList = Array.from(incoming).sort((a, b) =>
      a.localeCompare(b),
    );
    const outgoingList = Array.from(outgoing).sort((a, b) =>
      a.localeCompare(b),
    );

    incomingEl.innerHTML =
      incomingList.length === 0
        ? '<p class="muted tiny">No incoming service context for selected nodes.</p>'
        : incomingList
            .map(
              (value) => `<div class="service-item">${escapeHtml(value)}</div>`,
            )
            .join("");
    outgoingEl.innerHTML =
      outgoingList.length === 0
        ? '<p class="muted tiny">No outgoing service context for selected nodes.</p>'
        : outgoingList
            .map(
              (value) => `<div class="service-item">${escapeHtml(value)}</div>`,
            )
            .join("");
  }

  function beginInlineRename(refKey) {
    if (!refKey) {
      return;
    }
    const parsed = parseRef(refKey);
    let defaultValue = "";

    if (parsed.type === "candidate") {
      defaultValue =
        draftState.renameByRef[refKey] ||
        String(getCandidateByStationId(parsed.id)?.display_name || "").trim();
    } else if (parsed.type === "merge") {
      const mergeItem = getDraftMergeById(parsed.id);
      defaultValue =
        draftState.renameByRef[refKey] ||
        resolveDraftMergeDisplayName(mergeItem);
    } else if (parsed.type === "group") {
      const group = draftState.groups.find(
        (item) => item.group_id === parsed.id,
      );
      defaultValue =
        draftState.renameByRef[refKey] ||
        String(group?.section_name || "").trim();
    }

    renameEditorState = {
      refKey,
      value: defaultValue,
    };
    renderClusterDetail();
  }

  function cancelInlineRename() {
    renameEditorState = { refKey: "", value: "" };
    renderClusterDetail();
  }

  function updateInlineRenameDraft(value) {
    renameEditorState = {
      ...renameEditorState,
      value: String(value || ""),
    };
  }

  function commitInlineRename() {
    const refKey = String(renameEditorState.refKey || "").trim();
    if (!refKey) {
      return;
    }

    const value = String(renameEditorState.value || "").trim();
    if (value) {
      draftState.renameByRef[refKey] = value;
    } else {
      delete draftState.renameByRef[refKey];
    }

    const parsed = parseRef(refKey);
    if (parsed.type === "group") {
      const group = draftState.groups.find(
        (item) => item.group_id === parsed.id,
      );
      if (group && value) {
        group.section_name = value;
      }
    }

    renameEditorState = { refKey: "", value: "" };
    renderClusterDetail();
  }

  function getRefTitle(refKey) {
    const parsed = parseRef(refKey);
    if (parsed.type === "candidate") {
      const candidate = getCandidateByStationId(parsed.id);
      if (!candidate) {
        return parsed.id;
      }
      return String(
        draftState.renameByRef[refKey] || candidate.display_name || parsed.id,
      ).trim();
    }

    if (parsed.type === "merge") {
      return resolveDraftMergeDisplayName(getDraftMergeById(parsed.id));
    }

    return refKey;
  }

  function createGroupFromSelection() {
    const selectedRefs = uniqueStrings(getSelectedRefKeys());
    if (selectedRefs.length === 0) {
      showNotice(
        "Select candidates or merged items before creating a group.",
        "error",
      );
      return;
    }

    const groupNameInput = String(
      document.getElementById("groupNameInput").value || "",
    ).trim();
    const sectionType =
      String(
        document.getElementById("groupSectionPresetType").value || "other",
      ).trim() || "other";

    const inferredCandidate = getCandidateByStationId(
      parseRef(selectedRefs[0]).id || "",
    );
    const inferredName =
      groupNameInput ||
      getRefTitle(selectedRefs[0]) ||
      (inferredCandidate
        ? resolveCandidateLabel(inferredCandidate)
        : `Group ${draftState.groups.length + 1}`);

    const groupId = createDraftId("grp");
    draftState.groups.push({
      group_id: groupId,
      section_type: sectionType || inferCandidateCategory(inferredCandidate),
      section_name: inferredName,
      member_refs: selectedRefs,
    });

    draftState.renameByRef[toGroupRef(groupId)] = inferredName;
    document.getElementById("groupNameInput").value = "";
    clearSelection();
    ensurePairWalkDefaults();
    setActiveTool("group");
    renderClusterDetail();
  }

  function removeGroup(groupId) {
    draftState.groups = draftState.groups.filter(
      (group) => group.group_id !== groupId,
    );
    delete draftState.renameByRef[toGroupRef(groupId)];
    ensurePairWalkDefaults();
    renderClusterDetail();
  }

  function removeGroupMember(groupId, refKey) {
    const group = draftState.groups.find((item) => item.group_id === groupId);
    if (!group) {
      return;
    }

    group.member_refs = group.member_refs.filter(
      (memberRef) => memberRef !== refKey,
    );
    if (group.member_refs.length === 0) {
      removeGroup(groupId);
      return;
    }

    ensurePairWalkDefaults();
    renderClusterDetail();
  }

  function addSelectionToGroup() {
    const groupId = String(
      document.getElementById("groupTargetSelect").value || "",
    ).trim();
    if (!groupId) {
      showNotice("Select a target group first.", "error");
      return;
    }

    const group = draftState.groups.find((item) => item.group_id === groupId);
    if (!group) {
      showNotice("Target group not found.", "error");
      return;
    }

    const selectedRefs = uniqueStrings(getSelectedRefKeys());
    if (selectedRefs.length === 0) {
      showNotice("Select candidates or merged items to add.", "error");
      return;
    }

    group.member_refs = uniqueStrings([
      ...(group.member_refs || []),
      ...selectedRefs,
    ]);
    clearSelection();
    ensurePairWalkDefaults();
    renderClusterDetail();
  }

  function updateGroupName(groupId, value) {
    const group = draftState.groups.find((item) => item.group_id === groupId);
    if (!group) {
      return;
    }

    const cleaned = String(value || "").trim();
    if (cleaned) {
      group.section_name = cleaned;
      draftState.renameByRef[toGroupRef(groupId)] = cleaned;
    }
    renderClusterDetail();
  }

  function updateGroupType(groupId, value) {
    const group = draftState.groups.find((item) => item.group_id === groupId);
    if (!group) {
      return;
    }

    const cleanType = String(value || "").trim() || "other";
    group.section_type = cleanType;
    renderClusterDetail();
  }

  const pairKey = (a, b) => {
    const values = [String(a || "").trim(), String(b || "").trim()].sort(
      (x, y) => x.localeCompare(y),
    );
    return `${values[0]}|${values[1]}`;
  };

  function ensurePairWalkDefaults() {
    const groups = draftState.groups;
    const next = {};

    for (let i = 0; i < groups.length; i += 1) {
      for (let j = i + 1; j < groups.length; j += 1) {
        const key = pairKey(groups[i].group_id, groups[j].group_id);
        const existing = Number.parseInt(
          String(draftState.pairWalkMinutesByKey[key] ?? ""),
          10,
        );
        next[key] = Number.isFinite(existing) && existing >= 0 ? existing : 5;
      }
    }

    draftState.pairWalkMinutesByKey = next;
  }

  function setPairWalkMinutes(groupA, groupB, minutes) {
    const key = pairKey(groupA, groupB);
    const parsed = Number.parseInt(String(minutes || ""), 10);
    draftState.pairWalkMinutesByKey[key] =
      Number.isFinite(parsed) && parsed >= 0 ? parsed : 5;
    updateEditPreview();
  }

  function removeDraftMerge(mergeId) {
    draftState.mergeItems = draftState.mergeItems.filter(
      (item) => item.merge_id !== mergeId,
    );
    selectedDraftMergeIds.delete(mergeId);
    const mergeRef = toMergeRef(mergeId);
    delete draftState.renameByRef[mergeRef];
    for (const group of draftState.groups) {
      group.member_refs = (group.member_refs || []).filter(
        (refKey) => refKey !== mergeRef,
      );
    }
    pruneGroupReferences();
    ensurePairWalkDefaults();
    renderClusterDetail();
  }

  function buildRenameTargetsPayload() {
    const targets = [];
    for (const [refKey, renameTo] of Object.entries(
      draftState.renameByRef || {},
    )) {
      const parsed = parseRef(refKey);
      const cleanRename = String(renameTo || "").trim();
      if (!cleanRename || parsed.type !== "candidate" || !parsed.id) {
        continue;
      }

      const originalName = String(
        getCandidateByStationId(parsed.id)?.display_name || "",
      ).trim();
      if (cleanRename === originalName) {
        continue;
      }

      targets.push({
        canonical_station_id: parsed.id,
        rename_to: cleanRename,
      });
    }

    return targets;
  }

  function buildSegmentWalkLinksFromGroups(groups, stationToSegment) {
    const links = [];

    for (let i = 0; i < groups.length; i += 1) {
      for (let j = i + 1; j < groups.length; j += 1) {
        const groupA = groups[i];
        const groupB = groups[j];

        const segmentA =
          stationToSegment.get(groupA.member_station_ids[0]) || "";
        const segmentB =
          stationToSegment.get(groupB.member_station_ids[0]) || "";
        if (!segmentA || !segmentB || segmentA === segmentB) {
          continue;
        }

        links.push({
          from_segment_id: segmentA,
          to_segment_id: segmentB,
          min_walk_minutes:
            Number.parseInt(
              String(
                draftState.pairWalkMinutesByKey[
                  pairKey(groupA.group_id, groupB.group_id)
                ] ?? 5,
              ),
              10,
            ) || 5,
          bidirectional: true,
          metadata: {
            from_group_id: groupA.group_id,
            to_group_id: groupB.group_id,
            source: "staged-conflict-editor",
          },
        });
      }
    }

    return links;
  }

  function buildSplitGroupsFromDraft() {
    pruneGroupReferences();

    const groups = [];
    for (const draftGroup of draftState.groups) {
      const memberIds = uniqueStrings(
        (draftGroup.member_refs || []).flatMap((refKey) =>
          getRefMemberStationIds(refKey),
        ),
      );

      if (memberIds.length === 0) {
        continue;
      }

      const groupName =
        String(
          draftState.renameByRef[toGroupRef(draftGroup.group_id)] ||
            draftGroup.section_name ||
            `Group ${groups.length + 1}`,
        ).trim() || `Group ${groups.length + 1}`;

      groups.push({
        draft_group_id: draftGroup.group_id,
        group_label: groupName,
        section_type:
          String(draftGroup.section_type || "other").trim() || "other",
        section_name: groupName,
        rename_to: groupName,
        target_canonical_station_id: memberIds[0],
        member_station_ids: sortStationIdsByCandidateRank(memberIds),
      });
    }

    if (groups.length < 2) {
      throw new Error(
        "Split/group resolve needs at least two non-empty groups.",
      );
    }

    const stationToSegment = new Map(
      (Array.isArray(activeClusterDetail?.candidates)
        ? activeClusterDetail.candidates
        : []
      )
        .map((candidate) => [
          candidate.canonical_station_id,
          String(candidate?.segment_context?.segment_id || "").trim(),
        ])
        .filter((pair) => Boolean(pair[1])),
    );

    const walkLinks = buildSegmentWalkLinksFromGroups(groups, stationToSegment);
    if (groups[0] && walkLinks.length > 0) {
      groups[0].segment_action = {
        walk_links: walkLinks,
      };
    }

    return groups.map((group) => {
      const out = {
        group_label: group.group_label,
        section_type: group.section_type,
        section_name: group.section_name,
        target_canonical_station_id: group.target_canonical_station_id,
        member_station_ids: group.member_station_ids,
        rename_to: group.rename_to,
      };
      if (group.segment_action) {
        out.segment_action = group.segment_action;
      }
      return out;
    });
  }

  function buildResolvePayload() {
    pruneHiddenSelections();
    pruneGroupReferences();

    const note = String(
      document.getElementById("editNoteInput").value || "",
    ).trim();
    const renameTargets = buildRenameTargetsPayload();

    if (draftState.groups.length > 0) {
      const groups = buildSplitGroupsFromDraft();
      const selectedStationIds = uniqueStrings(
        groups.flatMap((group) => group.member_station_ids),
      );

      return {
        operation: "split",
        selected_station_ids: sortStationIdsByCandidateRank(selectedStationIds),
        groups,
        rename_targets: renameTargets,
        note,
      };
    }

    const selectedCandidates = uniqueStrings(
      sortStationIdsByCandidateRank(Array.from(selectedStationIds.values())),
    );
    if (selectedCandidates.length < 2) {
      throw new Error("Select at least two candidates before resolving.");
    }

    if (activeTool === "split") {
      const midpoint = Math.ceil(selectedCandidates.length / 2);
      const left = selectedCandidates.slice(0, midpoint);
      const right = selectedCandidates.slice(midpoint);
      if (left.length === 0 || right.length === 0) {
        throw new Error("Split resolve needs at least two groups.");
      }

      const groups = [
        {
          group_label: "Split A",
          section_type: inferCandidateCategory(
            getCandidateByStationId(left[0]),
          ),
          section_name: "Split A",
          target_canonical_station_id: left[0],
          member_station_ids: left,
          rename_to: draftState.renameByRef[toGroupRef("split_a")] || "Split A",
        },
        {
          group_label: "Split B",
          section_type: inferCandidateCategory(
            getCandidateByStationId(right[0]),
          ),
          section_name: "Split B",
          target_canonical_station_id: right[0],
          member_station_ids: right,
          rename_to: draftState.renameByRef[toGroupRef("split_b")] || "Split B",
        },
      ];

      const stationToSegment = new Map(
        (Array.isArray(activeClusterDetail?.candidates)
          ? activeClusterDetail.candidates
          : []
        )
          .map((candidate) => [
            candidate.canonical_station_id,
            String(candidate?.segment_context?.segment_id || "").trim(),
          ])
          .filter((pair) => Boolean(pair[1])),
      );

      const segmentA = stationToSegment.get(left[0]) || "";
      const segmentB = stationToSegment.get(right[0]) || "";
      if (segmentA && segmentB && segmentA !== segmentB) {
        groups[0].segment_action = {
          walk_links: [
            {
              from_segment_id: segmentA,
              to_segment_id: segmentB,
              min_walk_minutes: 5,
              bidirectional: true,
              metadata: {
                source: "split-auto",
              },
            },
          ],
        };
      }

      return {
        operation: "split",
        selected_station_ids: selectedCandidates,
        groups,
        rename_targets: renameTargets,
        note,
      };
    }

    const renameInput = String(
      document.getElementById("editMergeRenameInput").value || "",
    ).trim();
    const renameTo =
      renameInput || resolveMergeNameAssumption(selectedCandidates);
    return {
      operation: "merge",
      selected_station_ids: selectedCandidates,
      groups: [
        {
          group_label: "merge-selected",
          member_station_ids: selectedCandidates,
          rename_to: renameTo,
        },
      ],
      rename_to: renameTo || null,
      rename_targets: renameTargets,
      note,
    };
  }

  function updateToolAvailability() {
    const mergeBtn = document.getElementById("toolMergeBtn");
    const splitBtn = document.getElementById("toolSplitBtn");
    const groupBtn = document.getElementById("toolGroupBtn");
    const summaryEl = document.getElementById("toolAvailabilitySummary");

    const mergeAvailable = selectedStationIds.size >= 2;
    const splitAvailable = selectedStationIds.size >= 2;
    const groupAvailable =
      selectedStationIds.size > 0 ||
      selectedDraftMergeIds.size > 0 ||
      draftState.groups.length > 0;

    if (mergeBtn) {
      mergeBtn.disabled = !mergeAvailable;
      mergeBtn.classList.toggle("is-available", mergeAvailable);
    }
    if (splitBtn) {
      splitBtn.disabled = !splitAvailable;
      splitBtn.classList.toggle("is-available", splitAvailable);
    }
    if (groupBtn) {
      groupBtn.disabled = !groupAvailable;
      groupBtn.classList.toggle("is-available", groupAvailable);
    }

    if (summaryEl) {
      summaryEl.textContent = `Merge ${mergeAvailable ? "available" : "needs 2+"} • Split ${splitAvailable ? "available" : "needs 2+"} • Group ${groupAvailable ? "available" : "select entries"}`;
    }
  }

  function setActiveTool(tool) {
    activeTool = tool;

    document.querySelectorAll(".tool-btn").forEach((button) => {
      button.classList.toggle("active", button.dataset.tool === tool);
    });

    document.querySelectorAll("[data-tool-panel]").forEach((panel) => {
      panel.hidden = panel.dataset.toolPanel !== tool;
    });

    if (tool === "merge") {
      updateMergeNameAssumption();
    }

    updateEditPreview();
  }

  function buildInlineRenameEditorHtml(refKey) {
    if (renameEditorState.refKey !== refKey) {
      return "";
    }

    return `
      <div class="inline-rename-row" data-inline-rename-row="${escapeHtml(refKey)}">
        <input type="text" data-inline-rename-input="${escapeHtml(refKey)}" value="${escapeHtml(renameEditorState.value)}" />
        <button type="button" class="btn-secondary" data-inline-rename-save="${escapeHtml(refKey)}">Save</button>
        <button type="button" class="btn-secondary" data-inline-rename-cancel="${escapeHtml(refKey)}">Cancel</button>
      </div>
    `;
  }

  function bindInlineRenameControls(rootEl) {
    if (!rootEl) {
      return;
    }

    rootEl.querySelectorAll("[data-rename-ref]").forEach((button) => {
      button.addEventListener("click", () => {
        beginInlineRename(button.dataset.renameRef);
      });
    });

    rootEl.querySelectorAll("[data-inline-rename-input]").forEach((input) => {
      input.addEventListener("input", (event) => {
        updateInlineRenameDraft(event.target.value);
      });
      input.addEventListener("keydown", (event) => {
        if (event.key === "Enter") {
          event.preventDefault();
          commitInlineRename();
        }
      });
    });

    rootEl.querySelectorAll("[data-inline-rename-save]").forEach((button) => {
      button.addEventListener("click", commitInlineRename);
    });

    rootEl.querySelectorAll("[data-inline-rename-cancel]").forEach((button) => {
      button.addEventListener("click", cancelInlineRename);
    });
  }

  function renderGroupTargetOptions() {
    const selectEl = document.getElementById("groupTargetSelect");
    if (!selectEl) {
      return;
    }

    const previous = String(selectEl.value || "").trim();
    selectEl.innerHTML = "";

    if (draftState.groups.length === 0) {
      const option = document.createElement("option");
      option.value = "";
      option.textContent = "Create a group first";
      selectEl.appendChild(option);
      selectEl.disabled = true;
      return;
    }

    for (const group of draftState.groups) {
      const option = document.createElement("option");
      option.value = group.group_id;
      const label =
        String(
          draftState.renameByRef[toGroupRef(group.group_id)] ||
            group.section_name ||
            group.group_id,
        ).trim() || group.group_id;
      option.textContent = `${label} (${group.member_refs.length})`;
      selectEl.appendChild(option);
    }

    selectEl.disabled = false;
    if (draftState.groups.some((group) => group.group_id === previous)) {
      selectEl.value = previous;
    }
  }

  function renderPairWalkEditor() {
    const container = document.getElementById("groupPairWalkList");
    if (!container) {
      return;
    }

    container.innerHTML = "";
    if (draftState.groups.length < 2) {
      container.innerHTML =
        '<p class="muted tiny">Pairwise links appear when at least two groups exist.</p>';
      return;
    }

    ensurePairWalkDefaults();

    for (let i = 0; i < draftState.groups.length; i += 1) {
      for (let j = i + 1; j < draftState.groups.length; j += 1) {
        const groupA = draftState.groups[i];
        const groupB = draftState.groups[j];
        const key = pairKey(groupA.group_id, groupB.group_id);

        const row = document.createElement("div");
        row.className = "walk-link-item";
        row.innerHTML = `
          <span>
            ${escapeHtml(draftState.renameByRef[toGroupRef(groupA.group_id)] || groupA.section_name)} ↔
            ${escapeHtml(draftState.renameByRef[toGroupRef(groupB.group_id)] || groupB.section_name)}
          </span>
          <input
            type="number"
            min="0"
            step="1"
            value="${escapeHtml(draftState.pairWalkMinutesByKey[key])}"
            data-walk-pair-from="${escapeHtml(groupA.group_id)}"
            data-walk-pair-to="${escapeHtml(groupB.group_id)}"
          />
          <span class="muted tiny">min</span>
        `;

        const input = row.querySelector("input");
        input.addEventListener("input", (event) => {
          setPairWalkMinutes(
            groupA.group_id,
            groupB.group_id,
            event.target.value,
          );
        });

        container.appendChild(row);
      }
    }
  }

  function renderGroupEditor() {
    const listEl = document.getElementById("groupSectionList");
    if (!listEl) {
      return;
    }

    listEl.innerHTML = "";
    if (draftState.groups.length === 0) {
      listEl.innerHTML = '<p class="muted tiny">No groups yet.</p>';
      renderPairWalkEditor();
      renderGroupTargetOptions();
      return;
    }

    for (const group of draftState.groups) {
      const groupLabel =
        String(
          draftState.renameByRef[toGroupRef(group.group_id)] ||
            group.section_name ||
            group.group_id,
        ).trim() || group.group_id;

      const card = document.createElement("div");
      card.className = "group-section-card";
      const memberRows = (group.member_refs || [])
        .map((refKey) => {
          const parsed = parseRef(refKey);
          const title = getRefTitle(refKey);
          return `
          <li class="curated-member-item">
            <span>${escapeHtml(title)}</span>
            <span class="ui-tag">${escapeHtml(parsed.type)}</span>
            <button type="button" class="btn-secondary" data-remove-group-member="${escapeHtml(group.group_id)}|${escapeHtml(refKey)}">Remove</button>
          </li>
        `;
        })
        .join("");

      card.innerHTML = `
        <div class="row compact-row" style="grid-template-columns: 1fr auto auto; margin-bottom: 6px;">
          <strong>${escapeHtml(groupLabel)}</strong>
          <button type="button" class="rename-pencil" data-rename-ref="${escapeHtml(toGroupRef(group.group_id))}">✎</button>
          <button type="button" class="btn-danger" data-remove-group="${escapeHtml(group.group_id)}">Delete</button>
        </div>
        ${buildInlineRenameEditorHtml(toGroupRef(group.group_id))}
        <div class="row compact-row" style="grid-template-columns: 1fr 1fr; margin-bottom: 6px;">
          <input type="text" data-group-name="${escapeHtml(group.group_id)}" value="${escapeHtml(groupLabel)}" />
          <select data-group-type="${escapeHtml(group.group_id)}">
            <option value="main" ${group.section_type === "main" ? "selected" : ""}>main</option>
            <option value="secondary" ${group.section_type === "secondary" ? "selected" : ""}>secondary</option>
            <option value="subway" ${group.section_type === "subway" ? "selected" : ""}>subway</option>
            <option value="bus" ${group.section_type === "bus" ? "selected" : ""}>bus</option>
            <option value="tram" ${group.section_type === "tram" ? "selected" : ""}>tram</option>
            <option value="other" ${group.section_type === "other" ? "selected" : ""}>other</option>
          </select>
        </div>
        <ul class="curated-member-list">${memberRows || '<li class="muted tiny">No members yet.</li>'}</ul>
      `;

      card.querySelectorAll("[data-remove-group]").forEach((button) => {
        button.addEventListener("click", () =>
          removeGroup(button.dataset.removeGroup),
        );
      });

      card.querySelectorAll("[data-remove-group-member]").forEach((button) => {
        button.addEventListener("click", () => {
          const value = String(button.dataset.removeGroupMember || "").trim();
          const delimiter = value.indexOf("|");
          if (delimiter <= 0) {
            return;
          }
          const groupId = value.slice(0, delimiter);
          const refKey = value.slice(delimiter + 1);
          removeGroupMember(groupId, refKey);
        });
      });

      card.querySelectorAll("[data-group-name]").forEach((input) => {
        input.addEventListener("change", (event) => {
          updateGroupName(input.dataset.groupName, event.target.value);
        });
      });

      card.querySelectorAll("[data-group-type]").forEach((select) => {
        select.addEventListener("change", (event) => {
          updateGroupType(select.dataset.groupType, event.target.value);
        });
      });

      bindInlineRenameControls(card);
      listEl.appendChild(card);
    }

    renderPairWalkEditor();
    renderGroupTargetOptions();
  }

  function updateEditPreview() {
    const impactEl = document.getElementById("editImpact");
    const previewEl = document.getElementById("editPayloadPreview");

    try {
      const payload = buildResolvePayload();
      previewEl.textContent = JSON.stringify(payload, null, 2);

      const summary =
        payload.operation === "merge"
          ? `Final resolve will submit one merge decision for ${payload.selected_station_ids.length} candidates.`
          : `Final resolve will submit one split decision with ${payload.groups.length} group(s).`;

      impactEl.innerHTML = `
        <strong class="impact-title">Resolve Impact</strong>
        <ul class="impact-list">
          <li>${escapeHtml(summary)}</li>
          <li>Active groups: ${escapeHtml(draftState.groups.length)}</li>
          <li>Rename targets: ${escapeHtml((payload.rename_targets || []).length)}</li>
        </ul>
      `;
    } catch (err) {
      previewEl.textContent = "{}";
      impactEl.innerHTML = `<span>${escapeHtml(err.message)}</span>`;
    }

    updateToolAvailability();
  }

  async function resolveConflict() {
    if (!activeClusterDetail) {
      showNotice("Select a cluster first.", "error");
      return;
    }

    try {
      const payload = buildResolvePayload();
      const res = await fetch(
        `/api/qa/clusters/${encodeURIComponent(activeClusterDetail.cluster_id)}/decisions`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        },
      );

      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(data?.error || data?.message || `HTTP ${res.status}`);
      }

      showNotice(
        `Conflict resolved (decision id=${data.decision_id || "n/a"}).`,
        "success",
      );
      clearSelection();
      resetDraftState();
      await fetchClusters();
      await loadClusterDetail(activeClusterDetail.cluster_id, true);
    } catch (err) {
      showNotice(`Failed to resolve conflict: ${err.message}`, "error", true);
    }
  }

  const fetchCuratedProjection = async (clusterId) => {
    const cleanClusterId = String(clusterId || "").trim();
    if (!cleanClusterId) {
      return [];
    }

    const params = new URLSearchParams();
    params.set("cluster_id", cleanClusterId);
    params.set("status", "active");
    params.set("limit", "25");

    const res = await fetch(`/api/qa/curated-stations?${params.toString()}`);
    const payload = await res.json().catch(() => null);
    if (!res.ok) {
      throw new Error(payload?.error || `HTTP ${res.status}`);
    }
    return Array.isArray(payload) ? payload : [];
  };

  function resolveCandidateNameById(canonicalStationId) {
    const cleanId = String(canonicalStationId || "").trim();
    if (!cleanId) {
      return "";
    }

    const candidates = Array.isArray(activeClusterDetail?.candidates)
      ? activeClusterDetail.candidates
      : [];
    const match = candidates.find(
      (candidate) => candidate.canonical_station_id === cleanId,
    );
    if (!match) {
      return cleanId;
    }

    const renamed = draftState.renameByRef[toCandidateRef(cleanId)] || "";
    return renamed || String(match.display_name || "").trim() || cleanId;
  }

  function renderDraftMergesInline(candidatesEl) {
    if (!candidatesEl || draftState.mergeItems.length === 0) {
      return;
    }

    const intro = document.createElement("p");
    intro.className = "muted tiny";
    intro.textContent = `${draftState.mergeItems.length} staged merge item(s). Members are hidden from standalone cards and map markers until resolve.`;
    candidatesEl.appendChild(intro);

    for (const mergeItem of draftState.mergeItems) {
      const mergeRef = toMergeRef(mergeItem.merge_id);
      const selected = selectedDraftMergeIds.has(mergeItem.merge_id);
      const displayName = resolveDraftMergeDisplayName(mergeItem);
      const memberRows = mergeItem.member_station_ids
        .map((stationId) => {
          return `
          <li class="curated-member-item">
            <strong>${escapeHtml(resolveCandidateNameById(stationId) || stationId)}</strong>
            <span class="muted tiny">${escapeHtml(stationId)}</span>
          </li>
        `;
        })
        .join("");

      const card = document.createElement("details");
      card.className = `candidate-card curated-candidate-card ${selected ? "selected" : ""}`;
      card.open = true;
      card.innerHTML = `
        <summary class="curated-candidate-summary">
          <label class="candidate-select">
            <input type="checkbox" data-select-merge="${escapeHtml(mergeItem.merge_id)}" ${selected ? "checked" : ""} />
            <strong>${escapeHtml(displayName)}</strong>
          </label>
          <span class="ui-tag merged-tag">draft merge</span>
          <button type="button" class="rename-pencil" data-rename-ref="${escapeHtml(mergeRef)}">✎</button>
          <button type="button" class="btn-secondary" data-remove-merge="${escapeHtml(mergeItem.merge_id)}">Delete</button>
        </summary>
        ${buildInlineRenameEditorHtml(mergeRef)}
        <details class="curated-members-details">
          <summary>Members (${escapeHtml(mergeItem.member_station_ids.length)})</summary>
          <ul class="curated-member-list">
            ${memberRows || '<li class="muted tiny">No members.</li>'}
          </ul>
        </details>
      `;

      const checkbox = card.querySelector("[data-select-merge]");
      checkbox.addEventListener("change", (event) => {
        toggleDraftMergeSelection(mergeItem.merge_id, event.target.checked);
      });

      const removeBtn = card.querySelector("[data-remove-merge]");
      removeBtn.addEventListener("click", () => {
        removeDraftMerge(mergeItem.merge_id);
      });

      bindInlineRenameControls(card);
      candidatesEl.appendChild(card);
    }
  }

  function renderCuratedCandidatesInline(candidatesEl) {
    if (
      !candidatesEl ||
      !Array.isArray(activeCuratedProjectionItems) ||
      activeCuratedProjectionItems.length === 0
    ) {
      return;
    }

    const intro = document.createElement("p");
    intro.className = "muted tiny";
    intro.textContent = `${activeCuratedProjectionItems.length} merged/grouped candidate(s) derived from already-applied decisions in this cluster.`;
    candidatesEl.appendChild(intro);

    for (const item of activeCuratedProjectionItems) {
      const members = Array.isArray(item.members) ? item.members : [];
      const fieldNameSource = (
        Array.isArray(item.field_provenance) ? item.field_provenance : []
      ).find((row) => String(row?.field_name || "") === "display_name");
      const summary = document.createElement("details");
      summary.className = "candidate-card curated-candidate-card";
      summary.open = true;

      const memberRows = members
        .map((member) => {
          const stationId = String(member?.canonical_station_id || "").trim();
          const stationName = resolveCandidateNameById(stationId);
          const role =
            String(member?.member_role || "member").trim() || "member";
          return `
          <li class="curated-member-item">
            <strong>${escapeHtml(stationName || stationId || "Unknown")}</strong>
            <span class="ui-tag">${escapeHtml(role)}</span>
            <span class="muted tiny">${escapeHtml(stationId || "n/a")}</span>
          </li>
        `;
        })
        .join("");

      summary.innerHTML = `
        <summary class="curated-candidate-summary">
          <strong>${escapeHtml(item.display_name || item.curated_station_id)}</strong>
          <span class="ui-tag merged-tag">${escapeHtml(item.derived_operation || "derived")}</span>
          <span class="muted tiny">${escapeHtml(item.curated_station_id || "")}</span>
        </summary>
        <p class="muted tiny" style="margin-top: 8px;">
          Name source: ${escapeHtml(fieldNameSource?.source_kind || "n/a")} (${escapeHtml(fieldNameSource?.source_ref || "n/a")})
        </p>
        <details class="curated-members-details">
          <summary>Members (${escapeHtml(members.length)})</summary>
          <ul class="curated-member-list">
            ${memberRows || '<li class="muted tiny">No member rows.</li>'}
          </ul>
        </details>
      `;
      candidatesEl.appendChild(summary);
    }
  }

  function renderEmptyClusterDetail(elements) {
    elements.headerEl.textContent = "Select a cluster";
    elements.metaEl.textContent = "Cluster details will appear here.";
    elements.candidatesEl.innerHTML =
      '<p class="muted">No cluster selected.</p>';
    elements.evidenceEl.innerHTML = '<p class="muted">No cluster selected.</p>';
    elements.decisionsEl.innerHTML =
      '<p class="muted">No cluster selected.</p>';
    document.getElementById("editPayloadPreview").textContent = "{}";
    updateSelectionSummary();
    renderSelectedServiceContext();
    renderGroupEditor();
    updateEditPreview();
  }

  function renderStandaloneCandidates(candidatesEl, visibleCandidates) {
    if (visibleCandidates.length === 0) {
      const emptyStandaloneMessage = document.createElement("p");
      emptyStandaloneMessage.className =
        "muted tiny candidate-list-empty-muted";
      emptyStandaloneMessage.textContent =
        "No standalone candidates remain. Members are shown inside merged derived cards.";
      candidatesEl.appendChild(emptyStandaloneMessage);
      return;
    }

    for (const candidate of visibleCandidates) {
      const stationId = candidate.canonical_station_id;
      const refKey = toCandidateRef(stationId);
      const selected = selectedStationIds.has(stationId);

      const aliases = Array.isArray(candidate.aliases)
        ? candidate.aliases.filter(Boolean)
        : [];
      const providers = Array.isArray(candidate.provider_labels)
        ? candidate.provider_labels.filter(Boolean)
        : [];
      const lines = Array.isArray(candidate?.service_context?.lines)
        ? candidate.service_context.lines.slice(0, 8)
        : [];
      const completeness = candidate?.service_context?.completeness || {};
      const coverageStatus = String(completeness.status || "unknown");
      const coverageNotes = String(completeness.notes || "").trim();
      const renamed = draftState.renameByRef[refKey] || "";
      const displayName = renamed || candidate.display_name || stationId;

      const card = document.createElement("div");
      card.className = `candidate-card ${selected ? "selected" : ""}`;
      card.innerHTML = `
        <div class="row compact-row candidate-row-head" style="grid-template-columns: 1fr auto auto;">
          <label class="candidate-select">
            <input type="checkbox" data-station-id="${escapeHtml(stationId)}" ${selected ? "checked" : ""} />
            <span>
              <strong>${escapeHtml(displayName)}</strong>
              <span class="muted tiny">${escapeHtml(stationId)}</span>
            </span>
          </label>
          <button type="button" class="rename-pencil" data-rename-ref="${escapeHtml(refKey)}">✎</button>
          <span class="candidate-rank">#${escapeHtml(candidate.candidate_rank)}</span>
        </div>
        ${buildInlineRenameEditorHtml(refKey)}
        <div class="tag-row">
          <span class="ui-tag">feeds: ${escapeHtml(providers.join(", ") || "n/a")}</span>
          <span class="ui-tag">lines: ${escapeHtml(lines.length > 0 ? `${lines.join(", ")}` : "none")}</span>
          <span class="ui-tag" title="${escapeHtml(coverageNotes || "Coverage metadata unavailable")}">coverage: ${escapeHtml(coverageStatus)}</span>
        </div>
        <details class="candidate-details">
          <summary>Details</summary>
          <p class="muted tiny">Aliases: ${escapeHtml(aliases.join(", ") || "none")}</p>
          <p class="muted tiny">Incoming: ${escapeHtml((candidate?.service_context?.incoming || []).join(", ") || "none")}</p>
          <p class="muted tiny">Outgoing: ${escapeHtml((candidate?.service_context?.outgoing || []).join(", ") || "none")}</p>
        </details>
      `;

      const checkbox = card.querySelector('input[type="checkbox"]');
      checkbox.addEventListener("change", (event) => {
        toggleCandidateSelection(stationId, event.target.checked);
      });

      bindInlineRenameControls(card);
      candidatesEl.appendChild(card);
    }
  }

  function renderEvidenceRows(evidenceEl, evidenceRows) {
    evidenceEl.innerHTML = "";
    if (evidenceRows.length === 0) {
      evidenceEl.innerHTML =
        '<p class="muted">No evidence rows for this cluster.</p>';
      return;
    }

    for (const row of evidenceRows.slice(0, 30)) {
      const div = document.createElement("div");
      div.className = "evidence-row";
      div.innerHTML = `<strong>${escapeHtml(row.evidence_type)}</strong> • ${escapeHtml(row.source_canonical_station_id)} ↔ ${escapeHtml(row.target_canonical_station_id)} • score ${escapeHtml(row.score ?? "n/a")}`;
      evidenceEl.appendChild(div);
    }
  }

  function renderHistoryRows(decisionsEl, detail) {
    decisionsEl.innerHTML = "";
    const historyRows = [];
    for (const decision of detail.decisions || []) {
      historyRows.push({
        title: decision.operation,
        requested_by: decision.requested_by,
        created_at: decision.created_at,
      });
    }
    for (const event of detail.edit_history || []) {
      historyRows.push({
        title: event.event_type,
        requested_by: event.requested_by,
        created_at: event.created_at,
      });
    }

    if (historyRows.length === 0) {
      decisionsEl.innerHTML =
        '<p class="muted">No applied edits recorded for this cluster.</p>';
      return;
    }

    historyRows
      .toSorted((a, b) =>
        String(b.created_at || "").localeCompare(String(a.created_at || "")),
      )
      .slice(0, 15)
      .forEach((item) => {
        const div = document.createElement("div");
        div.className = "decision-row";
        div.innerHTML = `<strong>${escapeHtml(item.title)}</strong> • ${escapeHtml(item.requested_by)} • ${escapeHtml(item.created_at)}`;
        decisionsEl.appendChild(div);
      });
  }

  function renderClusterDetail() {
    const { headerEl, metaEl, candidatesEl, evidenceEl, decisionsEl } =
      getClusterDetailElements();

    if (!activeClusterDetail) {
      renderEmptyClusterDetail({
        headerEl,
        metaEl,
        candidatesEl,
        evidenceEl,
        decisionsEl,
      });
      return;
    }

    pruneHiddenSelections();
    pruneGroupReferences();

    headerEl.textContent =
      activeClusterDetail.display_name || activeClusterDetail.cluster_id;
    metaEl.textContent = `${activeClusterDetail.country} • ${activeClusterDetail.severity} • ${activeClusterDetail.status} • scope ${activeClusterDetail.scope_tag}`;

    const visibleCandidates = getVisibleStandaloneCandidates();

    candidatesEl.innerHTML = "";
    renderDraftMergesInline(candidatesEl);
    renderCuratedCandidatesInline(candidatesEl);
    renderStandaloneCandidates(candidatesEl, visibleCandidates);
    renderEvidenceRows(evidenceEl, activeClusterDetail.evidence || []);
    renderHistoryRows(decisionsEl, activeClusterDetail);

    focusLocations(visibleCandidates);
    updateSelectionSummary();
    renderSelectedServiceContext();
    renderGroupEditor();
    updateEditPreview();
  }

  async function loadClusterDetail(clusterId, resetSelection = true) {
    try {
      if (!map) {
        await initMap();
      }

      const [data, curatedRes] = await Promise.all([
        graphqlQuery(
          `
          query GetClusterDetail($id: ID!) {
            cluster(id: $id) {
              cluster_id
              country
              status
              scope_tag
              severity
              display_name
              candidates {
                canonical_station_id
                name
                lat
                lon
                service_context {
                  lines
                  incoming
                  outgoing
                }
              }
              evidence {
                evidence_type
                source_canonical_station_id
                target_canonical_station_id
                score
              }
              decisions {
                operation
                requested_by
                created_at
              }
              edit_history {
                event_type
                requested_by
                created_at
              }
            }
          }
        `,
          { id: clusterId },
        ).catch((err) => {
          throw err;
        }),
        fetchCuratedProjection(clusterId).catch((err) => {
          showNotice(`Curated projection unavailable: ${err.message}`, "info");
          return [];
        }),
      ]);

      const payload = data.cluster;
      if (!payload) {
        throw new Error("Cluster not found in GraphQL response.");
      }

      activeClusterDetail = payload;
      activeCuratedProjectionItems = Array.isArray(curatedRes)
        ? curatedRes
        : [];
      activeClusterSummary =
        clusterItems.find((item) => item.cluster_id === clusterId) || null;

      if (resetSelection) {
        clearSelection();
        resetDraftState();
      }

      renderClusterList();
      renderClusterDetail();
    } catch (err) {
      showNotice(`Failed to load cluster: ${err.message}`, "error", true);
    }
  }

  function bindEvents() {
    document
      .getElementById("refreshBtn")
      .addEventListener("click", fetchClusters);
    document
      .getElementById("countryFilter")
      .addEventListener("change", fetchClusters);
    document
      .getElementById("statusFilter")
      .addEventListener("change", fetchClusters);
    document
      .getElementById("scopeTagFilter")
      .addEventListener("change", fetchClusters);

    document
      .getElementById("candidateSelectAllBtn")
      .addEventListener("click", selectAllCandidates);
    document
      .getElementById("candidateClearBtn")
      .addEventListener("click", clearCandidateSelection);

    document.querySelectorAll(".tool-btn").forEach((button) => {
      button.addEventListener("click", () => {
        const tool = String(button.dataset.tool || "").trim();
        if (tool) {
          setActiveTool(tool);
        }
      });
    });

    const askAiBtn = document.getElementById("askAiBtn");
    if (askAiBtn) {
      askAiBtn.addEventListener("click", async () => {
        if (!activeClusterDetail) {
          showNotice("Select a cluster first.", "error");
          return;
        }

        const scoreEl = document.getElementById("aiScoreResult");
        scoreEl.hidden = false;
        scoreEl.className = "ui-notice ui-notice-info";
        scoreEl.innerText = "AI is analyzing candidates...";

        try {
          const data = await graphqlQuery(
            `
            mutation ScoreCluster($id: ID!) {
              requestAiScore(clusterId: $id) {
                confidence_score
                suggested_action
                reasoning
              }
            }
          `,
            { id: activeClusterDetail.cluster_id },
          );

          const ai = data.requestAiScore;
          if (!ai) throw new Error("No response from AI.");

          scoreEl.className = `ui-notice ${ai.suggested_action === "merge" ? "ui-notice-success" : "ui-notice-warning"}`;
          scoreEl.innerHTML = `<strong>AI Confidence ${(ai.confidence_score * 100).toFixed(0)}%:</strong> Suggests <strong>${ai.suggested_action.toUpperCase()}</strong>. ${escapeHtml(ai.reasoning)}`;

          // Auto-select UI tool based on suggestion
          if (["merge", "split", "group"].includes(ai.suggested_action)) {
            setActiveTool(ai.suggested_action);
          }
        } catch (e) {
          scoreEl.className = "ui-notice ui-notice-error";
          scoreEl.innerText = `AI failed: ${e.message}`;
        }
      });
    }

    const createGroupBtn = document.getElementById(
      "createGroupFromSelectionBtn",
    );
    if (createGroupBtn) {
      createGroupBtn.addEventListener("click", createGroupFromSelection);
    }

    const addSelectionBtn = document.getElementById("addSelectionToGroupBtn");
    if (addSelectionBtn) {
      addSelectionBtn.addEventListener("click", addSelectionToGroup);
    }

    document
      .getElementById("resolveConflictBtn")
      .addEventListener("click", resolveConflict);

    document
      .getElementById("editMergeRenameInput")
      .addEventListener("input", updateEditPreview);
    document
      .getElementById("groupNameInput")
      .addEventListener("input", updateEditPreview);
    document
      .getElementById("groupSectionPresetType")
      .addEventListener("change", updateEditPreview);
    document
      .getElementById("groupTargetSelect")
      .addEventListener("change", updateEditPreview);

    document
      .getElementById("editNoteInput")
      .addEventListener("input", (event) => {
        draftState.note = String(event.target.value || "");
        updateEditPreview();
      });
  }

  async function init() {
    bindEvents();
    try {
      await initMap();
    } catch (err) {
      showNotice(`Map init warning: ${err.message} `, "error", true);
    }
    await fetchClusters();
    renderClusterDetail();
    setActiveTool("merge");
  }

  init();

  return () => {
    if (showNotice._timer) {
      clearTimeout(showNotice._timer);
    }
    if (activeMarkerPopup) {
      activeMarkerPopup.remove();
      activeMarkerPopup = null;
    }
    clearMarkers();
    if (map) {
      map.remove();
      map = null;
    }
  };
}
