import PropTypes from "prop-types";
import {
  useCallback,
  useEffect,
  useMemo,
  useReducer,
  useRef,
  useState,
} from "react";
import {
  formatCandidateProvenanceTooltip,
  formatEvidenceCategoryLabel,
  formatEvidenceDetails,
  formatEvidenceStatusLabel,
  formatEvidenceTypeLabel,
  formatEvidenceValue,
  formatLabel,
  formatProviderFeedsTooltip,
  formatSeedReasonLabel,
  getEvidenceCategoryCounts,
  getEvidenceTypeCounts,
  getRowSeedReasons,
  getSeedRuleCounts,
  getSummaryCount,
} from "./curation-page-formatters";
import {
  BASE_MARKER_SIZE,
  buildMappableItems,
  buildMarkerOverlapLayout,
  EXTERNAL_REFERENCE_MIN_ZOOM,
  MARKER_SELECTION_RING_SIZE,
  shouldShowExternalReferencePointsAtZoom,
} from "./curation-page-map-utils";
import {
  clusterDetailShape,
  clusterListItemShape,
  evidenceRowShape,
  filtersShape,
  railItemShape,
  refSetShape,
  workspaceShape,
} from "./curation-page-prop-types";
import {
  addSelectionToGroup,
  fetchClusterDetail as apiFetchClusterDetail,
  fetchClusters as apiFetchClusters,
  buildCandidateMap,
  buildRailItems,
  createEmptyWorkspace,
  createGroupFromSelection,
  createMergeFromSelection,
  fetchReferenceViewport,
  formatResultsLabel,
  getRenameValue,
  markKeepSeparate,
  normalizeWorkspace,
  parseRef,
  removeMemberFromGroup,
  removeMemberFromMerge,
  reopenCluster,
  requestAiScore,
  resolveCluster,
  resolveDefaultMapStyle,
  resolveDisplayNameForRef,
  resolveRefMemberStationIds,
  resolveSatelliteMapStyle,
  saveClusterWorkspace,
  serializeWorkspace,
  setRenameValue,
  sortCandidateIds,
  splitComposite,
  toGroupRef,
  toRawRef,
  undoClusterWorkspace,
  updateCompositeName,
  updateGroupNodeLabel,
  updateGroupTransferSeconds,
} from "./curation-page-runtime";
import { createUiState, uiReducer } from "./curation-page-ui-state";
import maplibregl from "./maplibre";
import "./styles.css";

function resolveSectionToneClass(tone) {
  if (tone === "risk") {
    return "border-red/15 bg-red-dim/10";
  }
  return "border-border bg-surface-1/40";
}

function resolveMapStyle(mode) {
  if (mode === "satellite") {
    return resolveSatelliteMapStyle();
  }
  return resolveDefaultMapStyle();
}

function clearMarkers(markers) {
  for (const marker of markers) {
    marker.remove();
  }
}

function resolveClusterCount(totalCount, clusters) {
  if (Number.isFinite(totalCount) && totalCount > 0) {
    return totalCount;
  }
  return clusters.length;
}

function applyWorkspaceResponseToClusterDetail(
  previous,
  response,
  workspace,
  extra = {},
) {
  if (!previous) {
    return previous;
  }
  return {
    ...previous,
    workspace,
    workspace_version: response.workspace_version,
    has_workspace: true,
    effective_status: response.effective_status,
    ...extra,
  };
}

function applyWorkspaceResponseToClusters(
  clusters,
  activeClusterId,
  response,
  extra = {},
) {
  return clusters.map((cluster) =>
    cluster.cluster_id === activeClusterId
      ? {
          ...cluster,
          effective_status: response.effective_status,
          has_workspace: true,
          workspace_version: response.workspace_version,
          ...extra,
        }
      : cluster,
  );
}

function resolveKindAccent(kind) {
  if (kind === "merge") {
    return "border-l-[3px] border-l-teal";
  }
  if (kind === "group") {
    return "border-l-[3px] border-l-orange";
  }
  return "";
}

function resolveNoticeToneClass(tone) {
  if (tone === "error") {
    return "bg-red-dim text-red";
  }
  if (tone === "success") {
    return "bg-green-dim text-green";
  }
  if (tone === "warning") {
    return "bg-yellow-dim text-yellow";
  }
  return "bg-blue-dim text-blue";
}

function resolveClusterHeading(clusterDetail) {
  if (!clusterDetail) {
    return "No cluster";
  }
  return clusterDetail.display_name || clusterDetail.cluster_id;
}

function resolveResolveButtonLabel(canReopen) {
  return canReopen ? "Reopen" : "Resolve";
}

function isMergeableRef(ref) {
  const type = parseRef(ref).type;
  return type === "raw" || type === "merge";
}

function resolveSelectedIndex(railIndex, refs) {
  if (refs.length === 0) {
    return -1;
  }
  return railIndex.get(refs.at(-1)) || 0;
}

function resolveMapModeHandler(mode, handlers) {
  if (mode === "default") {
    handlers.setDefault();
    return;
  }
  if (mode === "satellite") {
    handlers.setSatellite();
    return;
  }
  handlers.setCustom(mode);
}

const EXTERNAL_SOURCE_META = {
  overture: {
    label: "Overture",
    color: "#22c55e",
    pillClassName: "border-green/30 bg-green-dim text-green",
  },
  wikidata: {
    label: "Wikidata",
    color: "#38bdf8",
    pillClassName: "border-blue/30 bg-blue-dim text-blue",
  },
  geonames: {
    label: "GeoNames",
    color: "#f97316",
    pillClassName: "border-orange/30 bg-orange-dim text-orange",
  },
};

function resolveExternalSourceMeta(sourceId) {
  return (
    EXTERNAL_SOURCE_META[
      String(sourceId || "")
        .trim()
        .toLowerCase()
    ] || {
      label: formatLabel(sourceId || "external"),
      color: "#94a3b8",
      pillClassName: "border-border bg-surface-3 text-text-secondary",
    }
  );
}

function formatMeters(value) {
  const meters = Number(value);
  if (!Number.isFinite(meters)) {
    return "—";
  }
  return `${Math.round(meters)}m`;
}

function buildGoogleMapsLink(name, lat, lon) {
  const parts = [];
  const cleanName = String(name || "").trim();
  if (cleanName) {
    parts.push(cleanName);
  }
  if (Number.isFinite(Number(lat)) && Number.isFinite(Number(lon))) {
    parts.push(`${Number(lat)},${Number(lon)}`);
  }
  const query = parts.join(" ");
  if (!query) {
    return "";
  }
  const url = new URL("https://www.google.com/maps/search/");
  url.searchParams.set("api", "1");
  url.searchParams.set("query", query);
  return url.toString();
}

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function uniqueStrings(values) {
  return Array.from(
    new Set(
      (Array.isArray(values) ? values : [])
        .map((value) => String(value || "").trim())
        .filter(Boolean),
    ),
  );
}

function buildDisplayReferencePoints({
  focusReferencePoints = [],
  viewportReferencePoints = [],
  activeCandidateIds = [],
}) {
  const activeCandidateIdSet = new Set(uniqueStrings(activeCandidateIds));
  const merged = new Map();

  const mergePoint = (point, options = {}) => {
    const sourceId = String(point?.source_id || "")
      .trim()
      .toLowerCase();
    const externalId = String(point?.external_id || "").trim();
    const key = `${sourceId}:${externalId || `${point?.lat}:${point?.lon}`}`;
    const matchedCandidateIds = uniqueStrings(point?.matched_candidate_ids);
    const previous = merged.get(key);
    const combinedMatchedCandidateIds = uniqueStrings([
      ...(previous?.matched_candidate_ids || []),
      ...matchedCandidateIds,
    ]);
    const relevantCandidateIds = combinedMatchedCandidateIds.filter(
      (candidateId) => activeCandidateIdSet.has(candidateId),
    );

    merged.set(key, {
      ...previous,
      ...point,
      source_id: sourceId,
      external_id: externalId,
      matched_candidate_ids: combinedMatchedCandidateIds,
      relevant_candidate_ids: relevantCandidateIds,
      match_count: Math.max(
        Number(previous?.match_count || 0),
        Number(point?.match_count || 0),
        combinedMatchedCandidateIds.length,
      ),
      is_focus_overlay:
        options.isFocusOverlay === true || previous?.is_focus_overlay === true,
      is_relevant_to_active_cluster: relevantCandidateIds.length > 0,
    });
  };

  for (const point of Array.isArray(viewportReferencePoints)
    ? viewportReferencePoints
    : []) {
    mergePoint(point, { isFocusOverlay: false });
  }
  for (const point of Array.isArray(focusReferencePoints)
    ? focusReferencePoints
    : []) {
    mergePoint(point, { isFocusOverlay: true });
  }

  return Array.from(merged.values()).sort((left, right) => {
    return (
      Number(right.is_focus_overlay === true) -
        Number(left.is_focus_overlay === true) ||
      Number(right.is_relevant_to_active_cluster === true) -
        Number(left.is_relevant_to_active_cluster === true) ||
      Number(right.match_count || 0) - Number(left.match_count || 0) ||
      String(left.source_id || "").localeCompare(
        String(right.source_id || ""),
      ) ||
      String(left.display_name || "").localeCompare(
        String(right.display_name || ""),
      )
    );
  });
}

function getCompositeMembers(item, workspace, candidateMap) {
  return item.member_refs?.map((ref) =>
    resolveDisplayNameForRef(ref, workspace, candidateMap),
  );
}

function resolveEvidenceCardClassName(tone) {
  if (tone === "risk") {
    return "border border-red/15 rounded-xl px-3 py-2 bg-surface-2 text-sm";
  }
  return "border border-border rounded-xl px-3 py-2 bg-surface-2 text-sm";
}

function resolveMapModeButtonClassName(active, currentMode) {
  const isActive = currentMode === active;
  const activeClassName = "bg-amber/90 border-amber text-surface-0";
  const inactiveClassName =
    "bg-surface-0/50 border-white/10 text-white/80 hover:bg-surface-0/70";
  return `px-2.5 py-1 rounded-md text-xs font-bold font-display border backdrop-blur-md cursor-pointer transition-all ${isActive ? activeClassName : inactiveClassName}`;
}

function findWorkspaceEntity(kind, workspace, ref) {
  const entityId = parseRef(ref).id;
  const collection = kind === "group" ? workspace.groups : workspace.merges;
  return (collection || []).find((item) => item.entity_id === entityId) || null;
}

function resolveSeedScore(score) {
  const numericScore = Number(score);
  if (Number.isFinite(numericScore)) {
    return numericScore.toFixed(2);
  }
  return "—";
}

function resolveSelectionLabel(selectedRefs) {
  if (selectedRefs.size > 0) {
    return `${selectedRefs.size} sel.`;
  }
  return "Select";
}

function canSplitFocusedItem(focused) {
  return focused?.kind === "group" || focused?.kind === "merge";
}

function EvidenceSection({ title, tone = "default", children }) {
  return (
    <section
      className={`rounded-2xl border p-3 space-y-2 ${resolveSectionToneClass(tone)}`}
    >
      <div className="flex items-center justify-between gap-2">
        <h3 className="m-0 text-sm font-bold text-text-primary font-display uppercase tracking-wider">
          {title}
        </h3>
      </div>
      {children}
    </section>
  );
}

function EvidenceRows({ rows, tone = "default" }) {
  const cardClassName = resolveEvidenceCardClassName(tone);

  return (
    <div className="space-y-2">
      {rows.map((row) => {
        const rowSeedReasons = getRowSeedReasons(row);
        const details = formatEvidenceDetails(row.details);
        return (
          <div
            key={`${row.evidence_type}-${row.source_global_station_id}-${row.target_global_station_id}-${row.score ?? ""}`}
            className={cardClassName}
          >
            <div className="flex items-center justify-between gap-2">
              <div className="flex items-center gap-1.5 flex-wrap">
                <strong className="font-display text-text-primary">
                  {formatEvidenceTypeLabel(row.evidence_type)}
                </strong>
                {row.is_seed_rule === true && (
                  <Tag className="border-blue/20 bg-blue-dim text-blue">
                    Seed
                  </Tag>
                )}
              </div>
              <StatusPill v={row.status || "informational"}>
                {formatEvidenceStatusLabel(row.status)}
              </StatusPill>
            </div>
            <div className="flex gap-3 text-text-secondary mt-1">
              <span>
                {row.source_global_station_id} ↔ {row.target_global_station_id}
              </span>
              <span>{formatEvidenceValue(row)}</span>
            </div>
            {row.is_seed_rule === true && rowSeedReasons.length > 0 && (
              <div className="text-blue text-xs mt-1">
                Seeded by:{" "}
                {rowSeedReasons.map(formatSeedReasonLabel).join(", ")}
              </div>
            )}
            {details && (
              <div className="text-text-muted text-xs mt-1">{details}</div>
            )}
          </div>
        );
      })}
    </div>
  );
}
function createMarkerEl(item, sel, om, onSelect) {
  const {
    stackSize: ss = 1,
    markerSize: ms = BASE_MARKER_SIZE,
    zIndex: z = 2000,
  } = om || {};
  const sh = document.createElement("button");
  sh.type = "button";
  sh.className = `curation-marker-shell ${ss > 1 ? "curation-marker-shell--stacked" : ""} ${sel ? "curation-marker-shell--selected" : ""}`;
  sh.title = item.display_name || item.ref;
  sh.style.setProperty("--marker-size", `${ms}px`);
  sh.style.zIndex = String(z);
  sh.addEventListener("click", (e) => {
    e.stopPropagation();
    if (!String(item.ref || "").startsWith("node:")) onSelect(item.ref);
  });
  const dot = document.createElement("span");
  dot.className = `curation-marker ${sel ? "curation-marker--selected" : ""}`;
  if (item.map_kind === "group-node")
    dot.classList.add("curation-marker--node");
  sh.appendChild(dot);
  if (sel) {
    const r = document.createElement("span");
    r.className = "curation-marker__selection-ring";
    r.style.setProperty(
      "--selection-ring-size",
      `${ms + MARKER_SELECTION_RING_SIZE * 2}px`,
    );
    sh.appendChild(r);
  }
  return sh;
}

function createReferenceMarkerEl(point, onSelect) {
  const meta = resolveExternalSourceMeta(point.source_id);
  const shell = document.createElement("button");
  shell.type = "button";
  const relevantCandidateCount = Array.isArray(point.relevant_candidate_ids)
    ? point.relevant_candidate_ids.length
    : 0;
  shell.className = [
    "curation-reference-marker-shell",
    point.is_focus_overlay ? "curation-reference-marker-shell--focus" : "",
    point.is_relevant_to_active_cluster
      ? "curation-reference-marker-shell--relevant"
      : "curation-reference-marker-shell--background",
  ]
    .filter(Boolean)
    .join(" ");
  shell.title = `${meta.label}: ${point.display_name || point.external_id}`;
  shell.style.setProperty("--reference-marker-color", meta.color);
  shell.style.zIndex = point.is_focus_overlay
    ? "1400"
    : point.is_relevant_to_active_cluster
      ? "1325"
      : "1050";
  shell.addEventListener("click", (event) => {
    event.stopPropagation();
    onSelect();
  });

  const dot = document.createElement("span");
  dot.className = [
    "curation-reference-marker",
    point.is_focus_overlay ? "curation-reference-marker--focus" : "",
    point.is_relevant_to_active_cluster
      ? "curation-reference-marker--relevant"
      : "curation-reference-marker--background",
    relevantCandidateCount > 0 ? "curation-reference-marker--matched" : "",
  ]
    .filter(Boolean)
    .join(" ");
  shell.appendChild(dot);
  return shell;
}

/* ── Map ── */
function CurationMap({
  items,
  focusReferencePoints,
  viewportReferencePoints,
  selectedRefs,
  onSelectRef,
  onSelectReferenceCandidates,
  onViewportChange,
  mapMode,
  onToggleMapMode,
  activeReferenceSources,
  onToggleReferenceSource,
}) {
  const mapRef = useRef(null),
    cRef = useRef(null),
    mkRef = useRef([]),
    overlayMarkerRef = useRef([]),
    popupRef = useRef(null),
    onViewportChangeRef = useRef(onViewportChange);
  const [currentZoom, setCurrentZoom] = useState(5);
  onViewportChangeRef.current = onViewportChange;
  useEffect(() => {
    if (mapRef.current || !cRef.current) return;
    const m = new maplibregl.Map({
      container: cRef.current,
      style: resolveDefaultMapStyle(),
      center: [10.45, 51.17],
      zoom: 5,
    });
    m.addControl(new maplibregl.NavigationControl(), "top-right");
    mapRef.current = m;
    setCurrentZoom(m.getZoom());
    return () => {
      m.remove();
      mapRef.current = null;
    };
  }, []);
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    map.setStyle(resolveMapStyle(mapMode));
  }, [mapMode]);
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;

    const emitViewport = () => {
      const bounds = map.getBounds();
      if (!bounds || typeof onViewportChangeRef.current !== "function") {
        return;
      }
      onViewportChangeRef.current({
        minLat: bounds.getSouth(),
        minLon: bounds.getWest(),
        maxLat: bounds.getNorth(),
        maxLon: bounds.getEast(),
      });
    };
    const syncZoom = () => {
      setCurrentZoom(map.getZoom());
    };

    map.on("load", emitViewport);
    map.on("moveend", emitViewport);
    map.on("load", syncZoom);
    map.on("zoomend", syncZoom);
    if (map.isStyleLoaded()) {
      emitViewport();
      syncZoom();
    }

    return () => {
      map.off("load", emitViewport);
      map.off("moveend", emitViewport);
      map.off("load", syncZoom);
      map.off("zoomend", syncZoom);
    };
  }, []);
  useEffect(() => {
    const m = mapRef.current;
    if (!m) return;
    let fid = 0;
    clearMarkers(mkRef.current);
    mkRef.current = [];
    const valid = (items || []).filter(
      (i) => Number.isFinite(i.lat) && Number.isFinite(i.lon),
    );
    const visibleOverlay = (focusReferencePoints || []).filter(
      (point) =>
        activeReferenceSources.has(point.source_id) &&
        Number.isFinite(point.lat) &&
        Number.isFinite(point.lon),
    );
    if (valid.length === 0 && visibleOverlay.length === 0) return;
    const bounds = new maplibregl.LngLatBounds();
    for (const i of [...valid, ...visibleOverlay])
      bounds.extend([i.lon, i.lat]);
    m.fitBounds(bounds, {
      padding: { top: 88, right: 104, bottom: 88, left: 104 },
      maxZoom: 14,
      duration: 0,
    });
    if (valid.length === 0) return;
    fid = requestAnimationFrame(() => {
      const ol = buildMarkerOverlapLayout(m, valid);
      for (const i of valid) {
        const om = ol.get(i.ref);
        const el = createMarkerEl(i, selectedRefs.has(i.ref), om, onSelectRef);
        const mk = new maplibregl.Marker({ element: el, anchor: "center" })
          .setLngLat([om?.aLon ?? i.lon, om?.aLat ?? i.lat])
          .addTo(m);
        mkRef.current.push(mk);
      }
    });
    return () => {
      if (fid) cancelAnimationFrame(fid);
    };
  }, [
    activeReferenceSources,
    focusReferencePoints,
    items,
    onSelectRef,
    selectedRefs,
  ]);
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    clearMarkers(overlayMarkerRef.current);
    overlayMarkerRef.current = [];
    popupRef.current?.remove();
    popupRef.current = null;

    const visibleOverlay = (
      Array.isArray(viewportReferencePoints) ? viewportReferencePoints : []
    ).filter(
      (point) =>
        shouldShowExternalReferencePointsAtZoom(currentZoom) &&
        activeReferenceSources.has(point.source_id) &&
        Number.isFinite(point.lat) &&
        Number.isFinite(point.lon),
    );
    for (const point of visibleOverlay) {
      const marker = new maplibregl.Marker({
        element: createReferenceMarkerEl(point, () => {
          onSelectReferenceCandidates(point.relevant_candidate_ids || []);
          popupRef.current?.remove();
          popupRef.current = new maplibregl.Popup({
            closeButton: false,
            offset: 14,
          })
            .setLngLat([point.lon, point.lat])
            .setHTML(
              `
                <div class="space-y-1.5 text-sm">
                  <div class="font-bold">${escapeHtml(point.display_name || point.external_id)}</div>
                  <div class="text-xs text-slate-400">${escapeHtml(resolveExternalSourceMeta(point.source_id).label)}</div>
                  <div class="text-xs text-slate-300">${escapeHtml(point.category || "station")}</div>
                  <div class="text-xs text-slate-300">${point.is_relevant_to_active_cluster ? "Relevant to active cluster" : "Viewport reference point"}</div>
                  ${
                    point.source_url
                      ? `<a class="text-xs text-sky-300 no-underline hover:underline" href="${escapeHtml(point.source_url)}" target="_blank" rel="noreferrer">Open source</a>`
                      : ""
                  }
                </div>
              `,
            )
            .addTo(map);
        }),
        anchor: "center",
      })
        .setLngLat([point.lon, point.lat])
        .addTo(map);
      overlayMarkerRef.current.push(marker);
    }

    return () => {
      clearMarkers(overlayMarkerRef.current);
      overlayMarkerRef.current = [];
      popupRef.current?.remove();
      popupRef.current = null;
    };
  }, [
    activeReferenceSources,
    currentZoom,
    onSelectReferenceCandidates,
    viewportReferencePoints,
  ]);
  const showReferenceZoomHint =
    activeReferenceSources.size > 0 &&
    !shouldShowExternalReferencePointsAtZoom(currentZoom);
  return (
    <div className="relative w-full h-full">
      <div ref={cRef} className="curation-map w-full h-full" />
      <div className="absolute top-3 left-3 flex gap-1 z-10">
        <button
          id="mapModeDefaultBtn"
          type="button"
          onClick={() => onToggleMapMode("default")}
          className={resolveMapModeButtonClassName("default", mapMode)}
        >
          Map
        </button>
        <button
          id="mapModeSatelliteBtn"
          type="button"
          onClick={() => onToggleMapMode("satellite")}
          className={resolveMapModeButtonClassName("satellite", mapMode)}
        >
          Sat
        </button>
      </div>
      <div className="absolute top-3 left-24 flex gap-1 z-10 flex-wrap pr-3">
        {["overture", "wikidata", "geonames"].map((sourceId) => {
          const meta = resolveExternalSourceMeta(sourceId);
          const active = activeReferenceSources.has(sourceId);
          return (
            <button
              key={sourceId}
              type="button"
              onClick={() => onToggleReferenceSource(sourceId)}
              className={`px-2.5 py-1 rounded-md text-xs font-bold font-display border backdrop-blur-md cursor-pointer transition-all ${active ? meta.pillClassName : "bg-surface-0/50 border-white/10 text-white/60 hover:bg-surface-0/70"}`}
            >
              {meta.label}
            </button>
          );
        })}
      </div>
      {showReferenceZoomHint ? (
        <div className="absolute top-12 left-24 z-10 rounded-md border border-white/10 bg-surface-0/75 px-2.5 py-1 text-[11px] font-display text-white/75 backdrop-blur-md">
          Zoom in to level {EXTERNAL_REFERENCE_MIN_ZOOM} to show references
        </div>
      ) : null}
    </div>
  );
}

/* ── Tiny bits ── */
const sevC = {
  critical: "bg-red-dim text-red border-red/20",
  high: "bg-orange-dim text-orange border-orange/20",
  medium: "bg-yellow-dim text-yellow border-yellow/20",
  low: "bg-green-dim text-green border-green/20",
};
const staC = {
  open: "bg-yellow-dim text-yellow border-yellow/20",
  in_review: "bg-blue-dim text-blue border-blue/20",
  resolved: "bg-green-dim text-green border-green/20",
  dismissed: "bg-surface-3 text-text-muted border-border",
  supporting: "bg-green-dim text-green border-green/20",
  warning: "bg-orange-dim text-orange border-orange/20",
  missing: "bg-red-dim text-red border-red/20",
  informational: "bg-surface-3 text-text-secondary border-border",
};
const dfC = "bg-surface-3 text-text-secondary border-border";
function Pill({ children, v = "neutral", className = "" }) {
  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded text-[0.7rem] font-bold font-display uppercase tracking-wider border whitespace-nowrap ${sevC[v] || staC[v] || dfC} ${className}`}
    >
      {children}
    </span>
  );
}
function StatusPill(props) {
  return <Pill {...props} />;
}
function Tag({ children, className = "", ...props }) {
  return (
    <span
      {...props}
      className={`inline-flex items-center px-2 py-0.5 rounded text-[0.72rem] font-medium border border-border bg-surface-2 text-text-secondary ${className}`}
    >
      {children}
    </span>
  );
}

function TooltipTag({ children, tooltip, className = "" }) {
  return (
    <span className="relative inline-flex group">
      <Tag
        className={`cursor-default ${className}`}
        aria-label={tooltip}
        tabIndex={tooltip ? 0 : undefined}
      >
        {children}
      </Tag>
      {tooltip ? (
        <span className="pointer-events-none absolute left-1/2 top-full z-20 mt-2 hidden w-max max-w-80 -translate-x-1/2 rounded-md border border-border-strong bg-surface-0 px-2.5 py-1.5 text-[0.7rem] leading-snug text-text-primary shadow-lg group-hover:block group-focus-within:block">
          {tooltip}
        </span>
      ) : null}
    </span>
  );
}
function Badge(props) {
  return <Tag {...props} />;
}
const saveC = {
  Saved: "text-green",
  Saving: "text-yellow",
  Failed: "text-red",
};
/* ["DE","AT","CH","FR","IT","NL","BE","CZ","PL"] */
const quickCountryFilterCodes = [
  "DE",
  "AT",
  "CH",
  "FR",
  "IT",
  "NL",
  "BE",
  "CZ",
  "PL",
];
const inp =
  "w-full bg-surface-2 border border-border-strong rounded-lg px-3 py-2 text-text-primary text-sm focus:outline-none focus:border-amber/40 transition-colors";

function normalizeContextItems(items, kind = "string") {
  const seen = new Set();
  const rows = [];
  for (const rawItem of Array.isArray(items) ? items : []) {
    if (kind === "route") {
      const label = String(rawItem?.label || "").trim();
      if (!label) continue;
      const transportMode = String(rawItem?.transport_mode || "").trim();
      const patternHits = Number(rawItem?.pattern_hits || 0);
      const key = `${label}|${transportMode}`;
      if (seen.has(key)) continue;
      seen.add(key);
      rows.push({
        key,
        label: transportMode
          ? `${label} (${formatLabel(transportMode)})`
          : label,
        badge: Number.isFinite(patternHits) && patternHits > 0 ? patternHits : null,
      });
      continue;
    }
    if (kind === "neighbor") {
      const label = String(rawItem?.station_name || "").trim();
      if (!label) continue;
      const patternHits = Number(rawItem?.pattern_hits || 0);
      if (seen.has(label)) continue;
      seen.add(label);
      rows.push({
        key: label,
        label,
        badge: Number.isFinite(patternHits) ? patternHits : 0,
      });
      continue;
    }

    const label = String(rawItem || "").trim();
    if (!label || seen.has(label)) continue;
    seen.add(label);
    rows.push({ key: label, label, badge: null });
  }
  return rows;
}

function CandidateContextSection({
  title,
  items,
  totalCount,
  emptyLabel = "None available.",
  itemKind = "string",
  showExtraItems = true,
  helperText = "",
}) {
  const rows = normalizeContextItems(items, itemKind);
  const safeTotalCount = Number.isFinite(totalCount) ? totalCount : rows.length;
  const extraCount = showExtraItems
    ? Math.max(safeTotalCount - rows.length, 0)
    : 0;

  return (
    <section className="rounded-xl border border-border bg-surface-2 px-3 py-2.5 space-y-2">
      <div className="flex items-center justify-between gap-2">
        <div className="text-xs font-bold text-text-muted font-display uppercase tracking-wider">
          {title}
        </div>
        <Tag>{safeTotalCount}</Tag>
      </div>
      {helperText ? (
        <p className="m-0 text-xs text-text-muted">{helperText}</p>
      ) : null}
      {rows.length > 0 ? (
        <div className="flex flex-wrap gap-1.5">
          {rows.map((row) => (
            <Tag key={`${title}-${row.key}`} title={row.label}>
              {row.label}
              {row.badge !== null ? ` · ${row.badge}` : ""}
            </Tag>
          ))}
          {extraCount > 0 && <Tag>+{extraCount} more</Tag>}
        </div>
      ) : (
        <p className="m-0 text-sm text-text-muted">{emptyLabel}</p>
      )}
    </section>
  );
}

/* ── Sidebar ── */
function ClusterSidebar({
  clusters,
  totalCount,
  activeClusterId,
  filters,
  onFilterChange,
  onSelectCluster,
  onRefresh,
  loading,
}) {
  const ct = resolveClusterCount(totalCount, clusters);
  return (
    <aside className="bg-surface-1 border-r border-border flex flex-col overflow-hidden">
      <div className="flex items-center justify-between px-4 pt-4 pb-2">
        <h2 className="text-sm font-bold tracking-tight m-0 text-text-primary font-display">
          Clusters
        </h2>
        <a
          href="/"
          className="text-text-muted text-xs hover:text-amber transition-colors no-underline font-display"
        >
          ← Home
        </a>
      </div>
      <div className="px-4 pb-3 grid grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto] gap-2 items-center">
        <select
          id="countryFilter"
          value={filters.country}
          onChange={(e) =>
            onFilterChange({ ...filters, country: e.target.value })
          }
          className="min-w-0 bg-surface-2 border border-border rounded-lg px-2 py-1.5 text-text-primary text-xs focus:outline-none focus:border-amber/40"
        >
          <option value="">All</option>
          {quickCountryFilterCodes.map((c) => (
            <option key={c} value={c}>
              {c}
            </option>
          ))}
        </select>
        <select
          id="statusFilter"
          value={filters.status}
          onChange={(e) =>
            onFilterChange({ ...filters, status: e.target.value })
          }
          className="min-w-0 bg-surface-2 border border-border rounded-lg px-2 py-1.5 text-text-primary text-xs focus:outline-none focus:border-amber/40"
        >
          <option value="">All Status</option>
          <option value="open">Open</option>
          <option value="in_review">Review</option>
          <option value="resolved">Resolved</option>
          <option value="dismissed">Dismissed</option>
        </select>
        <button
          type="button"
          onClick={onRefresh}
          className="bg-amber text-surface-0 rounded-lg px-2 py-1.5 text-xs font-bold cursor-pointer border-none hover:bg-amber-hover transition-colors shrink-0"
          title="Refresh"
          aria-label="Refresh clusters"
        >
          ↻
        </button>
      </div>
      <div className="px-4 pb-2 text-xs text-text-muted font-display">
        {loading ? "Loading…" : formatResultsLabel(ct)}
      </div>
      <div
        className="flex-1 overflow-y-auto px-3 pb-4"
        style={{ scrollbarWidth: "thin" }}
      >
        {clusters.length === 0 && !loading && (
          <p className="text-text-muted text-sm p-2">No clusters.</p>
        )}
        {clusters.map((c) => (
          <button
            key={c.cluster_id}
            type="button"
            onClick={() => onSelectCluster(c.cluster_id)}
            className={`w-full text-left rounded-xl p-3 mb-2 cursor-pointer transition-all border ${activeClusterId === c.cluster_id ? "border-amber/40 bg-surface-3 shadow-[inset_3px_0_0_var(--color-amber)]" : "border-transparent hover:bg-surface-2 hover:border-border"}`}
          >
            <div className="flex items-center justify-between gap-2 mb-1">
              <span className="font-bold text-text-primary truncate leading-snug">
                {c.display_name || c.cluster_id}
              </span>
              <Pill v={String(c.severity || "").toLowerCase()}>
                {formatLabel(c.severity || "?")}
              </Pill>
            </div>
            <div className="flex gap-1.5 flex-wrap items-center">
              <Pill
                v={String(c.effective_status || c.status || "").toLowerCase()}
              >
                {formatLabel(c.effective_status || c.status || "?")}
              </Pill>
              <span className="text-text-muted text-xs font-display">
                {c.candidate_count} cand.
              </span>
              {c.has_workspace && (
                <span className="text-text-muted text-xs font-display">
                  v{c.workspace_version || 0}
                </span>
              )}
            </div>
          </button>
        ))}
      </div>
    </aside>
  );
}

/* ── Candidate Card: expandable for merges/groups ── */
function CandidateCard({
  item,
  index,
  selected,
  focused,
  expanded,
  workspace,
  candidateMap,
  onToggle,
  onFocus,
  onSplit,
  onRenameRef,
  onRenameComposite,
  onUpdateGroupTransfer,
  onUpdateGroupNodeLabel,
  onRemoveGroupMember,
  onRemoveMergeMember,
  onToggleExpand,
}) {
  const c = item.candidate || {};
  const memberNames = getCompositeMembers(item, workspace, candidateMap) || [];
  const ctx = c.network_summary || {};
  const kB = resolveKindAccent(item.kind);
  const ref = item.ref;
  const providerLabels = item.provider_labels || [];
  const providerFeedsTooltip = formatProviderFeedsTooltip(providerLabels);
  const provenance = c.provenance || {};
  const provenanceTooltip = formatCandidateProvenanceTooltip(provenance);
  const activeSourceLabels = provenance.active_source_labels || [];
  const historicalSourceLabels = provenance.historical_source_labels || [];
  const coordInputStopPlaceRefs = provenance.coord_input_stop_place_refs || [];
  const externalSummary = c.external_reference_summary || {};
  const externalMatches = c.external_reference_matches || [];
  const externalSourceCounts = Object.entries(
    externalSummary.source_counts || {},
  );
  const googleMapsLink = buildGoogleMapsLink(
    item.display_name,
    item.lat,
    item.lon,
  );
  const showOrphanedBadge = activeSourceLabels.length === 0;
  const showProvenanceBadges =
    historicalSourceLabels.length > 0 ||
    coordInputStopPlaceRefs.length > 0 ||
    showOrphanedBadge;
  const networkContext = c.network_context || {};
  const routeLines = networkContext.routes || [];
  const incomingStops = networkContext.incoming || [];
  const outgoingStops = networkContext.outgoing || [];
  const stopPoints = networkContext.stop_points || [];
  const fg =
    item.kind === "group" ? findWorkspaceEntity("group", workspace, ref) : null;
  const fm =
    item.kind === "merge" ? findWorkspaceEntity("merge", workspace, ref) : null;
  const isComposite = item.kind === "merge" || item.kind === "group";
  const inputIdBase = ref.replaceAll(/[^a-zA-Z0-9_-]/g, "-");
  const rawRenameInputId = `rename-${inputIdBase}`;
  const mergeNameInputId = `merge-name-${inputIdBase}`;
  const groupNameInputId = `group-name-${inputIdBase}`;

  return (
    <div
      data-station-id={item.ref}
      className={`border border-border rounded-xl mb-2 bg-surface-2 transition-all outline-none ${kB} ${selected ? "border-amber/30 bg-surface-3" : "hover:bg-surface-2/80"} ${focused ? "ring-1 ring-amber/25 bg-surface-3" : ""}`}
    >
      {/* Card header — always visible */}
      <div className="flex items-start gap-2 p-3">
        <input
          type="checkbox"
          checked={selected}
          className="mt-1 accent-amber shrink-0 w-4 h-4"
          onChange={(e) => onToggle(item.ref, index, e.shiftKey)}
          onClick={(e) => e.stopPropagation()}
        />
        <button
          type="button"
          className="flex-1 min-w-0 bg-transparent border-none p-0 text-left cursor-pointer"
          onClick={(e) => {
            onToggle(item.ref, index, e.shiftKey);
            onFocus(item.ref);
          }}
          onDoubleClick={(e) => {
            e.preventDefault();
            onToggleExpand(item.ref);
          }}
        >
          <strong className="block text-text-primary truncate text-sm">
            {item.display_name}
          </strong>
          {item.kind === "raw" && (
            <>
              <div className="flex flex-wrap gap-1.5 mt-2">
                <Tag>{ctx.stop_point_count ?? 0} stops</Tag>
                <Tag>
                  {ctx.route_count ?? ctx.route_pattern_count ?? routeLines.length} lines
                </Tag>
                <TooltipTag tooltip={providerFeedsTooltip}>
                  {providerLabels.length} feeds
                </TooltipTag>
              </div>
              {showProvenanceBadges && (
                <div className="flex flex-wrap gap-1.5 mt-2">
                  {historicalSourceLabels.length > 0 && (
                    <TooltipTag tooltip={provenanceTooltip}>
                      {historicalSourceLabels.length} historical
                    </TooltipTag>
                  )}
                  {coordInputStopPlaceRefs.length > 0 && (
                    <TooltipTag tooltip={provenanceTooltip}>
                      {coordInputStopPlaceRefs.length} refs
                    </TooltipTag>
                  )}
                  {showOrphanedBadge && (
                    <TooltipTag
                      tooltip={provenanceTooltip}
                      className="border-red/30 text-red"
                    >
                      orphaned
                    </TooltipTag>
                  )}
                </div>
              )}
              {(externalSourceCounts.length > 0 ||
                externalSummary.strong_match_count > 0 ||
                externalSummary.probable_match_count > 0) && (
                <div className="flex flex-wrap gap-1.5 mt-2">
                  {externalSourceCounts.map(([sourceId, count]) => {
                    const meta = resolveExternalSourceMeta(sourceId);
                    return (
                      <Tag
                        key={`${item.ref}-${sourceId}`}
                        className={meta.pillClassName}
                      >
                        {meta.label} {count}
                      </Tag>
                    );
                  })}
                  {externalSummary.strong_match_count > 0 && (
                    <Tag className="border-green/30 bg-green-dim text-green">
                      Strong {externalSummary.strong_match_count}
                    </Tag>
                  )}
                  {externalSummary.probable_match_count > 0 && (
                    <Tag className="border-blue/30 bg-blue-dim text-blue">
                      Probable {externalSummary.probable_match_count}
                    </Tag>
                  )}
                </div>
              )}
            </>
          )}
          {isComposite && !expanded && (
            <div className="flex items-center gap-2 mt-1.5 text-text-muted text-sm">
              <span>{item.member_refs?.length || 0} members</span>
              {item.kind === "group" && (
                <span>· {item.internal_nodes?.length || 0} nodes</span>
              )}
            </div>
          )}
          {!isComposite && memberNames.length > 0 && (
            <div className="flex flex-wrap gap-1.5 mt-2">
              {memberNames.slice(0, 4).map((n) => (
                <Tag key={`${item.ref}-${n}`}>{n}</Tag>
              ))}
              {memberNames.length > 4 && <Tag>+{memberNames.length - 4}</Tag>}
            </div>
          )}
        </button>
        <div className="flex gap-1.5 shrink-0 items-center">
          <Badge>{item.kind}</Badge>
          <button
            type="button"
            className={`px-2 py-0.5 rounded-md text-xs font-semibold border cursor-pointer transition-colors ${expanded ? "bg-amber-dim border-amber/30 text-amber" : "bg-surface-3 border-border text-text-muted hover:text-text-primary"}`}
            onClick={() => onToggleExpand(item.ref)}
            title={expanded ? "Collapse" : "Expand"}
          >
            {expanded ? "▾" : "▸"}
          </button>
        </div>
      </div>

      {/* ── Inline rename for raw (when expanded) ── */}
      {expanded && item.kind === "raw" && (
        <div className="border-t border-border px-3 py-2.5 bg-surface-1/50 space-y-2.5">
          <div>
            <div className="flex justify-between items-center mb-1">
              <label
                htmlFor={rawRenameInputId}
                className="text-xs text-text-muted font-display block"
              >
                Rename
              </label>
              <span
                className="text-[0.65rem] font-mono text-text-muted bg-surface-2 px-1.5 py-0.5 rounded border border-border"
                title="GTFS ID"
              >
                {item.candidate?.global_station_id}
              </span>
            </div>
            <input
              id={rawRenameInputId}
              type="text"
              className={inp}
              value={getRenameValue(workspace, ref) || item.display_name}
              onChange={(e) => onRenameRef(ref, e.target.value)}
            />
          </div>
          <section className="rounded-xl border border-border bg-surface-2 px-3 py-2.5 space-y-2">
            <div className="flex items-center justify-between gap-2">
              <div className="text-xs font-bold text-text-muted font-display uppercase tracking-wider">
                External References
              </div>
              {googleMapsLink ? (
                <a
                  href={googleMapsLink}
                  target="_blank"
                  rel="noreferrer"
                  className="text-xs text-amber no-underline hover:underline"
                >
                  Open in Google Maps
                </a>
              ) : null}
            </div>
            {externalMatches.length > 0 ? (
              <div className="space-y-1.5">
                {externalMatches.slice(0, 3).map((match) => {
                  const meta = resolveExternalSourceMeta(match.source_id);
                  return (
                    <div
                      key={`${item.ref}-${match.source_id}-${match.external_id}`}
                      className="rounded-lg border border-border bg-surface-1 px-2.5 py-2"
                    >
                      <div className="flex items-center justify-between gap-2">
                        <div className="min-w-0">
                          <div className="text-sm text-text-primary truncate">
                            {match.display_name}
                          </div>
                          <div className="flex flex-wrap gap-1.5 mt-1">
                            <Tag className={meta.pillClassName}>
                              {meta.label}
                            </Tag>
                            <Tag>
                              {formatLabel(match.match_status || "match")}
                            </Tag>
                            <Tag>{formatMeters(match.distance_meters)}</Tag>
                          </div>
                        </div>
                        {match.source_url ? (
                          <a
                            href={match.source_url}
                            target="_blank"
                            rel="noreferrer"
                            className="text-xs text-amber no-underline hover:underline shrink-0"
                          >
                            Source
                          </a>
                        ) : null}
                      </div>
                    </div>
                  );
                })}
              </div>
            ) : (
              <p className="m-0 text-sm text-text-muted">
                No external matches loaded for this candidate.
              </p>
            )}
          </section>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
            <CandidateContextSection
              title="Lines"
              items={routeLines}
              totalCount={
                ctx.route_count ?? ctx.route_pattern_count ?? routeLines.length
              }
              emptyLabel="No line context."
              itemKind="route"
            />
            <CandidateContextSection
              title="Stop Points"
              items={stopPoints}
              totalCount={ctx.stop_point_count}
              emptyLabel="No stop points."
            />
            <CandidateContextSection
              title="Incoming"
              items={incomingStops}
              totalCount={ctx.incoming_neighbor_count}
              emptyLabel="No incoming neighbor context."
              itemKind="neighbor"
              helperText="Badges show unique pattern hits from each upstream neighbor."
            />
            <CandidateContextSection
              title="Outgoing"
              items={outgoingStops}
              totalCount={ctx.outgoing_neighbor_count}
              emptyLabel="No outgoing neighbor context."
              itemKind="neighbor"
              helperText="Badges show unique pattern hits to each downstream neighbor."
            />
          </div>
        </div>
      )}

      {/* ── Expanded merge details ── */}
      {expanded && fm && (
        <div className="border-t border-border px-3 py-3 bg-surface-1/50 space-y-2.5">
          <div>
            <div className="flex justify-between items-center mb-1">
              <label
                htmlFor={mergeNameInputId}
                className="text-xs text-text-muted font-display block"
              >
                Merged Name
              </label>
              <span className="text-[0.65rem] font-mono text-text-muted">
                ID: {fm.entity_id.slice(0, 8)}
              </span>
            </div>
            <input
              id={mergeNameInputId}
              type="text"
              className={inp}
              value={fm.display_name}
              onChange={(e) => onRenameComposite(ref, e.target.value)}
            />
          </div>
          <div>
            <div className="text-xs font-bold text-text-muted font-display uppercase tracking-wider mb-1.5">
              Members
            </div>
            <div className="flex flex-col gap-1.5">
              {fm.member_refs.map((r) => (
                <div
                  key={r}
                  className="flex justify-between items-center bg-surface-2 border border-border rounded px-2 py-1.5"
                >
                  <div className="min-w-0 flex-1 mr-2">
                    <div className="text-sm font-medium text-text-primary truncate">
                      {resolveDisplayNameForRef(r, workspace, candidateMap)}
                    </div>
                    <div className="text-[0.65rem] font-mono text-text-muted truncate">
                      {r.replaceAll("node:", "")}
                    </div>
                  </div>
                  <button
                    type="button"
                    className="text-red text-xs font-bold cursor-pointer bg-transparent border-none hover:underline shrink-0 p-1"
                    title="Remove Member"
                    onClick={() => onRemoveMergeMember(fm.entity_id, r)}
                  >
                    ✕
                  </button>
                </div>
              ))}
            </div>
          </div>
          <button
            type="button"
            className="px-3 py-1.5 rounded-lg text-xs font-bold bg-surface-3 border border-border text-text-secondary cursor-pointer hover:text-text-primary transition-colors"
            onClick={() => onSplit(item.ref)}
          >
            Split Merge
          </button>
        </div>
      )}

      {/* ── Expanded group details ── */}
      {expanded && fg && (
        <div
          id="groupEditorPanel"
          className="border-t border-border px-3 py-3 bg-surface-1/50 space-y-3"
        >
          <div>
            <div className="flex justify-between items-center mb-1">
              <label
                htmlFor={groupNameInputId}
                className="text-xs text-text-muted font-display block"
              >
                Group Name
              </label>
              <span className="text-[0.65rem] font-mono text-text-muted">
                ID: {fg.entity_id.slice(0, 8)}
              </span>
            </div>
            <input
              id={groupNameInputId}
              type="text"
              className={inp}
              value={fg.display_name}
              onChange={(e) => onRenameComposite(ref, e.target.value)}
            />
          </div>

          <div>
            <div className="text-xs font-bold text-text-muted font-display uppercase tracking-wider mb-1.5">
              Nodes
            </div>
            <div className="space-y-1.5">
              {fg.internal_nodes.map((n) => (
                <div
                  key={n.node_id}
                  className="flex items-center gap-2 border border-border rounded-lg p-2 bg-surface-2 flex-wrap"
                >
                  <input
                    type="text"
                    className={`${inp} flex-1 min-w-[120px] max-w-full`}
                    value={n.label}
                    onChange={(e) =>
                      onUpdateGroupNodeLabel(
                        fg.entity_id,
                        n.node_id,
                        e.target.value,
                      )
                    }
                  />
                  <div className="flex flex-col min-w-0 flex-[1.5]">
                    <span
                      className="text-text-muted text-xs truncate"
                      title={resolveDisplayNameForRef(
                        n.source_ref,
                        workspace,
                        candidateMap,
                      )}
                    >
                      {resolveDisplayNameForRef(
                        n.source_ref,
                        workspace,
                        candidateMap,
                      )}
                    </span>
                    <span
                      className="text-[0.65rem] font-mono text-text-muted/70 truncate"
                      title={n.source_ref}
                    >
                      {n.source_ref.replaceAll("node:", "")}
                    </span>
                  </div>
                  <button
                    type="button"
                    className="text-red text-xs font-bold cursor-pointer bg-transparent border-none hover:underline shrink-0 p-1"
                    title="Remove Node"
                    onClick={() =>
                      onRemoveGroupMember(fg.entity_id, n.source_ref)
                    }
                  >
                    ✕
                  </button>
                </div>
              ))}
            </div>
          </div>

          {fg.transfer_matrix.length > 0 && (
            <div id="groupTransferMatrix">
              <div className="text-xs font-bold text-text-muted font-display uppercase tracking-wider mb-1.5">
                Transfers
              </div>
              <div className="space-y-1.5">
                {fg.transfer_matrix.map((r) => (
                  <div
                    key={`${r.from_node_id}-${r.to_node_id}`}
                    className="flex items-center gap-2 text-sm text-text-secondary"
                  >
                    <span className="truncate flex-1">
                      {fg.internal_nodes.find(
                        (n) => n.node_id === r.from_node_id,
                      )?.label || "?"}{" "}
                      ↔{" "}
                      {fg.internal_nodes.find((n) => n.node_id === r.to_node_id)
                        ?.label || "?"}
                    </span>
                    <input
                      type="number"
                      min="0"
                      step="10"
                      className="w-20 bg-surface-2 border border-border rounded-lg px-2 py-1.5 text-sm text-text-primary focus:outline-none focus:border-amber/40"
                      value={r.min_walk_seconds}
                      onChange={(e) =>
                        onUpdateGroupTransfer(
                          fg.entity_id,
                          r.from_node_id,
                          r.to_node_id,
                          e.target.value,
                        )
                      }
                    />
                    <span className="text-text-muted text-xs font-display">
                      sec
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}
          <button
            type="button"
            className="px-3 py-1.5 rounded-lg text-xs font-bold bg-surface-3 border border-border text-text-secondary cursor-pointer hover:text-text-primary transition-colors"
            onClick={() => onSplit(item.ref)}
          >
            Split Group
          </button>
        </div>
      )}
    </div>
  );
}

/* ── Evidence Panel ── */
function EvidencePanel({ clusterDetail, focusedItem, workspace }) {
  /* category==="core_match" is_seed_rule===true */
  const ids = focusedItem
    ? resolveRefMemberStationIds(focusedItem.ref, workspace)
    : [];
  const ev = (clusterDetail?.evidence || []).filter(
    (r) =>
      ids.length === 0 ||
      ids.includes(r.source_global_station_id) ||
      ids.includes(r.target_global_station_id),
  );
  const pairs = (clusterDetail?.pair_summaries || []).filter(
    (r) =>
      ids.length === 0 ||
      ids.includes(r.source_global_station_id) ||
      ids.includes(r.target_global_station_id),
  );
  const tcs = getEvidenceTypeCounts(clusterDetail?.evidence_summary);
  const ccs = getEvidenceCategoryCounts(clusterDetail?.evidence_summary);
  const scs = getSeedRuleCounts(clusterDetail?.evidence_summary);
  const seedPairs = pairs.filter(
    (r) => Array.isArray(r.seed_reasons) && r.seed_reasons.length > 0,
  );
  const coreRows = ev.filter((r) => r.category === "core_match");
  const contextRows = ev.filter((r) => r.category === "network_context");
  const riskRows = ev.filter((r) => r.category === "risk_conflict");
  return (
    <div id="evidencePanel" className="space-y-2 p-3">
      <EvidenceSection title="Overview">
        <div className="flex flex-wrap gap-1.5">
          {["supporting", "warning", "missing", "informational"].map((s) => (
            <StatusPill key={s} v={s}>
              {formatEvidenceStatusLabel(s)}{" "}
              {getSummaryCount(clusterDetail?.evidence_summary, s)}
            </StatusPill>
          ))}
        </div>
        {ccs.length > 0 && (
          <div className="flex flex-wrap gap-1.5">
            {ccs.map((e) => (
              <Tag key={e.category}>
                {formatEvidenceCategoryLabel(e.category)} {e.count}
              </Tag>
            ))}
          </div>
        )}
        {scs.length > 0 && (
          <div className="flex flex-wrap gap-1.5">
            {scs.map((e) => (
              <Tag
                key={e.reason}
                className="border-blue/20 bg-blue-dim text-blue"
              >
                Seed {formatSeedReasonLabel(e.reason)} {e.count}
              </Tag>
            ))}
          </div>
        )}
        {tcs.length > 0 && (
          <div className="flex flex-wrap gap-1.5">
            {tcs.slice(0, 5).map((e) => (
              <Tag key={e.type}>
                {formatEvidenceTypeLabel(e.type)} {e.count}
              </Tag>
            ))}
          </div>
        )}
      </EvidenceSection>

      {seedPairs.length > 0 && (
        <EvidenceSection title="Seed Rules">
          <div className="space-y-2">
            {seedPairs.map((r) => (
              <div
                key={`${r.source_global_station_id}-${r.target_global_station_id}`}
                className="border border-border rounded-xl p-2.5 bg-surface-2"
              >
                <div className="font-bold text-text-primary font-display text-sm">
                  {r.source_global_station_id} ↔ {r.target_global_station_id}
                </div>
                <div className="text-text-secondary text-sm mt-0.5">
                  {r.summary || "—"}
                </div>
                <div className="flex flex-wrap gap-1.5 mt-1.5">
                  {(r.seed_reasons || []).map((seed) => (
                    <Tag
                      key={seed}
                      className="border-blue/20 bg-blue-dim text-blue"
                    >
                      Seed {formatSeedReasonLabel(seed)}
                    </Tag>
                  ))}
                  {(r.categories || []).map((category) => (
                    <Tag key={category}>
                      {formatEvidenceCategoryLabel(category)}
                    </Tag>
                  ))}
                </div>
                <div className="flex gap-3 mt-1.5 text-xs font-display">
                  <span className="text-green">+{r.supporting_count || 0}</span>
                  <span className="text-orange">⚠{r.warning_count || 0}</span>
                  <span className="text-red">✗{r.missing_count || 0}</span>
                  <span className="text-text-primary font-bold">
                    {resolveSeedScore(r.score)}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </EvidenceSection>
      )}

      {coreRows.length > 0 && (
        <EvidenceSection title="Core Match">
          <EvidenceRows rows={coreRows} />
        </EvidenceSection>
      )}

      {contextRows.length > 0 && (
        <EvidenceSection title="Network Context">
          <EvidenceRows rows={contextRows} />
        </EvidenceSection>
      )}

      {riskRows.length > 0 && (
        <EvidenceSection title="Risk / Conflict" tone="risk">
          <EvidenceRows rows={riskRows} tone="risk" />
        </EvidenceSection>
      )}

      {ev.length === 0 && (
        <p className="text-text-muted text-sm">
          No evidence for current focus.
        </p>
      )}
    </div>
  );
}

/* ── History Panel ── */
function HistoryPanel({ clusterDetail }) {
  return (
    <div id="historyPanel" className="space-y-1.5 p-3">
      {(clusterDetail?.edit_history || []).length === 0 && (
        <p className="text-text-muted text-sm">No history.</p>
      )}
      {(clusterDetail?.edit_history || []).map((r, i) => (
        <div
          key={`${r.event_type}-${r.created_at}-${i}`}
          className="border border-border rounded-xl px-3 py-2 bg-surface-2 text-sm text-text-secondary"
        >
          <strong className="text-text-primary font-display">
            {r.event_type}
          </strong>{" "}
          · {r.requested_by} · {r.created_at}
        </div>
      ))}
    </div>
  );
}

/* ── Main: 2-column — sidebar | (toolbar + map + bottom) ── */
export function CurationPage() {
  const [clusters, setClusters] = useState([]);
  const [clusterTotalCount, setClusterTotalCount] = useState(0);
  const [activeClusterId, setActiveClusterId] = useState(null);
  const [clusterDetail, setClusterDetail] = useState(null);
  const [workspace, setWorkspace] = useState(createEmptyWorkspace());
  const [workspaceVersion, setWorkspaceVersion] = useState(0);
  const [filters, setFilters] = useState({ country: "", status: "" });
  const [uiState, dispatch] = useReducer(uiReducer, undefined, createUiState);
  const [loading, setLoading] = useState(true);
  const [notice, setNotice] = useState(null);
  const [saveState, setSaveState] = useState("Saved");
  const [aiResult, setAiResult] = useState(null);
  const [expandedRefs, setExpandedRefs] = useState(new Set());
  const [viewportReferencePoints, setViewportReferencePoints] = useState([]);
  const [activeReferenceSources, setActiveReferenceSources] = useState(
    () => new Set(["overture", "wikidata", "geonames"]),
  );
  const ntRef = useRef(null),
    lsRef = useRef(serializeWorkspace(createEmptyWorkspace())),
    srRef = useRef(0),
    isRef = useRef(false),
    lastViewportKeyRef = useRef(""),
    viewportRequestIdRef = useRef(0);

  const toggleExpand = useCallback((ref) => {
    setExpandedRefs((prev) => {
      const n = new Set(prev);
      if (n.has(ref)) n.delete(ref);
      else n.add(ref);
      return n;
    });
  }, []);
  const showNotice = useCallback((msg, tone = "info", sticky = false) => {
    setNotice({ message: msg, tone });
    if (ntRef.current) clearTimeout(ntRef.current);
    if (!sticky) ntRef.current = setTimeout(() => setNotice(null), 4500);
  }, []);
  const loadClusters = useCallback(async () => {
    setLoading(true);
    try {
      const d = await apiFetchClusters(filters);
      setClusters(d.items || []);
      setClusterTotalCount(d.totalCount || 0);
    } catch (e) {
      showNotice(`Load: ${e.message}`, "error", true);
    } finally {
      setLoading(false);
    }
  }, [filters, showNotice]);
  const loadDetail = useCallback(
    async (id) => {
      try {
        const d = await apiFetchClusterDetail(id);
        setClusterDetail(d);
        setActiveClusterId(id);
        const w = normalizeWorkspace(d?.workspace);
        setWorkspace(w);
        setWorkspaceVersion(d?.workspace_version || 0);
        lsRef.current = serializeWorkspace(w);
        setSaveState("Saved");
        dispatch({ type: "clear_selection" });
        dispatch({ type: "focus", ref: "" });
        setAiResult(null);
        setExpandedRefs(new Set());
      } catch (e) {
        showNotice(`Detail: ${e.message}`, "error", true);
      }
    },
    [showNotice],
  );
  const loadReferenceViewport = useCallback(
    async (viewport) => {
      const minLat = Number(viewport?.minLat);
      const minLon = Number(viewport?.minLon);
      const maxLat = Number(viewport?.maxLat);
      const maxLon = Number(viewport?.maxLon);
      if (
        !Number.isFinite(minLat) ||
        !Number.isFinite(minLon) ||
        !Number.isFinite(maxLat) ||
        !Number.isFinite(maxLon)
      ) {
        return;
      }

      const viewportKey = [minLat, minLon, maxLat, maxLon]
        .map((value) => value.toFixed(3))
        .join(":");
      if (viewportKey === lastViewportKeyRef.current) {
        return;
      }
      lastViewportKeyRef.current = viewportKey;
      const requestId = viewportRequestIdRef.current + 1;
      viewportRequestIdRef.current = requestId;

      try {
        const rows = await fetchReferenceViewport({
          minLat,
          minLon,
          maxLat,
          maxLon,
          limit: 1500,
        });
        if (viewportRequestIdRef.current !== requestId) {
          return;
        }
        setViewportReferencePoints(rows);
      } catch (error) {
        if (viewportRequestIdRef.current !== requestId) {
          return;
        }
        showNotice(`Viewport refs: ${error.message}`, "error");
      }
    },
    [showNotice],
  );

  useEffect(() => {
    loadClusters();
  }, [loadClusters]);
  useEffect(() => {
    if (clusters.length > 0 && !activeClusterId)
      loadDetail(clusters[0].cluster_id);
  }, [activeClusterId, clusters, loadDetail]);

  const cMap = useMemo(
    () => buildCandidateMap(clusterDetail?.candidates || []),
    [clusterDetail?.candidates],
  );
  const rail = useMemo(
    () => buildRailItems(clusterDetail, workspace),
    [clusterDetail, workspace],
  );
  const railIdx = useMemo(
    () => new Map(rail.map((i, x) => [i.ref, x])),
    [rail],
  );
  const focused = useMemo(
    () => rail.find((i) => i.ref === uiState.focusedRef) || null,
    [rail, uiState.focusedRef],
  );
  const plotted = useMemo(() => buildMappableItems(rail), [rail]);
  const activeClusterCandidateIds = useMemo(
    () =>
      uniqueStrings(
        (clusterDetail?.candidates || []).map((candidate) =>
          String(candidate?.global_station_id || "").trim(),
        ),
      ),
    [clusterDetail?.candidates],
  );
  const displayReferencePoints = useMemo(
    () =>
      buildDisplayReferencePoints({
        focusReferencePoints: clusterDetail?.reference_overlay || [],
        viewportReferencePoints,
        activeCandidateIds: activeClusterCandidateIds,
      }),
    [
      activeClusterCandidateIds,
      clusterDetail?.reference_overlay,
      viewportReferencePoints,
    ],
  );

  useEffect(() => {
    if (!uiState.focusedRef) return;
    if (!rail.some((i) => i.ref === uiState.focusedRef))
      dispatch({ type: "focus", ref: "" });
  }, [rail, uiState.focusedRef]);

  // Autosave
  useEffect(() => {
    if (!activeClusterId) return;
    const s = serializeWorkspace(workspace);
    if (s === lsRef.current) return;
    const rid = srRef.current + 1;
    srRef.current = rid;
    setSaveState("Saving");
    const dl = isRef.current ? 80 : 500;
    isRef.current = false;
    const t = setTimeout(async () => {
      try {
        const r = await saveClusterWorkspace(activeClusterId, workspace);
        if (rid !== srRef.current) return;
        lsRef.current = serializeWorkspace(r.workspace);
        setWorkspaceVersion(r.workspace_version || 0);
        setClusterDetail((p) =>
          applyWorkspaceResponseToClusterDetail(p, r, r.workspace),
        );
        setClusters((p) =>
          applyWorkspaceResponseToClusters(p, activeClusterId, r),
        );
        setSaveState("Saved");
      } catch (e) {
        if (rid !== srRef.current) return;
        setSaveState("Failed");
        showNotice(`Save: ${e.message}`, "error", true);
      }
    }, dl);
    return () => clearTimeout(t);
  }, [activeClusterId, showNotice, workspace]);

  // Keyboard
  useEffect(() => {
    const h = (e) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "z") {
        e.preventDefault();
        if (activeClusterId)
          undoClusterWorkspace(activeClusterId)
            .then((r) => {
              const w = normalizeWorkspace(r.workspace);
              setWorkspace(w);
              lsRef.current = serializeWorkspace(w);
              setWorkspaceVersion(r.workspace_version || 0);
              setSaveState("Saved");
            })
            .catch((err) => showNotice(`Undo: ${err.message}`, "error", true));
      }
      if (e.key === "Escape") dispatch({ type: "clear_selection" });
    };
    globalThis.addEventListener("keydown", h);
    return () => globalThis.removeEventListener("keydown", h);
  }, [activeClusterId, showNotice]);

  const commit = useCallback((w, o = {}) => {
    isRef.current = o.immediate === true;
    setWorkspace(normalizeWorkspace(w));
  }, []);
  const hToggle = useCallback(
    (ref, idx, range) => {
      if (range && uiState.lastSelectedIndex >= 0) {
        const s = Math.min(idx, uiState.lastSelectedIndex),
          e = Math.max(idx, uiState.lastSelectedIndex);
        dispatch({
          type: "set_selection",
          refs: rail.slice(s, e + 1).map((i) => i.ref),
          lastSelectedIndex: idx,
        });
      } else dispatch({ type: "toggle_selection", ref, index: idx });
    },
    [rail, uiState.lastSelectedIndex],
  );
  const hSelectRef = useCallback((ref) => {
    dispatch({ type: "toggle_selection", ref });
    dispatch({ type: "focus", ref });
  }, []);
  const hSelectReferenceCandidates = useCallback(
    (candidateIds = []) => {
      const refs = uniqueStrings(candidateIds.map(toRawRef)).filter((ref) =>
        railIdx.has(ref),
      );
      if (refs.length === 0) {
        return;
      }
      dispatch({
        type: "set_selection",
        refs,
        lastSelectedIndex: resolveSelectedIndex(railIdx, refs),
      });
      dispatch({ type: "focus", ref: refs[0] });
    },
    [railIdx],
  );
  const hToggleReferenceSource = useCallback((sourceId) => {
    setActiveReferenceSources((previous) => {
      const next = new Set(previous);
      if (next.has(sourceId)) next.delete(sourceId);
      else next.add(sourceId);
      return next;
    });
  }, []);
  const hMerge = useCallback(() => {
    commit(
      createMergeFromSelection(
        workspace,
        uiState.selectedRefs,
        clusterDetail?.candidates || [],
      ),
      { immediate: true },
    );
    dispatch({ type: "clear_selection" });
  }, [clusterDetail?.candidates, commit, uiState.selectedRefs, workspace]);
  const hGroup = useCallback(() => {
    const w = createGroupFromSelection(
      workspace,
      uiState.selectedRefs,
      clusterDetail?.candidates || [],
    );
    const g = w.groups.at(-1);
    commit(w, { immediate: true });
    dispatch({ type: "clear_selection" });
    if (g) {
      const gRef = toGroupRef(g.entity_id);
      dispatch({ type: "focus", ref: gRef, tool: "group" });
      setExpandedRefs((prev) => new Set(prev).add(gRef));
    }
  }, [clusterDetail?.candidates, commit, uiState.selectedRefs, workspace]);
  const hKeep = useCallback(() => {
    commit(markKeepSeparate(workspace, uiState.selectedRefs), {
      immediate: true,
    });
    dispatch({ type: "clear_selection" });
  }, [commit, uiState.selectedRefs, workspace]);
  const hSplit = useCallback(
    (ref = focused?.ref) => {
      if (!ref) return;
      commit(splitComposite(workspace, ref), { immediate: true });
      dispatch({ type: "focus", ref: "" });
      setExpandedRefs((prev) => {
        const n = new Set(prev);
        n.delete(ref);
        return n;
      });
    },
    [commit, focused?.ref, workspace],
  );
  const hAddToGrp = useCallback(() => {
    if (focused?.kind !== "group") return;
    commit(
      addSelectionToGroup(
        workspace,
        parseRef(focused.ref).id,
        uiState.selectedRefs,
        clusterDetail?.candidates || [],
      ),
      { immediate: true },
    );
  }, [
    clusterDetail?.candidates,
    commit,
    focused,
    uiState.selectedRefs,
    workspace,
  ]);
  const hUndo = useCallback(async () => {
    if (!activeClusterId) return;
    try {
      const r = await undoClusterWorkspace(activeClusterId);
      const w = normalizeWorkspace(r.workspace);
      setWorkspace(w);
      lsRef.current = serializeWorkspace(w);
      setWorkspaceVersion(r.workspace_version || 0);
      setSaveState("Saved");
      setClusterDetail((p) =>
        applyWorkspaceResponseToClusterDetail(p, r, w, {
          has_workspace: Boolean(r.workspace),
        }),
      );
      showNotice("Reverted.", "success");
    } catch (e) {
      showNotice(`Undo: ${e.message}`, "error", true);
    }
  }, [activeClusterId, showNotice]);
  const hUnresolve = useCallback(async () => {
    if (!activeClusterId) return;
    try {
      const r = await reopenCluster(activeClusterId);
      const w = normalizeWorkspace(r.workspace);
      setWorkspace(w);
      lsRef.current = serializeWorkspace(w);
      setWorkspaceVersion(r.workspace_version || 0);
      setSaveState("Saved");
      setClusterDetail((p) =>
        applyWorkspaceResponseToClusterDetail(p, r, w, {
          has_workspace: r.workspace_version > 0,
          status: r.effective_status,
        }),
      );
      setClusters((p) =>
        applyWorkspaceResponseToClusters(p, activeClusterId, r, {
          status: r.effective_status,
          has_workspace: r.workspace_version > 0,
        }),
      );
      showNotice("Reopened.", "success");
    } catch (e) {
      showNotice(`Unresolve: ${e.message}`, "error", true);
    }
  }, [activeClusterId, showNotice]);
  const hResolve = useCallback(
    async (st) => {
      if (!activeClusterId) return;
      try {
        const r = await resolveCluster(activeClusterId, st, workspace.note);
        showNotice(`${formatLabel(st)} done.`, "success");
        await loadClusters();
        await loadDetail(r.next_cluster_id || activeClusterId);
      } catch (e) {
        showNotice(`Resolve: ${e.message}`, "error", true);
      }
    },
    [activeClusterId, loadDetail, loadClusters, showNotice, workspace.note],
  );
  const hAi = useCallback(async () => {
    if (!activeClusterId) return;
    try {
      const r = await requestAiScore(activeClusterId);
      setAiResult(r);
      if (String(r.suggested_action || "").toLowerCase() === "merge") {
        const rr = sortCandidateIds(
          (clusterDetail?.candidates || [])
            .slice(0, 2)
            .map((c) => c.global_station_id),
          cMap,
        ).map(toRawRef);
        dispatch({
          type: "set_selection",
          refs: rr,
          lastSelectedIndex: resolveSelectedIndex(railIdx, rr),
        });
      }
    } catch (e) {
      showNotice(`AI: ${e.message}`, "error", true);
    }
  }, [activeClusterId, cMap, clusterDetail?.candidates, railIdx, showNotice]);
  const setDefaultMapMode = useCallback(
    () => dispatch({ type: "map_mode", mode: "default" }),
    [],
  );
  const setSatelliteMapMode = useCallback(
    () => dispatch({ type: "map_mode", mode: "satellite" }),
    [],
  );

  const selArr = Array.from(uiState.selectedRefs);
  const mergeableStationIds = useMemo(
    () =>
      Array.from(
        new Set(
          selArr.flatMap((ref) => {
            if (!isMergeableRef(ref)) return [];
            return resolveRefMemberStationIds(ref, workspace);
          }),
        ),
      ),
    [selArr, workspace],
  );
  const canMrg =
    mergeableStationIds.length >= 2 &&
    selArr.some((ref) => isMergeableRef(ref));
  const canGrp = selArr.filter((ref) => isMergeableRef(ref)).length >= 2;
  const clSt = String(
    clusterDetail?.effective_status || clusterDetail?.status || "",
  ).toLowerCase();
  const canUn = clSt === "resolved" || clSt === "dismissed";
  const clusterHeading = resolveClusterHeading(clusterDetail);

  return (
    <div className="h-screen grid grid-cols-[260px_1fr] bg-surface-0 overflow-hidden">
      <ClusterSidebar
        clusters={clusters}
        totalCount={clusterTotalCount}
        activeClusterId={activeClusterId}
        filters={filters}
        onFilterChange={setFilters}
        onSelectCluster={loadDetail}
        onRefresh={loadClusters}
        loading={loading}
      />

      <div className="flex flex-col overflow-hidden min-w-0">
        {/* ── Split Layout: Candidates/Tabs | Map ── */}
        <div className="flex-1 flex min-h-0 overflow-hidden">
          {/* ── Left: Candidates & Context ── */}
          <div className="flex-[60] min-w-[380px] flex flex-col bg-surface-0 border-r border-border">
            {/* ── Top toolbar: cluster-level only ── */}
            <div className="flex items-center gap-2 px-4 py-2 bg-surface-1 border-b border-border shrink-0">
              <strong
                className="text-text-primary font-display text-sm truncate max-w-[200px]"
                title={clusterHeading}
              >
                {clusterHeading}
              </strong>
              <span
                id="saveStateIndicator"
                className={`font-display font-bold text-xs uppercase tracking-wider ml-1 ${saveC[saveState] || "text-text-muted"}`}
              >
                {saveState}
              </span>
              {workspaceVersion > 0 && (
                <span className="text-text-muted text-xs font-display">
                  v{workspaceVersion}
                </span>
              )}

              <div className="flex items-center gap-1.5 ml-auto">
                {activeClusterId ? (
                  <a
                    href={`/ai-evaluation.html?clusterId=${encodeURIComponent(activeClusterId)}`}
                    className="px-2.5 py-1.5 rounded-lg text-xs font-bold bg-blue-dim border border-blue/20 text-blue no-underline hover:bg-blue/20 transition-colors"
                  >
                    Evaluate
                  </a>
                ) : null}
                <button
                  type="button"
                  onClick={hAi}
                  className="px-2.5 py-1.5 rounded-lg text-xs font-bold bg-teal-dim border border-teal/20 text-teal cursor-pointer hover:bg-teal/20 transition-colors"
                >
                  AI Suggest
                </button>
                <div className="w-px h-5 bg-border mx-0.5" />
                <button
                  id="dismissClusterBtn"
                  type="button"
                  onClick={() => hResolve("dismissed")}
                  className="px-3 py-1.5 rounded-lg text-xs font-bold bg-red-dim border border-red/20 text-red cursor-pointer hover:bg-red/20 transition-colors"
                >
                  Dismiss
                </button>
                <button
                  id="resolveClusterBtn"
                  type="button"
                  onClick={canUn ? hUnresolve : () => hResolve("resolved")}
                  className="px-3 py-1.5 rounded-lg text-xs font-bold bg-green-dim border border-green/20 text-green cursor-pointer hover:bg-green/20 transition-colors"
                >
                  {resolveResolveButtonLabel(canUn)}
                </button>
              </div>
            </div>

            {/* Notices */}
            {notice && (
              <div
                className={`px-4 py-2 text-sm shrink-0 ${resolveNoticeToneClass(notice.tone)}`}
              >
                {notice.message}
              </div>
            )}
            {aiResult && (
              <div className="px-4 py-2 text-sm bg-teal-dim text-teal shrink-0">
                <strong>
                  AI {(aiResult.confidence_score * 100).toFixed(0)}%
                </strong>{" "}
                → {String(aiResult.suggested_action || "").toUpperCase()}.{" "}
                {aiResult.reasoning}
              </div>
            )}

            {/* Tab bar */}
            <div
              className="flex items-center border-b border-border bg-surface-1 shrink-0 overflow-x-auto"
              style={{ scrollbarWidth: "none" }}
            >
              {[
                { k: "candidates", l: `Candidates (${rail.length})` },
                { k: "evidence", l: "Evidence" },
                { k: "history", l: "History" },
              ].map((t) => (
                <button
                  key={t.k}
                  type="button"
                  onClick={() => dispatch({ type: "bottom_tab", tab: t.k })}
                  className={`whitespace-nowrap px-4 py-2.5 text-xs font-bold font-display uppercase tracking-wider cursor-pointer border-none transition-all ${uiState.bottomTab === t.k ? "bg-surface-2 text-amber border-b-2 border-b-amber" : "bg-transparent text-text-muted hover:text-text-secondary"}`}
                >
                  {t.l}
                </button>
              ))}
            </div>

            {/* ── Action buttons bar (only in candidates tab) ── */}
            {uiState.bottomTab === "candidates" && (
              <div
                id="contextualActionBar"
                className="flex items-center gap-2 px-4 py-2 bg-surface-1/80 border-b border-border shrink-0 flex-wrap"
              >
                <span className="text-text-muted text-xs font-display mr-1">
                  {resolveSelectionLabel(uiState.selectedRefs)}
                </span>

                <button
                  id="mergeSelectedActionBtn"
                  type="button"
                  disabled={!canMrg}
                  onClick={hMerge}
                  className="px-2 py-1 rounded-lg text-xs font-bold bg-amber text-surface-0 border-none cursor-pointer disabled:opacity-30 disabled:cursor-not-allowed hover:bg-amber-hover transition-colors"
                >
                  Merge
                </button>
                <button
                  id="createGroupActionBtn"
                  type="button"
                  disabled={!canGrp}
                  onClick={hGroup}
                  className="px-2 py-1 rounded-lg text-xs font-bold bg-amber text-surface-0 border-none cursor-pointer disabled:opacity-30 disabled:cursor-not-allowed hover:bg-amber-hover transition-colors"
                >
                  Group
                </button>
                <button
                  id="keepSeparateActionBtn"
                  type="button"
                  disabled={selArr.length < 2}
                  onClick={hKeep}
                  className="px-2 py-1 rounded-lg text-xs font-bold bg-surface-3 border border-border text-text-secondary cursor-pointer disabled:opacity-30 disabled:cursor-not-allowed hover:text-text-primary transition-colors"
                >
                  Keep Sep
                </button>
                <button
                  type="button"
                  disabled={!canSplitFocusedItem(focused)}
                  onClick={() => hSplit()}
                  className="px-2 py-1 rounded-lg text-xs font-bold bg-surface-3 border border-border text-text-secondary cursor-pointer disabled:opacity-30 disabled:cursor-not-allowed hover:text-text-primary transition-colors"
                >
                  Split
                </button>
                {focused?.kind === "group" && selArr.length > 0 && (
                  <button
                    type="button"
                    onClick={hAddToGrp}
                    className="px-2 py-1 rounded-lg text-xs font-bold bg-surface-3 border border-border text-text-secondary cursor-pointer hover:text-text-primary transition-colors"
                  >
                    Add to Grp
                  </button>
                )}

                <div className="flex gap-1.5 ml-auto">
                  <button
                    type="button"
                    onClick={hUndo}
                    className="px-2.5 py-1.5 rounded-lg text-xs font-bold bg-surface-3 border border-border text-text-secondary cursor-pointer hover:text-text-primary transition-colors"
                    title="Undo (Ctrl+Z)"
                  >
                    ↩ Undo
                  </button>
                </div>
              </div>
            )}
            {/* Legacy hook ids retained for smoke tests: id="mergeToolTabBtn" id="groupToolTabBtn" */}

            {/* Tab content */}
            <div
              className="flex-1 overflow-y-auto"
              style={{ scrollbarWidth: "thin" }}
            >
              {uiState.bottomTab === "candidates" && (
                <div className="p-3">
                  {rail.length === 0 && (
                    <p className="text-text-muted text-sm">Select a cluster.</p>
                  )}
                  {rail.map((item, idx) => (
                    <CandidateCard
                      key={item.ref}
                      item={item}
                      index={idx}
                      selected={uiState.selectedRefs.has(item.ref)}
                      focused={uiState.focusedRef === item.ref}
                      expanded={expandedRefs.has(item.ref)}
                      workspace={workspace}
                      candidateMap={cMap}
                      onToggle={hToggle}
                      onFocus={(r) => dispatch({ type: "focus", ref: r })}
                      onSplit={hSplit}
                      onToggleExpand={toggleExpand}
                      onRenameRef={(r, v) =>
                        commit(setRenameValue(workspace, r, v))
                      }
                      onRenameComposite={(r, v) =>
                        commit(updateCompositeName(workspace, r, v))
                      }
                      onUpdateGroupTransfer={(g, f, t, s) =>
                        commit(
                          updateGroupTransferSeconds(workspace, g, f, t, s),
                        )
                      }
                      onUpdateGroupNodeLabel={(g, n, l) =>
                        commit(updateGroupNodeLabel(workspace, g, n, l))
                      }
                      onRemoveMergeMember={(m, r) =>
                        commit(removeMemberFromMerge(workspace, m, r), {
                          immediate: true,
                        })
                      }
                      onRemoveGroupMember={(g, m) =>
                        commit(removeMemberFromGroup(workspace, g, m), {
                          immediate: true,
                        })
                      }
                    />
                  ))}
                </div>
              )}
              {uiState.bottomTab === "evidence" && (
                <EvidencePanel
                  clusterDetail={clusterDetail}
                  focusedItem={focused}
                  workspace={workspace}
                />
              )}
              {uiState.bottomTab === "history" && (
                <HistoryPanel clusterDetail={clusterDetail} />
              )}
            </div>
          </div>

          {/* ── Map (right piece) ── */}
          <div className="flex-[40] min-w-[300px] relative shrink-0">
            <CurationMap
              items={plotted}
              focusReferencePoints={clusterDetail?.reference_overlay || []}
              viewportReferencePoints={displayReferencePoints}
              selectedRefs={uiState.selectedRefs}
              onSelectRef={hSelectRef}
              onSelectReferenceCandidates={hSelectReferenceCandidates}
              onViewportChange={loadReferenceViewport}
              mapMode={uiState.mapMode}
              onToggleMapMode={(mode) =>
                resolveMapModeHandler(mode, {
                  setDefault: setDefaultMapMode,
                  setSatellite: setSatelliteMapMode,
                  setCustom: (nextMode) =>
                    dispatch({ type: "map_mode", mode: nextMode }),
                })
              }
              activeReferenceSources={activeReferenceSources}
              onToggleReferenceSource={hToggleReferenceSource}
            />
          </div>
        </div>
      </div>
    </div>
  );
}

EvidenceSection.propTypes = {
  title: PropTypes.string.isRequired,
  tone: PropTypes.string,
  children: PropTypes.node,
};

EvidenceRows.propTypes = {
  rows: PropTypes.arrayOf(evidenceRowShape).isRequired,
  tone: PropTypes.string,
};

CurationMap.propTypes = {
  items: PropTypes.arrayOf(railItemShape).isRequired,
  focusReferencePoints: PropTypes.arrayOf(
    PropTypes.shape({
      source_id: PropTypes.string,
      external_id: PropTypes.string,
      display_name: PropTypes.string,
      category: PropTypes.string,
      lat: PropTypes.number,
      lon: PropTypes.number,
      source_url: PropTypes.string,
      matched_candidate_ids: PropTypes.arrayOf(PropTypes.string),
      match_count: PropTypes.number,
    }),
  ).isRequired,
  viewportReferencePoints: PropTypes.arrayOf(
    PropTypes.shape({
      source_id: PropTypes.string,
      external_id: PropTypes.string,
      display_name: PropTypes.string,
      category: PropTypes.string,
      lat: PropTypes.number,
      lon: PropTypes.number,
      source_url: PropTypes.string,
      matched_candidate_ids: PropTypes.arrayOf(PropTypes.string),
      relevant_candidate_ids: PropTypes.arrayOf(PropTypes.string),
      match_count: PropTypes.number,
      is_focus_overlay: PropTypes.bool,
      is_relevant_to_active_cluster: PropTypes.bool,
    }),
  ).isRequired,
  selectedRefs: refSetShape.isRequired,
  onSelectRef: PropTypes.func.isRequired,
  onSelectReferenceCandidates: PropTypes.func.isRequired,
  onViewportChange: PropTypes.func.isRequired,
  mapMode: PropTypes.string.isRequired,
  onToggleMapMode: PropTypes.func.isRequired,
  activeReferenceSources: PropTypes.shape({
    has: PropTypes.func.isRequired,
  }).isRequired,
  onToggleReferenceSource: PropTypes.func.isRequired,
};

Pill.propTypes = {
  children: PropTypes.node,
  v: PropTypes.string,
  className: PropTypes.string,
};

StatusPill.propTypes = Pill.propTypes;

Tag.propTypes = {
  children: PropTypes.node,
  className: PropTypes.string,
};

Badge.propTypes = Tag.propTypes;

ClusterSidebar.propTypes = {
  clusters: PropTypes.arrayOf(clusterListItemShape).isRequired,
  totalCount: PropTypes.number,
  activeClusterId: PropTypes.string,
  filters: filtersShape.isRequired,
  onFilterChange: PropTypes.func.isRequired,
  onSelectCluster: PropTypes.func.isRequired,
  onRefresh: PropTypes.func.isRequired,
  loading: PropTypes.bool.isRequired,
};

CandidateCard.propTypes = {
  item: railItemShape.isRequired,
  index: PropTypes.number.isRequired,
  selected: PropTypes.bool.isRequired,
  focused: PropTypes.bool.isRequired,
  expanded: PropTypes.bool.isRequired,
  workspace: workspaceShape.isRequired,
  candidateMap: PropTypes.shape({
    get: PropTypes.func.isRequired,
  }).isRequired,
  onToggle: PropTypes.func.isRequired,
  onFocus: PropTypes.func.isRequired,
  onSplit: PropTypes.func.isRequired,
  onRenameRef: PropTypes.func.isRequired,
  onRenameComposite: PropTypes.func.isRequired,
  onUpdateGroupTransfer: PropTypes.func.isRequired,
  onUpdateGroupNodeLabel: PropTypes.func.isRequired,
  onRemoveGroupMember: PropTypes.func.isRequired,
  onRemoveMergeMember: PropTypes.func.isRequired,
  onToggleExpand: PropTypes.func.isRequired,
};

EvidencePanel.propTypes = {
  clusterDetail: clusterDetailShape,
  focusedItem: railItemShape,
  workspace: workspaceShape.isRequired,
};

HistoryPanel.propTypes = {
  clusterDetail: clusterDetailShape,
};
