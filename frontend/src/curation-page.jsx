import {
  useCallback,
  useEffect,
  useMemo,
  useReducer,
  useRef,
  useState,
} from "react";
import {
  addSelectionToGroup,
  fetchClusterDetail as apiFetchClusterDetail,
  fetchClusters as apiFetchClusters,
  buildCandidateMap,
  buildRailItems,
  createEmptyWorkspace,
  createGroupFromSelection,
  createMergeFromSelection,
  formatResultsLabel,
  getRenameValue,
  markKeepSeparate,
  normalizeWorkspace,
  parseRef,
  removeMemberFromGroup,
  reopenCluster,
  requestAiScore,
  resetClusterWorkspace,
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
import maplibregl from "./maplibre";
import "./styles.css";

function formatToneLabel(value) {
  return String(value || "")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function formatCountryLabel(countryTags, fallback) {
  const tags = Array.isArray(countryTags)
    ? countryTags.filter(Boolean).slice(0, 3)
    : [];
  if (tags.length > 0) return tags.join(" / ");
  return fallback || "EU";
}

function formatEvidenceTypeLabel(value) {
  const labels = {
    name_exact: "Exact Name",
    name_loose_similarity: "Loose Name Similarity",
    token_overlap: "Token Overlap",
    geographic_distance: "Geographic Distance",
    coordinate_quality: "Coordinate Quality",
    shared_provider_sources: "Shared Sources",
    shared_route_context: "Shared Route Context",
    shared_adjacent_stations: "Shared Adjacent Stations",
    country_relation: "Country Relation",
    generic_name_penalty: "Generic Name Penalty",
  };
  return labels[value] || formatToneLabel(value || "unknown");
}

function formatEvidenceStatusLabel(value) {
  const labels = {
    supporting: "Supporting",
    warning: "Warning",
    missing: "Missing",
    informational: "Context",
    same_location: "Same Location",
    nearby: "Nearby",
    far_apart: "Far Apart",
    too_far: "Too Far",
    missing_coordinates: "Missing Coordinates",
    coordinates_present: "Coordinates Present",
  };
  return labels[value] || formatToneLabel(value || "unknown");
}

function formatCoordStatus(value) {
  return formatEvidenceStatusLabel(value || "missing_coordinates");
}

function formatEvidenceValue(row) {
  if (!row) return "No data";
  if (row.evidence_type === "geographic_distance") {
    const meters = Number(row.raw_value ?? row.details?.distance_meters);
    if (Number.isFinite(meters)) {
      return `${Math.round(meters)} m`;
    }
    return formatEvidenceStatusLabel(row.details?.distance_status);
  }
  if (
    ["name_loose_similarity", "token_overlap"].includes(row.evidence_type) &&
    Number.isFinite(Number(row.raw_value))
  ) {
    return `${Math.round(Number(row.raw_value) * 100)}%`;
  }
  if (
    [
      "shared_provider_sources",
      "shared_route_context",
      "shared_adjacent_stations",
      "coordinate_quality",
      "generic_name_penalty",
    ].includes(row.evidence_type) &&
    Number.isFinite(Number(row.raw_value))
  ) {
    return String(Number(row.raw_value));
  }
  if (row.evidence_type === "country_relation") {
    if (row.details?.same_country === true) return "Same country";
    if (row.details?.same_country === false) return "Cross-border";
    return "Country unknown";
  }
  if (Number.isFinite(Number(row.score))) {
    return `${Math.round(Number(row.score) * 100)}%`;
  }
  return "No data";
}

function formatEvidenceDetails(details) {
  if (!details || typeof details !== "object") {
    return "";
  }
  if (details.explanation) {
    return String(details.explanation);
  }
  if (details.distance_status) {
    return formatEvidenceStatusLabel(details.distance_status);
  }
  if (details.reason) {
    return String(details.reason);
  }
  const pairs = Object.entries(details)
    .filter(
      ([, value]) => value !== null && value !== undefined && value !== "",
    )
    .slice(0, 3)
    .map(([key, value]) => `${formatToneLabel(key)}: ${String(value)}`);
  return pairs.join(" · ");
}

function getSummaryCounts(summary, key) {
  const container =
    summary && typeof summary === "object"
      ? summary.status_counts || summary
      : {};
  return Number.parseInt(String(container?.[key] ?? 0), 10) || 0;
}

function getTypeCounts(summary) {
  const container =
    summary && typeof summary === "object" && summary.type_counts
      ? summary.type_counts
      : {};
  return Object.entries(container)
    .map(([type, count]) => ({
      type,
      count: Number.parseInt(String(count ?? 0), 10) || 0,
    }))
    .filter((entry) => entry.count > 0)
    .sort(
      (left, right) =>
        right.count - left.count || left.type.localeCompare(right.type),
    );
}

function createUiState() {
  return {
    selectedRefs: new Set(),
    focusedRef: "",
    activeTool: "merge",
    mapMode: "default",
    lastSelectedIndex: -1,
  };
}

function uiReducer(state, action) {
  switch (action.type) {
    case "clear_selection":
      return {
        ...state,
        selectedRefs: new Set(),
        lastSelectedIndex: -1,
      };
    case "set_selection":
      return {
        ...state,
        selectedRefs: new Set(action.refs || []),
        lastSelectedIndex: Number.isFinite(action.lastSelectedIndex)
          ? action.lastSelectedIndex
          : state.lastSelectedIndex,
      };
    case "toggle_selection": {
      const next = new Set(state.selectedRefs);
      if (next.has(action.ref)) next.delete(action.ref);
      else next.add(action.ref);
      return {
        ...state,
        selectedRefs: next,
        lastSelectedIndex: Number.isFinite(action.index)
          ? action.index
          : state.lastSelectedIndex,
      };
    }
    case "focus":
      return {
        ...state,
        focusedRef: action.ref || "",
        activeTool:
          action.tool ||
          (parseRef(action.ref).type === "group"
            ? "group"
            : parseRef(action.ref).type === "merge"
              ? "merge"
              : state.activeTool),
      };
    case "tool":
      return {
        ...state,
        activeTool: action.tool,
      };
    case "map_mode":
      return {
        ...state,
        mapMode: action.mode,
      };
    default:
      return state;
  }
}

const OVERLAP_COORDINATE_PRECISION = 7;
const OVERLAP_BASE_MARKER_SIZE = 24;
const OVERLAP_SELECTION_RING_SIZE = 3;
const OVERLAP_SCREEN_DISTANCE_PX = 24;

function buildCoordinateKey(lat, lon) {
  return `${Number(lat).toFixed(OVERLAP_COORDINATE_PRECISION)}:${Number(lon).toFixed(OVERLAP_COORDINATE_PRECISION)}`;
}

function buildMarkerOverlapGroups(items) {
  const overlapGroups = new Map();

  for (const item of items) {
    const key = buildCoordinateKey(item.lat, item.lon);
    const existing = overlapGroups.get(key);
    if (existing) existing.items.push(item);
    else
      overlapGroups.set(key, {
        key,
        lat: item.lat,
        lon: item.lon,
        items: [item],
      });
  }

  return Array.from(overlapGroups.values());
}

function buildMarkerOverlapLayout(map, items) {
  if (!map) return new Map();

  const layout = new Map();
  const screenGroups = [];

  for (const group of buildMarkerOverlapGroups(items)) {
    for (const item of group.items) {
      const point = map.project([item.lon, item.lat]);
      let targetGroup = null;

      for (const candidateGroup of screenGroups) {
        const dx = candidateGroup.screenX - point.x;
        const dy = candidateGroup.screenY - point.y;
        if (Math.hypot(dx, dy) <= OVERLAP_SCREEN_DISTANCE_PX) {
          targetGroup = candidateGroup;
          break;
        }
      }

      if (!targetGroup) {
        targetGroup = {
          items: [],
          screenX: point.x,
          screenY: point.y,
          anchorLat: item.lat,
          anchorLon: item.lon,
        };
        screenGroups.push(targetGroup);
      }

      targetGroup.items.push(item);
      const count = targetGroup.items.length;
      targetGroup.screenX =
        (targetGroup.screenX * (count - 1) + point.x) / count;
      targetGroup.screenY =
        (targetGroup.screenY * (count - 1) + point.y) / count;
      targetGroup.anchorLat =
        (targetGroup.anchorLat * (count - 1) + item.lat) / count;
      targetGroup.anchorLon =
        (targetGroup.anchorLon * (count - 1) + item.lon) / count;
    }
  }

  for (const group of screenGroups) {
    const stackSize = group.items.length;
    for (const [index, item] of group.items.entries()) {
      const sizeMultiplier = Math.max(1, stackSize - index);
      layout.set(item.ref, {
        stackIndex: index,
        stackSize,
        sizeMultiplier,
        markerSize: OVERLAP_BASE_MARKER_SIZE * sizeMultiplier,
        zIndex: 2000 + index,
        anchorLat: group.anchorLat,
        anchorLon: group.anchorLon,
      });
    }
  }

  return layout;
}

function buildMappableItems(items) {
  const rows = Array.isArray(items) ? items : [];
  return rows
    .map((item) => {
      if (Number.isFinite(item.lat) && Number.isFinite(item.lon)) {
        return {
          ...item,
          approximatePosition: false,
        };
      }

      const displayName = String(item.display_name || "")
        .trim()
        .toLowerCase();
      const rankedPeers = rows
        .filter(
          (candidate) =>
            candidate.ref !== item.ref &&
            Number.isFinite(candidate.lat) &&
            Number.isFinite(candidate.lon) &&
            String(candidate.display_name || "")
              .trim()
              .toLowerCase() === displayName,
        )
        .sort((left, right) => {
          const leftRank = Math.abs(
            Number(left.candidate?.candidate_rank || 9999) -
              Number(item.candidate?.candidate_rank || 9999),
          );
          const rightRank = Math.abs(
            Number(right.candidate?.candidate_rank || 9999) -
              Number(item.candidate?.candidate_rank || 9999),
          );
          return leftRank - rightRank;
        });

      if (rankedPeers.length === 0) {
        return {
          ...item,
          approximatePosition: false,
        };
      }

      const sourcePeers = rankedPeers.slice(0, Math.min(2, rankedPeers.length));
      const lat =
        sourcePeers.reduce((sum, candidate) => sum + Number(candidate.lat), 0) /
        sourcePeers.length;
      const lon =
        sourcePeers.reduce((sum, candidate) => sum + Number(candidate.lon), 0) /
        sourcePeers.length;

      return {
        ...item,
        lat,
        lon,
        approximatePosition: true,
      };
    })
    .filter((item) => Number.isFinite(item.lat) && Number.isFinite(item.lon));
}

function createMarkerElement(item, isSelected, overlapMeta, onSelectRef) {
  const {
    stackSize = 1,
    sizeMultiplier = 1,
    markerSize = OVERLAP_BASE_MARKER_SIZE,
    zIndex = 2000,
  } = overlapMeta || {};
  const shell = document.createElement("button");
  shell.type = "button";
  shell.className = `curation-marker-shell ${stackSize > 1 ? "curation-marker-shell--stacked" : ""} ${isSelected ? "curation-marker-shell--selected" : ""}`;
  shell.title = `${item.display_name || item.ref}${item.approximatePosition ? " (approximate map position)" : ""}`;
  shell.setAttribute("aria-label", item.display_name || item.ref);
  shell.setAttribute("aria-pressed", isSelected ? "true" : "false");
  shell.style.setProperty("--marker-size", `${markerSize}px`);
  shell.style.setProperty("--marker-scale", String(sizeMultiplier));
  shell.style.zIndex = String(zIndex);
  shell.addEventListener("click", (event) => {
    event.stopPropagation();
    if (!String(item.ref || "").startsWith("node:")) {
      onSelectRef(item.ref);
    }
  });

  const dot = document.createElement("span");
  dot.className = `curation-marker ${isSelected ? "curation-marker--selected" : ""}`;
  if (item.map_kind === "group-node") {
    dot.classList.add("curation-marker--node");
  }
  shell.appendChild(dot);

  if (isSelected) {
    const ring = document.createElement("span");
    ring.className = "curation-marker__selection-ring";
    ring.style.setProperty(
      "--selection-ring-size",
      `${markerSize + OVERLAP_SELECTION_RING_SIZE * 2}px`,
    );
    shell.appendChild(ring);
  }

  return shell;
}

/* ── Map Component ── */
function CurationMap({
  items,
  selectedRefs,
  onSelectRef,
  mapMode = "default",
}) {
  const mapRef = useRef(null);
  const mapContainerRef = useRef(null);
  const markersRef = useRef([]);

  useEffect(() => {
    if (mapRef.current || !mapContainerRef.current) return;
    const map = new maplibregl.Map({
      container: mapContainerRef.current,
      style: resolveDefaultMapStyle(),
      center: [10.4515, 51.1657],
      zoom: 5,
    });
    map.addControl(new maplibregl.NavigationControl(), "top-right");
    mapRef.current = map;
    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    const nextStyle =
      mapMode === "satellite"
        ? resolveSatelliteMapStyle()
        : resolveDefaultMapStyle();
    map.setStyle(nextStyle);
  }, [mapMode]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    let frameId = 0;

    for (const marker of markersRef.current) marker.remove();
    markersRef.current = [];

    const valid = (items || []).filter(
      (item) => Number.isFinite(item.lat) && Number.isFinite(item.lon),
    );
    if (valid.length === 0) return;

    const bounds = new maplibregl.LngLatBounds();
    for (const item of valid) {
      bounds.extend([item.lon, item.lat]);
    }
    map.fitBounds(bounds, { padding: 60, maxZoom: 15, duration: 0 });

    frameId = globalThis.requestAnimationFrame(() => {
      const overlapLayout = buildMarkerOverlapLayout(map, valid);
      for (const item of valid) {
        const overlapMeta = overlapLayout.get(item.ref);
        const el = createMarkerElement(
          item,
          selectedRefs.has(item.ref),
          overlapMeta,
          onSelectRef,
        );
        const marker = new maplibregl.Marker({
          element: el,
          anchor: "center",
        })
          .setLngLat([
            overlapMeta?.anchorLon ?? item.lon,
            overlapMeta?.anchorLat ?? item.lat,
          ])
          .addTo(map);
        markersRef.current.push(marker);
      }
    });

    return () => {
      if (frameId) globalThis.cancelAnimationFrame(frameId);
    };
  }, [items, onSelectRef, selectedRefs]);

  return (
    <div
      ref={mapContainerRef}
      className="curation-map flex-1 min-h-[200px] w-full"
    />
  );
}

/* ── Badge Helpers ── */
const severityColors = {
  critical: "bg-red-dim text-red border border-red/20",
  high: "bg-orange-dim text-orange border border-orange/20",
  medium: "bg-yellow-dim text-yellow border border-yellow/20",
  low: "bg-green-dim text-green border border-green/20",
};

const statusColors = {
  open: "bg-yellow-dim text-yellow border border-yellow/20",
  in_review: "bg-blue-dim text-blue border border-blue/20",
  resolved: "bg-green-dim text-green border border-green/20",
  dismissed: "bg-surface-3 text-text-muted border border-border",
  supporting: "bg-green-dim text-green border border-green/20",
  warning: "bg-orange-dim text-orange border border-orange/20",
  missing: "bg-red-dim text-red border border-red/20",
  informational: "bg-surface-3 text-text-secondary border border-border",
};

function Badge({ children, variant = "neutral", className = "" }) {
  const base =
    "inline-flex items-center px-2.5 py-1 rounded-md text-[0.7rem] font-semibold font-display uppercase tracking-wide whitespace-nowrap";
  const color =
    variant === "neutral"
      ? "bg-surface-3 text-text-secondary border border-border"
      : severityColors[variant] ||
        statusColors[variant] ||
        "bg-surface-3 text-text-secondary border border-border";
  return <span className={`${base} ${color} ${className}`}>{children}</span>;
}

function StatusPill({ status, children }) {
  const base =
    "inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-[0.72rem] font-bold font-display border";
  const color =
    statusColors[status] || "bg-surface-3 text-text-secondary border-border";
  return <span className={`${base} ${color}`}>{children}</span>;
}

function Tag({ children, variant = "", className = "" }) {
  const base =
    "inline-flex items-center px-2.5 py-1 rounded-md text-[0.73rem] font-semibold border border-border bg-surface-2 text-text-secondary";
  const merged =
    variant === "merged" ? "border-red/20 text-red bg-red-dim" : "";
  return <span className={`${base} ${merged} ${className}`}>{children}</span>;
}

/* ── Cluster Sidebar ── */
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
  const displayTotalCount =
    Number.isFinite(totalCount) && totalCount > 0
      ? totalCount
      : clusters.length;
  return (
    <aside className="bg-surface-1 border-r border-border flex flex-col overflow-hidden">
      <div className="flex justify-between items-start px-5 pt-5 pb-3.5 border-b border-border">
        <div>
          <p className="m-0 mb-1 text-[0.72rem] tracking-widest uppercase text-amber font-bold font-display">
            QA Workspace
          </p>
          <h2 className="text-lg font-bold tracking-tight m-0 text-text-primary">
            Station Curation
          </h2>
        </div>
        <a
          href="/"
          className="no-underline text-amber text-[0.82rem] font-bold px-3 py-2 rounded-lg bg-amber-dim hover:bg-amber/20 transition-colors"
        >
          Home
        </a>
      </div>

      <div className="px-5 py-4 border-b border-border space-y-2.5">
        <div className="grid grid-cols-[68px_1fr] gap-2.5 items-center">
          <label
            htmlFor="countryFilter"
            className="text-[0.8rem] text-text-secondary font-semibold font-display"
          >
            Country
          </label>
          <select
            id="countryFilter"
            value={filters.country}
            onChange={(event) =>
              onFilterChange({ ...filters, country: event.target.value })
            }
            className="bg-surface-2 border border-border-strong rounded-lg px-2.5 py-2 text-text-primary text-sm focus:outline-none focus:border-amber/40 transition-colors"
          >
            <option value="">All</option>
            <option value="DE">DE</option>
            <option value="AT">AT</option>
            <option value="CH">CH</option>
            <option value="FR">FR</option>
            <option value="IT">IT</option>
            <option value="NL">NL</option>
            <option value="BE">BE</option>
            <option value="CZ">CZ</option>
            <option value="PL">PL</option>
          </select>
        </div>
        <div className="grid grid-cols-[68px_1fr] gap-2.5 items-center">
          <label
            htmlFor="statusFilter"
            className="text-[0.8rem] text-text-secondary font-semibold font-display"
          >
            Status
          </label>
          <select
            id="statusFilter"
            value={filters.status}
            onChange={(event) =>
              onFilterChange({ ...filters, status: event.target.value })
            }
            className="bg-surface-2 border border-border-strong rounded-lg px-2.5 py-2 text-text-primary text-sm focus:outline-none focus:border-amber/40 transition-colors"
          >
            <option value="">All</option>
            <option value="open">Open</option>
            <option value="in_review">In Review</option>
            <option value="resolved">Resolved</option>
            <option value="dismissed">Dismissed</option>
          </select>
        </div>
        <button
          id="refreshBtn"
          type="button"
          className="w-full py-2.5 px-4 rounded-lg font-semibold text-sm bg-amber text-surface-0 hover:bg-amber-hover transition-all shadow-[0_4px_14px_rgba(245,158,11,0.25)] cursor-pointer border-none"
          onClick={onRefresh}
        >
          Refresh List
        </button>
      </div>

      <p className="px-5 pt-3.5 pb-2.5 m-0 text-[0.76rem] text-text-muted tracking-wide font-display">
        {loading ? "Loading..." : formatResultsLabel(displayTotalCount)}
      </p>

      <div
        className="flex-1 overflow-y-auto px-3.5 pb-4"
        style={{ scrollbarWidth: "thin" }}
      >
        {clusters.length === 0 && !loading && (
          <p className="text-text-muted m-0 px-2 py-4">
            No clusters found for this filter.
          </p>
        )}
        {clusters.map((cluster) => (
          <button
            key={cluster.cluster_id}
            type="button"
            className={`w-full text-left border rounded-xl p-3.5 mb-2.5 cursor-pointer transition-all duration-150 bg-surface-2 hover:bg-surface-3 hover:border-amber/30 ${activeClusterId === cluster.cluster_id ? "border-amber/40 bg-surface-3 shadow-[inset_3px_0_0_var(--color-amber),0_4px_20px_rgba(245,158,11,0.08)]" : "border-border"}`}
            onClick={() => onSelectCluster(cluster.cluster_id)}
          >
            <div className="flex items-start justify-between gap-2.5 mb-2.5">
              <span className="m-0 text-[0.96rem] leading-snug font-bold tracking-tight text-text-primary">
                {cluster.display_name || cluster.cluster_id}
              </span>
              <Badge variant={String(cluster.severity || "").toLowerCase()}>
                {formatToneLabel(cluster.severity || "Unknown")}
              </Badge>
            </div>
            <div className="flex flex-wrap gap-1.5">
              <Badge
                variant={String(
                  cluster.effective_status || cluster.status || "",
                ).toLowerCase()}
              >
                {formatToneLabel(
                  cluster.effective_status || cluster.status || "Unknown",
                )}
              </Badge>
              {cluster.has_workspace && (
                <Badge variant="neutral">
                  ws v{cluster.workspace_version || 0}
                </Badge>
              )}
              <Badge variant="neutral">
                {formatCountryLabel(cluster.country_tags, cluster.country)}
              </Badge>
            </div>
            <div className="flex items-baseline gap-1.5 mt-3 text-[0.82rem] text-text-muted">
              <strong className="text-base tracking-tight text-text-primary">
                {cluster.candidate_count}
              </strong>
              <span className="text-[0.78rem] uppercase tracking-wider text-text-muted font-bold font-display">
                candidates
              </span>
            </div>
          </button>
        ))}
      </div>
    </aside>
  );
}

/* ── Candidate Rail Card ── */
function CandidateRailCard({
  item,
  index,
  selected,
  focused,
  workspace,
  candidateMap,
  onToggleSelection,
  onFocus,
  onSplit,
}) {
  const candidate = item.candidate || {};
  const memberNames =
    item.member_refs?.map((ref) =>
      resolveDisplayNameForRef(ref, workspace, candidateMap),
    ) || [];
  const aliases = Array.isArray(candidate.aliases)
    ? candidate.aliases.filter(Boolean)
    : [];
  const transportModes = Array.isArray(
    candidate.service_context?.transport_modes,
  )
    ? candidate.service_context.transport_modes
    : [];
  const incoming = Array.isArray(candidate.service_context?.incoming)
    ? candidate.service_context.incoming
    : [];
  const outgoing = Array.isArray(candidate.service_context?.outgoing)
    ? candidate.service_context.outgoing
    : [];
  const contextSummary = candidate.context_summary || {};

  const handleCardSelection = (event) => {
    onToggleSelection(item.ref, index, event.shiftKey);
    onFocus(item.ref);
  };

  const kindBorder =
    item.kind === "merge"
      ? "border-l-[4px] border-l-teal"
      : item.kind === "group"
        ? "border-l-[4px] border-l-orange"
        : "";

  return (
    /* biome-ignore lint/a11y/useSemanticElements: the card container needs a non-button wrapper because it contains nested controls */
    <div
      className={`border border-border rounded-xl p-3 mb-2.5 bg-surface-2 cursor-pointer transition-all duration-150 outline-none animate-fade-in hover:translate-y-[-1px] hover:bg-surface-3 ${kindBorder} ${selected ? "border-amber/30 shadow-[0_0_20px_rgba(245,158,11,0.08)]" : ""} ${focused ? "border-text-muted/30 shadow-[0_0_0_2px_rgba(245,158,11,0.12)]" : ""}`}
      role="button"
      aria-pressed={selected}
      onClick={handleCardSelection}
      onKeyDown={(event) => {
        if (event.target !== event.currentTarget) return;
        if (event.key !== "Enter" && event.key !== " ") return;
        event.preventDefault();
        handleCardSelection(event);
      }}
      tabIndex={0}
    >
      <div className="flex justify-between gap-2 items-start">
        <label className="flex gap-2.5 items-start min-w-0 cursor-pointer">
          <input
            type="checkbox"
            checked={selected}
            data-station-id={item.ref}
            onChange={(event) =>
              onToggleSelection(item.ref, index, event.shiftKey)
            }
            onClick={(event) => event.stopPropagation()}
            className="mt-1 accent-amber"
          />
        </label>
        <button
          type="button"
          className="appearance-none border-0 bg-transparent p-0 m-0 min-w-0 text-left cursor-pointer font-[inherit] text-[inherit] flex-1"
          onClick={() => onFocus(item.ref)}
        >
          <strong className="block text-text-primary">
            {item.display_name}
          </strong>
          <span className="text-text-muted text-[0.78rem] block mt-0.5 break-all truncate-id">
            {item.ref}
          </span>
        </button>
        <Tag>{item.kind}</Tag>
      </div>
      {item.kind === "raw" ? (
        <>
          <div className="flex flex-wrap gap-2 mt-3">
            <Tag>{formatCountryLabel([candidate.metadata?.country || ""])}</Tag>
            <Tag>{(item.provider_labels || []).length} feeds</Tag>
            <StatusPill status={candidate.coord_status}>
              {formatCoordStatus(candidate.coord_status)}
            </StatusPill>
          </div>
          <div className="flex flex-wrap gap-1.5 mt-2">
            {transportModes.slice(0, 3).map((mode) => (
              <Tag key={`${item.ref}-mode-${mode}`}>{mode}</Tag>
            ))}
            <Tag>{contextSummary.stop_point_count ?? 0} stop points</Tag>
            <Tag>{contextSummary.route_count ?? 0} routes</Tag>
          </div>
          {aliases.length > 0 && (
            <p className="text-text-muted text-[0.78rem] mt-2.5 m-0 leading-relaxed">
              Aliases: {aliases.slice(0, 4).join(", ")}
            </p>
          )}
          <div className="grid gap-1 mt-2 text-[0.76rem] text-text-secondary">
            <span>In: {incoming.slice(0, 2).join(", ") || "none"}</span>
            <span>Out: {outgoing.slice(0, 2).join(", ") || "none"}</span>
          </div>
        </>
      ) : (
        <div className="flex items-center gap-2 flex-wrap mt-2 text-text-secondary text-sm">
          <span>{item.member_refs?.length || 0} members</span>
          {item.kind === "group" && (
            <span>{item.internal_nodes?.length || 0} nodes</span>
          )}
        </div>
      )}
      {memberNames.length > 0 && (
        <div className="flex flex-wrap gap-1.5 mt-2.5">
          {memberNames.slice(0, 4).map((name) => (
            <Tag key={`${item.ref}-${name}`}>{name}</Tag>
          ))}
          {memberNames.length > 4 && <Tag>+{memberNames.length - 4}</Tag>}
        </div>
      )}
      {item.kind !== "raw" && (
        <div className="flex items-center gap-2 flex-wrap mt-2">
          <button
            type="button"
            className="px-2.5 py-1 rounded-lg text-[0.78rem] font-semibold bg-surface-3 border border-border text-text-secondary hover:text-text-primary hover:border-border-strong transition-colors cursor-pointer"
            onClick={(event) => {
              event.stopPropagation();
              onSplit(item.ref);
            }}
          >
            Split
          </button>
        </div>
      )}
    </div>
  );
}

/* ── Workspace Panel ── */
function WorkspacePanel({
  clusterDetail,
  saveState,
  notice,
  toolMode,
  selectedRefs,
  focusedItem,
  workspace,
  candidateMap,
  onMergeSelection,
  onCreateGroup,
  onKeepSeparate,
  onSplitFocused,
  onAddSelectionToGroup,
  onUndo,
  onReset,
  onResolve,
  onUnresolve,
  onDismiss,
  onAiScore,
  aiResult,
  onRenameRef,
  onRenameComposite,
  onUpdateGroupTransfer,
  onUpdateGroupNodeLabel,
  onRemoveGroupMember,
  onToolModeChange,
}) {
  const selectedArray = Array.from(selectedRefs);
  const selectedRawRefs = selectedArray.filter(
    (ref) => parseRef(ref).type === "raw",
  );
  const clusterStatus = String(
    clusterDetail?.effective_status || clusterDetail?.status || "",
  ).toLowerCase();
  const canUnresolve =
    clusterStatus === "resolved" || clusterStatus === "dismissed";
  const canMerge = selectedRawRefs.length >= 2;
  const canGroup =
    selectedArray.filter((ref) => {
      const type = parseRef(ref).type;
      return type === "raw" || type === "merge";
    }).length >= 2;

  const saveStateColors = {
    Saved: "bg-green-dim text-green",
    Saving: "bg-yellow-dim text-yellow",
    Failed: "bg-red-dim text-red",
  };

  return (
    <section className="min-h-0 border border-border rounded-2xl bg-surface-1/80 backdrop-blur-sm shadow-[0_8px_32px_rgba(0,0,0,0.2)] p-4 flex flex-col gap-3.5">
      <div
        id="contextualActionBar"
        className="grid grid-cols-[minmax(0,1fr)_auto] gap-3 items-center"
      >
        <div className="flex items-center gap-2.5 flex-wrap min-w-0">
          <strong className="leading-tight text-text-primary font-display">
            {clusterDetail
              ? clusterDetail.display_name || clusterDetail.cluster_id
              : "No cluster selected"}
          </strong>
          <span
            id="saveStateIndicator"
            className={`inline-flex items-center px-2.5 py-1 rounded-md text-[0.72rem] font-bold font-display uppercase tracking-wide ${saveStateColors[saveState] || "bg-surface-3 text-text-muted"}`}
          >
            {saveState}
          </span>
        </div>

        <div className="flex items-center gap-2.5 flex-wrap justify-end">
          <div className="flex flex-wrap gap-2">
            <button
              id="undoWorkspaceBtn"
              type="button"
              className="px-3.5 py-2 rounded-lg font-semibold text-[0.85rem] bg-surface-3 border border-border text-text-secondary hover:text-text-primary hover:border-border-strong transition-all cursor-pointer"
              onClick={onUndo}
            >
              Undo
            </button>
            <button
              id="resetWorkspaceBtn"
              type="button"
              className="px-3.5 py-2 rounded-lg font-semibold text-[0.85rem] bg-surface-3 border border-border text-text-secondary hover:text-text-primary hover:border-border-strong transition-all cursor-pointer"
              onClick={onReset}
            >
              Reset
            </button>
            <button
              type="button"
              className="px-3.5 py-2 rounded-lg font-semibold text-[0.85rem] bg-teal-dim border border-teal/20 text-teal hover:bg-teal/20 transition-all cursor-pointer"
              onClick={onAiScore}
            >
              AI Suggest
            </button>
          </div>

          <div className="flex items-center gap-2.5">
            <button
              id="dismissClusterBtn"
              type="button"
              className="min-w-[100px] inline-flex items-center justify-center px-3.5 py-2 rounded-lg font-semibold text-[0.85rem] bg-red-dim border border-red/20 text-red hover:bg-red/20 transition-all cursor-pointer"
              onClick={onDismiss}
            >
              Dismiss
            </button>
            <button
              id="resolveClusterBtn"
              type="button"
              className="min-w-[100px] inline-flex items-center justify-center px-3.5 py-2 rounded-lg font-semibold text-[0.85rem] bg-green-dim border border-green/20 text-green hover:bg-green/20 transition-all cursor-pointer"
              onClick={canUnresolve ? onUnresolve : onResolve}
            >
              {canUnresolve ? "Unresolve" : "Resolve"}
            </button>
          </div>
        </div>
      </div>

      {notice && (
        <div
          className={`w-full rounded-xl px-3.5 py-3 text-[0.88rem] leading-relaxed border animate-fade-in ${
            notice.tone === "info"
              ? "bg-blue-dim border-blue/20 text-blue"
              : notice.tone === "success"
                ? "bg-green-dim border-green/20 text-green"
                : notice.tone === "error"
                  ? "bg-red-dim border-red/20 text-red"
                  : notice.tone === "warning"
                    ? "bg-yellow-dim border-yellow/20 text-yellow"
                    : "bg-surface-3 border-border text-text-secondary"
          }`}
        >
          {notice.message}
        </div>
      )}

      {aiResult && (
        <div
          id="aiScoreResult"
          className="w-full rounded-xl px-3.5 py-3 text-[0.88rem] leading-relaxed bg-teal-dim border border-teal/20 text-teal animate-fade-in"
        >
          <strong>AI {(aiResult.confidence_score * 100).toFixed(0)}%</strong>{" "}
          suggests {String(aiResult.suggested_action || "").toUpperCase()}.{" "}
          {aiResult.reasoning}
        </div>
      )}

      <div className="border border-border rounded-2xl p-3.5 bg-surface-2/80">
        <div
          className="flex gap-2 justify-start"
          role="tablist"
          aria-label="Curation tools"
        >
          <button
            id="mergeToolTabBtn"
            type="button"
            className={`border rounded-full px-3 py-1.5 font-bold text-sm cursor-pointer transition-all font-display ${toolMode === "merge" ? "bg-amber-dim border-amber/30 text-amber" : "bg-surface-3 border-border text-text-muted hover:text-text-secondary hover:bg-surface-4"}`}
            onClick={() => onToolModeChange("merge")}
          >
            Merge
          </button>
          <button
            id="groupToolTabBtn"
            type="button"
            className={`border rounded-full px-3 py-1.5 font-bold text-sm cursor-pointer transition-all font-display ${toolMode === "group" ? "bg-amber-dim border-amber/30 text-amber" : "bg-surface-3 border-border text-text-muted hover:text-text-secondary hover:bg-surface-4"}`}
            onClick={() => onToolModeChange("group")}
          >
            Group
          </button>
        </div>

        {toolMode === "merge" && (
          <div className="flex flex-col gap-2.5 mt-3">
            <div className="flex gap-2 flex-wrap">
              <button
                id="mergeSelectedActionBtn"
                type="button"
                className="px-3.5 py-2 rounded-lg font-semibold text-sm bg-amber text-surface-0 hover:bg-amber-hover transition-all cursor-pointer shadow-[0_2px_10px_rgba(245,158,11,0.2)] disabled:opacity-40 disabled:cursor-not-allowed border-none"
                onClick={onMergeSelection}
                disabled={!canMerge}
              >
                Merge selected
              </button>
              <button
                id="keepSeparateActionBtn"
                type="button"
                className="px-3.5 py-2 rounded-lg font-semibold text-sm bg-surface-3 border border-border text-text-secondary hover:text-text-primary hover:border-border-strong transition-all cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed"
                onClick={onKeepSeparate}
                disabled={selectedArray.length < 2}
              >
                Keep separate
              </button>
              {focusedItem?.kind === "merge" && (
                <button
                  id="splitCompositeActionBtn"
                  type="button"
                  className="px-3.5 py-2 rounded-lg font-semibold text-sm bg-surface-3 border border-border text-text-secondary hover:text-text-primary hover:border-border-strong transition-all cursor-pointer"
                  onClick={onSplitFocused}
                >
                  Split
                </button>
              )}
            </div>
            <p className="text-text-muted text-[0.82rem] m-0">
              Select at least two raw candidates to merge them into one station
              draft.
            </p>
          </div>
        )}

        {toolMode === "group" && (
          <div className="flex flex-col gap-2.5 mt-3">
            <div className="flex gap-2 flex-wrap">
              <button
                id="createGroupActionBtn"
                type="button"
                className="px-3.5 py-2 rounded-lg font-semibold text-sm bg-amber text-surface-0 hover:bg-amber-hover transition-all cursor-pointer shadow-[0_2px_10px_rgba(245,158,11,0.2)] disabled:opacity-40 disabled:cursor-not-allowed border-none"
                onClick={onCreateGroup}
                disabled={!canGroup}
              >
                Create group
              </button>
              <button
                id="groupEditorActionBtn"
                type="button"
                className="px-3.5 py-2 rounded-lg font-semibold text-sm bg-surface-3 border border-border text-text-secondary hover:text-text-primary hover:border-border-strong transition-all cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed"
                onClick={onAddSelectionToGroup}
                disabled={
                  focusedItem?.kind !== "group" || selectedArray.length === 0
                }
              >
                Add selected
              </button>
              {focusedItem?.kind === "group" && (
                <button
                  type="button"
                  className="px-3.5 py-2 rounded-lg font-semibold text-sm bg-surface-3 border border-border text-text-secondary hover:text-text-primary hover:border-border-strong transition-all cursor-pointer"
                  onClick={onSplitFocused}
                >
                  Split group
                </button>
              )}
            </div>
            <p className="text-text-muted text-[0.82rem] m-0">
              Use groups for one station with multiple internal stop points and
              transfer times.
            </p>
          </div>
        )}
      </div>

      <DraftTab
        workspace={workspace}
        focusedItem={focusedItem}
        candidateMap={candidateMap}
        onRenameRef={onRenameRef}
        onRenameComposite={onRenameComposite}
        onUpdateGroupTransfer={onUpdateGroupTransfer}
        onUpdateGroupNodeLabel={onUpdateGroupNodeLabel}
        onRemoveGroupMember={onRemoveGroupMember}
      />
    </section>
  );
}

/* ── Expandable Panel ── */
function ExpandablePanel({ id, title, children, defaultOpen = false }) {
  return (
    <details
      id={id}
      className="min-h-0 border border-border rounded-2xl bg-surface-1/80 backdrop-blur-sm shadow-[0_8px_32px_rgba(0,0,0,0.2)] overflow-hidden"
      open={defaultOpen}
    >
      <summary className="list-none cursor-pointer px-5 py-3.5 text-[0.92rem] font-bold tracking-tight font-display text-text-primary hover:text-amber transition-colors select-none [&::-webkit-details-marker]:hidden">
        {title}
      </summary>
      <div className="px-5 pb-5 pt-0 border-t border-border">{children}</div>
    </details>
  );
}

/* ── Draft Tab ── */
function DraftTab({
  workspace,
  focusedItem,
  candidateMap,
  onRenameRef,
  onRenameComposite,
  onUpdateGroupTransfer,
  onUpdateGroupNodeLabel,
  onRemoveGroupMember,
}) {
  const focusedRef = focusedItem?.ref || "";
  const focusedGroup =
    focusedItem?.kind === "group"
      ? (workspace.groups || []).find(
          (group) => group.entity_id === parseRef(focusedRef).id,
        )
      : null;
  const focusedMerge =
    focusedItem?.kind === "merge"
      ? (workspace.merges || []).find(
          (merge) => merge.entity_id === parseRef(focusedRef).id,
        )
      : null;

  const inputClasses =
    "w-full bg-surface-2 border border-border-strong rounded-lg px-3 py-2 text-text-primary text-sm focus:outline-none focus:border-amber/40 transition-colors font-[inherit]";

  return (
    <div id="draftTabPanel" className="min-h-0 flex flex-col gap-3">
      {focusedItem?.kind === "raw" && (
        <div className="border border-border rounded-2xl p-3.5 bg-surface-2/80 animate-fade-in">
          <label
            className="text-[0.78rem] text-text-muted font-display font-semibold"
            htmlFor="rawRenameInput"
          >
            Rename candidate
          </label>
          <input
            id="rawRenameInput"
            type="text"
            className={`${inputClasses} mt-1.5`}
            value={
              getRenameValue(workspace, focusedRef) || focusedItem.display_name
            }
            onChange={(event) => onRenameRef(focusedRef, event.target.value)}
          />
        </div>
      )}

      {focusedMerge && (
        <div className="border border-border rounded-2xl p-3.5 bg-surface-2/80 animate-fade-in">
          <label
            className="text-[0.78rem] text-text-muted font-display font-semibold"
            htmlFor="mergeRenameInput"
          >
            Rename merged entity
          </label>
          <input
            id="mergeRenameInput"
            type="text"
            className={`${inputClasses} mt-1.5`}
            value={focusedMerge.display_name}
            onChange={(event) =>
              onRenameComposite(focusedRef, event.target.value)
            }
          />
          <div className="flex flex-wrap gap-1.5 mt-2.5">
            {focusedMerge.member_refs.map((ref) => (
              <Tag key={ref}>
                {resolveDisplayNameForRef(ref, workspace, candidateMap)}
              </Tag>
            ))}
          </div>
        </div>
      )}

      {focusedGroup && (
        <div
          id="groupEditorPanel"
          className="flex flex-col gap-3 animate-fade-in"
        >
          <div className="border border-border rounded-2xl p-3.5 bg-surface-2/80">
            <label
              className="text-[0.78rem] text-text-muted font-display font-semibold"
              htmlFor="groupRenameInput"
            >
              Group name
            </label>
            <input
              id="groupRenameInput"
              type="text"
              className={`${inputClasses} mt-1.5`}
              value={focusedGroup.display_name}
              onChange={(event) =>
                onRenameComposite(focusedRef, event.target.value)
              }
            />
          </div>

          <div className="border border-border rounded-2xl p-3.5 bg-surface-2/60">
            <div className="text-[0.78rem] font-extrabold tracking-widest uppercase text-text-muted font-display mb-2.5">
              Internal nodes
            </div>
            <div className="grid gap-2 content-start">
              {focusedGroup.internal_nodes.map((node) => (
                <div
                  key={node.node_id}
                  className="flex items-center gap-2 flex-wrap border border-border rounded-xl px-3 py-2.5 bg-surface-2/90"
                >
                  <input
                    type="text"
                    className={`${inputClasses} min-w-[180px] flex-1`}
                    value={node.label}
                    onChange={(event) =>
                      onUpdateGroupNodeLabel(
                        focusedGroup.entity_id,
                        node.node_id,
                        event.target.value,
                      )
                    }
                  />
                  <span className="text-text-muted text-[0.78rem]">
                    {resolveDisplayNameForRef(
                      node.source_ref,
                      workspace,
                      candidateMap,
                    )}
                  </span>
                  <button
                    type="button"
                    className="px-2.5 py-1 rounded-lg text-[0.78rem] font-semibold bg-red-dim border border-red/20 text-red hover:bg-red/20 transition-colors cursor-pointer"
                    onClick={() =>
                      onRemoveGroupMember(
                        focusedGroup.entity_id,
                        node.source_ref,
                      )
                    }
                  >
                    Remove
                  </button>
                </div>
              ))}
            </div>
          </div>

          <div className="border border-border rounded-2xl p-3.5 bg-surface-2/60">
            <div className="text-[0.78rem] font-extrabold tracking-widest uppercase text-text-muted font-display mb-2.5">
              Transfer matrix
            </div>
            <div id="groupTransferMatrix" className="grid gap-2 content-start">
              {focusedGroup.transfer_matrix.map((row) => (
                <div
                  key={`${row.from_node_id}-${row.to_node_id}`}
                  className="flex items-center gap-2 flex-wrap border border-border rounded-xl px-3 py-2.5 bg-surface-2/90"
                >
                  <span className="text-text-secondary text-sm">
                    {focusedGroup.internal_nodes.find(
                      (node) => node.node_id === row.from_node_id,
                    )?.label || row.from_node_id}{" "}
                    ↔{" "}
                    {focusedGroup.internal_nodes.find(
                      (node) => node.node_id === row.to_node_id,
                    )?.label || row.to_node_id}
                  </span>
                  <input
                    type="number"
                    min="0"
                    step="10"
                    className="w-[92px] bg-surface-2 border border-border-strong rounded-lg px-2 py-1.5 text-text-primary text-sm focus:outline-none focus:border-amber/40 transition-colors"
                    value={row.min_walk_seconds}
                    onChange={(event) =>
                      onUpdateGroupTransfer(
                        focusedGroup.entity_id,
                        row.from_node_id,
                        row.to_node_id,
                        event.target.value,
                      )
                    }
                  />
                  <span className="text-text-muted text-[0.78rem] font-display">
                    sec
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

/* ── Evidence Tab ── */
function EvidenceTab({ clusterDetail, focusedItem, workspace }) {
  const focusedStationIds = focusedItem
    ? resolveRefMemberStationIds(focusedItem.ref, workspace)
    : [];
  const evidenceRows = (clusterDetail?.evidence || []).filter((row) => {
    if (focusedStationIds.length === 0) return true;
    return (
      focusedStationIds.includes(row.source_global_station_id) ||
      focusedStationIds.includes(row.target_global_station_id)
    );
  });
  const pairSummaries = (clusterDetail?.pair_summaries || []).filter((row) => {
    if (focusedStationIds.length === 0) return true;
    return (
      focusedStationIds.includes(row.source_global_station_id) ||
      focusedStationIds.includes(row.target_global_station_id)
    );
  });
  const typeCounts = getTypeCounts(clusterDetail?.evidence_summary);

  return (
    <div id="evidenceTabPanel" className="min-h-0 flex flex-col gap-3 pt-3">
      <div className="flex flex-wrap gap-1.5">
        {["supporting", "warning", "missing", "informational"].map((status) => (
          <StatusPill key={status} status={status}>
            {formatEvidenceStatusLabel(status)}{" "}
            {getSummaryCounts(clusterDetail?.evidence_summary, status)}
          </StatusPill>
        ))}
      </div>
      {typeCounts.length > 0 && (
        <div className="flex flex-wrap gap-1.5">
          {typeCounts.slice(0, 6).map((entry) => (
            <Tag key={entry.type}>
              {formatEvidenceTypeLabel(entry.type)} {entry.count}
            </Tag>
          ))}
        </div>
      )}
      {pairSummaries.length > 0 && (
        <div className="grid gap-2 mb-3">
          {pairSummaries.slice(0, 8).map((row) => (
            <div
              key={`${row.source_global_station_id}-${row.target_global_station_id}`}
              className="border border-border rounded-xl p-2.5 bg-surface-2/80"
            >
              <div className="grid gap-1 mb-1.5">
                <strong className="text-text-primary text-sm font-display">
                  {row.source_global_station_id} ↔{" "}
                  {row.target_global_station_id}
                </strong>
                <span className="text-text-secondary text-sm">
                  {row.summary || "Evidence summary"}
                </span>
              </div>
              <div className="flex flex-wrap gap-2.5 text-[0.76rem] text-text-muted font-display">
                <span className="text-green">
                  support {row.supporting_count || 0}
                </span>
                <span className="text-orange">
                  warn {row.warning_count || 0}
                </span>
                <span className="text-red">
                  missing {row.missing_count || 0}
                </span>
                <span className="text-text-secondary">
                  context {row.informational_count || 0}
                </span>
                <span className="text-text-primary">
                  score{" "}
                  {Number.isFinite(Number(row.score))
                    ? Number(row.score).toFixed(2)
                    : "n/a"}
                </span>
              </div>
            </div>
          ))}
        </div>
      )}
      {evidenceRows.length === 0 ? (
        <p className="text-text-muted m-0">
          No evidence for the current focus.
        </p>
      ) : (
        <div id="evidenceList" className="grid gap-2 content-start">
          {evidenceRows.map((row) => (
            <div
              key={`${row.evidence_type}-${row.source_global_station_id}-${row.target_global_station_id}-${row.score ?? "na"}`}
              className="border border-border rounded-xl px-3 py-2.5 bg-surface-2/80 text-sm"
            >
              <div className="flex items-center justify-between gap-2 mb-1">
                <strong className="text-text-primary font-display">
                  {formatEvidenceTypeLabel(row.evidence_type)}
                </strong>
                <StatusPill status={row.status || "informational"}>
                  {formatEvidenceStatusLabel(row.status)}
                </StatusPill>
              </div>
              <div className="flex flex-wrap gap-2.5 mb-1 text-text-secondary">
                <span>
                  {row.source_global_station_id} ↔{" "}
                  {row.target_global_station_id}
                </span>
                <span>{formatEvidenceValue(row)}</span>
                <span>
                  score{" "}
                  {Number.isFinite(Number(row.score))
                    ? Number(row.score).toFixed(2)
                    : "n/a"}
                </span>
              </div>
              {formatEvidenceDetails(row.details) && (
                <div className="text-text-muted text-[0.78rem]">
                  {formatEvidenceDetails(row.details)}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

/* ── History Tab ── */
function HistoryTab({ clusterDetail }) {
  return (
    <div id="historyTabPanel" className="min-h-0 flex flex-col gap-3 pt-3">
      <div className="grid gap-2 content-start">
        {(clusterDetail?.edit_history || []).map((row, index) => (
          <div
            key={`${row.event_type}-${row.created_at}-${index}`}
            className="border border-border rounded-xl px-3 py-2.5 bg-surface-2/80 text-sm text-text-secondary"
          >
            <strong className="text-text-primary font-display">
              {row.event_type}
            </strong>{" "}
            · {row.requested_by} · {row.created_at}
          </div>
        ))}
      </div>
    </div>
  );
}

/* ── Main Page ── */
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
  const noticeTimerRef = useRef(null);
  const lastSavedSerializedRef = useRef(
    serializeWorkspace(createEmptyWorkspace()),
  );
  const saveRequestIdRef = useRef(0);
  const immediateSaveRef = useRef(false);

  const showNotice = useCallback((message, tone = "info", sticky = false) => {
    setNotice({ message, tone });
    if (noticeTimerRef.current) clearTimeout(noticeTimerRef.current);
    if (!sticky) {
      noticeTimerRef.current = setTimeout(() => setNotice(null), 4500);
    }
  }, []);

  const loadClusters = useCallback(async () => {
    setLoading(true);
    try {
      const data = await apiFetchClusters(filters);
      setClusters(data.items || []);
      setClusterTotalCount(data.totalCount || 0);
    } catch (error) {
      showNotice(`Failed to load clusters: ${error.message}`, "error", true);
    } finally {
      setLoading(false);
    }
  }, [filters, showNotice]);

  const loadClusterDetail = useCallback(
    async (clusterId) => {
      try {
        const detail = await apiFetchClusterDetail(clusterId);
        setClusterDetail(detail);
        setActiveClusterId(clusterId);
        const normalizedWorkspace = normalizeWorkspace(detail?.workspace);
        setWorkspace(normalizedWorkspace);
        setWorkspaceVersion(detail?.workspace_version || 0);
        lastSavedSerializedRef.current =
          serializeWorkspace(normalizedWorkspace);
        setSaveState("Saved");
        dispatch({ type: "clear_selection" });
        dispatch({ type: "focus", ref: "" });
        setAiResult(null);
      } catch (error) {
        showNotice(`Failed to load cluster: ${error.message}`, "error", true);
      }
    },
    [showNotice],
  );

  useEffect(() => {
    loadClusters();
  }, [loadClusters]);

  useEffect(() => {
    if (clusters.length > 0 && !activeClusterId) {
      loadClusterDetail(clusters[0].cluster_id);
    }
  }, [activeClusterId, clusters, loadClusterDetail]);

  const candidateMap = useMemo(
    () => buildCandidateMap(clusterDetail?.candidates || []),
    [clusterDetail?.candidates],
  );
  const railItems = useMemo(
    () => buildRailItems(clusterDetail, workspace),
    [clusterDetail, workspace],
  );
  const railIndexByRef = useMemo(
    () => new Map(railItems.map((item, index) => [item.ref, index])),
    [railItems],
  );
  const focusedItem = useMemo(
    () => railItems.find((item) => item.ref === uiState.focusedRef) || null,
    [railItems, uiState.focusedRef],
  );
  const mapItems = useMemo(() => railItems, [railItems]);
  const plottedMapItems = useMemo(
    () => buildMappableItems(mapItems),
    [mapItems],
  );

  useEffect(() => {
    if (!uiState.focusedRef) return;
    if (!railItems.some((item) => item.ref === uiState.focusedRef)) {
      dispatch({ type: "focus", ref: "" });
    }
  }, [railItems, uiState.focusedRef]);

  useEffect(() => {
    if (!activeClusterId) return undefined;
    const serialized = serializeWorkspace(workspace);
    if (serialized === lastSavedSerializedRef.current) return undefined;

    const requestId = saveRequestIdRef.current + 1;
    saveRequestIdRef.current = requestId;
    setSaveState("Saving");
    const delay = immediateSaveRef.current ? 80 : 500;
    immediateSaveRef.current = false;

    const timer = setTimeout(async () => {
      try {
        const result = await saveClusterWorkspace(activeClusterId, workspace);
        if (requestId !== saveRequestIdRef.current) return;
        lastSavedSerializedRef.current = serializeWorkspace(result.workspace);
        setWorkspaceVersion(result.workspace_version || 0);
        setClusterDetail((previous) =>
          previous
            ? {
                ...previous,
                workspace: result.workspace,
                workspace_version: result.workspace_version,
                has_workspace: true,
                effective_status: result.effective_status,
              }
            : previous,
        );
        setClusters((previous) =>
          previous.map((cluster) =>
            cluster.cluster_id === activeClusterId
              ? {
                  ...cluster,
                  effective_status: result.effective_status,
                  has_workspace: true,
                  workspace_version: result.workspace_version,
                }
              : cluster,
          ),
        );
        setSaveState("Saved");
      } catch (error) {
        if (requestId !== saveRequestIdRef.current) return;
        setSaveState("Failed");
        showNotice(`Workspace save failed: ${error.message}`, "error", true);
      }
    }, delay);

    return () => clearTimeout(timer);
  }, [activeClusterId, showNotice, workspace]);

  useEffect(() => {
    const handleKeyDown = (event) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "z") {
        event.preventDefault();
        if (activeClusterId) {
          undoClusterWorkspace(activeClusterId)
            .then((result) => {
              const nextWorkspace = normalizeWorkspace(result.workspace);
              setWorkspace(nextWorkspace);
              lastSavedSerializedRef.current =
                serializeWorkspace(nextWorkspace);
              setWorkspaceVersion(result.workspace_version || 0);
              setSaveState("Saved");
            })
            .catch((error) =>
              showNotice(`Undo failed: ${error.message}`, "error", true),
            );
        }
      }
      if (event.key === "Escape") {
        dispatch({ type: "clear_selection" });
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [activeClusterId, showNotice]);

  const commitWorkspace = useCallback((nextWorkspace, options = {}) => {
    immediateSaveRef.current = options.immediate === true;
    setWorkspace(normalizeWorkspace(nextWorkspace));
  }, []);

  const handleToggleSelection = useCallback(
    (ref, index, useRange) => {
      if (useRange && uiState.lastSelectedIndex >= 0) {
        const start = Math.min(index, uiState.lastSelectedIndex);
        const end = Math.max(index, uiState.lastSelectedIndex);
        dispatch({
          type: "set_selection",
          refs: railItems.slice(start, end + 1).map((item) => item.ref),
          lastSelectedIndex: index,
        });
      } else {
        dispatch({ type: "toggle_selection", ref, index });
      }
    },
    [railItems, uiState.lastSelectedIndex],
  );

  const handleSelectRef = useCallback((ref) => {
    dispatch({ type: "toggle_selection", ref });
    dispatch({ type: "focus", ref });
  }, []);

  const handleMergeSelection = useCallback(() => {
    commitWorkspace(
      createMergeFromSelection(
        workspace,
        uiState.selectedRefs,
        clusterDetail?.candidates || [],
      ),
      {
        immediate: true,
      },
    );
    dispatch({ type: "clear_selection" });
  }, [
    clusterDetail?.candidates,
    commitWorkspace,
    uiState.selectedRefs,
    workspace,
  ]);

  const handleCreateGroup = useCallback(() => {
    const nextWorkspace = createGroupFromSelection(
      workspace,
      uiState.selectedRefs,
      clusterDetail?.candidates || [],
    );
    const createdGroup = nextWorkspace.groups.at(-1);
    commitWorkspace(nextWorkspace, { immediate: true });
    dispatch({ type: "clear_selection" });
    if (createdGroup) {
      dispatch({
        type: "focus",
        ref: toGroupRef(createdGroup.entity_id),
        tool: "group",
      });
    }
  }, [
    clusterDetail?.candidates,
    commitWorkspace,
    uiState.selectedRefs,
    workspace,
  ]);

  const handleKeepSeparate = useCallback(() => {
    commitWorkspace(markKeepSeparate(workspace, uiState.selectedRefs), {
      immediate: true,
    });
    dispatch({ type: "clear_selection" });
  }, [commitWorkspace, uiState.selectedRefs, workspace]);

  const handleSplitComposite = useCallback(
    (ref = focusedItem?.ref) => {
      if (!ref) return;
      commitWorkspace(splitComposite(workspace, ref), { immediate: true });
      dispatch({ type: "focus", ref: "" });
    },
    [commitWorkspace, focusedItem?.ref, workspace],
  );

  const handleAddSelectionToGroup = useCallback(() => {
    if (focusedItem?.kind !== "group") return;
    commitWorkspace(
      addSelectionToGroup(
        workspace,
        parseRef(focusedItem.ref).id,
        uiState.selectedRefs,
        clusterDetail?.candidates || [],
      ),
      { immediate: true },
    );
  }, [
    clusterDetail?.candidates,
    commitWorkspace,
    focusedItem,
    uiState.selectedRefs,
    workspace,
  ]);

  const handleUndo = useCallback(async () => {
    if (!activeClusterId) return;
    try {
      const result = await undoClusterWorkspace(activeClusterId);
      const nextWorkspace = normalizeWorkspace(result.workspace);
      setWorkspace(nextWorkspace);
      lastSavedSerializedRef.current = serializeWorkspace(nextWorkspace);
      setWorkspaceVersion(result.workspace_version || 0);
      setSaveState("Saved");
      setClusterDetail((previous) =>
        previous
          ? {
              ...previous,
              workspace: nextWorkspace,
              workspace_version: result.workspace_version,
              has_workspace: Boolean(result.workspace),
              effective_status: result.effective_status,
            }
          : previous,
      );
      showNotice("Workspace reverted to the previous snapshot.", "success");
    } catch (error) {
      showNotice(`Undo failed: ${error.message}`, "error", true);
    }
  }, [activeClusterId, showNotice]);

  const handleReset = useCallback(async () => {
    if (!activeClusterId) return;
    try {
      const result = await resetClusterWorkspace(activeClusterId);
      const nextWorkspace = normalizeWorkspace(result.workspace);
      setWorkspace(nextWorkspace);
      lastSavedSerializedRef.current = serializeWorkspace(nextWorkspace);
      setWorkspaceVersion(0);
      setSaveState("Saved");
      dispatch({ type: "clear_selection" });
      dispatch({ type: "focus", ref: "" });
      setClusterDetail((previous) =>
        previous
          ? {
              ...previous,
              workspace: nextWorkspace,
              workspace_version: 0,
              has_workspace: false,
              effective_status: result.effective_status,
            }
          : previous,
      );
      showNotice("Workspace cleared and cluster returned to open.", "success");
    } catch (error) {
      showNotice(`Reset failed: ${error.message}`, "error", true);
    }
  }, [activeClusterId, showNotice]);

  const handleUnresolve = useCallback(async () => {
    if (!activeClusterId) return;
    try {
      const result = await reopenCluster(activeClusterId);
      const nextWorkspace = normalizeWorkspace(result.workspace);
      setWorkspace(nextWorkspace);
      lastSavedSerializedRef.current = serializeWorkspace(nextWorkspace);
      setWorkspaceVersion(result.workspace_version || 0);
      setSaveState("Saved");
      setClusterDetail((previous) =>
        previous
          ? {
              ...previous,
              workspace: nextWorkspace,
              workspace_version: result.workspace_version,
              has_workspace: result.workspace_version > 0,
              effective_status: result.effective_status,
              status: result.effective_status,
            }
          : previous,
      );
      setClusters((previous) =>
        previous.map((cluster) =>
          cluster.cluster_id === activeClusterId
            ? {
                ...cluster,
                effective_status: result.effective_status,
                status: result.effective_status,
                has_workspace: result.workspace_version > 0,
                workspace_version: result.workspace_version,
              }
            : cluster,
        ),
      );
      showNotice("Cluster reopened for editing.", "success");
    } catch (error) {
      showNotice(`Unresolve failed: ${error.message}`, "error", true);
    }
  }, [activeClusterId, showNotice]);

  const handleResolveStatus = useCallback(
    async (status) => {
      if (!activeClusterId) return;
      try {
        const result = await resolveCluster(
          activeClusterId,
          status,
          workspace.note,
        );
        showNotice(
          `${formatToneLabel(status)} complete (decision id=${result.decision_id || "n/a"}).`,
          "success",
        );
        await loadClusters();
        const nextClusterId = result.next_cluster_id || null;
        if (nextClusterId) {
          await loadClusterDetail(nextClusterId);
        } else {
          await loadClusterDetail(activeClusterId);
        }
      } catch (error) {
        showNotice(`Resolve failed: ${error.message}`, "error", true);
      }
    },
    [
      activeClusterId,
      loadClusterDetail,
      loadClusters,
      showNotice,
      workspace.note,
    ],
  );

  const handleAiScore = useCallback(async () => {
    if (!activeClusterId) return;
    try {
      const result = await requestAiScore(activeClusterId);
      setAiResult(result);
      if (String(result.suggested_action || "").toLowerCase() === "merge") {
        const rawRefs = sortCandidateIds(
          (clusterDetail?.candidates || [])
            .slice(0, 2)
            .map((candidate) => candidate.global_station_id),
          candidateMap,
        ).map(toRawRef);
        dispatch({
          type: "set_selection",
          refs: rawRefs,
          lastSelectedIndex:
            rawRefs.length > 0 ? railIndexByRef.get(rawRefs.at(-1)) || 0 : -1,
        });
      }
    } catch (error) {
      showNotice(`AI failed: ${error.message}`, "error", true);
    }
  }, [
    activeClusterId,
    candidateMap,
    clusterDetail?.candidates,
    railIndexByRef,
    showNotice,
  ]);

  return (
    <div className="min-h-screen grid grid-cols-[320px_430px_minmax(0,1fr)] bg-surface-0">
      <ClusterSidebar
        clusters={clusters}
        totalCount={clusterTotalCount}
        activeClusterId={activeClusterId}
        filters={filters}
        onFilterChange={setFilters}
        onSelectCluster={loadClusterDetail}
        onRefresh={loadClusters}
        loading={loading}
      />

      <aside className="border-r border-border bg-surface-1/60 flex flex-col min-w-0">
        <div className="sticky top-0 z-[2] px-4 pt-5 pb-3 bg-surface-1/95 backdrop-blur-sm border-b border-border">
          <div className="flex justify-between items-start">
            <div>
              <h4 className="m-0 text-base tracking-tight font-display text-text-primary">
                Candidate Rail
              </h4>
              <p
                id="selectionSummary"
                className="text-text-muted text-[0.78rem] m-0 mt-1 font-display"
              >
                {uiState.selectedRefs.size === 0
                  ? "No items selected."
                  : `Selected: ${uiState.selectedRefs.size} item(s).`}
              </p>
            </div>
          </div>
          <div className="flex gap-2 mt-2">
            <button
              id="candidateSelectAllBtn"
              type="button"
              className="px-2.5 py-1 rounded-lg text-[0.78rem] font-semibold bg-surface-3 border border-border text-text-secondary hover:text-text-primary hover:border-border-strong transition-colors cursor-pointer"
              onClick={() =>
                dispatch({
                  type: "set_selection",
                  refs: railItems.map((item) => item.ref),
                  lastSelectedIndex: railItems.length - 1,
                })
              }
            >
              All
            </button>
            <button
              id="candidateClearBtn"
              type="button"
              className="px-2.5 py-1 rounded-lg text-[0.78rem] font-semibold bg-surface-3 border border-border text-text-secondary hover:text-text-primary hover:border-border-strong transition-colors cursor-pointer"
              onClick={() => dispatch({ type: "clear_selection" })}
            >
              Clear
            </button>
          </div>
        </div>

        <div
          className="min-h-0 overflow-auto px-3.5 py-3 pb-5"
          style={{ scrollbarWidth: "thin" }}
        >
          {railItems.length === 0 && (
            <p className="text-text-muted m-0 p-3">No cluster selected.</p>
          )}
          {railItems.map((item, index) => (
            <CandidateRailCard
              key={item.ref}
              item={item}
              index={index}
              selected={uiState.selectedRefs.has(item.ref)}
              focused={uiState.focusedRef === item.ref}
              workspace={workspace}
              candidateMap={candidateMap}
              onToggleSelection={handleToggleSelection}
              onFocus={(ref) => dispatch({ type: "focus", ref })}
              onSplit={handleSplitComposite}
            />
          ))}
        </div>
      </aside>

      <main className="min-w-0 grid grid-rows-[minmax(360px,52vh)_auto_auto] gap-3.5 p-5 content-start overflow-y-auto">
        <section className="min-h-0 border border-border rounded-2xl bg-surface-1/80 backdrop-blur-sm shadow-[0_8px_32px_rgba(0,0,0,0.2)] overflow-hidden grid grid-rows-[auto_minmax(0,1fr)]">
          <div className="flex justify-between items-center px-4 py-3 border-b border-border bg-surface-2/60">
            <span
              id="curationMapStatus"
              className="text-text-muted text-[0.78rem] font-display"
            >
              {mapItems.length > 0
                ? `${plottedMapItems.length}/${mapItems.length} workspace items plotted · v${workspaceVersion || 0}.`
                : "Select a cluster."}
            </span>
            <div className="flex gap-1.5">
              <button
                id="mapModeDefaultBtn"
                type="button"
                className={`px-2.5 py-1 rounded-lg text-[0.78rem] font-semibold border cursor-pointer transition-all ${uiState.mapMode === "default" ? "bg-amber-dim border-amber/30 text-amber" : "bg-surface-3 border-border text-text-secondary hover:text-text-primary"}`}
                aria-pressed={uiState.mapMode === "default"}
                disabled={uiState.mapMode === "default"}
                onClick={() => dispatch({ type: "map_mode", mode: "default" })}
              >
                Map
              </button>
              <button
                id="mapModeSatelliteBtn"
                type="button"
                className={`px-2.5 py-1 rounded-lg text-[0.78rem] font-semibold border cursor-pointer transition-all ${uiState.mapMode === "satellite" ? "bg-amber-dim border-amber/30 text-amber" : "bg-surface-3 border-border text-text-secondary hover:text-text-primary"}`}
                aria-pressed={uiState.mapMode === "satellite"}
                disabled={uiState.mapMode === "satellite"}
                onClick={() =>
                  dispatch({ type: "map_mode", mode: "satellite" })
                }
              >
                Sat
              </button>
            </div>
          </div>
          <CurationMap
            items={plottedMapItems}
            selectedRefs={uiState.selectedRefs}
            onSelectRef={handleSelectRef}
            mapMode={uiState.mapMode}
          />
        </section>

        <WorkspacePanel
          clusterDetail={clusterDetail}
          saveState={saveState}
          notice={notice}
          toolMode={uiState.activeTool}
          selectedRefs={uiState.selectedRefs}
          focusedItem={focusedItem}
          workspace={workspace}
          candidateMap={candidateMap}
          onMergeSelection={handleMergeSelection}
          onCreateGroup={handleCreateGroup}
          onKeepSeparate={handleKeepSeparate}
          onSplitFocused={() => handleSplitComposite()}
          onAddSelectionToGroup={handleAddSelectionToGroup}
          onUndo={handleUndo}
          onReset={handleReset}
          onResolve={() => handleResolveStatus("resolved")}
          onUnresolve={handleUnresolve}
          onDismiss={() => handleResolveStatus("dismissed")}
          onAiScore={handleAiScore}
          aiResult={aiResult}
          onRenameRef={(ref, value) =>
            commitWorkspace(setRenameValue(workspace, ref, value))
          }
          onRenameComposite={(ref, value) =>
            commitWorkspace(updateCompositeName(workspace, ref, value))
          }
          onUpdateGroupTransfer={(groupId, fromNodeId, toNodeId, seconds) =>
            commitWorkspace(
              updateGroupTransferSeconds(
                workspace,
                groupId,
                fromNodeId,
                toNodeId,
                seconds,
              ),
            )
          }
          onUpdateGroupNodeLabel={(groupId, nodeId, label) =>
            commitWorkspace(
              updateGroupNodeLabel(workspace, groupId, nodeId, label),
            )
          }
          onRemoveGroupMember={(groupId, memberRef) =>
            commitWorkspace(
              removeMemberFromGroup(workspace, groupId, memberRef),
              {
                immediate: true,
              },
            )
          }
          onToolModeChange={(tool) => dispatch({ type: "tool", tool })}
        />

        <section className="grid gap-3">
          <ExpandablePanel id="evidencePanel" title="Evidence">
            <EvidenceTab
              clusterDetail={clusterDetail}
              focusedItem={focusedItem}
              workspace={workspace}
            />
          </ExpandablePanel>
          <ExpandablePanel id="historyPanel" title="History">
            <HistoryTab clusterDetail={clusterDetail} />
          </ExpandablePanel>
        </section>
      </main>
    </div>
  );
}
