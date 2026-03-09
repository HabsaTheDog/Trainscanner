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

  return <div ref={mapContainerRef} className="curation-map" />;
}

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
    <aside className="curation-sidebar">
      <div className="curation-sidebar__header">
        <div>
          <p className="curation-sidebar__eyebrow">QA Workspace</p>
          <h2 className="curation-sidebar__title">Station Curation</h2>
        </div>
        <a href="/" className="curation-sidebar__home-link">
          Home
        </a>
      </div>

      <div className="curation-sidebar__filters">
        <div className="curation-filter-row">
          <label htmlFor="countryFilter" className="curation-filter-label">
            Country
          </label>
          <select
            id="countryFilter"
            value={filters.country}
            onChange={(event) =>
              onFilterChange({ ...filters, country: event.target.value })
            }
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
        <div className="curation-filter-row">
          <label htmlFor="statusFilter" className="curation-filter-label">
            Status
          </label>
          <select
            id="statusFilter"
            value={filters.status}
            onChange={(event) =>
              onFilterChange({ ...filters, status: event.target.value })
            }
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
          className="curation-btn curation-btn--full"
          onClick={onRefresh}
        >
          Refresh List
        </button>
      </div>

      <p className="curation-sidebar__meta">
        {loading ? "Loading..." : formatResultsLabel(displayTotalCount)}
      </p>

      <div className="curation-sidebar__list">
        {clusters.length === 0 && !loading && (
          <p className="curation-muted">No clusters found for this filter.</p>
        )}
        {clusters.map((cluster) => (
          <button
            key={cluster.cluster_id}
            type="button"
            className={`curation-cluster-item ${activeClusterId === cluster.cluster_id ? "curation-cluster-item--active" : ""}`}
            onClick={() => onSelectCluster(cluster.cluster_id)}
          >
            <div className="curation-cluster-item__top">
              <span className="curation-cluster-item__name">
                {cluster.display_name || cluster.cluster_id}
              </span>
              <span
                className={`curation-badge curation-badge--severity-${String(cluster.severity || "").toLowerCase() || "default"}`}
              >
                {formatToneLabel(cluster.severity || "Unknown")}
              </span>
            </div>
            <div className="curation-cluster-item__badges">
              <span
                className={`curation-badge curation-badge--status-${String(cluster.effective_status || cluster.status || "").toLowerCase() || "default"}`}
              >
                {formatToneLabel(
                  cluster.effective_status || cluster.status || "Unknown",
                )}
              </span>
              {cluster.has_workspace && (
                <span className="curation-badge curation-badge--neutral">
                  ws v{cluster.workspace_version || 0}
                </span>
              )}
              <span className="curation-badge curation-badge--neutral">
                {formatCountryLabel(cluster.country_tags, cluster.country)}
              </span>
            </div>
            <div className="curation-cluster-item__meta">
              <strong>{cluster.candidate_count}</strong>
              <span>candidates</span>
            </div>
          </button>
        ))}
      </div>
    </aside>
  );
}

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

  return (
    /* biome-ignore lint/a11y/useSemanticElements: the card container needs a non-button wrapper because it contains nested controls */
    <div
      className={`curation-rail-card curation-rail-card--${item.kind} ${selected ? "curation-rail-card--selected" : ""} ${focused ? "curation-rail-card--focused" : ""}`}
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
      <div className="curation-rail-card__header">
        <label className="curation-rail-card__select">
          <input
            type="checkbox"
            checked={selected}
            data-station-id={item.ref}
            onChange={(event) =>
              onToggleSelection(item.ref, index, event.shiftKey)
            }
            onClick={(event) => event.stopPropagation()}
          />
        </label>
        <button
          type="button"
          className="curation-rail-card__focus"
          onClick={() => onFocus(item.ref)}
        >
          <strong>{item.display_name}</strong>
          <span className="curation-candidate__id">{item.ref}</span>
        </button>
        <span className="curation-tag">{item.kind}</span>
      </div>
      {item.kind === "raw" ? (
        <>
          <div className="curation-candidate__meta">
            <span className="curation-candidate__meta-item">
              {formatCountryLabel([candidate.metadata?.country || ""])}
            </span>
            <span className="curation-candidate__meta-item">
              {(item.provider_labels || []).length} feeds
            </span>
            <span className="curation-status-pill">
              {formatCoordStatus(candidate.coord_status)}
            </span>
          </div>
          <div className="curation-context-chips">
            {transportModes.slice(0, 3).map((mode) => (
              <span key={`${item.ref}-mode-${mode}`} className="curation-tag">
                {mode}
              </span>
            ))}
            <span className="curation-tag">
              {contextSummary.stop_point_count ?? 0} stop points
            </span>
            <span className="curation-tag">
              {contextSummary.route_count ?? 0} routes
            </span>
          </div>
          {aliases.length > 0 && (
            <p className="curation-muted curation-tiny curation-candidate__aliases">
              Aliases: {aliases.slice(0, 4).join(", ")}
            </p>
          )}
          <div className="curation-candidate__adjacency">
            <span>In: {incoming.slice(0, 2).join(", ") || "none"}</span>
            <span>Out: {outgoing.slice(0, 2).join(", ") || "none"}</span>
          </div>
        </>
      ) : (
        <div className="curation-rail-card__summary">
          <span>{item.member_refs?.length || 0} members</span>
          {item.kind === "group" && (
            <span>{item.internal_nodes?.length || 0} nodes</span>
          )}
        </div>
      )}
      {memberNames.length > 0 && (
        <div className="curation-rail-card__members">
          {memberNames.slice(0, 4).map((name) => (
            <span key={`${item.ref}-${name}`} className="curation-tag">
              {name}
            </span>
          ))}
          {memberNames.length > 4 && (
            <span className="curation-tag">+{memberNames.length - 4}</span>
          )}
        </div>
      )}
      {item.kind !== "raw" && (
        <div className="curation-rail-card__inline-actions">
          <button
            type="button"
            className="curation-btn curation-btn--secondary curation-tiny"
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

  return (
    <section className="curation-workspace-panel">
      <div id="contextualActionBar" className="curation-action-bar">
        <div className="curation-action-bar__summary">
          <strong className="curation-action-bar__title">
            {clusterDetail
              ? clusterDetail.display_name || clusterDetail.cluster_id
              : "No cluster selected"}
          </strong>
          <span
            id="saveStateIndicator"
            className={`curation-save-state curation-save-state--${saveState.toLowerCase()}`}
          >
            {saveState}
          </span>
        </div>

        <div className="curation-action-bar__controls">
          <div className="curation-action-bar__actions">
            <button
              id="undoWorkspaceBtn"
              type="button"
              className="curation-btn curation-btn--secondary"
              onClick={onUndo}
            >
              Undo
            </button>
            <button
              id="resetWorkspaceBtn"
              type="button"
              className="curation-btn curation-btn--secondary"
              onClick={onReset}
            >
              Reset
            </button>
            <button
              type="button"
              className="curation-btn curation-btn--secondary"
              onClick={onAiScore}
            >
              AI Suggest
            </button>
          </div>

          <div className="curation-action-bar__resolve">
            <button
              id="dismissClusterBtn"
              type="button"
              className="curation-btn curation-btn--danger"
              onClick={onDismiss}
            >
              Dismiss
            </button>
            <button
              id="resolveClusterBtn"
              type="button"
              className="curation-btn curation-btn--save"
              onClick={canUnresolve ? onUnresolve : onResolve}
            >
              {canUnresolve ? "Unresolve" : "Resolve"}
            </button>
          </div>
        </div>
      </div>

      {notice && (
        <div className={`curation-notice curation-notice--${notice.tone}`}>
          {notice.message}
        </div>
      )}

      {aiResult && (
        <div
          id="aiScoreResult"
          className="curation-notice curation-notice--info"
        >
          <strong>AI {(aiResult.confidence_score * 100).toFixed(0)}%</strong>{" "}
          suggests {String(aiResult.suggested_action || "").toUpperCase()}.{" "}
          {aiResult.reasoning}
        </div>
      )}

      <div className="curation-tool-panel">
        <div
          className="curation-tool-tabs"
          role="tablist"
          aria-label="Curation tools"
        >
          <button
            id="mergeToolTabBtn"
            type="button"
            className={`curation-tool-tab ${toolMode === "merge" ? "curation-tool-tab--active" : ""}`}
            onClick={() => onToolModeChange("merge")}
          >
            Merge
          </button>
          <button
            id="groupToolTabBtn"
            type="button"
            className={`curation-tool-tab ${toolMode === "group" ? "curation-tool-tab--active" : ""}`}
            onClick={() => onToolModeChange("group")}
          >
            Group
          </button>
        </div>

        {toolMode === "merge" && (
          <div className="curation-tool-body">
            <div className="curation-tool-body__actions">
              <button
                id="mergeSelectedActionBtn"
                type="button"
                className="curation-btn"
                onClick={onMergeSelection}
                disabled={!canMerge}
              >
                Merge selected
              </button>
              <button
                id="keepSeparateActionBtn"
                type="button"
                className="curation-btn curation-btn--secondary"
                onClick={onKeepSeparate}
                disabled={selectedArray.length < 2}
              >
                Keep separate
              </button>
              {focusedItem?.kind === "merge" && (
                <button
                  id="splitCompositeActionBtn"
                  type="button"
                  className="curation-btn curation-btn--secondary"
                  onClick={onSplitFocused}
                >
                  Split
                </button>
              )}
            </div>
            <p className="curation-muted curation-tiny">
              Select at least two raw candidates to merge them into one station
              draft.
            </p>
          </div>
        )}

        {toolMode === "group" && (
          <div className="curation-tool-body">
            <div className="curation-tool-body__actions">
              <button
                id="createGroupActionBtn"
                type="button"
                className="curation-btn"
                onClick={onCreateGroup}
                disabled={!canGroup}
              >
                Create group
              </button>
              <button
                id="groupEditorActionBtn"
                type="button"
                className="curation-btn curation-btn--secondary"
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
                  className="curation-btn curation-btn--secondary"
                  onClick={onSplitFocused}
                >
                  Split group
                </button>
              )}
            </div>
            <p className="curation-muted curation-tiny">
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

function ExpandablePanel({ id, title, children, defaultOpen = false }) {
  return (
    <details id={id} className="curation-expandable" open={defaultOpen}>
      <summary className="curation-expandable__summary">{title}</summary>
      <div className="curation-expandable__body">{children}</div>
    </details>
  );
}

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

  return (
    <div id="draftTabPanel" className="curation-draft-stack">
      {focusedItem?.kind === "raw" && (
        <div className="curation-edit-panel">
          <label className="curation-tiny" htmlFor="rawRenameInput">
            Rename candidate
          </label>
          <input
            id="rawRenameInput"
            type="text"
            value={
              getRenameValue(workspace, focusedRef) || focusedItem.display_name
            }
            onChange={(event) => onRenameRef(focusedRef, event.target.value)}
          />
        </div>
      )}

      {focusedMerge && (
        <div className="curation-edit-panel">
          <label className="curation-tiny" htmlFor="mergeRenameInput">
            Rename merged entity
          </label>
          <input
            id="mergeRenameInput"
            type="text"
            value={focusedMerge.display_name}
            onChange={(event) =>
              onRenameComposite(focusedRef, event.target.value)
            }
          />
          <div className="curation-rail-card__members">
            {focusedMerge.member_refs.map((ref) => (
              <span key={ref} className="curation-tag">
                {resolveDisplayNameForRef(ref, workspace, candidateMap)}
              </span>
            ))}
          </div>
        </div>
      )}

      {focusedGroup && (
        <div id="groupEditorPanel" className="curation-group-editor">
          <div className="curation-edit-panel">
            <label className="curation-tiny" htmlFor="groupRenameInput">
              Group name
            </label>
            <input
              id="groupRenameInput"
              type="text"
              value={focusedGroup.display_name}
              onChange={(event) =>
                onRenameComposite(focusedRef, event.target.value)
              }
            />
          </div>

          <div className="curation-group-editor__section">
            <div className="curation-group-editor__heading">Internal nodes</div>
            <div className="curation-group-node-list">
              {focusedGroup.internal_nodes.map((node) => (
                <div key={node.node_id} className="curation-group-node-row">
                  <input
                    type="text"
                    value={node.label}
                    onChange={(event) =>
                      onUpdateGroupNodeLabel(
                        focusedGroup.entity_id,
                        node.node_id,
                        event.target.value,
                      )
                    }
                  />
                  <span className="curation-muted curation-tiny">
                    {resolveDisplayNameForRef(
                      node.source_ref,
                      workspace,
                      candidateMap,
                    )}
                  </span>
                  <button
                    type="button"
                    className="curation-btn curation-btn--danger curation-tiny"
                    onClick={() =>
                      onRemoveGroupMember(
                        focusedGroup.entity_id,
                        node.source_ref,
                      )
                    }
                  >
                    Remove member
                  </button>
                </div>
              ))}
            </div>
          </div>

          <div className="curation-group-editor__section">
            <div className="curation-group-editor__heading">
              Transfer matrix
            </div>
            <div id="groupTransferMatrix" className="curation-transfer-matrix">
              {focusedGroup.transfer_matrix.map((row) => (
                <div
                  key={`${row.from_node_id}-${row.to_node_id}`}
                  className="curation-transfer-matrix__row"
                >
                  <span>
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
                  <span className="curation-muted curation-tiny">sec</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

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
    <div id="evidenceTabPanel" className="curation-tab-panel">
      <div className="curation-evidence-summary">
        {["supporting", "warning", "missing", "informational"].map((status) => (
          <span
            key={status}
            className={`curation-status-pill curation-status-pill--${status}`}
          >
            {formatEvidenceStatusLabel(status)}{" "}
            {getSummaryCounts(clusterDetail?.evidence_summary, status)}
          </span>
        ))}
      </div>
      {typeCounts.length > 0 && (
        <div className="curation-context-chips">
          {typeCounts.slice(0, 6).map((entry) => (
            <span key={entry.type} className="curation-tag">
              {formatEvidenceTypeLabel(entry.type)} {entry.count}
            </span>
          ))}
        </div>
      )}
      {pairSummaries.length > 0 && (
        <div className="curation-pair-summary-list">
          {pairSummaries.slice(0, 8).map((row) => (
            <div
              key={`${row.source_global_station_id}-${row.target_global_station_id}`}
              className="curation-pair-summary"
            >
              <div className="curation-pair-summary__header">
                <strong>
                  {row.source_global_station_id} ↔{" "}
                  {row.target_global_station_id}
                </strong>
                <span>{row.summary || "Evidence summary"}</span>
              </div>
              <div className="curation-pair-summary__metrics">
                <span>support {row.supporting_count || 0}</span>
                <span>warn {row.warning_count || 0}</span>
                <span>missing {row.missing_count || 0}</span>
                <span>context {row.informational_count || 0}</span>
                <span>
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
        <p className="curation-muted">No evidence for the current focus.</p>
      ) : (
        <div id="evidenceList" className="curation-evidence-list">
          {evidenceRows.map((row) => (
            <div
              key={`${row.evidence_type}-${row.source_global_station_id}-${row.target_global_station_id}-${row.score ?? "na"}`}
              className="curation-evidence-row"
            >
              <div className="curation-evidence-row__top">
                <strong>{formatEvidenceTypeLabel(row.evidence_type)}</strong>
                <span
                  className={`curation-status-pill curation-status-pill--${row.status || "informational"}`}
                >
                  {formatEvidenceStatusLabel(row.status)}
                </span>
              </div>
              <div className="curation-evidence-row__meta">
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
                <div className="curation-muted curation-tiny">
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

function HistoryTab({ clusterDetail }) {
  return (
    <div id="historyTabPanel" className="curation-tab-panel">
      <div className="curation-history-list">
        {(clusterDetail?.edit_history || []).map((row, index) => (
          <div
            key={`${row.event_type}-${row.created_at}-${index}`}
            className="curation-history-row"
          >
            <strong>{row.event_type}</strong> · {row.requested_by} ·{" "}
            {row.created_at}
          </div>
        ))}
      </div>
    </div>
  );
}

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
    <div className="curation-layout">
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

      <aside className="curation-rail">
        <div className="curation-candidates-header">
          <div>
            <h4>Candidate Rail</h4>
            <p id="selectionSummary" className="curation-muted curation-tiny">
              {uiState.selectedRefs.size === 0
                ? "No items selected."
                : `Selected: ${uiState.selectedRefs.size} item(s).`}
            </p>
          </div>
          <div className="curation-candidates-actions">
            <button
              id="candidateSelectAllBtn"
              type="button"
              className="curation-btn curation-btn--secondary curation-tiny"
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
              className="curation-btn curation-btn--secondary curation-tiny"
              onClick={() => dispatch({ type: "clear_selection" })}
            >
              Clear
            </button>
          </div>
        </div>

        <div className="curation-candidates-scroll">
          {railItems.length === 0 && (
            <p className="curation-muted" style={{ padding: "12px" }}>
              No cluster selected.
            </p>
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

      <main className="curation-workspace">
        <section className="curation-map-shell">
          <div className="curation-map-toolbar">
            <span
              id="curationMapStatus"
              className="curation-muted curation-tiny"
            >
              {mapItems.length > 0
                ? `${plottedMapItems.length}/${mapItems.length} workspace items plotted · v${workspaceVersion || 0}.`
                : "Select a cluster."}
            </span>
            <div className="curation-map-mode-toggle">
              <button
                id="mapModeDefaultBtn"
                type="button"
                className="curation-btn curation-btn--secondary curation-tiny"
                aria-pressed={uiState.mapMode === "default"}
                disabled={uiState.mapMode === "default"}
                onClick={() => dispatch({ type: "map_mode", mode: "default" })}
              >
                Map
              </button>
              <button
                id="mapModeSatelliteBtn"
                type="button"
                className="curation-btn curation-btn--secondary curation-tiny"
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

        <section className="curation-expandable-stack">
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
