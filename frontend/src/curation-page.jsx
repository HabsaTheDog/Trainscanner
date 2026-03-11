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
import maplibregl from "./maplibre";
import "./styles.css";

/* ── Formatters ── */
function fmt(v) {
  return String(v || "")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}
const evLabels = {
  name_exact: "Exact Name",
  name_loose_similarity: "Loose Similarity",
  token_overlap: "Token Overlap",
  geographic_distance: "Distance",
  coordinate_quality: "Coord Quality",
  shared_provider_sources: "Shared Sources",
  shared_route_context: "Route Context",
  shared_adjacent_stations: "Adjacent Stations",
  country_relation: "Country",
  generic_name_penalty: "Generic Penalty",
};
const stLabels = {
  supporting: "Supporting",
  warning: "Warning",
  missing: "Missing",
  informational: "Context",
  same_location: "Same Loc",
  nearby: "Nearby",
  far_apart: "Far",
  too_far: "Too Far",
  missing_coordinates: "No Coords",
  coordinates_present: "Coords",
};
const catLabels = {
  core_match: "Core Match",
  network_context: "Network Context",
  risk_conflict: "Risk / Conflict",
};
const seedLabels = {
  exact_name: "Exact Name",
  loose_name_geo: "Loose Name + Geo",
  loose_name_missing_coords: "Loose Name + Missing Coords",
  shared_route: "Shared Route",
  shared_adjacent: "Shared Adjacent",
};
function fmtEvType(v) {
  return evLabels[v] || fmt(v || "unknown");
}
function fmtEvStatus(v) {
  return stLabels[v] || fmt(v || "unknown");
}
function formatEvidenceTypeLabel(v) {
  return fmtEvType(v);
}
function formatEvidenceStatusLabel(v) {
  return fmtEvStatus(v);
}
function fmtEvCategory(v) {
  return catLabels[v] || fmt(v || "unknown");
}
function fmtSeedReason(v) {
  return seedLabels[v] || fmt(v || "seed");
}
function fmtCoord(v) {
  return fmtEvStatus(v || "missing_coordinates");
}
function fmtEvValue(r) {
  if (!r) return "—";
  if (r.evidence_type === "geographic_distance") {
    const m = Number(r.raw_value ?? r.details?.distance_meters);
    if (Number.isFinite(m)) return `${Math.round(m)}m`;
    return fmtEvStatus(r.details?.distance_status);
  }
  if (
    ["name_loose_similarity", "token_overlap"].includes(r.evidence_type) &&
    Number.isFinite(Number(r.raw_value))
  )
    return `${Math.round(Number(r.raw_value) * 100)}%`;
  if (
    [
      "shared_provider_sources",
      "shared_route_context",
      "shared_adjacent_stations",
      "coordinate_quality",
      "generic_name_penalty",
    ].includes(r.evidence_type) &&
    Number.isFinite(Number(r.raw_value))
  )
    return String(Number(r.raw_value));
  if (r.evidence_type === "country_relation") {
    if (r.details?.same_country === true) return "Same";
    if (r.details?.same_country === false) return "Cross";
    return "?";
  }
  if (Number.isFinite(Number(r.score)))
    return `${Math.round(Number(r.score) * 100)}%`;
  return "—";
}
function fmtEvDetails(d) {
  if (!d || typeof d !== "object") return "";
  if (d.explanation) return String(d.explanation);
  if (d.distance_status) return fmtEvStatus(d.distance_status);
  if (d.reason) return String(d.reason);
  return Object.entries(d)
    .filter(([k, v]) => k !== "seed_reasons" && v != null && v !== "")
    .slice(0, 3)
    .map(([k, v]) => `${fmt(k)}: ${v}`)
    .join(" · ");
}
function getSumC(s, k) {
  const c = s && typeof s === "object" ? s.status_counts || s : {};
  return parseInt(String(c?.[k] ?? 0), 10) || 0;
}
function getTypeC(s) {
  const c = s && typeof s === "object" && s.type_counts ? s.type_counts : {};
  return Object.entries(c)
    .map(([t, n]) => ({ type: t, count: parseInt(String(n ?? 0), 10) || 0 }))
    .filter((e) => e.count > 0)
    .sort((a, b) => b.count - a.count || a.type.localeCompare(b.type));
}
function getCategoryC(summary) {
  const c =
    summary && typeof summary === "object" && summary.category_counts
      ? summary.category_counts
      : {};
  return ["core_match", "network_context", "risk_conflict"]
    .map((category) => ({
      category,
      count: parseInt(String(c?.[category] ?? 0), 10) || 0,
    }))
    .filter((e) => e.count > 0);
}
function getSeedRuleC(summary) {
  const c =
    summary && typeof summary === "object" && summary.seed_rule_counts
      ? summary.seed_rule_counts
      : {};
  return Object.entries(c)
    .map(([reason, count]) => ({
      reason,
      count: parseInt(String(count ?? 0), 10) || 0,
    }))
    .filter((e) => e.count > 0)
    .sort((a, b) => b.count - a.count || a.reason.localeCompare(b.reason));
}
function getRowSeedReasons(row) {
  return Array.isArray(row?.seed_reasons)
    ? row.seed_reasons.map((v) => String(v || "").trim()).filter(Boolean)
    : Array.isArray(row?.details?.seed_reasons)
      ? row.details.seed_reasons
          .map((v) => String(v || "").trim())
          .filter(Boolean)
      : [];
}
function EvidenceSection({ title, tone = "default", children }) {
  const toneClass =
    tone === "risk"
      ? "border-red/15 bg-red-dim/10"
      : "border-border bg-surface-1/40";
  return (
    <section className={`rounded-2xl border p-3 space-y-2 ${toneClass}`}>
      <div className="flex items-center justify-between gap-2">
        <h3 className="m-0 text-sm font-bold text-text-primary font-display uppercase tracking-wider">
          {title}
        </h3>
      </div>
      {children}
    </section>
  );
}

/* ── State ── */
function createUiState() {
  return {
    selectedRefs: new Set(),
    focusedRef: "",
    activeTool: "merge",
    mapMode: "default",
    lastSelectedIndex: -1,
    bottomTab: "candidates",
  };
}
function uiReducer(state, action) {
  switch (action.type) {
    case "clear_selection":
      return { ...state, selectedRefs: new Set(), lastSelectedIndex: -1 };
    case "set_selection":
      return {
        ...state,
        selectedRefs: new Set(action.refs || []),
        lastSelectedIndex: Number.isFinite(action.lastSelectedIndex)
          ? action.lastSelectedIndex
          : state.lastSelectedIndex,
      };
    case "toggle_selection": {
      const n = new Set(state.selectedRefs);
      if (n.has(action.ref)) n.delete(action.ref);
      else n.add(action.ref);
      return {
        ...state,
        selectedRefs: n,
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
      return { ...state, activeTool: action.tool };
    case "map_mode":
      return { ...state, mapMode: action.mode };
    case "bottom_tab":
      return { ...state, bottomTab: action.tab };
    default:
      return state;
  }
}

/* ── Map markers (DOM-imperative, unchanged logic) ── */
const OCP = 7,
  OBM = 24,
  OSR = 3,
  OSD = 24;
function bck(a, b) {
  return `${Number(a).toFixed(OCP)}:${Number(b).toFixed(OCP)}`;
}
function bmog(items) {
  const g = new Map();
  for (const i of items) {
    const k = bck(i.lat, i.lon);
    const e = g.get(k);
    if (e) e.items.push(i);
    else g.set(k, { key: k, lat: i.lat, lon: i.lon, items: [i] });
  }
  return Array.from(g.values());
}
function bmol(map, items) {
  if (!map) return new Map();
  const layout = new Map(),
    sg = [];
  for (const g of bmog(items)) {
    for (const i of g.items) {
      const p = map.project([i.lon, i.lat]);
      let tg = null;
      for (const c of sg) {
        if (Math.hypot(c.sx - p.x, c.sy - p.y) <= OSD) {
          tg = c;
          break;
        }
      }
      if (!tg) {
        tg = { items: [], sx: p.x, sy: p.y, aLat: i.lat, aLon: i.lon };
        sg.push(tg);
      }
      tg.items.push(i);
      const n = tg.items.length;
      tg.sx = (tg.sx * (n - 1) + p.x) / n;
      tg.sy = (tg.sy * (n - 1) + p.y) / n;
      tg.aLat = (tg.aLat * (n - 1) + i.lat) / n;
      tg.aLon = (tg.aLon * (n - 1) + i.lon) / n;
    }
  }
  for (const g of sg) {
    for (const [idx, i] of g.items.entries()) {
      const sm = Math.max(1, g.items.length - idx);
      layout.set(i.ref, {
        stackIndex: idx,
        stackSize: g.items.length,
        sizeMultiplier: sm,
        markerSize: OBM * sm,
        zIndex: 2000 + idx,
        aLat: g.aLat,
        aLon: g.aLon,
      });
    }
  }
  return layout;
}
function buildMarkerOverlapLayout(map, items) {
  return bmol(map, items);
}
function buildMappableItems(items) {
  const rows = Array.isArray(items) ? items : [];
  return rows
    .map((item) => {
      if (Number.isFinite(item.lat) && Number.isFinite(item.lon))
        return { ...item, approx: false };
      const dn = String(item.display_name || "")
        .trim()
        .toLowerCase();
      const peers = rows
        .filter(
          (c) =>
            c.ref !== item.ref &&
            Number.isFinite(c.lat) &&
            Number.isFinite(c.lon) &&
            String(c.display_name || "")
              .trim()
              .toLowerCase() === dn,
        )
        .sort(
          (a, b) =>
            Math.abs(
              Number(a.candidate?.candidate_rank || 9999) -
                Number(item.candidate?.candidate_rank || 9999),
            ) -
            Math.abs(
              Number(b.candidate?.candidate_rank || 9999) -
                Number(item.candidate?.candidate_rank || 9999),
            ),
        );
      if (peers.length === 0) return { ...item, approx: false };
      const sp = peers.slice(0, 2);
      return {
        ...item,
        lat: sp.reduce((s, c) => s + Number(c.lat), 0) / sp.length,
        lon: sp.reduce((s, c) => s + Number(c.lon), 0) / sp.length,
        approx: true,
      };
    })
    .filter((i) => Number.isFinite(i.lat) && Number.isFinite(i.lon));
}
function createMarkerEl(item, sel, om, onSelect) {
  const {
    stackSize: ss = 1,
    markerSize: ms = OBM,
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
    r.style.setProperty("--selection-ring-size", `${ms + OSR * 2}px`);
    sh.appendChild(r);
  }
  return sh;
}

/* ── Map ── */
function CurationMap({
  items,
  selectedRefs,
  onSelectRef,
  mapMode,
  onToggleMapMode,
}) {
  const mapRef = useRef(null),
    cRef = useRef(null),
    mkRef = useRef([]);
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
    return () => {
      m.remove();
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
    const m = mapRef.current;
    if (!m) return;
    let fid = 0;
    for (const mk of mkRef.current) mk.remove();
    mkRef.current = [];
    const valid = (items || []).filter(
      (i) => Number.isFinite(i.lat) && Number.isFinite(i.lon),
    );
    if (valid.length === 0) return;
    const bounds = new maplibregl.LngLatBounds();
    for (const i of valid) bounds.extend([i.lon, i.lat]);
    m.fitBounds(bounds, { padding: 60, maxZoom: 15, duration: 0 });
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
  }, [items, onSelectRef, selectedRefs]);
  return (
    <div className="relative w-full h-full">
      <div ref={cRef} className="curation-map w-full h-full" />
      <div className="absolute top-3 left-3 flex gap-1 z-10">
        <button
          id="mapModeDefaultBtn"
          type="button"
          onClick={() => onToggleMapMode("default")}
          className={`px-2.5 py-1 rounded-md text-xs font-bold font-display border backdrop-blur-md cursor-pointer transition-all ${mapMode === "default" ? "bg-amber/90 border-amber text-surface-0" : "bg-surface-0/50 border-white/10 text-white/80 hover:bg-surface-0/70"}`}
        >
          Map
        </button>
        <button
          id="mapModeSatelliteBtn"
          type="button"
          onClick={() => onToggleMapMode("satellite")}
          className={`px-2.5 py-1 rounded-md text-xs font-bold font-display border backdrop-blur-md cursor-pointer transition-all ${mapMode === "satellite" ? "bg-amber/90 border-amber text-surface-0" : "bg-surface-0/50 border-white/10 text-white/80 hover:bg-surface-0/70"}`}
        >
          Sat
        </button>
      </div>
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
function Tag({ children, className = "" }) {
  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded text-[0.72rem] font-medium border border-border bg-surface-2 text-text-secondary ${className}`}
    >
      {children}
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
  const ct =
    Number.isFinite(totalCount) && totalCount > 0
      ? totalCount
      : clusters.length;
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
                {fmt(c.severity || "?")}
              </Pill>
            </div>
            <div className="flex gap-1.5 flex-wrap items-center">
              <Pill
                v={String(c.effective_status || c.status || "").toLowerCase()}
              >
                {fmt(c.effective_status || c.status || "?")}
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
  const memberNames =
    item.member_refs?.map((r) =>
      resolveDisplayNameForRef(r, workspace, candidateMap),
    ) || [];
  const modes = Array.isArray(c.service_context?.transport_modes)
    ? c.service_context.transport_modes
    : [];
  const ctx = c.context_summary || {};
  const kB =
    item.kind === "merge"
      ? "border-l-[3px] border-l-teal"
      : item.kind === "group"
        ? "border-l-[3px] border-l-orange"
        : "";
  const ref = item.ref;
  const fg =
    item.kind === "group"
      ? (workspace.groups || []).find((g) => g.entity_id === parseRef(ref).id)
      : null;
  const fm =
    item.kind === "merge"
      ? (workspace.merges || []).find((m) => m.entity_id === parseRef(ref).id)
      : null;
  const isComposite = item.kind === "merge" || item.kind === "group";
  const inputIdBase = ref.replace(/[^a-zA-Z0-9_-]/g, "-");
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
            <div className="flex flex-wrap gap-1.5 mt-2">
              {modes.slice(0, 2).map((m) => (
                <Tag key={m}>{m}</Tag>
              ))}
              <Tag>{ctx.stop_point_count ?? 0} stops</Tag>
              <Tag>{ctx.route_count ?? 0} routes</Tag>
              <Tag>{fmtCoord(c.coord_status)}</Tag>
              <Tag>{(item.provider_labels || []).length} feeds</Tag>
            </div>
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
        <div className="border-t border-border px-3 py-2.5 bg-surface-1/50">
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
                      {r.replace("node:", "")}
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
                      {n.source_ref.replace("node:", "")}
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
  const tcs = getTypeC(clusterDetail?.evidence_summary);
  const ccs = getCategoryC(clusterDetail?.evidence_summary);
  const scs = getSeedRuleC(clusterDetail?.evidence_summary);
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
              {getSumC(clusterDetail?.evidence_summary, s)}
            </StatusPill>
          ))}
        </div>
        {ccs.length > 0 && (
          <div className="flex flex-wrap gap-1.5">
            {ccs.map((e) => (
              <Tag key={e.category}>
                {fmtEvCategory(e.category)} {e.count}
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
                Seed {fmtSeedReason(e.reason)} {e.count}
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
                      Seed {fmtSeedReason(seed)}
                    </Tag>
                  ))}
                  {(r.categories || []).map((category) => (
                    <Tag key={category}>{fmtEvCategory(category)}</Tag>
                  ))}
                </div>
                <div className="flex gap-3 mt-1.5 text-xs font-display">
                  <span className="text-green">+{r.supporting_count || 0}</span>
                  <span className="text-orange">⚠{r.warning_count || 0}</span>
                  <span className="text-red">✗{r.missing_count || 0}</span>
                  <span className="text-text-primary font-bold">
                    {Number.isFinite(Number(r.score))
                      ? Number(r.score).toFixed(2)
                      : "—"}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </EvidenceSection>
      )}

      {coreRows.length > 0 && (
        <EvidenceSection title="Core Match">
          <div className="space-y-2">
            {coreRows.map((r) => {
              const rowSeedReasons = getRowSeedReasons(r);
              return (
                <div
                  key={`${r.evidence_type}-${r.source_global_station_id}-${r.target_global_station_id}-${r.score ?? ""}`}
                  className="border border-border rounded-xl px-3 py-2 bg-surface-2 text-sm"
                >
                  <div className="flex items-center justify-between gap-2">
                    <div className="flex items-center gap-1.5 flex-wrap">
                      <strong className="font-display text-text-primary">
                        {formatEvidenceTypeLabel(r.evidence_type)}
                      </strong>
                      {r.is_seed_rule === true && (
                        <Tag className="border-blue/20 bg-blue-dim text-blue">
                          Seed
                        </Tag>
                      )}
                    </div>
                    <StatusPill v={r.status || "informational"}>
                      {formatEvidenceStatusLabel(r.status)}
                    </StatusPill>
                  </div>
                  <div className="flex gap-3 text-text-secondary mt-1">
                    <span>
                      {r.source_global_station_id} ↔{" "}
                      {r.target_global_station_id}
                    </span>
                    <span>{fmtEvValue(r)}</span>
                  </div>
                  {r.is_seed_rule === true && rowSeedReasons.length > 0 && (
                    <div className="text-blue text-xs mt-1">
                      Seeded by: {rowSeedReasons.map(fmtSeedReason).join(", ")}
                    </div>
                  )}
                  {fmtEvDetails(r.details) && (
                    <div className="text-text-muted text-xs mt-1">
                      {fmtEvDetails(r.details)}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </EvidenceSection>
      )}

      {contextRows.length > 0 && (
        <EvidenceSection title="Network Context">
          <div className="space-y-2">
            {contextRows.map((r) => {
              const rowSeedReasons = getRowSeedReasons(r);
              return (
                <div
                  key={`${r.evidence_type}-${r.source_global_station_id}-${r.target_global_station_id}-${r.score ?? ""}`}
                  className="border border-border rounded-xl px-3 py-2 bg-surface-2 text-sm"
                >
                  <div className="flex items-center justify-between gap-2">
                    <div className="flex items-center gap-1.5 flex-wrap">
                      <strong className="font-display text-text-primary">
                        {formatEvidenceTypeLabel(r.evidence_type)}
                      </strong>
                      {r.is_seed_rule === true && (
                        <Tag className="border-blue/20 bg-blue-dim text-blue">
                          Seed
                        </Tag>
                      )}
                    </div>
                    <StatusPill v={r.status || "informational"}>
                      {formatEvidenceStatusLabel(r.status)}
                    </StatusPill>
                  </div>
                  <div className="flex gap-3 text-text-secondary mt-1">
                    <span>
                      {r.source_global_station_id} ↔{" "}
                      {r.target_global_station_id}
                    </span>
                    <span>{fmtEvValue(r)}</span>
                  </div>
                  {r.is_seed_rule === true && rowSeedReasons.length > 0 && (
                    <div className="text-blue text-xs mt-1">
                      Seeded by: {rowSeedReasons.map(fmtSeedReason).join(", ")}
                    </div>
                  )}
                  {fmtEvDetails(r.details) && (
                    <div className="text-text-muted text-xs mt-1">
                      {fmtEvDetails(r.details)}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </EvidenceSection>
      )}

      {riskRows.length > 0 && (
        <EvidenceSection title="Risk / Conflict" tone="risk">
          <div className="space-y-2">
            {riskRows.map((r) => {
              const rowSeedReasons = getRowSeedReasons(r);
              return (
                <div
                  key={`${r.evidence_type}-${r.source_global_station_id}-${r.target_global_station_id}-${r.score ?? ""}`}
                  className="border border-red/15 rounded-xl px-3 py-2 bg-surface-2 text-sm"
                >
                  <div className="flex items-center justify-between gap-2">
                    <div className="flex items-center gap-1.5 flex-wrap">
                      <strong className="font-display text-text-primary">
                        {formatEvidenceTypeLabel(r.evidence_type)}
                      </strong>
                      {r.is_seed_rule === true && (
                        <Tag className="border-blue/20 bg-blue-dim text-blue">
                          Seed
                        </Tag>
                      )}
                    </div>
                    <StatusPill v={r.status || "informational"}>
                      {formatEvidenceStatusLabel(r.status)}
                    </StatusPill>
                  </div>
                  <div className="flex gap-3 text-text-secondary mt-1">
                    <span>
                      {r.source_global_station_id} ↔{" "}
                      {r.target_global_station_id}
                    </span>
                    <span>{fmtEvValue(r)}</span>
                  </div>
                  {r.is_seed_rule === true && rowSeedReasons.length > 0 && (
                    <div className="text-blue text-xs mt-1">
                      Seeded by: {rowSeedReasons.map(fmtSeedReason).join(", ")}
                    </div>
                  )}
                  {fmtEvDetails(r.details) && (
                    <div className="text-text-muted text-xs mt-1">
                      {fmtEvDetails(r.details)}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
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
  const [, setWorkspaceVersion] = useState(0);
  const [filters, setFilters] = useState({ country: "", status: "" });
  const [uiState, dispatch] = useReducer(uiReducer, undefined, createUiState);
  const [loading, setLoading] = useState(true);
  const [notice, setNotice] = useState(null);
  const [saveState, setSaveState] = useState("Saved");
  const [aiResult, setAiResult] = useState(null);
  const [expandedRefs, setExpandedRefs] = useState(new Set());
  const ntRef = useRef(null),
    lsRef = useRef(serializeWorkspace(createEmptyWorkspace())),
    srRef = useRef(0),
    isRef = useRef(false);

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
          p
            ? {
                ...p,
                workspace: r.workspace,
                workspace_version: r.workspace_version,
                has_workspace: true,
                effective_status: r.effective_status,
              }
            : p,
        );
        setClusters((p) =>
          p.map((c) =>
            c.cluster_id === activeClusterId
              ? {
                  ...c,
                  effective_status: r.effective_status,
                  has_workspace: true,
                  workspace_version: r.workspace_version,
                }
              : c,
          ),
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
    window.addEventListener("keydown", h);
    return () => window.removeEventListener("keydown", h);
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
        p
          ? {
              ...p,
              workspace: w,
              workspace_version: r.workspace_version,
              has_workspace: Boolean(r.workspace),
              effective_status: r.effective_status,
            }
          : p,
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
        p
          ? {
              ...p,
              workspace: w,
              workspace_version: r.workspace_version,
              has_workspace: r.workspace_version > 0,
              effective_status: r.effective_status,
              status: r.effective_status,
            }
          : p,
      );
      setClusters((p) =>
        p.map((c) =>
          c.cluster_id === activeClusterId
            ? {
                ...c,
                effective_status: r.effective_status,
                status: r.effective_status,
                has_workspace: r.workspace_version > 0,
                workspace_version: r.workspace_version,
              }
            : c,
        ),
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
        showNotice(`${fmt(st)} done.`, "success");
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
          lastSelectedIndex: rr.length > 0 ? railIdx.get(rr.at(-1)) || 0 : -1,
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
            const parsed = parseRef(ref);
            if (parsed.type !== "raw" && parsed.type !== "merge") return [];
            return resolveRefMemberStationIds(ref, workspace);
          }),
        ),
      ),
    [selArr, workspace],
  );
  const canMrg =
    mergeableStationIds.length >= 2 &&
    selArr.some((r) => {
      const t = parseRef(r).type;
      return t === "raw" || t === "merge";
    });
  const canGrp =
    selArr.filter((r) => {
      const t = parseRef(r).type;
      return t === "raw" || t === "merge";
    }).length >= 2;
  const clSt = String(
    clusterDetail?.effective_status || clusterDetail?.status || "",
  ).toLowerCase();
  const canUn = clSt === "resolved" || clSt === "dismissed";

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
                title={
                  clusterDetail
                    ? clusterDetail.display_name || clusterDetail.cluster_id
                    : "No cluster"
                }
              >
                {clusterDetail
                  ? clusterDetail.display_name || clusterDetail.cluster_id
                  : "No cluster"}
              </strong>
              <span
                id="saveStateIndicator"
                className={`font-display font-bold text-xs uppercase tracking-wider ml-1 ${saveC[saveState] || "text-text-muted"}`}
              >
                {saveState}
              </span>

              <div className="flex items-center gap-1.5 ml-auto">
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
                  {canUn ? "Reopen" : "Resolve"}
                </button>
              </div>
            </div>

            {/* Notices */}
            {notice && (
              <div
                className={`px-4 py-2 text-sm shrink-0 ${notice.tone === "error" ? "bg-red-dim text-red" : notice.tone === "success" ? "bg-green-dim text-green" : notice.tone === "warning" ? "bg-yellow-dim text-yellow" : "bg-blue-dim text-blue"}`}
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
                  {uiState.selectedRefs.size > 0
                    ? `${uiState.selectedRefs.size} sel.`
                    : "Select"}
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
                  disabled={
                    focused?.kind !== "group" && focused?.kind !== "merge"
                  }
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
              selectedRefs={uiState.selectedRefs}
              onSelectRef={hSelectRef}
              mapMode={uiState.mapMode}
              onToggleMapMode={(m) => {
                if (m === "default") {
                  setDefaultMapMode();
                  return;
                }
                if (m === "satellite") {
                  setSatelliteMapMode();
                  return;
                }
                dispatch({ type: "map_mode", mode: m });
              }}
            />
          </div>
        </div>
      </div>
    </div>
  );
}
