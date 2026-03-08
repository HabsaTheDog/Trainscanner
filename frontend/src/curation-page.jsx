import maplibregl from "maplibre-gl";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  fetchClusterDetail as apiFetchClusterDetail,
  fetchClusters as apiFetchClusters,
  buildResolvePayload,
  createDraftId,
  createEmptyDraftState,
  fetchCuratedProjection,
  inferCandidateCategory,
  pairKey,
  parseRef,
  requestAiScore,
  resolveDefaultMapStyle,
  submitDecision,
  toCandidateRef,
  toGroupRef,
} from "./curation-page-runtime";

// ─── Map Component ────────────────────────────────────────────────────────────

function CurationMap({ candidates, selectedIds, onToggleCandidate }) {
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

    // Clear old markers
    for (const m of markersRef.current) m.remove();
    markersRef.current = [];

    const valid = (candidates || []).filter(
      (c) => Number.isFinite(c.lat) && Number.isFinite(c.lon),
    );
    if (valid.length === 0) return;

    const bounds = new maplibregl.LngLatBounds();
    for (const c of valid) {
      const el = document.createElement("div");
      const isSelected = selectedIds.has(c.global_station_id);
      el.className = `curation-marker ${isSelected ? "curation-marker--selected" : ""}`;
      el.title = c.display_name || c.global_station_id;
      el.addEventListener("click", (e) => {
        e.stopPropagation();
        onToggleCandidate(c.global_station_id);
      });

      const marker = new maplibregl.Marker({ element: el, anchor: "center" })
        .setLngLat([c.lon, c.lat])
        .addTo(map);
      markersRef.current.push(marker);
      bounds.extend([c.lon, c.lat]);
    }
    map.fitBounds(bounds, { padding: 60, maxZoom: 15 });
  }, [candidates, selectedIds, onToggleCandidate]);

  return <div ref={mapContainerRef} className="curation-map" />;
}

// ─── Left Sidebar ─────────────────────────────────────────────────────────────

function ClusterSidebar({
  clusters,
  totalCount,
  listLimit,
  activeClusterId,
  filters,
  onFilterChange,
  onSelectCluster,
  onRefresh,
  loading,
}) {
  const showingSubset =
    totalCount > clusters.length &&
    Number.isFinite(listLimit) &&
    clusters.length >= listLimit;

  return (
    <aside className="curation-sidebar">
      <div className="curation-sidebar__header">
        <h2 className="curation-sidebar__title">Station Curation</h2>
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
            onChange={(e) =>
              onFilterChange({ ...filters, country: e.target.value })
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
            onChange={(e) =>
              onFilterChange({ ...filters, status: e.target.value })
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
        {loading
          ? "Loading..."
          : showingSubset
            ? `${totalCount} matching clusters · showing ${clusters.length}`
            : `${totalCount} matching clusters`}
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
            <span className="curation-cluster-item__name">
              {cluster.display_name || cluster.cluster_id}
            </span>
            <span className="curation-cluster-item__detail">
              {cluster.country} · {cluster.severity} · {cluster.status}
            </span>
            <span className="curation-cluster-item__detail">
              {cluster.candidate_count} candidates · {cluster.issue_count}{" "}
              issues · {cluster.scope_tag}
            </span>
          </button>
        ))}
      </div>
    </aside>
  );
}

// ─── Candidate Card ───────────────────────────────────────────────────────────

function CandidateCard({
  candidate,
  selected,
  renameByRef,
  onToggle,
  rawCandidates,
}) {
  const id = candidate.global_station_id;
  const renamed = renameByRef?.[toCandidateRef(id)] || "";
  const displayName = renamed || candidate.display_name || id;
  const aliases = Array.isArray(candidate.aliases)
    ? candidate.aliases.filter(Boolean)
    : [];
  const providers = Array.isArray(candidate.provider_labels)
    ? candidate.provider_labels.filter(Boolean)
    : [];
  const lines = candidate.service_context?.lines?.slice(0, 8) || [];
  const isMerged =
    candidate.is_curated &&
    Array.isArray(candidate.members) &&
    candidate.members.length >= 2;

  return (
    <div
      className={`curation-candidate ${selected ? "curation-candidate--selected" : ""} ${isMerged ? "curation-candidate--merged" : ""}`}
    >
      <div className="curation-candidate__header">
        <label className="curation-candidate__select">
          <input
            type="checkbox"
            checked={selected}
            data-station-id={id}
            onChange={() => onToggle(id)}
          />
          <span>
            <strong>{displayName}</strong>
            <span className="curation-muted curation-tiny"> {id}</span>
          </span>
        </label>
        <span className="curation-candidate__rank">
          {isMerged ? (
            <span className="curation-tag curation-tag--merged">
              merged · {candidate.members.length} members
            </span>
          ) : (
            `#${candidate.candidate_rank}`
          )}
        </span>
      </div>
      <div className="curation-candidate__tags">
        {providers.length > 0 && (
          <span className="curation-tag">feeds: {providers.join(", ")}</span>
        )}
        {lines.length > 0 && (
          <span className="curation-tag">lines: {lines.join(", ")}</span>
        )}
        {candidate.segment_context?.segment_type && (
          <span className="curation-tag">
            type: {candidate.segment_context.segment_type}
          </span>
        )}
      </div>
      {isMerged && (
        <details
          className="curation-muted curation-tiny"
          style={{ marginTop: 4 }}
        >
          <summary style={{ cursor: "pointer" }}>
            Members ({candidate.members.length})
          </summary>
          <ul style={{ margin: "4px 0 0 16px", padding: 0 }}>
            {candidate.members.map((m) => {
              const orig = rawCandidates?.find(
                (c) => c.global_station_id === m.global_station_id,
              );
              const providers = orig?.provider_labels?.filter(Boolean) || [];
              const sourceText =
                providers.length > 0
                  ? providers.join(", ")
                  : m.global_station_id.split(":")[0] || "unknown";
              const name = orig?.display_name || m.global_station_id;

              return (
                <li key={m.global_station_id} style={{ marginBottom: 4 }}>
                  <strong>{name}</strong>
                  <span
                    className="curation-muted curation-tiny"
                    style={{ marginLeft: 4 }}
                  >
                    ({m.global_station_id})
                  </span>
                  <span className="curation-tag" style={{ marginLeft: 6 }}>
                    feed: {sourceText}
                  </span>
                  {m.member_role && m.member_role !== "member" ? (
                    <span className="curation-tag" style={{ marginLeft: 6 }}>
                      role: {m.member_role}
                    </span>
                  ) : null}
                </li>
              );
            })}
          </ul>
        </details>
      )}
      {aliases.length > 0 && (
        <p className="curation-muted curation-tiny">
          Aliases: {aliases.join(", ")}
        </p>
      )}
    </div>
  );
}

// ─── Tools Panel ──────────────────────────────────────────────────────────────

function CurationTools({
  activeTool,
  onSetTool,
  draftState,
  setDraftState,
  selectedIds,
  candidates,
  clusterDetail,
  onResolve,
  onAiScore,
  aiResult,
  notice,
}) {
  const selectedCount = selectedIds.size;
  const mergeAvailable = selectedCount >= 2;
  const selectedCuratedItems = [...selectedIds].filter((id) => {
    const c = (candidates || []).find((x) => x.global_station_id === id);
    return c?.is_curated && Array.isArray(c.members) && c.members.length >= 2;
  });
  const splitAvailable = selectedCuratedItems.length > 0;
  const groupAvailable = selectedCount > 0 || draftState.groups.length > 0;

  const handleCreateGroup = () => {
    const refs = Array.from(selectedIds).map(toCandidateRef);
    if (refs.length === 0) return;
    const groupId = createDraftId("grp");
    const firstRef = parseRef(refs[0]);
    const firstCandidate = (candidates || []).find(
      (c) => c.global_station_id === firstRef.id,
    );
    const groupName =
      firstCandidate?.display_name || `Group ${draftState.groups.length + 1}`;
    const sectionType = inferCandidateCategory(firstCandidate);

    setDraftState((prev) => ({
      ...prev,
      groups: [
        ...prev.groups,
        {
          group_id: groupId,
          section_type: sectionType,
          section_name: groupName,
          member_refs: refs,
        },
      ],
      renameByRef: { ...prev.renameByRef, [toGroupRef(groupId)]: groupName },
    }));
  };

  const handleRemoveGroup = (groupId) => {
    setDraftState((prev) => ({
      ...prev,
      groups: prev.groups.filter((g) => g.group_id !== groupId),
    }));
  };

  return (
    <aside className="curation-tools">
      <div className="curation-tools__header">
        <h3>Curation Tools</h3>
        <button
          id="resolveConflictBtn"
          type="button"
          className="curation-btn curation-btn--save"
          onClick={onResolve}
        >
          Apply Decision
        </button>
      </div>

      {notice && (
        <div className={`curation-notice curation-notice--${notice.tone}`}>
          {notice.message}
        </div>
      )}

      <div className="curation-tools__operations">
        <div className="curation-filter-label" style={{ fontWeight: 600 }}>
          Operation
        </div>
        <div className="curation-tool-strip">
          <button
            id="toolMergeBtn"
            type="button"
            className={`curation-tool-btn ${activeTool === "merge" ? "curation-tool-btn--active" : ""}`}
            data-tool="merge"
            onClick={() => onSetTool("merge")}
            disabled={!mergeAvailable}
          >
            Merge
          </button>
          <button
            id="toolSplitBtn"
            type="button"
            className={`curation-tool-btn ${activeTool === "split" ? "curation-tool-btn--active" : ""}`}
            data-tool="split"
            onClick={() => onSetTool("split")}
            disabled={!splitAvailable}
          >
            Split
          </button>
          <button
            id="toolGroupBtn"
            type="button"
            className={`curation-tool-btn ${activeTool === "group" ? "curation-tool-btn--active" : ""}`}
            data-tool="group"
            onClick={() => onSetTool("group")}
            disabled={!groupAvailable}
          >
            Group
          </button>
        </div>
        <p
          className="curation-muted curation-tiny"
          style={{ marginTop: 4, fontStyle: "italic" }}
        >
          {mergeAvailable ? "Merge ✓" : "Merge needs 2+"} ·{" "}
          {splitAvailable ? "Split ✓" : "Split: select merged"} ·{" "}
          {groupAvailable ? "Group ✓" : "Group: select entries"}
        </p>
      </div>

      {/* Merge Panel */}
      {activeTool === "merge" && (
        <div
          id="toolPanelMerge"
          className="curation-edit-panel"
          data-tool-panel="merge"
        >
          <label className="curation-tiny" htmlFor="editMergeRenameInput">
            Merged Name <span style={{ color: "red" }}>*</span>
          </label>
          <input
            id="editMergeRenameInput"
            type="text"
            placeholder="e.g. Central Station"
            value={draftState.renameByRef.__merge_name || ""}
            onChange={(e) =>
              setDraftState((prev) => ({
                ...prev,
                renameByRef: {
                  ...prev.renameByRef,
                  __merge_name: e.target.value,
                },
              }))
            }
          />
        </div>
      )}

      {/* Split Panel */}
      {activeTool === "split" && (
        <div
          id="toolPanelSplit"
          className="curation-edit-panel"
          data-tool-panel="split"
        >
          {splitAvailable ? (
            <>
              <p className="curation-muted curation-tiny">
                Split selected merged candidate(s) back into their individual
                members.
              </p>
              {selectedCuratedItems.map((id) => {
                const c = (candidates || []).find(
                  (x) => x.global_station_id === id,
                );
                if (!c) return null;
                return (
                  <div
                    key={id}
                    className="curation-curated-card"
                    style={{ margin: "4px 0", padding: "4px 8px" }}
                  >
                    <strong>{c.display_name}</strong>
                    <span className="curation-muted curation-tiny">
                      {" "}
                      — {c.members?.length || 0} members will be separated
                    </span>
                  </div>
                );
              })}
            </>
          ) : (
            <p className="curation-muted curation-tiny">
              Select a merged candidate to split it back into individual
              stations.
            </p>
          )}
        </div>
      )}

      {/* Group Panel */}
      {activeTool === "group" && (
        <div
          id="toolPanelGroup"
          className="curation-edit-panel"
          data-tool-panel="group"
        >
          <div className="curation-group-creator">
            <button
              type="button"
              className="curation-btn curation-btn--full"
              onClick={handleCreateGroup}
            >
              Create Group from Selection
            </button>
          </div>

          <div id="groupSectionList" style={{ marginTop: 12 }}>
            {draftState.groups.length === 0 && (
              <p className="curation-muted curation-tiny">No groups yet.</p>
            )}
            {draftState.groups.map((group) => {
              const groupLabel =
                draftState.renameByRef[toGroupRef(group.group_id)] ||
                group.section_name ||
                group.group_id;
              return (
                <div key={group.group_id} className="curation-group-card">
                  <div className="curation-group-card__header">
                    <strong>{groupLabel}</strong>
                    <span className="curation-tag">{group.section_type}</span>
                    <button
                      type="button"
                      className="curation-btn curation-btn--danger curation-tiny"
                      onClick={() => handleRemoveGroup(group.group_id)}
                    >
                      Delete
                    </button>
                  </div>
                  <p className="curation-muted curation-tiny">
                    {group.member_refs.length} member(s)
                  </p>
                </div>
              );
            })}
          </div>

          {draftState.groups.length >= 2 && (
            <div id="groupPairWalkList" style={{ marginTop: 12 }}>
              <div className="curation-tiny" style={{ fontWeight: 600 }}>
                Walk Times
              </div>
              {draftState.groups.map((gA, i) =>
                draftState.groups.slice(i + 1).map((gB) => {
                  const pk = pairKey(gA.group_id, gB.group_id);
                  return (
                    <div key={pk} className="curation-walk-link">
                      <span>
                        {draftState.renameByRef[toGroupRef(gA.group_id)] ||
                          gA.section_name}{" "}
                        ↔{" "}
                        {draftState.renameByRef[toGroupRef(gB.group_id)] ||
                          gB.section_name}
                      </span>
                      <input
                        type="number"
                        min="0"
                        step="1"
                        value={draftState.pairWalkMinutesByKey[pk] ?? 5}
                        onChange={(e) =>
                          setDraftState((prev) => ({
                            ...prev,
                            pairWalkMinutesByKey: {
                              ...prev.pairWalkMinutesByKey,
                              [pk]: Number(e.target.value) || 5,
                            },
                          }))
                        }
                      />
                      <span className="curation-muted curation-tiny">min</span>
                    </div>
                  );
                }),
              )}
            </div>
          )}
        </div>
      )}

      {/* AI Score */}
      <div style={{ marginTop: 16 }}>
        <button
          type="button"
          className="curation-btn curation-btn--secondary"
          onClick={onAiScore}
          style={{ width: "100%" }}
        >
          ✨ AI Suggest
        </button>
        {aiResult && (
          <div
            id="aiScoreResult"
            className={`curation-notice ${aiResult.suggested_action === "merge" ? "curation-notice--success" : "curation-notice--warning"}`}
            style={{ marginTop: 8 }}
          >
            <strong>
              AI Confidence {(aiResult.confidence_score * 100).toFixed(0)}%:
            </strong>{" "}
            Suggests{" "}
            <strong>{(aiResult.suggested_action || "").toUpperCase()}</strong>.{" "}
            {aiResult.reasoning}
          </div>
        )}
      </div>

      {/* Decision Note */}
      <div style={{ marginTop: 16 }}>
        <label className="curation-tiny" htmlFor="editNoteInput">
          Decision Note (optional)
        </label>
        <textarea
          id="editNoteInput"
          rows="3"
          placeholder="Explain why this change is correct..."
          value={draftState.note}
          onChange={(e) =>
            setDraftState((prev) => ({ ...prev, note: e.target.value }))
          }
        />
      </div>

      {/* Edit History */}
      {clusterDetail?.decisions?.length > 0 && (
        <details style={{ marginTop: 16 }}>
          <summary
            className="curation-tiny"
            style={{ fontWeight: 600, cursor: "pointer" }}
          >
            Edit History
          </summary>
          <div id="decisionHistoryList" style={{ marginTop: 8 }}>
            {clusterDetail.decisions.map((d) => (
              <div
                key={`${d.created_at ?? "unknown"}-${d.operation ?? "unknown"}-${d.requested_by ?? "unknown"}`}
                className="curation-history-row"
              >
                <strong>{d.operation}</strong> · {d.requested_by} ·{" "}
                {d.created_at}
              </div>
            ))}
          </div>
        </details>
      )}
    </aside>
  );
}

// ─── Main Page Component ──────────────────────────────────────────────────────

export function CurationPage() {
  const [clusters, setClusters] = useState([]);
  const [clusterTotalCount, setClusterTotalCount] = useState(0);
  const [clusterListLimit, setClusterListLimit] = useState(50);
  const [activeClusterId, setActiveClusterId] = useState(null);
  const [clusterDetail, setClusterDetail] = useState(null);
  const [curatedItems, setCuratedItems] = useState([]);
  const [optimisticItems, setOptimisticItems] = useState([]);
  const [hiddenServerIds, setHiddenServerIds] = useState(new Set());
  const [filters, setFilters] = useState({ country: "", status: "" });
  const [selectedIds, setSelectedIds] = useState(new Set());
  const [draftState, setDraftState] = useState(createEmptyDraftState());
  const [activeTool, setActiveTool] = useState("merge");
  const [loading, setLoading] = useState(true);
  const [notice, setNotice] = useState(null);
  const [aiResult, setAiResult] = useState(null);
  const noticeTimerRef = useRef(null);

  const showNotice = useCallback((message, tone = "info", sticky = false) => {
    setNotice({ message, tone });
    if (noticeTimerRef.current) clearTimeout(noticeTimerRef.current);
    if (!sticky) {
      noticeTimerRef.current = setTimeout(() => setNotice(null), 4500);
    }
  }, []);

  // Load clusters
  const loadClusters = useCallback(async () => {
    setLoading(true);
    try {
      const data = await apiFetchClusters(filters);
      setClusters(data.items || []);
      setClusterTotalCount(data.totalCount || 0);
      setClusterListLimit(data.limit || 50);
    } catch (err) {
      showNotice(`Failed to load clusters: ${err.message}`, "error", true);
    } finally {
      setLoading(false);
    }
  }, [filters, showNotice]);

  // Load cluster detail
  const loadClusterDetail = useCallback(
    async (clusterId) => {
      try {
        const [detail, curated] = await Promise.all([
          apiFetchClusterDetail(clusterId),
          fetchCuratedProjection(clusterId).catch(() => []),
        ]);
        setClusterDetail(detail);
        setCuratedItems(curated);
        setActiveClusterId((prev) => {
          if (prev !== clusterId) {
            setOptimisticItems([]);
            setHiddenServerIds(new Set());
          }
          return clusterId;
        });
        setSelectedIds(new Set());
        setDraftState(createEmptyDraftState());
        setAiResult(null);
      } catch (err) {
        showNotice(`Failed to load cluster: ${err.message}`, "error", true);
      }
    },
    [showNotice],
  );

  // Initial load
  useEffect(() => {
    loadClusters();
  }, [loadClusters]);

  // Auto-select first cluster
  useEffect(() => {
    if (clusters.length > 0 && !activeClusterId) {
      loadClusterDetail(clusters[0].cluster_id);
    }
  }, [clusters, activeClusterId, loadClusterDetail]);

  const handleToggleCandidate = useCallback((stationId) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(stationId)) next.delete(stationId);
      else next.add(stationId);
      return next;
    });
  }, []);

  const combinedCuratedItems = useMemo(() => {
    const activeCurated = curatedItems.filter(
      (ci) => !hiddenServerIds.has(ci.curated_station_id),
    );
    const combined = [...activeCurated];

    for (const opt of optimisticItems) {
      const optMemberIds = opt.members.map((m) => m.global_station_id);
      const isCovered = activeCurated.some((ci) =>
        ci.members?.some((cm) => optMemberIds.includes(cm.global_station_id)),
      );
      if (!isCovered) {
        combined.push(opt);
      }
    }
    return combined;
  }, [curatedItems, optimisticItems, hiddenServerIds]);

  const candidates = useMemo(() => {
    const rawCandidates = clusterDetail?.candidates || [];
    const absorbedIds = new Set();
    const curatedCandidates = [];

    for (const item of combinedCuratedItems) {
      if (Array.isArray(item.members)) {
        for (const m of item.members) {
          absorbedIds.add(m.global_station_id);
        }
      }

      let lat = 0,
        lon = 0,
        count = 0;
      if (Array.isArray(item.members)) {
        for (const m of item.members) {
          const match = rawCandidates.find(
            (c) => c.global_station_id === m.global_station_id,
          );
          if (
            match &&
            Number.isFinite(match.lat) &&
            Number.isFinite(match.lon)
          ) {
            lat += match.lat;
            lon += match.lon;
            count++;
          }
        }
      }
      if (count > 0) {
        lat /= count;
        lon /= count;
      } else {
        lat = undefined;
        lon = undefined;
      }

      curatedCandidates.push({
        global_station_id: item.curated_station_id,
        display_name: item.display_name || item.curated_station_id,
        candidate_rank: 0,
        aliases: [],
        provider_labels: ["curated", item.derived_operation].filter(Boolean),
        lat,
        lon,
        is_curated: true,
        derived_operation: item.derived_operation,
        members: item.members,
        service_context: {},
        segment_context: {},
      });
    }

    const unabsorbed = rawCandidates.filter(
      (c) => !absorbedIds.has(c.global_station_id),
    );
    return [...curatedCandidates, ...unabsorbed];
  }, [clusterDetail?.candidates, combinedCuratedItems]);

  // Auto-fill Merged Name with the first selection
  useEffect(() => {
    if (activeTool === "merge") {
      if (selectedIds.size > 0) {
        setDraftState((prev) => {
          if (!prev.renameByRef.__merge_name) {
            const firstId = Array.from(selectedIds)[0];
            const firstSelected = candidates.find(
              (c) => c.global_station_id === firstId,
            );
            if (firstSelected) {
              return {
                ...prev,
                renameByRef: {
                  ...prev.renameByRef,
                  __merge_name:
                    firstSelected.display_name ||
                    firstSelected.global_station_id,
                },
              };
            }
          }
          return prev;
        });
      } else {
        setDraftState((prev) => {
          if (prev.renameByRef.__merge_name) {
            return {
              ...prev,
              renameByRef: { ...prev.renameByRef, __merge_name: "" },
            };
          }
          return prev;
        });
      }
    }
  }, [selectedIds, activeTool, candidates]);

  const handleSelectAll = useCallback(() => {
    const all = candidates.map((c) => c.global_station_id);
    setSelectedIds(new Set(all));
  }, [candidates]);

  const handleClearSelection = useCallback(() => {
    setSelectedIds(new Set());
  }, []);

  const handleResolve = useCallback(async () => {
    if (!clusterDetail) {
      showNotice("Select a cluster first.", "error");
      return;
    }

    if (activeTool === "merge") {
      const mergedName = draftState.renameByRef.__merge_name?.trim();
      if (!mergedName) {
        showNotice("Merged Name is a mandatory field.", "error");
        return;
      }
    }
    try {
      const payload = buildResolvePayload({
        draftState,
        selectedStationIds: selectedIds,
        activeTool,
        clusterDetail,
        curatedItems: combinedCuratedItems,
      });

      // Optimistic state backwards compatible backup
      const prevOptimistic = [...optimisticItems];
      const prevHidden = new Set(hiddenServerIds);

      // Optimistic state updates
      const actedUponIds = new Set(payload.selected_global_station_ids);
      const newHidden = new Set(hiddenServerIds);
      for (const ci of curatedItems) {
        if (ci.members?.some((m) => actedUponIds.has(m.global_station_id))) {
          newHidden.add(ci.curated_station_id);
        }
      }
      setHiddenServerIds(newHidden);

      const newOptimistic = [];
      for (const g of payload.groups || []) {
        if (
          g.member_global_station_ids &&
          g.member_global_station_ids.length > 1
        ) {
          newOptimistic.push({
            curated_station_id: `opt_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`,
            display_name: g.rename_to || g.group_label || "Optimistic Action",
            derived_operation:
              payload.operation === "split" ? "group" : "merge",
            members: g.member_global_station_ids.map((id) => ({
              global_station_id: id,
              member_role: "member",
            })),
          });
        }
      }

      setOptimisticItems((prev) => {
        const next = [...prev];
        const filtered = next.filter(
          (opt) =>
            !opt.members.some((m) => actedUponIds.has(m.global_station_id)),
        );
        return [...filtered, ...newOptimistic];
      });

      setSelectedIds(new Set());
      setDraftState(createEmptyDraftState());

      try {
        const result = await submitDecision(clusterDetail.cluster_id, payload);
        showNotice(
          `Conflict resolved (decision id=${result.decision_id || "n/a"}).`,
          "success",
        );
        await loadClusters(); // This might trigger updates, but that's fine
        await loadClusterDetail(clusterDetail.cluster_id);
      } catch (err) {
        // Rollback optimistic map on failure
        setOptimisticItems(prevOptimistic);
        setHiddenServerIds(prevHidden);
        throw err;
      }
    } catch (err) {
      showNotice(`Failed: ${err.message}`, "error", true);
    }
  }, [
    clusterDetail,
    draftState,
    selectedIds,
    activeTool,
    showNotice,
    loadClusters,
    loadClusterDetail,
    curatedItems,
    hiddenServerIds,
    optimisticItems,
    combinedCuratedItems,
  ]);

  const handleAiScore = useCallback(async () => {
    if (!clusterDetail) {
      showNotice("Select a cluster first.", "error");
      return;
    }
    setAiResult(null);
    try {
      const result = await requestAiScore(clusterDetail.cluster_id);
      setAiResult(result);
      if (["merge", "split", "group"].includes(result.suggested_action)) {
        setActiveTool(result.suggested_action);
      }
    } catch (err) {
      showNotice(`AI failed: ${err.message}`, "error");
    }
  }, [clusterDetail, showNotice]);

  return (
    <div className="curation-container">
      <ClusterSidebar
        clusters={clusters}
        totalCount={clusterTotalCount}
        listLimit={clusterListLimit}
        activeClusterId={activeClusterId}
        filters={filters}
        onFilterChange={setFilters}
        onSelectCluster={loadClusterDetail}
        onRefresh={loadClusters}
        loading={loading}
      />

      <main className="curation-center">
        <div className="curation-center__map-section">
          <div className="curation-map-toolbar">
            <span
              className="curation-muted curation-tiny"
              id="curationMapStatus"
            >
              {candidates.length > 0
                ? `${candidates.length} candidates plotted.`
                : "Select a cluster."}
            </span>
            <div className="curation-map-mode-toggle">
              <button
                id="mapModeDefaultBtn"
                type="button"
                className="curation-btn curation-btn--secondary curation-tiny"
              >
                Map
              </button>
              <button
                id="mapModeSatelliteBtn"
                type="button"
                className="curation-btn curation-btn--secondary curation-tiny"
              >
                Sat
              </button>
            </div>
          </div>
          <CurationMap
            candidates={candidates}
            selectedIds={selectedIds}
            onToggleCandidate={handleToggleCandidate}
          />
        </div>

        <section className="curation-center__candidates">
          <div className="curation-candidates-header">
            <h4>Cluster Candidates</h4>
            <div className="curation-candidates-actions">
              <button
                id="candidateSelectAllBtn"
                type="button"
                className="curation-btn curation-btn--secondary curation-tiny"
                onClick={handleSelectAll}
              >
                All
              </button>
              <button
                id="candidateClearBtn"
                type="button"
                className="curation-btn curation-btn--secondary curation-tiny"
                onClick={handleClearSelection}
              >
                Clear
              </button>
            </div>
          </div>
          <p
            id="selectionSummary"
            className="curation-muted curation-tiny"
            style={{ padding: "0 12px" }}
          >
            {selectedIds.size === 0
              ? "No candidates selected."
              : `Selected: ${selectedIds.size} candidate(s).`}
          </p>

          {/* Standalone candidates */}
          <div className="curation-candidates-scroll">
            {candidates.length === 0 && (
              <p className="curation-muted" style={{ padding: "12px" }}>
                No cluster selected.
              </p>
            )}
            {candidates.map((candidate) => (
              <CandidateCard
                key={candidate.global_station_id}
                candidate={candidate}
                selected={selectedIds.has(candidate.global_station_id)}
                renameByRef={draftState.renameByRef}
                onToggle={handleToggleCandidate}
                rawCandidates={clusterDetail?.candidates || []}
              />
            ))}
          </div>

          {/* Service context */}
          {selectedIds.size > 0 && (
            <details
              className="curation-service-context"
              style={{ padding: "8px 12px" }}
            >
              <summary
                className="curation-tiny"
                style={{ fontWeight: 600, cursor: "pointer" }}
              >
                Context & Evidence
              </summary>
              <div className="curation-service-grid" style={{ marginTop: 8 }}>
                <div>
                  <strong className="curation-tiny">Incoming</strong>
                  <div
                    id="selectedServiceIncoming"
                    className="curation-service-list"
                  >
                    {(() => {
                      const incoming = new Set();
                      for (const sid of selectedIds) {
                        const c = candidates.find(
                          (x) => x.global_station_id === sid,
                        );
                        for (const v of c?.service_context?.incoming || [])
                          incoming.add(v);
                      }
                      return incoming.size === 0 ? (
                        <p className="curation-muted curation-tiny">None</p>
                      ) : (
                        Array.from(incoming)
                          .sort()
                          .map((v) => (
                            <div key={v} className="curation-service-item">
                              {v}
                            </div>
                          ))
                      );
                    })()}
                  </div>
                </div>
                <div>
                  <strong className="curation-tiny">Outgoing</strong>
                  <div
                    id="selectedServiceOutgoing"
                    className="curation-service-list"
                  >
                    {(() => {
                      const outgoing = new Set();
                      for (const sid of selectedIds) {
                        const c = candidates.find(
                          (x) => x.global_station_id === sid,
                        );
                        for (const v of c?.service_context?.outgoing || [])
                          outgoing.add(v);
                      }
                      return outgoing.size === 0 ? (
                        <p className="curation-muted curation-tiny">None</p>
                      ) : (
                        Array.from(outgoing)
                          .sort()
                          .map((v) => (
                            <div key={v} className="curation-service-item">
                              {v}
                            </div>
                          ))
                      );
                    })()}
                  </div>
                </div>
              </div>

              {clusterDetail?.evidence?.length > 0 && (
                <div id="evidenceList" style={{ marginTop: 12 }}>
                  {clusterDetail.evidence.slice(0, 20).map((row) => (
                    <div
                      key={`${row.evidence_type ?? "evidence"}-${row.source_global_station_id ?? "source"}-${row.target_global_station_id ?? "target"}-${row.score ?? "na"}`}
                      className="curation-evidence-row"
                    >
                      <strong>{row.evidence_type}</strong> ·{" "}
                      {row.source_global_station_id} ↔{" "}
                      {row.target_global_station_id} · score{" "}
                      {row.score ?? "n/a"}
                    </div>
                  ))}
                </div>
              )}
            </details>
          )}
        </section>
      </main>

      <CurationTools
        activeTool={activeTool}
        onSetTool={setActiveTool}
        draftState={draftState}
        setDraftState={setDraftState}
        selectedIds={selectedIds}
        candidates={candidates}
        clusterDetail={clusterDetail}
        onResolve={handleResolve}
        onAiScore={handleAiScore}
        aiResult={aiResult}
        notice={notice}
      />
    </div>
  );
}
