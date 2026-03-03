import { graphqlQuery } from "./graphql";

// ─── GraphQL Queries ──────────────────────────────────────────────────────────

const CLUSTERS_QUERY = `
  query GetClusters($country: String) {
    clusters(country: $country) {
      cluster_id
      country
      status
      display_name
      severity
      candidate_count
      issue_count
      scope_tag
    }
  }
`;

const CLUSTER_DETAIL_QUERY = `
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
        display_name
        candidate_rank
        aliases
        provider_labels
        lat
        lon
        service_context {
          lines
          incoming
          outgoing
        }
        segment_context {
          segment_id
          segment_name
          segment_type
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
`;

const SUBMIT_DECISION_MUTATION = `
  mutation SubmitDecision($clusterId: ID!, $input: ClusterDecisionInput!) {
    submitClusterDecision(clusterId: $clusterId, input: $input) {
      ok
      decision_id
      operation
    }
  }
`;

const AI_SCORE_MUTATION = `
  mutation ScoreCluster($id: ID!) {
    requestAiScore(clusterId: $id) {
      cluster_id
      confidence_score
      suggested_action
      reasoning
    }
  }
`;

// ─── API Functions ────────────────────────────────────────────────────────────

export async function fetchClusters(filters = {}) {
  const data = await graphqlQuery(CLUSTERS_QUERY, {
    country: filters.country || null,
  });
  return Array.isArray(data.clusters) ? data.clusters : [];
}

export async function fetchClusterDetail(clusterId) {
  const data = await graphqlQuery(CLUSTER_DETAIL_QUERY, { id: clusterId });
  return data.cluster || null;
}

export async function fetchCuratedProjection(clusterId) {
  const cleanId = String(clusterId || "").trim();
  if (!cleanId) return [];

  const params = new URLSearchParams();
  params.set("cluster_id", cleanId);
  params.set("status", "active");
  params.set("limit", "25");

  const res = await fetch(`/api/qa/curated-stations?${params.toString()}`);
  const payload = await res.json().catch(() => null);
  if (!res.ok) throw new Error(payload?.error || `HTTP ${res.status}`);
  return Array.isArray(payload) ? payload : [];
}

export async function submitDecision(clusterId, payload) {
  const data = await graphqlQuery(SUBMIT_DECISION_MUTATION, {
    clusterId,
    input: {
      operation: payload.operation,
      selected_station_ids: payload.selected_station_ids,
      groups: payload.groups,
      note: payload.note,
      rename_targets: payload.rename_targets,
      rename_to: payload.rename_to,
    },
  });
  if (!data?.submitClusterDecision?.ok) {
    throw new Error("Mutation returned false or missing OK status.");
  }
  return data.submitClusterDecision;
}

export async function requestAiScore(clusterId) {
  const data = await graphqlQuery(AI_SCORE_MUTATION, { id: clusterId });
  if (!data?.requestAiScore) throw new Error("No response from AI.");
  return data.requestAiScore;
}

// ─── Pure Utility Functions ───────────────────────────────────────────────────

export function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

export function normalizeText(value) {
  return String(value || "")
    .trim()
    .toLowerCase();
}

export function uniqueStrings(values) {
  const out = [];
  const seen = new Set();
  for (const value of Array.isArray(values) ? values : []) {
    const clean = String(value || "").trim();
    if (!clean || seen.has(clean)) continue;
    seen.add(clean);
    out.push(clean);
  }
  return out;
}

export function createDraftId(prefix) {
  return `${prefix}_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`;
}

export function createEmptyDraftState() {
  return {
    mergeItems: [],
    groups: [],
    pairWalkMinutesByKey: {},
    renameByRef: {},
    note: "",
  };
}

// ─── Ref Helpers ──────────────────────────────────────────────────────────────

export const toCandidateRef = (id) => `candidate:${String(id || "").trim()}`;
export const toMergeRef = (id) => `merge:${String(id || "").trim()}`;
export const toGroupRef = (id) => `group:${String(id || "").trim()}`;

export function parseRef(refKey) {
  const raw = String(refKey || "").trim();
  const idx = raw.indexOf(":");
  if (idx <= 0) return { type: "", id: "" };
  return { type: raw.slice(0, idx), id: raw.slice(idx + 1) };
}

// ─── Candidate Helpers ────────────────────────────────────────────────────────

export function inferCandidateCategory(candidate) {
  const segment = candidate?.segment_context || {};
  const text = normalizeText(
    [
      candidate?.display_name,
      segment.segment_name,
      segment.segment_type,
      ...(Array.isArray(candidate?.aliases) ? candidate.aliases : []),
    ].join(" "),
  );

  if (!text) return "other";
  if (text.includes("bus") || text.includes("zob")) return "bus";
  if (text.includes("tram") || text.includes("streetcar")) return "tram";
  if (/subway|u-bahn|ubahn|metro/.test(text)) return "subway";
  if (/north|south|east|west|secondary/.test(text)) return "secondary";
  if (/main|hbf|hauptbahnhof|rail|platform/.test(text)) return "main";
  return "other";
}

export function compareCandidateRank(a, b) {
  const rankA = Number.parseInt(String(a?.candidate_rank ?? ""), 10);
  const rankB = Number.parseInt(String(b?.candidate_rank ?? ""), 10);
  const safeA =
    Number.isFinite(rankA) && rankA > 0 ? rankA : Number.MAX_SAFE_INTEGER;
  const safeB =
    Number.isFinite(rankB) && rankB > 0 ? rankB : Number.MAX_SAFE_INTEGER;
  if (safeA !== safeB) return safeA - safeB;
  return String(a?.canonical_station_id || "").localeCompare(
    String(b?.canonical_station_id || ""),
  );
}

export function sortStationIdsByRank(stationIds, candidates) {
  const lookup = new Map(
    (Array.isArray(candidates) ? candidates : []).map((c) => [
      c.canonical_station_id,
      c,
    ]),
  );
  return (Array.isArray(stationIds) ? stationIds : []).slice().sort((a, b) => {
    const ca = lookup.get(a) || {
      canonical_station_id: a,
      candidate_rank: Number.MAX_SAFE_INTEGER,
    };
    const cb = lookup.get(b) || {
      canonical_station_id: b,
      candidate_rank: Number.MAX_SAFE_INTEGER,
    };
    return compareCandidateRank(ca, cb);
  });
}

export function resolveCandidateLabel(candidate, renameByRef) {
  if (!candidate) return "Unknown candidate";
  const id = String(candidate.canonical_station_id || "").trim();
  const base = String(candidate.display_name || "").trim();
  const renamed = renameByRef?.[toCandidateRef(id)] || "";
  const name = renamed || base;
  if (name && id) return `${name} (${id})`;
  return name || id || "Unknown candidate";
}

// ─── Payload Builder ──────────────────────────────────────────────────────────

export function pairKey(a, b) {
  const vals = [String(a || "").trim(), String(b || "").trim()].sort((x, y) =>
    x.localeCompare(y),
  );
  return `${vals[0]}|${vals[1]}`;
}

export function buildResolvePayload({
  draftState,
  selectedStationIds,
  activeTool,
  clusterDetail,
  curatedItems = [],
}) {
  const allCandidates = Array.isArray(clusterDetail?.candidates)
    ? clusterDetail.candidates
    : [];

  const getCandidateByStationId = (id) => {
    const raw = allCandidates.find((c) => c.canonical_station_id === id);
    if (raw) return raw;
    const curated = curatedItems.find((c) => c.curated_station_id === id);
    if (curated) return { display_name: curated.display_name };
    return null;
  };

  const expandCuratedIds = (ids) => {
    const rawIds = new Set();
    for (const id of ids) {
      const curated = curatedItems.find((c) => c.curated_station_id === id);
      if (curated && Array.isArray(curated.members)) {
        for (const m of curated.members) rawIds.add(m.canonical_station_id);
      } else {
        rawIds.add(id);
      }
    }
    return Array.from(rawIds);
  };

  const resolveNameAssumption = (ids = []) => {
    const sorted = sortStationIdsByRank(ids, allCandidates);
    const first = sorted[0] ? getCandidateByStationId(sorted[0]) : null;
    return String(
      clusterDetail?.display_name || first?.display_name || "",
    ).trim();
  };

  const buildRenameTargets = () => {
    const targets = [];
    for (const [refKey, renameTo] of Object.entries(
      draftState.renameByRef || {},
    )) {
      const parsed = parseRef(refKey);
      const clean = String(renameTo || "").trim();
      if (!clean || parsed.type !== "candidate" || !parsed.id) continue;
      const orig = String(
        getCandidateByStationId(parsed.id)?.display_name || "",
      ).trim();
      if (clean === orig) continue;
      targets.push({ canonical_station_id: parsed.id, rename_to: clean });
    }
    return targets;
  };

  const note = String(draftState.note || "").trim();
  const renameTargets = buildRenameTargets();

  // Groups mode (split/group with sections)
  if (draftState.groups.length > 0) {
    const groups = [];
    for (const dg of draftState.groups) {
      const rawRefs = uniqueStrings(
        (dg.member_refs || []).flatMap((refKey) => {
          const p = parseRef(refKey);
          if (p.type === "candidate") return p.id ? [p.id] : [];
          if (p.type === "merge") {
            const mi = draftState.mergeItems.find((m) => m.merge_id === p.id);
            return uniqueStrings(mi?.member_station_ids || []);
          }
          return [];
        }),
      );
      const memberIds = expandCuratedIds(rawRefs);
      if (memberIds.length === 0) continue;

      const groupName =
        String(
          draftState.renameByRef[toGroupRef(dg.group_id)] ||
            dg.section_name ||
            `Group ${groups.length + 1}`,
        ).trim() || `Group ${groups.length + 1}`;

      groups.push({
        group_label: groupName,
        section_type: String(dg.section_type || "other").trim() || "other",
        section_name: groupName,
        rename_to: groupName,
        target_canonical_station_id: memberIds[0],
        member_station_ids: sortStationIdsByRank(memberIds, allCandidates),
      });
    }

    if (groups.length < 2)
      throw new Error(
        "Split/group resolve needs at least two non-empty groups.",
      );

    // Build walk links between groups
    const stationToSegment = new Map(
      allCandidates
        .map((c) => [
          c.canonical_station_id,
          c.segment_context?.segment_id || "",
        ])
        .filter(([, seg]) => Boolean(seg)),
    );

    const walkLinks = [];
    for (let i = 0; i < groups.length; i++) {
      for (let j = i + 1; j < groups.length; j++) {
        const segA =
          stationToSegment.get(groups[i].member_station_ids[0]) || "";
        const segB =
          stationToSegment.get(groups[j].member_station_ids[0]) || "";
        if (!segA || !segB || segA === segB) continue;
        // Find matching draft group ids
        const dgI = draftState.groups[i];
        const dgJ = draftState.groups[j];
        const pk = dgI && dgJ ? pairKey(dgI.group_id, dgJ.group_id) : "";
        walkLinks.push({
          from_segment_id: segA,
          to_segment_id: segB,
          min_walk_minutes:
            Number.parseInt(
              String(draftState.pairWalkMinutesByKey[pk] ?? 5),
              10,
            ) || 5,
          bidirectional: true,
        });
      }
    }

    if (groups[0] && walkLinks.length > 0) {
      groups[0].segment_action = { walk_links: walkLinks };
    }

    const allIds = uniqueStrings(groups.flatMap((g) => g.member_station_ids));
    return {
      operation: "split",
      selected_station_ids: sortStationIdsByRank(allIds, allCandidates),
      groups,
      rename_targets: renameTargets,
      note,
    };
  }

  // Simple selection mode
  const rawSelected = expandCuratedIds(Array.from(selectedStationIds.values()));
  const selected = uniqueStrings(
    sortStationIdsByRank(rawSelected, allCandidates),
  );
  if (selected.length < 2)
    throw new Error("Select at least two candidates before resolving.");

  if (activeTool === "split") {
    // Un-merge: each expanded member becomes its own group
    const groups = selected.map((id) => {
      const c = getCandidateByStationId(id);
      const label = c?.display_name || id;
      return {
        group_label: label,
        section_type: inferCandidateCategory(c),
        section_name: label,
        target_canonical_station_id: id,
        member_station_ids: [id],
        rename_to: label,
      };
    });
    if (groups.length < 2)
      throw new Error(
        "Split needs a merged candidate with at least 2 members.",
      );
    return {
      operation: "split",
      selected_station_ids: selected,
      groups,
      rename_targets: renameTargets,
      note,
    };
  }

  // Default merge
  const renameTo = resolveNameAssumption(selected);
  return {
    operation: "merge",
    selected_station_ids: selected,
    groups: [
      {
        group_label: "merge-selected",
        member_station_ids: selected,
        rename_to: renameTo,
      },
    ],
    rename_to: renameTo || null,
    rename_targets: renameTargets,
    note,
  };
}

// ─── Map Helpers ──────────────────────────────────────────────────────────────

export function resolveDefaultMapStyle() {
  if (globalThis.MAP_STYLE_URL) return globalThis.MAP_STYLE_URL;
  if (globalThis.PROTOMAPS_API_KEY) {
    return `https://api.protomaps.com/styles/v2/light.json?key=${globalThis.PROTOMAPS_API_KEY}`;
  }
  return "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json";
}

export function resolveSatelliteMapStyle() {
  if (globalThis.SATELLITE_MAP_STYLE_URL)
    return globalThis.SATELLITE_MAP_STYLE_URL;
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
    layers: [{ id: "satellite", type: "raster", source: "satellite" }],
  };
}
