import maplibregl from "maplibre-gl";
import PropTypes from "prop-types";
import { useCallback, useEffect, useRef, useState } from "react";
import { graphqlQuery } from "./graphql";

// ---------------------------------------------------------------------------
// GraphQL strings
// ---------------------------------------------------------------------------

const QUEUE_QUERY = `
  query LowConfidenceQueue($limit: Int, $offset: Int) {
    lowConfidenceQueue(limit: $limit, offset: $offset) {
      total
      items {
        evidence_id
        cluster_id
        source_canonical_station_id
        target_canonical_station_id
        evidence_type
        ai_confidence
        ai_suggested_action
        cluster_display_name
        source_lat
        source_lon
        target_lat
        target_lon
      }
    }
  }
`;

const APPROVE_MUTATION = `
  mutation Approve($clusterId: ID!, $evidenceId: ID!) {
    approveAiMatch(clusterId: $clusterId, evidenceId: $evidenceId) {
      ok
      decision_id
      operation
    }
  }
`;

const REJECT_MUTATION = `
  mutation Reject($clusterId: ID!, $evidenceId: ID!) {
    rejectAiMatch(clusterId: $clusterId, evidenceId: $evidenceId) {
      ok
      decision_id
      operation
    }
  }
`;

const SET_WALK_TIME_MUTATION = `
  mutation SetWalkTime($hubId: ID!, $walkMinutes: Int!) {
    setMegaHubWalkTime(hubId: $hubId, walkMinutes: $walkMinutes) {
      ok
      rule_id
      hub_id
      walk_minutes
    }
  }
`;

// ---------------------------------------------------------------------------
// Top-100 EU mega-hubs (hardcoded; walk-time overrides kept in localStorage)
// ---------------------------------------------------------------------------

const MEGA_HUBS = [
  { id: "paris-cdg", name: "Paris CDG" },
  { id: "paris-nord", name: "Paris Gare du Nord" },
  { id: "paris-lyon", name: "Paris Gare de Lyon" },
  { id: "frankfurt-hbf", name: "Frankfurt Hbf" },
  { id: "amsterdam-centraal", name: "Amsterdam Centraal" },
  { id: "berlin-hbf", name: "Berlin Hbf" },
  { id: "brussels-midi", name: "Brussels-Midi / Bruxelles-Midi" },
  { id: "london-stpancras", name: "London St Pancras" },
  { id: "london-victoria", name: "London Victoria" },
  { id: "london-waterloo", name: "London Waterloo" },
  { id: "madrid-atocha", name: "Madrid Atocha" },
  { id: "madrid-chamartin", name: "Madrid Chamartín" },
  { id: "barcelona-sants", name: "Barcelona Sants" },
  { id: "milan-centrale", name: "Milano Centrale" },
  { id: "rome-termini", name: "Roma Termini" },
  { id: "zurich-hb", name: "Zürich HB" },
  { id: "vienna-hbf", name: "Wien Hbf" },
  { id: "vienna-westbf", name: "Wien Westbahnhof" },
  { id: "munich-hbf", name: "München Hbf" },
  { id: "hamburg-hbf", name: "Hamburg Hbf" },
  { id: "cologne-hbf", name: "Köln Hbf" },
  { id: "dusseldorf-hbf", name: "Düsseldorf Hbf" },
  { id: "stuttgart-hbf", name: "Stuttgart Hbf" },
  { id: "leipzig-hbf", name: "Leipzig Hbf" },
  { id: "dresden-hbf", name: "Dresden Hbf" },
  { id: "prague-hlavni", name: "Praha hlavní nádraží" },
  { id: "warsaw-centralna", name: "Warszawa Centralna" },
  { id: "budapest-keleti", name: "Budapest-Keleti" },
  { id: "bucharest-nord", name: "București Nord" },
  { id: "sofia-central", name: "Sofia Central" },
  { id: "athens-larissa", name: "Athens Larissa" },
  { id: "lisbon-oriente", name: "Lisboa Oriente" },
  { id: "porto-campanha", name: "Porto Campanhã" },
  { id: "lyon-partdieu", name: "Lyon Part-Dieu" },
  { id: "marseille-stcharles", name: "Marseille St-Charles" },
  { id: "lille-europe", name: "Lille-Europe" },
  { id: "strasbourg", name: "Strasbourg" },
  { id: "bordeaux-stjean", name: "Bordeaux St-Jean" },
  { id: "toulouse-matabiau", name: "Toulouse-Matabiau" },
  { id: "nice-ville", name: "Nice-Ville" },
  { id: "nantes", name: "Nantes" },
  { id: "rennes", name: "Rennes" },
  { id: "geneva", name: "Genève" },
  { id: "basel-sbb", name: "Basel SBB" },
  { id: "bern", name: "Bern" },
  { id: "lausanne", name: "Lausanne" },
  { id: "innsbruck-hbf", name: "Innsbruck Hbf" },
  { id: "salzburg-hbf", name: "Salzburg Hbf" },
  { id: "graz-hbf", name: "Graz Hbf" },
  { id: "linz-hbf", name: "Linz Hbf" },
  { id: "hannover-hbf", name: "Hannover Hbf" },
  { id: "nuremberg-hbf", name: "Nürnberg Hbf" },
  { id: "dortmund-hbf", name: "Dortmund Hbf" },
  { id: "essen-hbf", name: "Essen Hbf" },
  { id: "duisburg-hbf", name: "Duisburg Hbf" },
  { id: "bochum-hbf", name: "Bochum Hbf" },
  { id: "wuppertal-hbf", name: "Wuppertal Hbf" },
  { id: "bielefeld-hbf", name: "Bielefeld Hbf" },
  { id: "kassel-wilhelmshoehe", name: "Kassel-Wilhelmshöhe" },
  { id: "mannheim-hbf", name: "Mannheim Hbf" },
  { id: "karlsruhe-hbf", name: "Karlsruhe Hbf" },
  { id: "freiburg-hbf", name: "Freiburg (Breisgau) Hbf" },
  { id: "augsburg-hbf", name: "Augsburg Hbf" },
  { id: "ulm-hbf", name: "Ulm Hbf" },
  { id: "wuerzburg-hbf", name: "Würzburg Hbf" },
  { id: "erfurt-hbf", name: "Erfurt Hbf" },
  { id: "magdeburg-hbf", name: "Magdeburg Hbf" },
  { id: "rostock-hbf", name: "Rostock Hbf" },
  { id: "kiel-hbf", name: "Kiel Hbf" },
  { id: "amsterdam-south", name: "Amsterdam Zuid" },
  { id: "rotterdam-centraal", name: "Rotterdam Centraal" },
  { id: "the-hague-hs", name: "Den Haag HS" },
  { id: "utrecht-centraal", name: "Utrecht Centraal" },
  { id: "eindhoven", name: "Eindhoven" },
  { id: "antwerp-centraal", name: "Antwerpen-Centraal" },
  { id: "ghent-stpieters", name: "Gent-Sint-Pieters" },
  { id: "liege-guillemins", name: "Liège-Guillemins" },
  { id: "luxembourg-gare", name: "Luxembourg Gare" },
  { id: "copenhagen-h", name: "København H" },
  { id: "oslo-s", name: "Oslo S" },
  { id: "stockholm-c", name: "Stockholm C" },
  { id: "gothenburg-c", name: "Göteborg C" },
  { id: "helsinki-central", name: "Helsinki asema" },
  { id: "riga-central", name: "Rīga Centrāla stacija" },
  { id: "tallinn-balti", name: "Tallinn Balti jaam" },
  { id: "vilnius-central", name: "Vilnius" },
  { id: "poznan-glowny", name: "Poznań Główny" },
  { id: "wroclaw-glowny", name: "Wrocław Główny" },
  { id: "krakow-glowny", name: "Kraków Główny" },
  { id: "gdansk-glowny", name: "Gdańsk Główny" },
  { id: "brno-hlavni", name: "Brno hlavní nádraží" },
  { id: "bratislava-hlavna", name: "Bratislava hlavná stanica" },
  { id: "zagreb-glavni", name: "Zagreb Glavni kolodvor" },
  { id: "sarajevo-central", name: "Sarajevo" },
  { id: "belgrade-central", name: "Beograd Centar" },
  { id: "skopje", name: "Skopje" },
  { id: "podgorica", name: "Podgorica" },
  { id: "tirana", name: "Tirana" },
  { id: "tbilisi-central", name: "Tbilisi Central" },
];

// ---------------------------------------------------------------------------
// Helper utilities
// ---------------------------------------------------------------------------

function confidenceColor(score) {
  if (score == null) return "#94a3b8";
  if (score >= 0.75) return "#22c55e";
  if (score >= 0.5) return "#f59e0b";
  return "#ef4444";
}

function actionBadge(action) {
  if (!action) return null;
  const map = {
    approve: { bg: "#dcfce7", color: "#166534", label: "✓ approve" },
    reject: { bg: "#fee2e2", color: "#991b1b", label: "✗ reject" },
    review: { bg: "#fef3c7", color: "#92400e", label: "⚠ review" },
    error: { bg: "#f1f5f9", color: "#475569", label: "— error" },
  };
  const style = map[action] || {
    bg: "#f1f5f9",
    color: "#475569",
    label: action,
  };
  return (
    <span
      style={{
        display: "inline-block",
        padding: "2px 8px",
        borderRadius: 999,
        fontSize: "0.75rem",
        fontWeight: 600,
        background: style.bg,
        color: style.color,
      }}
    >
      {style.label}
    </span>
  );
}

// Return real [lon, lat] from item, falling back to a deterministic hash
// scatter across Europe if coords are not yet populated in the DB.
function itemCoords(stationId, lat, lon) {
  if (lat != null && lon != null) return [lon, lat];
  // Deterministic fallback (safe until all stations have coords)
  let h = 0;
  for (let i = 0; i < stationId.length; i += 1) {
    h = (h * 31 + (stationId.codePointAt(i) ?? 0)) >>> 0;
  }
  const fbLat = 48 + ((h % 1000) / 1000) * 12;
  const fbLon = 2 + (((h >> 8) % 1000) / 1000) * 28;
  return [fbLon, fbLat];
}

// ---------------------------------------------------------------------------
// Sub-component: single queue row
// ---------------------------------------------------------------------------

function QueueRow({
  item,
  selected,
  active,
  onSelect,
  onActivate,
  onApprove,
  onReject,
  busy,
}) {
  const score = item.ai_confidence;
  return (
    <tr
      className={`qa-table-row${active ? " qa-row-active" : ""}${selected ? " qa-row-selected" : ""}`}
      onClick={() => onActivate(item)}
    >
      <td style={{ width: 28 }}>
        <input
          type="checkbox"
          checked={selected}
          onClick={(e) => e.stopPropagation()}
          onChange={() => onSelect(item.evidence_id)}
          id={`chk-${item.evidence_id}`}
        />
      </td>
      <td>
        <span style={{ fontWeight: 500 }}>
          {item.cluster_display_name || item.cluster_id}
        </span>
        <div className="qa-cell-sub">{item.evidence_type}</div>
      </td>
      <td style={{ whiteSpace: "nowrap" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <div className="confidence-bar-wrap">
            <div
              className="confidence-bar-fill"
              style={{
                width: `${Math.round((score ?? 0) * 100)}%`,
                background: confidenceColor(score),
              }}
            />
          </div>
          <span
            style={{
              fontSize: "0.82rem",
              color: confidenceColor(score),
              fontWeight: 600,
            }}
          >
            {score == null ? "—" : `${Math.round(score * 100)}%`}
          </span>
        </div>
      </td>
      <td>{actionBadge(item.ai_suggested_action)}</td>
      <td>
        <div className="qa-action-btns">
          <button
            type="button"
            className="qa-btn qa-btn-approve"
            disabled={busy}
            onClick={(e) => {
              e.stopPropagation();
              onApprove(item);
            }}
            title="Approve this match"
          >
            ✓
          </button>
          <button
            type="button"
            className="qa-btn qa-btn-reject"
            disabled={busy}
            onClick={(e) => {
              e.stopPropagation();
              onReject(item);
            }}
            title="Reject this match"
          >
            ✗
          </button>
        </div>
      </td>
    </tr>
  );
}

QueueRow.propTypes = {
  item: PropTypes.shape({
    ai_confidence: PropTypes.number,
    ai_suggested_action: PropTypes.string,
    cluster_display_name: PropTypes.string,
    cluster_id: PropTypes.string,
    evidence_id: PropTypes.string.isRequired,
    evidence_type: PropTypes.string,
  }).isRequired,
  selected: PropTypes.bool.isRequired,
  active: PropTypes.bool.isRequired,
  onSelect: PropTypes.func.isRequired,
  onActivate: PropTypes.func.isRequired,
  onApprove: PropTypes.func.isRequired,
  onReject: PropTypes.func.isRequired,
  busy: PropTypes.bool.isRequired,
};

// ---------------------------------------------------------------------------
// Sub-component: Transfer Matrix Override
// ---------------------------------------------------------------------------

const WALK_KEY = "qa_walk_overrides";

function loadOverrides() {
  try {
    return JSON.parse(localStorage.getItem(WALK_KEY) || "{}");
  } catch {
    return {};
  }
}

function saveOverrides(data) {
  localStorage.setItem(WALK_KEY, JSON.stringify(data));
}

function TransferMatrix() {
  const [overrides, setOverrides] = useState(loadOverrides);
  const [dirty, setDirty] = useState({});
  const [saved, setSaved] = useState({});

  function handleChange(hubId, val) {
    setDirty((d) => ({ ...d, [hubId]: val }));
    setSaved((s) => {
      const n = { ...s };
      delete n[hubId];
      return n;
    });
  }

  function handleSave(hubId) {
    const minutes = Number(dirty[hubId] ?? overrides[hubId] ?? 0);
    // Optimistically update local cache so it survives page reload
    const next = { ...overrides, [hubId]: String(minutes) };
    setOverrides(next);
    saveOverrides(next);
    setSaved((s) => ({ ...s, [hubId]: "saving" }));
    setDirty((d) => {
      const n = { ...d };
      delete n[hubId];
      return n;
    });
    // Persist to DB
    graphqlQuery(SET_WALK_TIME_MUTATION, { hubId, walkMinutes: minutes })
      .then(() => setSaved((s) => ({ ...s, [hubId]: "ok" })))
      .catch(() => setSaved((s) => ({ ...s, [hubId]: "error" })));
  }

  const currentVal = (hubId) => dirty[hubId] ?? overrides[hubId] ?? "";
  function renderSavedState(hubId) {
    const state = saved[hubId];
    if (state === "ok") {
      return <span className="qa-saved-badge">Saved ✓</span>;
    }
    if (state === "saving") {
      return (
        <span className="qa-saved-badge" style={{ color: "var(--muted)" }}>
          Saving…
        </span>
      );
    }
    if (state === "error") {
      return (
        <span className="qa-saved-badge" style={{ color: "var(--danger)" }}>
          ⚠ Error
        </span>
      );
    }
    return (
      <button
        type="button"
        className="qa-btn qa-btn-save"
        onClick={() => handleSave(hubId)}
      >
        Save
      </button>
    );
  }

  return (
    <section className="qa-matrix-section">
      <div className="qa-matrix-header">
        <h2 style={{ margin: 0 }}>Manual Walk-Time Override</h2>
        <span className="muted" style={{ fontSize: "0.85rem" }}>
          Top 100 EU Mega-Hubs — enter walking time (minutes) between platforms.
          Overrides are saved locally.
        </span>
      </div>
      <div className="qa-matrix-scroll">
        <table className="hub-override-table">
          <thead>
            <tr>
              <th>Hub Station</th>
              <th style={{ width: 160 }}>Walk-time (min)</th>
              <th style={{ width: 80 }}></th>
            </tr>
          </thead>
          <tbody>
            {MEGA_HUBS.map((hub) => (
              <tr key={hub.id} className={saved[hub.id] ? "hub-row-saved" : ""}>
                <td>{hub.name}</td>
                <td>
                  <input
                    type="number"
                    min="0"
                    max="120"
                    step="1"
                    placeholder="—"
                    value={currentVal(hub.id)}
                    onChange={(e) => handleChange(hub.id, e.target.value)}
                    className="hub-input"
                    id={`walk-${hub.id}`}
                  />
                </td>
                <td>{renderSavedState(hub.id)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

const PAGE_SIZE = 50;

export function QAQueuePage() {
  // --- data state ---
  const [items, setItems] = useState([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // --- sort state ---
  const [sortCol, setSortCol] = useState("ai_confidence");
  const [sortAsc, setSortAsc] = useState(true);

  // --- selection state ---
  const [selected, setSelected] = useState(new Set()); // evidence_ids
  const [activeItem, setActiveItem] = useState(null);

  // --- bulk action state ---
  const [busyIds, setBusyIds] = useState(new Set());
  const [banner, setBanner] = useState(null);

  // --- bbox drawing state ---
  const [bboxDrawing, setBboxDrawing] = useState(false);
  const bboxStart = useRef(null);

  // --- map refs ---
  const mapContainerRef = useRef(null);
  const mapRef = useRef(null);
  const markersRef = useRef([]); // [{ marker, item }]

  // -------------------------------------------------------------------------
  // Fetch queue
  // -------------------------------------------------------------------------

  const fetchQueue = useCallback(async (off = 0) => {
    setLoading(true);
    setError(null);
    try {
      const data = await graphqlQuery(QUEUE_QUERY, {
        limit: PAGE_SIZE,
        offset: off,
      });
      setItems(data.lowConfidenceQueue.items);
      setTotal(data.lowConfidenceQueue.total);
      setOffset(off);
      setSelected(new Set());
      setActiveItem(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchQueue(0);
  }, [fetchQueue]);

  // -------------------------------------------------------------------------
  // MapLibre init
  // -------------------------------------------------------------------------

  useEffect(() => {
    if (!mapContainerRef.current || mapRef.current) return;

    const map = new maplibregl.Map({
      container: mapContainerRef.current,
      style: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
      center: [15, 51],
      zoom: 4,
    });

    map.addControl(new maplibregl.NavigationControl(), "top-right");
    mapRef.current = map;

    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, []);

  // -------------------------------------------------------------------------
  // Sync markers to items
  // -------------------------------------------------------------------------

  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;

    // Remove old markers
    for (const { srcMarker, tgtMarker } of markersRef.current) {
      srcMarker.remove();
      tgtMarker.remove();
    }
    markersRef.current = [];

    // Add new markers (one pair per item)
    for (const item of items) {
      const srcEl = document.createElement("div");
      srcEl.className = "qa-marker qa-marker-src";
      srcEl.title = item.source_canonical_station_id;

      const tgtEl = document.createElement("div");
      tgtEl.className = "qa-marker qa-marker-tgt";
      tgtEl.title = item.target_canonical_station_id;

      const [sLon, sLat] = itemCoords(
        item.source_canonical_station_id,
        item.source_lat,
        item.source_lon,
      );
      const [tLon, tLat] = itemCoords(
        item.target_canonical_station_id,
        item.target_lat,
        item.target_lon,
      );

      const srcMarker = new maplibregl.Marker({ element: srcEl })
        .setLngLat([sLon, sLat])
        .setPopup(
          new maplibregl.Popup({ closeButton: false }).setHTML(
            `<strong>${item.cluster_display_name || item.cluster_id}</strong><br/>Source: ${item.source_canonical_station_id}`,
          ),
        )
        .addTo(map);

      const tgtMarker = new maplibregl.Marker({ element: tgtEl })
        .setLngLat([tLon, tLat])
        .setPopup(
          new maplibregl.Popup({ closeButton: false }).setHTML(
            `<strong>${item.cluster_display_name || item.cluster_id}</strong><br/>Target: ${item.target_canonical_station_id}`,
          ),
        )
        .addTo(map);

      markersRef.current.push({ srcMarker, tgtMarker, item });
    }
  }, [items]);

  // -------------------------------------------------------------------------
  // Focus map on active item
  // -------------------------------------------------------------------------

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !activeItem) return;

    const [sLon, sLat] = itemCoords(
      activeItem.source_canonical_station_id,
      activeItem.source_lat,
      activeItem.source_lon,
    );
    const [tLon, tLat] = itemCoords(
      activeItem.target_canonical_station_id,
      activeItem.target_lat,
      activeItem.target_lon,
    );

    const bounds = new maplibregl.LngLatBounds();
    bounds.extend([sLon, sLat]);
    bounds.extend([tLon, tLat]);

    map.fitBounds(bounds, { padding: 80, maxZoom: 13, duration: 700 });
  }, [activeItem]);

  // -------------------------------------------------------------------------
  // BBox draw: select all visible markers inside drawn rect
  // -------------------------------------------------------------------------

  function startBbox(e) {
    if (!bboxDrawing) return;
    bboxStart.current = { x: e.clientX, y: e.clientY };
  }

  function endBbox(e) {
    if (!bboxDrawing || !bboxStart.current) return;
    const map = mapRef.current;
    if (!map) return;

    const containerRect = mapContainerRef.current.getBoundingClientRect();
    const x1 = Math.min(bboxStart.current.x, e.clientX) - containerRect.left;
    const y1 = Math.min(bboxStart.current.y, e.clientY) - containerRect.top;
    const x2 = Math.max(bboxStart.current.x, e.clientX) - containerRect.left;
    const y2 = Math.max(bboxStart.current.y, e.clientY) - containerRect.top;

    const sw = map.unproject([x1, y2]);
    const ne = map.unproject([x2, y1]);

    const newSelected = new Set(selected);
    for (const { srcMarker, tgtMarker, item } of markersRef.current) {
      const srcLl = srcMarker.getLngLat();
      const tgtLl = tgtMarker.getLngLat();
      const srcInBox =
        srcLl.lng >= sw.lng &&
        srcLl.lng <= ne.lng &&
        srcLl.lat >= sw.lat &&
        srcLl.lat <= ne.lat;
      const tgtInBox =
        tgtLl.lng >= sw.lng &&
        tgtLl.lng <= ne.lng &&
        tgtLl.lat >= sw.lat &&
        tgtLl.lat <= ne.lat;
      if (srcInBox || tgtInBox) {
        newSelected.add(item.evidence_id);
      }
    }

    setSelected(newSelected);
    bboxStart.current = null;
    setBboxDrawing(false);
  }

  // -------------------------------------------------------------------------
  // Approve / reject single
  // -------------------------------------------------------------------------

  async function applySingle(item, operation) {
    setBusyIds((b) => new Set([...b, item.evidence_id]));
    try {
      const mutation =
        operation === "approve" ? APPROVE_MUTATION : REJECT_MUTATION;
      await graphqlQuery(mutation, {
        clusterId: item.cluster_id,
        evidenceId: item.evidence_id,
      });
      setItems((prev) =>
        prev.filter((i) => i.evidence_id !== item.evidence_id),
      );
      setSelected((s) => {
        const n = new Set(s);
        n.delete(item.evidence_id);
        return n;
      });
      if (activeItem?.evidence_id === item.evidence_id) setActiveItem(null);
    } catch (err) {
      setBanner({
        type: "error",
        msg: `Failed to ${operation}: ${err.message}`,
      });
    } finally {
      setBusyIds((b) => {
        const n = new Set(b);
        n.delete(item.evidence_id);
        return n;
      });
    }
  }

  // -------------------------------------------------------------------------
  // Bulk approve / reject
  // -------------------------------------------------------------------------

  async function applyBulk(operation) {
    if (selected.size === 0) return;
    const targets = items.filter((i) => selected.has(i.evidence_id));
    setBusyIds(new Set(targets.map((i) => i.evidence_id)));
    setBanner({
      type: "info",
      msg: `Running bulk ${operation} on ${targets.length} items…`,
    });

    let ok = 0;
    let fail = 0;
    const mutation =
      operation === "approve" ? APPROVE_MUTATION : REJECT_MUTATION;

    for (const item of targets) {
      try {
        await graphqlQuery(mutation, {
          clusterId: item.cluster_id,
          evidenceId: item.evidence_id,
        });
        ok++;
        setItems((prev) =>
          prev.filter((i) => i.evidence_id !== item.evidence_id),
        );
      } catch {
        fail++;
      }
    }

    setSelected(new Set());
    setBusyIds(new Set());
    setBanner({
      type: fail === 0 ? "ok" : "error",
      msg: `Bulk ${operation}: ${ok} succeeded, ${fail} failed.`,
    });
  }

  // -------------------------------------------------------------------------
  // Sort
  // -------------------------------------------------------------------------

  function toggleSort(col) {
    if (sortCol === col) setSortAsc((a) => !a);
    else {
      setSortCol(col);
      setSortAsc(true);
    }
  }

  const sorted = [...items].sort((a, b) => {
    const av = a[sortCol] ?? "";
    const bv = b[sortCol] ?? "";
    if (typeof av === "number" && typeof bv === "number") {
      return sortAsc ? av - bv : bv - av;
    }
    return sortAsc
      ? String(av).localeCompare(String(bv))
      : String(bv).localeCompare(String(av));
  });

  function thSort(col, label) {
    let arrow = "";
    if (sortCol === col) {
      arrow = sortAsc ? " ↑" : " ↓";
    }
    return (
      <th
        onClick={() => toggleSort(col)}
        style={{ cursor: "pointer", userSelect: "none" }}
      >
        {label}
        {arrow}
      </th>
    );
  }

  // -------------------------------------------------------------------------
  // Selection helpers
  // -------------------------------------------------------------------------

  function toggleSelect(evidenceId) {
    setSelected((prev) => {
      const next = new Set(prev);
      next.has(evidenceId) ? next.delete(evidenceId) : next.add(evidenceId);
      return next;
    });
  }

  function selectAll() {
    setSelected(new Set(items.map((i) => i.evidence_id)));
  }
  function clearAll() {
    setSelected(new Set());
  }

  const allChecked = items.length > 0 && selected.size === items.length;
  let bannerTone = "info";
  if (banner?.type === "ok") {
    bannerTone = "success";
  } else if (banner?.type === "error") {
    bannerTone = "error";
  }

  // -------------------------------------------------------------------------
  // Render
  // -------------------------------------------------------------------------

  return (
    <div className="qa-page">
      {/* ── Header ── */}
      <header className="qa-header">
        <div className="qa-header-left">
          <a href="/curation.html" className="qa-back-link">
            ← Station Curation
          </a>
          <h1 style={{ margin: 0 }}>QA Operator Interface</h1>
          <span className="muted" style={{ fontSize: "0.88rem" }}>
            AI match review queue
          </span>
        </div>
        <div className="qa-header-right">
          {total > 0 && <span className="qa-total-badge">{total} pending</span>}
          <button
            type="button"
            className="btn-secondary"
            onClick={() => fetchQueue(0)}
            disabled={loading}
          >
            {loading ? "Loading…" : "↺ Refresh"}
          </button>
        </div>
      </header>

      {/* ── Banner ── */}
      {banner && (
        <div
          className={`ui-notice ui-notice-${bannerTone}`}
          style={{ margin: "0 16px" }}
        >
          {banner.msg}
          <button
            type="button"
            className="btn-secondary"
            style={{ marginLeft: 12, padding: "2px 10px", fontSize: "0.8rem" }}
            onClick={() => setBanner(null)}
          >
            ✕
          </button>
        </div>
      )}

      {/* ── Bulk action bar (visible when items selected) ── */}
      {selected.size > 0 && (
        <div className="bulk-action-bar">
          <span>
            {selected.size} item{selected.size === 1 ? "" : "s"} selected
          </span>
          <button
            type="button"
            className="qa-btn qa-btn-approve"
            onClick={() => applyBulk("approve")}
          >
            ✓ Bulk Approve
          </button>
          <button
            type="button"
            className="qa-btn qa-btn-reject"
            onClick={() => applyBulk("reject")}
          >
            ✗ Bulk Reject
          </button>
          <button
            type="button"
            className="btn-secondary"
            onClick={clearAll}
            style={{ padding: "4px 10px" }}
          >
            Clear selection
          </button>
        </div>
      )}

      {/* ── Main two-column layout ── */}
      <div className="qa-layout">
        {/* ── Left: Queue Table ── */}
        <section className="qa-table-panel">
          <div className="qa-table-controls">
            <span className="muted tiny">
              Showing {sorted.length} of {total}
            </span>
            <div style={{ display: "flex", gap: 6 }}>
              <button
                type="button"
                className="btn-secondary"
                style={{ padding: "4px 8px", fontSize: "0.8rem" }}
                onClick={selectAll}
              >
                Select all
              </button>
              <button
                type="button"
                className="btn-secondary"
                style={{ padding: "4px 8px", fontSize: "0.8rem" }}
                onClick={clearAll}
              >
                Clear
              </button>
            </div>
          </div>

          {error && (
            <div
              className="ui-notice ui-notice-error"
              style={{ margin: "8px 0" }}
            >
              Error: {error}
              <button
                type="button"
                className="btn-secondary"
                style={{
                  marginLeft: 8,
                  padding: "2px 8px",
                  fontSize: "0.8rem",
                }}
                onClick={() => fetchQueue(0)}
              >
                Retry
              </button>
            </div>
          )}

          {loading && (
            <p className="muted" style={{ padding: 12 }}>
              Loading queue…
            </p>
          )}

          {!loading && sorted.length === 0 && !error && (
            <div
              className="ui-notice ui-notice-info"
              style={{ margin: "8px 0" }}
            >
              🎉 No low-confidence items in the queue. All matches have been
              reviewed!
            </div>
          )}

          {sorted.length > 0 && (
            <div className="qa-table-scroll">
              <table className="qa-table">
                <thead>
                  <tr>
                    <th style={{ width: 28 }}>
                      <input
                        type="checkbox"
                        checked={allChecked}
                        onChange={allChecked ? clearAll : selectAll}
                        id="chk-all"
                      />
                    </th>
                    {thSort("cluster_display_name", "Station")}
                    {thSort("ai_confidence", "Confidence")}
                    {thSort("ai_suggested_action", "Action")}
                    <th>Decide</th>
                  </tr>
                </thead>
                <tbody>
                  {sorted.map((item) => (
                    <QueueRow
                      key={item.evidence_id}
                      item={item}
                      selected={selected.has(item.evidence_id)}
                      active={activeItem?.evidence_id === item.evidence_id}
                      onSelect={toggleSelect}
                      onActivate={setActiveItem}
                      onApprove={(i) => applySingle(i, "approve")}
                      onReject={(i) => applySingle(i, "reject")}
                      busy={busyIds.has(item.evidence_id)}
                    />
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* Pagination */}
          {total > PAGE_SIZE && (
            <div className="qa-pagination">
              <button
                type="button"
                className="btn-secondary"
                disabled={offset === 0}
                onClick={() => fetchQueue(Math.max(0, offset - PAGE_SIZE))}
              >
                ← Prev
              </button>
              <span className="muted tiny">
                Page {Math.floor(offset / PAGE_SIZE) + 1} /{" "}
                {Math.ceil(total / PAGE_SIZE)}
              </span>
              <button
                type="button"
                className="btn-secondary"
                disabled={offset + PAGE_SIZE >= total}
                onClick={() => fetchQueue(offset + PAGE_SIZE)}
              >
                Next →
              </button>
            </div>
          )}
        </section>

        {/* ── Right: Map Panel ── */}
        <section className="qa-map-panel">
          <div className="qa-map-toolbar">
            <span className="muted" style={{ fontSize: "0.83rem" }}>
              {activeItem
                ? `Viewing: ${activeItem.cluster_display_name || activeItem.cluster_id}`
                : "Click a row to focus the map"}
            </span>
            <button
              type="button"
              className={`qa-btn ${bboxDrawing ? "qa-btn-active" : "btn-secondary"}`}
              onClick={() => setBboxDrawing((d) => !d)}
              title="Draw a bounding box to select all stations inside it"
              style={{ fontSize: "0.82rem", padding: "4px 10px" }}
            >
              {bboxDrawing ? "Cancel draw" : "⬚ Select Region"}
            </button>
          </div>

          {bboxDrawing && (
            <div
              className="ui-notice ui-notice-info"
              style={{
                borderRadius: 0,
                borderLeft: "none",
                borderRight: "none",
              }}
            >
              Click and drag on the map to select all stations within the
              region.
            </div>
          )}

          <div
            ref={mapContainerRef}
            className="qa-map"
            role="application"
            aria-label="Station evidence map"
            style={{ cursor: bboxDrawing ? "crosshair" : "grab" }}
            onPointerDown={startBbox}
            onPointerUp={endBbox}
            onPointerCancel={() => {
              bboxStart.current = null;
            }}
          />

          {activeItem && (
            <div className="qa-map-detail">
              <div className="qa-detail-row">
                <span className="qa-detail-label">Source</span>
                <code className="qa-detail-val">
                  {activeItem.source_canonical_station_id}
                </code>
              </div>
              <div className="qa-detail-row">
                <span className="qa-detail-label">Target</span>
                <code className="qa-detail-val">
                  {activeItem.target_canonical_station_id}
                </code>
              </div>
              <div className="qa-detail-row">
                <span className="qa-detail-label">Evidence type</span>
                <span className="qa-detail-val">
                  {activeItem.evidence_type}
                </span>
              </div>
              <div className="qa-detail-row">
                <span className="qa-detail-label">Confidence</span>
                <span
                  className="qa-detail-val"
                  style={{
                    color: confidenceColor(activeItem.ai_confidence),
                    fontWeight: 600,
                  }}
                >
                  {activeItem.ai_confidence == null
                    ? "—"
                    : `${Math.round(activeItem.ai_confidence * 100)}%`}
                </span>
              </div>
              <div className="qa-detail-actions">
                <button
                  type="button"
                  className="qa-btn qa-btn-approve"
                  onClick={() => applySingle(activeItem, "approve")}
                  disabled={busyIds.has(activeItem.evidence_id)}
                >
                  ✓ Approve
                </button>
                <button
                  type="button"
                  className="qa-btn qa-btn-reject"
                  onClick={() => applySingle(activeItem, "reject")}
                  disabled={busyIds.has(activeItem.evidence_id)}
                >
                  ✗ Reject
                </button>
              </div>
            </div>
          )}
        </section>
      </div>

      {/* ── Transfer Matrix Override ── */}
      <TransferMatrix />
    </div>
  );
}
