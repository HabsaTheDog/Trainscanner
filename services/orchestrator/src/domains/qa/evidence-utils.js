function normalizeLooseStationName(value) {
  return String(value || "")
    .normalize("NFD")
    .replaceAll(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replaceAll(/\b(abzw|abzw\.)\b/g, " abzweig ")
    .replaceAll(/\b(bhf|bf)\b/g, " bahnhof ")
    .replaceAll(/\b(hbf)\b/g, " hauptbahnhof ")
    .replaceAll(/\b(str|str\.)\b/g, " strasse ")
    .replaceAll(/[^a-z0-9]+/g, " ")
    .replaceAll(/\s+/g, " ")
    .trim();
}

function tokenizeLooseStationName(value) {
  return normalizeLooseStationName(value)
    .split(/\s+/)
    .filter(Boolean)
    .filter(
      (token) =>
        ![
          "bahnhof",
          "hauptbahnhof",
          "station",
          "halt",
          "haltestelle",
          "busbahnhof",
          "bus",
          "tram",
        ].includes(token),
    );
}

function isGenericStationName(value, frequency = 0) {
  const normalized = normalizeLooseStationName(value);
  return (
    /^(steig|gleis|platform|plattform|quai|bussteig|bahnsteig)( [a-z0-9]+)?$/.test(
      normalized,
    ) || Number(frequency || 0) >= 12
  );
}

function classifyDistanceEvidence(distanceMeters) {
  if (distanceMeters === null || distanceMeters === undefined || distanceMeters === "") {
    return {
      status: "missing",
      distance_status: "missing_coordinates",
      score: 0,
    };
  }
  const value = Number(distanceMeters);
  if (!Number.isFinite(value)) {
    return {
      status: "missing",
      distance_status: "missing_coordinates",
      score: 0,
    };
  }
  if (value <= 50) {
    return { status: "supporting", distance_status: "same_location", score: 1 };
  }
  if (value <= 250) {
    return { status: "supporting", distance_status: "nearby", score: 0.85 };
  }
  if (value <= 1000) {
    return {
      status: "informational",
      distance_status: "far_apart",
      score: 0.6,
    };
  }
  if (value <= 5000) {
    return { status: "warning", distance_status: "far_apart", score: 0.25 };
  }
  return { status: "warning", distance_status: "too_far", score: 0.05 };
}

function summarizeEvidenceRows(rows = []) {
  const statusCounts = {
    supporting: 0,
    warning: 0,
    missing: 0,
    informational: 0,
  };
  const typeCounts = {};
  const pairMap = new Map();

  for (const row of Array.isArray(rows) ? rows : []) {
    const status = String(row?.status || "informational").trim();
    if (status in statusCounts) {
      statusCounts[status] += 1;
    } else {
      statusCounts.informational += 1;
    }
    const evidenceType = String(row?.evidence_type || "unknown").trim() || "unknown";
    typeCounts[evidenceType] = (typeCounts[evidenceType] || 0) + 1;

    const sourceId = String(row?.source_global_station_id || "").trim();
    const targetId = String(row?.target_global_station_id || "").trim();
    const pairKey = [sourceId, targetId].sort().join("|");
    const entry = pairMap.get(pairKey) || {
      source_global_station_id: [sourceId, targetId].sort()[0] || sourceId,
      target_global_station_id: [sourceId, targetId].sort()[1] || targetId,
      supporting_count: 0,
      warning_count: 0,
      missing_count: 0,
      informational_count: 0,
      score_total: 0,
      score_count: 0,
      highlights: {
        evidence_types: [],
        distance_status: "",
        shared_signal_count: 0,
      },
    };

    if (status === "supporting") entry.supporting_count += 1;
    else if (status === "warning") entry.warning_count += 1;
    else if (status === "missing") entry.missing_count += 1;
    else entry.informational_count += 1;

    const score = Number(row?.score);
    if (Number.isFinite(score)) {
      entry.score_total += score;
      entry.score_count += 1;
    }

    if (
      ["warning", "missing", "supporting"].includes(status) &&
      !entry.highlights.evidence_types.includes(evidenceType) &&
      entry.highlights.evidence_types.length < 4
    ) {
      entry.highlights.evidence_types.push(evidenceType);
    }
    if (
      row?.evidence_type === "geographic_distance" &&
      row?.details?.distance_status
    ) {
      entry.highlights.distance_status = String(row.details.distance_status);
    }
    if (
      [
        "shared_provider_sources",
        "shared_route_context",
        "shared_adjacent_stations",
      ].includes(evidenceType) &&
      Number.isFinite(Number(row?.raw_value))
    ) {
      entry.highlights.shared_signal_count += Number(row.raw_value);
    }

    pairMap.set(pairKey, entry);
  }

  const evidenceSummary = {
    ...statusCounts,
    status_counts: statusCounts,
    type_counts: typeCounts,
  };

  const pairSummaries = Array.from(pairMap.values())
    .map((entry) => {
      const score =
        entry.score_count > 0 ? entry.score_total / entry.score_count : 0;
      const summary =
        entry.warning_count > 0
          ? "Conflicting evidence needs review"
          : entry.missing_count > 0
            ? "Key evidence is missing"
            : entry.supporting_count > 0
              ? "Signals are mostly supportive"
              : "Mostly contextual evidence";
      return {
        source_global_station_id: entry.source_global_station_id,
        target_global_station_id: entry.target_global_station_id,
        supporting_count: entry.supporting_count,
        warning_count: entry.warning_count,
        missing_count: entry.missing_count,
        informational_count: entry.informational_count,
        score: Number(score.toFixed(3)),
        summary,
        highlights: entry.highlights,
      };
    })
    .sort((a, b) => {
      if (b.warning_count !== a.warning_count) {
        return b.warning_count - a.warning_count;
      }
      if (a.score !== b.score) {
        return b.score - a.score;
      }
      return `${a.source_global_station_id}|${a.target_global_station_id}`.localeCompare(
        `${b.source_global_station_id}|${b.target_global_station_id}`,
      );
    });

  return { evidenceSummary, pairSummaries };
}

module.exports = {
  classifyDistanceEvidence,
  isGenericStationName,
  normalizeLooseStationName,
  summarizeEvidenceRows,
  tokenizeLooseStationName,
};
