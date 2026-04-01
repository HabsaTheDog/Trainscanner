function stableJson(value) {
  return JSON.stringify(value);
}

function buildRelationMap(snapshot) {
  const relations = new Map();
  const setRelation = (ids, label) => {
    for (let i = 0; i < ids.length; i += 1) {
      for (let j = i + 1; j < ids.length; j += 1) {
        const key = [ids[i], ids[j]].sort((a, b) => a.localeCompare(b)).join("|");
        relations.set(key, label);
      }
    }
  };
  for (const merge of Array.isArray(snapshot?.merges) ? snapshot.merges : []) {
    setRelation(merge, "merge");
  }
  for (const setRow of Array.isArray(snapshot?.keep_separate_sets)
    ? snapshot.keep_separate_sets
    : []) {
    setRelation(setRow, "keep_separate");
  }
  for (const group of Array.isArray(snapshot?.groups) ? snapshot.groups : []) {
    for (const node of Array.isArray(group?.nodes) ? group.nodes : []) {
      setRelation(Array.isArray(node?.station_ids) ? node.station_ids : [], "group");
    }
  }
  return relations;
}

function comparePrediction(truth, prediction) {
  const truthSnapshot = truth || {};
  const predictionSnapshot = prediction || {};
  const truthJson = stableJson(truthSnapshot);
  const predictionJson = stableJson(predictionSnapshot);
  const truthRelations = buildRelationMap(truthSnapshot);
  const predictionRelations = buildRelationMap(predictionSnapshot);
  const relationKeys = new Set([
    ...truthRelations.keys(),
    ...predictionRelations.keys(),
  ]);
  let relationMatches = 0;
  for (const key of relationKeys) {
    if ((truthRelations.get(key) || "none") === (predictionRelations.get(key) || "none")) {
      relationMatches += 1;
    }
  }
  const pairwiseAgreement =
    relationKeys.size === 0 ? 1 : relationMatches / relationKeys.size;
  return {
    verdict_exact:
      String(truthSnapshot.verdict || "") === String(predictionSnapshot.verdict || ""),
    strict_exact: truthJson === predictionJson,
    pairwise_agreement: Number(pairwiseAgreement.toFixed(4)),
    false_merge:
      String(predictionSnapshot.verdict || "").includes("merge") &&
      !String(truthSnapshot.verdict || "").includes("merge"),
    false_dismiss:
      String(predictionSnapshot.verdict || "") === "dismiss" &&
      String(truthSnapshot.verdict || "") !== "dismiss",
    truth_verdict: truthSnapshot.verdict || "",
    predicted_verdict: predictionSnapshot.verdict || "",
    diff_summary:
      truthJson === predictionJson
        ? "Exact match"
        : `Truth=${truthSnapshot.verdict || "unknown"} vs AI=${predictionSnapshot.verdict || "unknown"}`,
  };
}

function percentile(sortedValues, pct) {
  if (sortedValues.length === 0) {
    return 0;
  }
  const index = Math.min(
    sortedValues.length - 1,
    Math.max(0, Math.ceil((pct / 100) * sortedValues.length) - 1),
  );
  return sortedValues[index];
}

function aggregateRunMetrics(items) {
  const rows = Array.isArray(items) ? items : [];
  const scored = rows.filter((item) => item?.comparison);
  const latencies = rows
    .map((item) => Number(item?.latency_ms || 0))
    .filter((value) => Number.isFinite(value) && value > 0)
    .sort((a, b) => a - b);
  const totalTokens = rows.reduce((sum, item) => {
    const usage = item?.token_usage && typeof item.token_usage === "object"
      ? item.token_usage
      : {};
    return sum + Number(usage.total_tokens || 0);
  }, 0);
  const estimatedCostUsd = rows.reduce(
    (sum, item) => sum + Number(item?.estimated_cost_usd || 0),
    0,
  );
  const verdictExact = scored.filter((item) => item.comparison.verdict_exact).length;
  const strictExact = scored.filter((item) => item.comparison.strict_exact).length;
  const parseFailures = rows.filter((item) => item?.item_status === "failed").length;
  const providerFailures = rows.filter((item) =>
    String(item?.error_message || "").toLowerCase().includes("provider"),
  ).length;
  const pairwiseAverage =
    scored.length === 0
      ? 0
      : scored.reduce(
          (sum, item) => sum + Number(item.comparison.pairwise_agreement || 0),
          0,
        ) / scored.length;

  return {
    total_items: rows.length,
    scored_items: scored.length,
    verdict_exact_rate:
      scored.length === 0 ? 0 : Number((verdictExact / scored.length).toFixed(4)),
    strict_exact_rate:
      scored.length === 0 ? 0 : Number((strictExact / scored.length).toFixed(4)),
    pairwise_agreement_rate: Number(pairwiseAverage.toFixed(4)),
    false_merge_rate:
      scored.length === 0
        ? 0
        : Number(
            (
              scored.filter((item) => item.comparison.false_merge).length /
              scored.length
            ).toFixed(4),
          ),
    false_dismiss_rate:
      scored.length === 0
        ? 0
        : Number(
            (
              scored.filter((item) => item.comparison.false_dismiss).length /
              scored.length
            ).toFixed(4),
          ),
    parse_failure_rate:
      rows.length === 0 ? 0 : Number((parseFailures / rows.length).toFixed(4)),
    provider_error_rate:
      rows.length === 0 ? 0 : Number((providerFailures / rows.length).toFixed(4)),
    median_latency_ms: percentile(latencies, 50),
    p95_latency_ms: percentile(latencies, 95),
    total_tokens: totalTokens,
    estimated_cost_usd: Number(estimatedCostUsd.toFixed(6)),
  };
}

module.exports = {
  aggregateRunMetrics,
  comparePrediction,
};
