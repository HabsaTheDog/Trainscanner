import { useEffect, useMemo, useState } from "react";
import {
  createEvaluationConfigVersion,
  createGoldSet,
  fetchClusterPicker,
  fetchEvaluationConfigs,
  fetchEvaluationRun,
  fetchEvaluationRuns,
  fetchGoldSet,
  fetchGoldSets,
  replaceGoldSetItems,
  runEvaluationPreview,
  startEvaluationBenchmark,
} from "./ai-evaluation-runtime";
import "./styles.css";

const DEFAULT_SECTIONS = [
  "cluster_summary",
  "candidate_core",
  "aliases",
  "provenance",
  "network_context",
  "network_summary",
  "external_reference_summary",
  "evidence_summary",
  "pair_summaries",
  "cluster_metadata",
];

function prettyJson(value) {
  return JSON.stringify(value ?? {}, null, 2);
}

function parseJson(text, fallback) {
  try {
    return text.trim() ? JSON.parse(text) : fallback;
  } catch {
    return fallback;
  }
}

const VERDICT_COPY = {
  dismiss: "Dismiss cluster",
  merge_only: "Merge candidates",
  group_only: "Group into transfer structure",
  keep_separate_only: "Keep candidates separate",
  rename_only: "Rename only",
  mixed_resolution: "Mixed resolution",
  needs_review: "Needs human review",
};

function getVerdictLabel(verdict) {
  return VERDICT_COPY[verdict] || verdict || "Not available";
}

function formatDateTime(value) {
  if (!value) {
    return "Not available";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return date.toLocaleString();
}

function metricPercent(value) {
  if (typeof value !== "number") {
    return "0%";
  }
  return `${Math.round(value * 100)}%`;
}

function badgeClass(status) {
  switch (status) {
    case "succeeded":
      return "border-green/30 bg-green-dim text-green";
    case "failed":
      return "border-red/30 bg-red-dim text-red";
    case "running":
      return "border-blue/30 bg-blue-dim text-blue";
    case "queued":
      return "border-yellow/30 bg-yellow-dim text-yellow";
    default:
      return "border-border bg-surface-2 text-text-secondary";
  }
}

export function AiEvaluationPage() {
  const url = new URL(globalThis.window.location.href);
  const [loading, setLoading] = useState(true);
  const [notice, setNotice] = useState("");
  const [errorState, setErrorState] = useState(null);
  const [pendingAction, setPendingAction] = useState("");
  const [configs, setConfigs] = useState([]);
  const [runs, setRuns] = useState([]);
  const [goldSets, setGoldSets] = useState([]);
  const [selectedRun, setSelectedRun] = useState(null);
  const [selectedGoldSet, setSelectedGoldSet] = useState(null);
  const [previewResult, setPreviewResult] = useState(null);
  const [previewPickMode, setPreviewPickMode] = useState("resolved");
  const [draft, setDraft] = useState({
    config_key: "",
    name: "Baseline evaluator",
    description: "",
    provider: "litellm",
    model: "openrouter/auto",
    model_params: prettyJson({ temperature: 0, top_p: 1 }),
    system_prompt:
      "Evaluate whether candidate train station records should be merged, grouped, renamed, dismissed, or kept separate. Return compact JSON only.",
    context_sections: DEFAULT_SECTIONS.join("\n"),
    context_preamble: "",
  });
  const [previewClusterId, setPreviewClusterId] = useState(
    url.searchParams.get("clusterId") || "",
  );
  const [selectedConfigRef, setSelectedConfigRef] = useState("");
  const [benchmarkForm, setBenchmarkForm] = useState({
    dataset_source: "resolved_history",
    gold_set_id: "",
    country: "",
    severity: "",
    limit: "100",
  });
  const [goldSetForm, setGoldSetForm] = useState({
    name: "Regression Gold Set",
    description: "",
    clusterIds: "",
  });

  async function loadOverview() {
    setLoading(true);
    try {
      const [nextConfigs, nextRuns, nextGoldSets] = await Promise.all([
        fetchEvaluationConfigs(),
        fetchEvaluationRuns({ limit: 20 }),
        fetchGoldSets(),
      ]);
      setConfigs(nextConfigs);
      setRuns(Array.isArray(nextRuns.items) ? nextRuns.items : []);
      setGoldSets(nextGoldSets);
      if (!selectedConfigRef && nextConfigs[0]) {
        setSelectedConfigRef(
          `${nextConfigs[0].config_key}:${nextConfigs[0].version}`,
        );
      }
    } catch (error) {
      setNotice(error.message);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadOverview();
  }, []);

  useEffect(() => {
    if (!selectedRun || !["queued", "running"].includes(selectedRun.status)) {
      return undefined;
    }
    const timer = globalThis.setInterval(async () => {
      try {
        const nextRun = await fetchEvaluationRun(selectedRun.run_id);
        setSelectedRun(nextRun);
        await loadOverview();
      } catch (error) {
        setErrorState({
          actionKey: "select-run",
          message: `Could not refresh the running benchmark. ${error.message}`,
        });
      }
    }, 3000);
    return () => globalThis.clearInterval(timer);
  }, [selectedRun]);

  const selectedConfig = useMemo(
    () =>
      configs.find(
        (config) =>
          `${config.config_key}:${config.version}` === selectedConfigRef,
      ) || null,
    [configs, selectedConfigRef],
  );
  const activeConfig = selectedConfig || {
    ...draft,
    context_sections: draft.context_sections
      .split("\n")
      .map((value) => value.trim())
      .filter(Boolean),
    model_params: parseJson(draft.model_params, { temperature: 0, top_p: 1 }),
  };
  const previewTruth = previewResult?.result?.truth_snapshot || null;
  const previewPrediction = previewResult?.result?.normalized_prediction || null;
  const previewComparison = previewResult?.result?.comparison || null;
  const benchmarkRuns = runs.filter((run) => run.mode === "benchmark");
  const selectedRunItems = Array.isArray(selectedRun?.items) ? selectedRun.items : [];

  function clearMessages() {
    setNotice("");
    setErrorState(null);
  }

  async function runAction(actionKey, callback, options = {}) {
    const { preserveNotice = false, failureContext = "" } = options;
    setPendingAction(actionKey);
    if (!preserveNotice) {
      setNotice("");
    }
    setErrorState(null);
    try {
      return await callback();
    } catch (error) {
      const message = failureContext
        ? `${failureContext} ${error.message}`
        : error.message;
      setErrorState({
        actionKey,
        message,
      });
      return null;
    } finally {
      setPendingAction("");
    }
  }

  async function handleCreateConfig(event) {
    event.preventDefault();
    await runAction(
      "save-config",
      async () => {
        const created = await createEvaluationConfigVersion({
          config_key: draft.config_key || undefined,
          name: draft.name,
          description: draft.description,
          provider: draft.provider,
          model: draft.model,
          model_params: parseJson(draft.model_params, { temperature: 0, top_p: 1 }),
          system_prompt: draft.system_prompt,
          context_sections: draft.context_sections
            .split("\n")
            .map((value) => value.trim())
            .filter(Boolean),
          context_preamble: draft.context_preamble,
        });
        setNotice(`Saved config ${created.config_key} v${created.version}`);
        await loadOverview();
        setSelectedConfigRef(`${created.config_key}:${created.version}`);
      },
      {
        failureContext: "Could not save the config version.",
      },
    );
  }

  async function handlePreview(event) {
    event.preventDefault();
    if (!previewClusterId.trim()) {
      setErrorState({
        actionKey: "preview",
        message: "Enter a cluster id before running the test.",
      });
      return;
    }
    await runAction(
      "preview",
      async () => {
        const payload = selectedConfig
          ? {
              config_key: selectedConfig.config_key,
              version: selectedConfig.version,
            }
          : {
              draft_config: {
                config_key: draft.config_key || undefined,
                name: draft.name,
                description: draft.description,
                provider: draft.provider,
                model: draft.model,
                model_params: parseJson(draft.model_params, {
                  temperature: 0,
                  top_p: 1,
                }),
                system_prompt: draft.system_prompt,
                context_sections: draft.context_sections
                  .split("\n")
                  .map((value) => value.trim())
                  .filter(Boolean),
                context_preamble: draft.context_preamble,
              },
            };
        const result = await runEvaluationPreview(previewClusterId, payload);
        setPreviewResult(result);
        setSelectedRun(result.run);
        setNotice(`Completed cluster test for ${previewClusterId}.`);
        await loadOverview();
      },
      {
        failureContext: "Could not run the single-cluster test.",
      },
    );
  }

  async function handlePickCluster(randomize = false) {
    await runAction(
      randomize ? "pick-random-cluster" : "pick-next-cluster",
      async () => {
        const rows = await fetchClusterPicker(previewPickMode);
        if (!rows.length) {
          setNotice(`No ${previewPickMode} clusters available.`);
          return;
        }
        const picked = randomize
          ? rows[Math.floor(Math.random() * rows.length)]
          : rows[0];
        setPreviewClusterId(picked.cluster_id);
        setNotice(
          `${randomize ? "Picked random" : "Picked next"} ${previewPickMode} cluster: ${picked.cluster_id}`,
        );
      },
      {
        preserveNotice: true,
        failureContext: `Could not load ${previewPickMode} clusters.`,
      },
    );
  }

  async function handleStartBenchmark(event) {
    event.preventDefault();
    if (!selectedConfig) {
      setNotice("Select a saved config version before starting a benchmark.");
      return;
    }
    await runAction(
      "start-benchmark",
      async () => {
        const run = await startEvaluationBenchmark({
          config_key: selectedConfig.config_key,
          version: selectedConfig.version,
          dataset_source: benchmarkForm.dataset_source,
          gold_set_id: benchmarkForm.gold_set_id || null,
          filters: {
            country: benchmarkForm.country || null,
            severity: benchmarkForm.severity || null,
            limit: Number.parseInt(benchmarkForm.limit, 10) || 100,
          },
        });
        setSelectedRun(run);
        setNotice(`Started benchmark ${run.run_id}`);
        await loadOverview();
      },
      {
        failureContext: "Could not start the benchmark.",
      },
    );
  }

  async function handleSelectRun(runId) {
    await runAction(
      "select-run",
      async () => {
        setSelectedRun(await fetchEvaluationRun(runId));
      },
      {
        preserveNotice: true,
        failureContext: "Could not load that benchmark run.",
      },
    );
  }

  async function handleCreateGoldSet(event) {
    event.preventDefault();
    await runAction(
      "create-gold-set",
      async () => {
        const created = await createGoldSet({
          name: goldSetForm.name,
          description: goldSetForm.description,
        });
        setSelectedGoldSet(await fetchGoldSet(created.gold_set_id));
        setNotice(`Created gold set ${created.name}`);
        await loadOverview();
      },
      {
        failureContext: "Could not create the gold set.",
      },
    );
  }

  async function handleReplaceGoldSetItems(event) {
    event.preventDefault();
    if (!selectedGoldSet) {
      setNotice("Create or select a gold set first.");
      return;
    }
    await runAction(
      "replace-gold-set-items",
      async () => {
        const clusterIds = goldSetForm.clusterIds
          .split(/\s+/)
          .map((value) => value.trim())
          .filter(Boolean);
        const updated = await replaceGoldSetItems(
          selectedGoldSet.gold_set_id,
          clusterIds,
        );
        setSelectedGoldSet(updated);
        setNotice(`Updated ${updated.items.length} gold-set items.`);
        await loadOverview();
      },
      {
        failureContext: "Could not update the gold set items.",
      },
    );
  }

  async function handleRetryLastAction() {
    if (!errorState?.actionKey) {
      return;
    }
    switch (errorState.actionKey) {
      case "pick-next-cluster":
        await handlePickCluster(false);
        break;
      case "pick-random-cluster":
        await handlePickCluster(true);
        break;
      case "preview":
        await handlePreview({ preventDefault() {} });
        break;
      case "start-benchmark":
        await handleStartBenchmark({ preventDefault() {} });
        break;
      case "select-run":
      case "save-config":
      case "create-gold-set":
      case "replace-gold-set-items":
      default:
        await loadOverview();
        break;
    }
  }

  return (
    <main className="max-w-[1520px] mx-auto my-6 px-4 grid grid-cols-1 gap-4">
      <section className="relative overflow-hidden rounded-[28px] border border-border bg-surface-1 p-6 shadow-[0_24px_80px_rgba(0,0,0,0.35)]">
        <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top_left,rgba(6,182,212,0.16),transparent_32%),radial-gradient(circle_at_top_right,rgba(245,158,11,0.12),transparent_28%)]" />
        <div className="relative flex flex-wrap items-start gap-4">
          <div className="max-w-3xl">
            <div className="inline-flex items-center rounded-full border border-teal/30 bg-teal-dim px-3 py-1 text-[11px] uppercase tracking-[0.24em] text-teal font-display">
              Evaluation Harness
            </div>
            <h1 className="mt-4 mb-3 text-3xl font-bold tracking-tight text-text-primary font-display">
              Test the model against clusters humans already reviewed
            </h1>
            <p className="m-0 max-w-2xl text-text-secondary leading-7">
              This page does not let the AI edit anything. It shows the cluster
              context to the model, captures the model&apos;s proposed resolution,
              and compares that prediction against the human-reviewed outcome
              already stored in QA.
            </p>
          </div>
          <div className="ml-auto flex gap-3 text-sm">
            <a
              href="/curation.html"
              className="text-amber no-underline font-semibold hover:text-amber-hover"
            >
              &larr; Back To Curation
            </a>
            <a
              href="/"
              className="text-teal no-underline font-semibold hover:text-white"
            >
              Home
            </a>
          </div>
        </div>
        <div className="relative mt-6 grid grid-cols-1 md:grid-cols-3 gap-3">
          {[
            [
              "1. Pick a cluster",
              "Paste an id or grab the next or a random cluster from the queue.",
            ],
            [
              "2. Ask the model",
              "The AI gets the system prompt plus sanitized cluster context and returns JSON only.",
            ],
            [
              "3. Compare to human truth",
              "We compare the model verdict and structure against the existing reviewed decision.",
            ],
          ].map(([title, text]) => (
            <div
              key={title}
              className="rounded-2xl border border-border bg-[linear-gradient(180deg,rgba(255,255,255,0.03),rgba(255,255,255,0.01))] p-4"
            >
              <div className="text-sm font-semibold text-text-primary">{title}</div>
              <div className="mt-2 text-sm leading-6 text-text-secondary">{text}</div>
            </div>
          ))}
        </div>
        {notice ? (
          <div className="relative mt-4 rounded-2xl border border-blue/20 bg-blue-dim px-4 py-3 text-sm text-blue">
            {notice}
          </div>
        ) : null}
        {errorState ? (
          <div className="relative mt-4 flex flex-wrap items-center justify-between gap-3 rounded-2xl border border-red/25 bg-red-dim px-4 py-3 text-sm text-red">
            <span>{errorState.message}</span>
            <div className="flex gap-2">
              <button
                type="button"
                onClick={() => void handleRetryLastAction()}
                className="rounded-xl border border-red/40 bg-surface-1 px-3 py-2 text-xs font-semibold text-red cursor-pointer"
              >
                Retry
              </button>
              <button
                type="button"
                onClick={clearMessages}
                className="rounded-xl border border-border bg-surface-1 px-3 py-2 text-xs font-semibold text-text-secondary cursor-pointer"
              >
                Dismiss
              </button>
            </div>
          </div>
        ) : null}
      </section>

      <div className="grid grid-cols-1 xl:grid-cols-[360px_1fr] gap-4">
        <aside className="grid grid-cols-1 gap-4">
          <section className="rounded-[24px] border border-border bg-surface-1 p-5">
            <div className="flex items-center justify-between gap-3">
              <div>
                <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted">
                  Active Setup
                </div>
                <h2 className="m-0 mt-2 text-xl font-bold font-display text-text-primary">
                  Model + prompt
                </h2>
              </div>
              <span
                className={`rounded-full border px-3 py-1 text-xs font-display ${selectedConfig ? "border-green/30 bg-green-dim text-green" : "border-yellow/30 bg-yellow-dim text-yellow"}`}
              >
                {selectedConfig ? "saved config" : "draft config"}
              </span>
            </div>

            <div className="mt-4 grid grid-cols-1 gap-3">
              <div className="rounded-2xl border border-border bg-surface-2 p-4">
                <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted">
                  Using
                </div>
                <div className="mt-2 text-base font-semibold text-text-primary">
                  {selectedConfig
                    ? `${selectedConfig.config_key} v${selectedConfig.version}`
                    : draft.name}
                </div>
                <div className="mt-1 text-sm text-text-secondary">
                  Model: {activeConfig.model || "not set"}
                </div>
                <div className="mt-1 text-sm text-text-secondary">
                  Context sections:{" "}
                  {Array.isArray(activeConfig.context_sections)
                    ? activeConfig.context_sections.length
                    : 0}
                </div>
              </div>

              <div>
                <label className="block text-xs uppercase tracking-[0.2em] font-display text-text-muted mb-2">
                  Saved Config Versions
                </label>
                <select
                  value={selectedConfigRef}
                  onChange={(event) => setSelectedConfigRef(event.target.value)}
                  className="w-full bg-surface-2 border border-border rounded-xl px-3 py-3 text-sm text-text-primary"
                >
                  <option value="">Use current draft</option>
                  {configs.map((config) => (
                    <option
                      key={`${config.config_key}:${config.version}`}
                      value={`${config.config_key}:${config.version}`}
                    >
                      {config.config_key} v{config.version} · {config.model}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            <details className="mt-4 rounded-2xl border border-border bg-surface-2">
              <summary className="cursor-pointer list-none px-4 py-3 text-sm font-semibold text-text-primary">
                Edit prompt, model, and context
              </summary>
              <form className="grid grid-cols-1 gap-2 px-4 pb-4" onSubmit={handleCreateConfig}>
                <input
                  value={draft.config_key}
                  onChange={(event) =>
                    setDraft((prev) => ({ ...prev, config_key: event.target.value }))
                  }
                  placeholder="config key"
                  className="bg-surface-1 border border-border rounded-xl px-3 py-2 text-sm text-text-primary"
                />
                <input
                  value={draft.name}
                  onChange={(event) =>
                    setDraft((prev) => ({ ...prev, name: event.target.value }))
                  }
                  placeholder="display name"
                  className="bg-surface-1 border border-border rounded-xl px-3 py-2 text-sm text-text-primary"
                />
                <input
                  value={draft.model}
                  onChange={(event) =>
                    setDraft((prev) => ({ ...prev, model: event.target.value }))
                  }
                  placeholder="model id"
                  className="bg-surface-1 border border-border rounded-xl px-3 py-2 text-sm text-text-primary"
                />
                <textarea
                  rows={3}
                  value={draft.description}
                  onChange={(event) =>
                    setDraft((prev) => ({ ...prev, description: event.target.value }))
                  }
                  placeholder="description"
                  className="bg-surface-1 border border-border rounded-xl px-3 py-2 text-sm text-text-primary"
                />
                <textarea
                  rows={4}
                  value={draft.model_params}
                  onChange={(event) =>
                    setDraft((prev) => ({ ...prev, model_params: event.target.value }))
                  }
                  className="bg-surface-1 border border-border rounded-xl px-3 py-2 text-sm text-text-primary font-display"
                />
                <textarea
                  rows={4}
                  value={draft.context_sections}
                  onChange={(event) =>
                    setDraft((prev) => ({
                      ...prev,
                      context_sections: event.target.value,
                    }))
                  }
                  className="bg-surface-1 border border-border rounded-xl px-3 py-2 text-sm text-text-primary font-display"
                />
                <textarea
                  rows={8}
                  value={draft.system_prompt}
                  onChange={(event) =>
                    setDraft((prev) => ({ ...prev, system_prompt: event.target.value }))
                  }
                  className="bg-surface-1 border border-border rounded-xl px-3 py-2 text-sm text-text-primary"
                />
                <button
                  type="submit"
                  className="mt-2 px-4 py-3 rounded-xl font-semibold text-sm bg-amber text-surface-0 hover:bg-amber-hover transition-all border-none cursor-pointer"
                >
                  Save Config Version
                </button>
              </form>
            </details>
          </section>

          <section className="rounded-[24px] border border-border bg-surface-1 p-5">
            <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted">
              What the model can decide
            </div>
            <div className="mt-3 grid grid-cols-1 gap-2">
              {Object.entries(VERDICT_COPY).map(([key, label]) => (
                <div
                  key={key}
                  className="rounded-xl border border-border bg-surface-2 px-3 py-2"
                >
                  <div className="text-sm font-semibold text-text-primary">{label}</div>
                  <div className="mt-1 text-xs font-display text-text-muted">{key}</div>
                </div>
              ))}
            </div>
            <div className="mt-4 rounded-xl border border-border bg-surface-2 p-3 text-sm leading-6 text-text-secondary">
              The model cannot apply changes to QA. It only returns a proposed
              resolution in JSON. Humans remain the source of truth.
            </div>
          </section>
        </aside>

        <section className="grid grid-cols-1 gap-4">
          <section className="rounded-[24px] border border-border bg-surface-1 p-5">
            <div className="flex flex-wrap items-end gap-3 justify-between">
              <div>
                <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted">
                  Test One Cluster
                </div>
                <h2 className="m-0 mt-2 text-2xl font-bold font-display text-text-primary">
                  Run a single AI evaluation
                </h2>
                <p className="m-0 mt-2 text-text-secondary max-w-2xl leading-7">
                  Use this when you want to inspect exactly what the AI predicts
                  for one cluster and compare it to the stored human decision.
                </p>
              </div>
              <div className="rounded-2xl border border-border bg-surface-2 px-4 py-3 text-sm text-text-secondary">
                Active model: <span className="text-text-primary font-semibold">{activeConfig.model || "not set"}</span>
              </div>
            </div>

            <form className="mt-5 grid grid-cols-1 gap-3" onSubmit={handlePreview}>
              <div className="grid grid-cols-1 lg:grid-cols-[1fr_auto] gap-3">
                <input
                  value={previewClusterId}
                  onChange={(event) => setPreviewClusterId(event.target.value)}
                  placeholder="Enter a cluster id"
                  className="bg-surface-2 border border-border rounded-2xl px-4 py-3 text-sm text-text-primary"
                />
                <button
                  type="submit"
                  disabled={pendingAction === "preview"}
                  className="px-5 py-3 rounded-2xl font-semibold text-sm bg-teal text-surface-0 border-none cursor-pointer"
                >
                  {pendingAction === "preview" ? "Testing…" : "Test This Cluster"}
                </button>
              </div>
              <div className="flex flex-wrap items-center gap-2">
                <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted">
                  Quick pick
                </div>
                <select
                  value={previewPickMode}
                  onChange={(event) => setPreviewPickMode(event.target.value)}
                  className="bg-surface-2 border border-border rounded-xl px-3 py-2 text-sm text-text-primary"
                >
                  <option value="resolved">resolved clusters</option>
                  <option value="dismissed">dismissed clusters</option>
                  <option value="open">open clusters</option>
                  <option value="in_review">in review clusters</option>
                </select>
                <button
                  type="button"
                  disabled={pendingAction === "pick-next-cluster"}
                  onClick={() => void handlePickCluster(false)}
                  className="px-4 py-2 rounded-xl font-semibold text-sm bg-blue text-surface-0 border-none cursor-pointer"
                >
                  {pendingAction === "pick-next-cluster"
                    ? "Loading…"
                    : "Use Next Cluster"}
                </button>
                <button
                  type="button"
                  disabled={pendingAction === "pick-random-cluster"}
                  onClick={() => void handlePickCluster(true)}
                  className="px-4 py-2 rounded-xl font-semibold text-sm bg-orange text-surface-0 border-none cursor-pointer"
                >
                  {pendingAction === "pick-random-cluster"
                    ? "Loading…"
                    : "Use Random Cluster"}
                </button>
              </div>
            </form>

            {previewResult ? (
              <div className="mt-6 grid grid-cols-1 xl:grid-cols-[1.2fr_1fr] gap-4">
                <div className="grid grid-cols-1 gap-4">
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                    <div className="rounded-2xl border border-border bg-surface-2 p-4">
                      <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted">
                        AI predicted
                      </div>
                      <div className="mt-2 text-lg font-semibold text-text-primary">
                        {getVerdictLabel(previewPrediction?.verdict)}
                      </div>
                      <div className="mt-1 text-xs font-display text-text-muted">
                        {previewPrediction?.verdict || "n/a"}
                      </div>
                    </div>
                    <div className="rounded-2xl border border-border bg-surface-2 p-4">
                      <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted">
                        Human decided
                      </div>
                      <div className="mt-2 text-lg font-semibold text-text-primary">
                        {getVerdictLabel(previewTruth?.verdict)}
                      </div>
                      <div className="mt-1 text-xs font-display text-text-muted">
                        {previewTruth?.verdict || "n/a"}
                      </div>
                    </div>
                    <div className="rounded-2xl border border-border bg-surface-2 p-4">
                      <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted">
                        Agreement
                      </div>
                      <div className="mt-2 text-lg font-semibold text-text-primary">
                        {previewComparison?.verdict_exact ? "Verdict match" : "Mismatch"}
                      </div>
                      <div className="mt-1 text-xs text-text-muted">
                        {previewComparison?.diff_summary || "No comparison available"}
                      </div>
                    </div>
                  </div>

                  <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                    <div className="rounded-2xl border border-border bg-surface-2 p-4">
                      <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted">
                        Confidence
                      </div>
                      <div className="mt-2 text-xl font-semibold text-text-primary">
                        {metricPercent(previewPrediction?.confidence_score)}
                      </div>
                    </div>
                    <div className="rounded-2xl border border-border bg-surface-2 p-4">
                      <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted">
                        Latency
                      </div>
                      <div className="mt-2 text-xl font-semibold text-text-primary">
                        {previewResult.result.latency_ms ?? 0} ms
                      </div>
                    </div>
                    <div className="rounded-2xl border border-border bg-surface-2 p-4">
                      <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted">
                        Estimated cost
                      </div>
                      <div className="mt-2 text-xl font-semibold text-text-primary">
                        ${Number(previewResult.result.estimated_cost_usd || 0).toFixed(6)}
                      </div>
                    </div>
                  </div>

                  <div className="rounded-[24px] border border-border bg-surface-2 p-4">
                    <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted">
                      Why the model chose this
                    </div>
                    <div className="mt-3 text-sm leading-7 text-text-primary">
                      {previewPrediction?.rationale || "No rationale returned."}
                    </div>
                  </div>

                  <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                    <div className="rounded-[24px] border border-border bg-surface-2 p-4">
                      <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted mb-3">
                        Human truth snapshot
                      </div>
                      <pre className="m-0 max-h-[360px] overflow-auto text-xs text-text-secondary">
                        {prettyJson(previewTruth)}
                      </pre>
                    </div>
                    <div className="rounded-[24px] border border-border bg-surface-2 p-4">
                      <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted mb-3">
                        AI prediction snapshot
                      </div>
                      <pre className="m-0 max-h-[360px] overflow-auto text-xs text-text-secondary">
                        {prettyJson(previewPrediction)}
                      </pre>
                    </div>
                  </div>
                </div>

                <div className="grid grid-cols-1 gap-4">
                  <div className="rounded-[24px] border border-border bg-surface-2 p-4">
                    <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted mb-3">
                      Comparison details
                    </div>
                    <pre className="m-0 max-h-[220px] overflow-auto text-xs text-text-secondary">
                      {prettyJson(previewComparison)}
                    </pre>
                  </div>
                  <div className="rounded-[24px] border border-border bg-surface-2 p-4">
                    <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted mb-3">
                      Raw model response
                    </div>
                    <pre className="m-0 max-h-[420px] overflow-auto text-xs text-text-secondary">
                      {prettyJson(previewResult.result.raw_model_response)}
                    </pre>
                  </div>
                </div>
              </div>
            ) : (
              <div className="mt-6 rounded-[24px] border border-dashed border-border-strong bg-surface-2 px-5 py-8 text-sm leading-7 text-text-secondary">
                Run a single cluster test to see three things side by side:
                what the AI predicted, what the human ultimately decided, and
                how closely those two match.
              </div>
            )}
          </section>

          <section className="rounded-[24px] border border-border bg-surface-1 p-5">
            <div className="flex flex-wrap items-end gap-3 justify-between">
              <div>
                <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted">
                  Test Many Reviewed Clusters
                </div>
                <h2 className="m-0 mt-2 text-2xl font-bold font-display text-text-primary">
                  Run a benchmark
                </h2>
                <p className="m-0 mt-2 max-w-2xl text-text-secondary leading-7">
                  This runs the chosen model and prompt across already reviewed
                  clusters and reports how often the AI agrees with the human
                  decision.
                </p>
              </div>
              <div className="rounded-2xl border border-border bg-surface-2 px-4 py-3 text-sm text-text-secondary">
                Primary dataset: <span className="text-text-primary font-semibold">reviewed history</span>
              </div>
            </div>

            <form className="mt-5 grid grid-cols-1 lg:grid-cols-[1fr_1fr_1fr_auto] gap-3" onSubmit={handleStartBenchmark}>
              <input
                value={benchmarkForm.country}
                onChange={(event) =>
                  setBenchmarkForm((prev) => ({ ...prev, country: event.target.value }))
                }
                placeholder="optional country filter"
                className="bg-surface-2 border border-border rounded-2xl px-4 py-3 text-sm text-text-primary"
              />
              <input
                value={benchmarkForm.severity}
                onChange={(event) =>
                  setBenchmarkForm((prev) => ({ ...prev, severity: event.target.value }))
                }
                placeholder="optional severity filter"
                className="bg-surface-2 border border-border rounded-2xl px-4 py-3 text-sm text-text-primary"
              />
              <input
                value={benchmarkForm.limit}
                onChange={(event) =>
                  setBenchmarkForm((prev) => ({ ...prev, limit: event.target.value }))
                }
                placeholder="how many reviewed clusters"
                className="bg-surface-2 border border-border rounded-2xl px-4 py-3 text-sm text-text-primary"
              />
              <button
                type="submit"
                disabled={pendingAction === "start-benchmark"}
                className="px-5 py-3 rounded-2xl font-semibold text-sm bg-amber text-surface-0 border-none cursor-pointer"
              >
                {pendingAction === "start-benchmark"
                  ? "Starting…"
                  : "Start Benchmark"}
              </button>
            </form>

            <details className="mt-4 rounded-2xl border border-border bg-surface-2">
              <summary className="cursor-pointer list-none px-4 py-3 text-sm font-semibold text-text-primary">
                Advanced benchmark options
              </summary>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3 px-4 pb-4">
                <div>
                  <label className="block text-xs uppercase tracking-[0.2em] font-display text-text-muted mb-2">
                    Dataset source
                  </label>
                  <select
                    value={benchmarkForm.dataset_source}
                    onChange={(event) =>
                      setBenchmarkForm((prev) => ({
                        ...prev,
                        dataset_source: event.target.value,
                      }))
                    }
                    className="w-full bg-surface-1 border border-border rounded-xl px-3 py-2 text-sm text-text-primary"
                  >
                    <option value="resolved_history">resolved_history</option>
                    <option value="gold_set">gold_set</option>
                    <option value="combined">combined</option>
                  </select>
                </div>
                <div>
                  <label className="block text-xs uppercase tracking-[0.2em] font-display text-text-muted mb-2">
                    Gold set
                  </label>
                  <select
                    value={benchmarkForm.gold_set_id}
                    onChange={(event) =>
                      setBenchmarkForm((prev) => ({
                        ...prev,
                        gold_set_id: event.target.value,
                      }))
                    }
                    className="w-full bg-surface-1 border border-border rounded-xl px-3 py-2 text-sm text-text-primary"
                  >
                    <option value="">none</option>
                    {goldSets.map((goldSet) => (
                      <option key={goldSet.gold_set_id} value={goldSet.gold_set_id}>
                        {goldSet.name}
                      </option>
                    ))}
                  </select>
                </div>
              </div>
            </details>

            <div className="mt-6 grid grid-cols-1 xl:grid-cols-[340px_1fr] gap-4">
              <div className="rounded-[24px] border border-border bg-surface-2 p-4">
                <div className="flex items-center justify-between gap-3">
                  <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted">
                    Recent benchmark runs
                  </div>
                  <div className="text-xs font-display text-text-muted">
                    {benchmarkRuns.length} shown
                  </div>
                </div>
                <div className="mt-3 grid grid-cols-1 gap-2">
                  {benchmarkRuns.map((run) => (
                    <button
                      key={run.run_id}
                      type="button"
                      disabled={pendingAction === "select-run"}
                      onClick={() => void handleSelectRun(run.run_id)}
                      className="text-left rounded-2xl border border-border bg-surface-1 px-3 py-3 cursor-pointer hover:border-border-strong"
                    >
                      <div className="flex items-center justify-between gap-3">
                        <span
                          className={`rounded-full border px-2 py-1 text-[11px] uppercase tracking-[0.14em] font-display ${badgeClass(run.status)}`}
                        >
                          {run.status}
                        </span>
                        <span className="text-xs text-text-muted">
                          {formatDateTime(run.created_at)}
                        </span>
                      </div>
                      <div className="mt-3 text-sm font-semibold text-text-primary">
                        {run.config_snapshot?.name || run.config_snapshot?.config_key || "benchmark"}
                      </div>
                      <div className="mt-1 text-xs text-text-muted">
                        {run.progress?.completed_items || 0}/{run.progress?.total_items || 0} clusters
                      </div>
                    </button>
                  ))}
                </div>
              </div>

              <div className="rounded-[24px] border border-border bg-surface-2 p-4">
                {selectedRun ? (
                  <div className="grid grid-cols-1 gap-4">
                    <div className="flex flex-wrap items-start justify-between gap-3">
                      <div>
                        <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted">
                          Selected run
                        </div>
                        <div className="mt-2 text-xl font-semibold text-text-primary">
                          {selectedRun.config_snapshot?.name || selectedRun.run_id}
                        </div>
                        <div className="mt-1 text-sm text-text-secondary">
                          {selectedRun.mode} · created {formatDateTime(selectedRun.created_at)}
                        </div>
                      </div>
                      <span
                        className={`rounded-full border px-3 py-1 text-xs uppercase tracking-[0.14em] font-display ${badgeClass(selectedRun.status)}`}
                      >
                        {selectedRun.status}
                      </span>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
                      <div className="rounded-2xl border border-border bg-surface-1 p-4">
                        <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted">
                          Verdict match
                        </div>
                        <div className="mt-2 text-xl font-semibold text-text-primary">
                          {metricPercent(selectedRun.summary_metrics?.verdict_exact_rate)}
                        </div>
                      </div>
                      <div className="rounded-2xl border border-border bg-surface-1 p-4">
                        <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted">
                          Strict match
                        </div>
                        <div className="mt-2 text-xl font-semibold text-text-primary">
                          {metricPercent(selectedRun.summary_metrics?.strict_exact_rate)}
                        </div>
                      </div>
                      <div className="rounded-2xl border border-border bg-surface-1 p-4">
                        <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted">
                          Pairwise agreement
                        </div>
                        <div className="mt-2 text-xl font-semibold text-text-primary">
                          {metricPercent(selectedRun.summary_metrics?.pairwise_agreement_rate)}
                        </div>
                      </div>
                      <div className="rounded-2xl border border-border bg-surface-1 p-4">
                        <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted">
                          Total clusters
                        </div>
                        <div className="mt-2 text-xl font-semibold text-text-primary">
                          {selectedRun.summary_metrics?.total_items || 0}
                        </div>
                      </div>
                    </div>

                    <div className="grid grid-cols-1 xl:grid-cols-[1fr_420px] gap-4">
                      <div className="rounded-2xl border border-border bg-surface-1 p-4">
                        <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted mb-3">
                          Run items
                        </div>
                        <div className="grid grid-cols-1 gap-2 max-h-[420px] overflow-auto pr-1">
                          {selectedRunItems.length ? (
                            selectedRunItems.map((item) => (
                              <div
                                key={item.run_item_id}
                                className="rounded-2xl border border-border bg-surface-2 px-3 py-3"
                              >
                                <div className="flex items-center justify-between gap-3">
                                  <div className="text-sm font-semibold text-text-primary">
                                    {item.merge_cluster_id}
                                  </div>
                                  <span
                                    className={`rounded-full border px-2 py-1 text-[11px] uppercase tracking-[0.14em] font-display ${badgeClass(item.item_status)}`}
                                  >
                                    {item.item_status}
                                  </span>
                                </div>
                                <div className="mt-2 text-sm text-text-secondary">
                                  Human: {getVerdictLabel(item.truth_snapshot?.verdict)}
                                </div>
                                <div className="mt-1 text-sm text-text-secondary">
                                  AI: {getVerdictLabel(item.normalized_prediction?.verdict)}
                                </div>
                                <div className="mt-1 text-xs text-text-muted">
                                  {item.comparison?.diff_summary || item.error_message || "No diff yet"}
                                </div>
                              </div>
                            ))
                          ) : (
                            <div className="rounded-2xl border border-dashed border-border-strong bg-surface-2 px-4 py-6 text-sm text-text-secondary">
                              This run does not have item details yet.
                            </div>
                          )}
                        </div>
                      </div>
                      <div className="rounded-2xl border border-border bg-surface-1 p-4">
                        <div className="text-xs uppercase tracking-[0.2em] font-display text-text-muted mb-3">
                          Raw run JSON
                        </div>
                        <pre className="m-0 max-h-[420px] overflow-auto text-xs text-text-secondary">
                          {prettyJson(selectedRun)}
                        </pre>
                      </div>
                    </div>
                  </div>
                ) : (
                  <div className="rounded-2xl border border-dashed border-border-strong bg-surface-1 px-5 py-8 text-sm leading-7 text-text-secondary">
                    Start a benchmark or open a recent run to see the batch
                    metrics here.
                  </div>
                )}
              </div>
            </div>
          </section>

          <details className="rounded-[24px] border border-border bg-surface-1 p-5">
            <summary className="cursor-pointer list-none text-lg font-bold font-display text-text-primary">
              Advanced: gold sets and dataset curation
            </summary>
            <p className="mt-3 mb-0 max-w-3xl text-sm leading-7 text-text-secondary">
              Gold sets are optional. You do not need them to test single clusters
              or benchmark against reviewed history. Keep using the sections above
              unless you are explicitly curating a frozen regression dataset.
            </p>
            <div className="mt-4 grid grid-cols-1 xl:grid-cols-[320px_1fr] gap-4">
              <div className="rounded-2xl border border-border bg-surface-2 p-4">
                <form className="grid grid-cols-1 gap-2" onSubmit={handleCreateGoldSet}>
                  <input
                    value={goldSetForm.name}
                    onChange={(event) =>
                      setGoldSetForm((prev) => ({ ...prev, name: event.target.value }))
                    }
                    className="bg-surface-1 border border-border rounded-xl px-3 py-2 text-sm text-text-primary"
                    placeholder="gold set name"
                  />
                  <input
                    value={goldSetForm.description}
                    onChange={(event) =>
                      setGoldSetForm((prev) => ({
                        ...prev,
                        description: event.target.value,
                      }))
                    }
                    className="bg-surface-1 border border-border rounded-xl px-3 py-2 text-sm text-text-primary"
                    placeholder="description"
                  />
                  <button
                    type="submit"
                    disabled={pendingAction === "create-gold-set"}
                    className="px-4 py-2 rounded-xl font-semibold text-sm bg-green text-surface-0 border-none cursor-pointer"
                  >
                    {pendingAction === "create-gold-set"
                      ? "Creating…"
                      : "Create Gold Set"}
                  </button>
                </form>
                <div className="mt-4 grid grid-cols-1 gap-2">
                  {goldSets.map((goldSet) => (
                    <button
                      key={goldSet.gold_set_id}
                      type="button"
                      onClick={async () =>
                        setSelectedGoldSet(await fetchGoldSet(goldSet.gold_set_id))
                      }
                      className="text-left rounded-xl border border-border bg-surface-1 px-3 py-2 cursor-pointer hover:border-border-strong"
                    >
                      <div className="text-sm font-semibold text-text-primary">{goldSet.name}</div>
                      <div className="text-xs text-text-muted">{goldSet.slug}</div>
                    </button>
                  ))}
                </div>
              </div>
              <div className="rounded-2xl border border-border bg-surface-2 p-4">
                <form className="grid grid-cols-1 gap-2" onSubmit={handleReplaceGoldSetItems}>
                  <textarea
                    rows={5}
                    value={goldSetForm.clusterIds}
                    onChange={(event) =>
                      setGoldSetForm((prev) => ({
                        ...prev,
                        clusterIds: event.target.value,
                      }))
                    }
                    placeholder="cluster ids separated by whitespace"
                    className="bg-surface-1 border border-border rounded-xl px-3 py-2 text-sm text-text-primary font-display"
                  />
                  <button
                    type="submit"
                    disabled={pendingAction === "replace-gold-set-items"}
                    className="px-4 py-2 rounded-xl font-semibold text-sm bg-blue text-surface-0 border-none cursor-pointer"
                  >
                    {pendingAction === "replace-gold-set-items"
                      ? "Updating…"
                      : "Replace Gold Set Items"}
                  </button>
                </form>
                <pre className="m-0 mt-3 rounded-2xl border border-border bg-surface-1 p-3 text-xs text-text-secondary overflow-auto min-h-[180px]">
                  {prettyJson(selectedGoldSet)}
                </pre>
              </div>
            </div>
          </details>
        </section>
      </div>

      {loading ? (
        <section className="text-text-muted text-sm font-display">Loading…</section>
      ) : null}
    </main>
  );
}
