const crypto = require("node:crypto");
const fs = require("node:fs/promises");
const path = require("node:path");

const { createPostgisClient } = require("../../data/postgis/client");
const {
  createPipelineJobsRepo,
} = require("../../data/postgis/repositories/pipeline-jobs-repo");
const { createJobOrchestrator } = require("../../core/job-orchestrator");
const {
  buildIdempotencyKey,
  createPipelineLogger,
} = require("../../core/pipeline-runner");
const { fetchSources } = require("../source-discovery/service");
const { ingestNetex } = require("../ingest/service");
const {
  buildCanonicalStations,
  buildReviewQueue,
} = require("../canonical/service");
const { AppError, toAppError } = require("../../core/errors");
const {
  normalizeClusterDecision,
  normalizeIsoCountry,
} = require("./v2-contracts");
const {
  buildCuratedProjectionRowsV1,
  persistCuratedProjectionV1,
} = require("./curated-projection");
const { isStrictIsoDate } = require("../../core/date");

let dbClient = null;
const refreshWorkers = new Map();

const REFRESH_JOB_TYPE = "qa.refresh-pipeline";
const ACTIVE_REFRESH_JOB_STATUSES = new Set([
  "queued",
  "running",
  "retry_wait",
]);
const REFRESH_PIPELINE_STEP_COUNT = 4;
const REFRESH_PROGRESS_POLL_MS = 1500;

function normalizeRefreshScope(input) {
  const payload = input && typeof input === "object" ? input : {};
  const countryInput = String(payload.country || "")
    .trim()
    .toUpperCase();
  const asOfInput = String(payload.asOf || payload.as_of || "").trim();
  const sourceIdInput = String(
    payload.sourceId || payload.source_id || "",
  ).trim();

  if (countryInput && !["DE", "AT", "CH"].includes(countryInput)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "country must be one of 'DE', 'AT', 'CH'",
    });
  }

  if (asOfInput && !isStrictIsoDate(asOfInput)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "asOf must be an ISO date in YYYY-MM-DD format",
    });
  }

  return {
    country: countryInput || "",
    asOf: asOfInput || "",
    sourceId: sourceIdInput || "",
  };
}

function appendArgIfSet(args, key, value) {
  if (!value) {
    return;
  }
  args.push(key, value);
}

function buildFetchArgs(scope) {
  const args = [];
  appendArgIfSet(args, "--as-of", scope.asOf);
  appendArgIfSet(args, "--country", scope.country);
  appendArgIfSet(args, "--source-id", scope.sourceId);
  return args;
}

const buildIngestArgs = buildFetchArgs;
const buildCanonicalArgs = buildFetchArgs;

function buildReviewQueueArgs(scope) {
  const args = [];
  appendArgIfSet(args, "--as-of", scope.asOf);
  appendArgIfSet(args, "--country", scope.country);
  return args;
}

function mapRefreshJobStatus(status) {
  if (status === "succeeded") {
    return "completed";
  }
  if (status === "failed") {
    return "failed";
  }
  return "running";
}

function toNonNegativeInt(value) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return 0;
  }
  return parsed;
}

function normalizeDownloadProgress(input) {
  if (!input || typeof input !== "object") {
    return null;
  }

  const stage = String(input.stage || "").trim();
  if (!stage) {
    return null;
  }

  return {
    stage,
    source_id: String(input.source_id || "").trim() || null,
    source_index: toNonNegativeInt(input.source_index),
    total_sources: toNonNegativeInt(input.total_sources),
    file_name: String(input.file_name || "").trim() || null,
    downloaded_bytes: toNonNegativeInt(input.downloaded_bytes),
    total_bytes: toNonNegativeInt(input.total_bytes),
    message: String(input.message || "").trim() || null,
    updated_at: String(input.updated_at || "").trim() || null,
  };
}

async function readJsonObject(filePath, options = {}) {
  const onError =
    typeof options.onError === "function" ? options.onError : null;
  try {
    const raw = await fs.readFile(filePath, "utf8");
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") {
      return null;
    }
    return parsed;
  } catch (err) {
    if (err?.code === "ENOENT") {
      return null;
    }

    const appErr =
      err instanceof SyntaxError
        ? new AppError({
            code: "INVALID_JSON",
            statusCode: 400,
            message: `Failed to parse JSON file '${filePath}'`,
            details: { filePath },
            cause: err,
          })
        : toAppError(
            err,
            "INTERNAL_ERROR",
            `Failed to read JSON file '${filePath}'`,
          );

    if (onError) {
      await onError(appErr);
      return null;
    }
    throw appErr;
  }
}

function createProgressPoller(options = {}) {
  const readProgress =
    typeof options.readProgress === "function"
      ? options.readProgress
      : async () => null;
  const onProgress =
    typeof options.onProgress === "function"
      ? options.onProgress
      : async () => {};
  const onError =
    typeof options.onError === "function" ? options.onError : async () => {};
  const pollMs = Number.isFinite(options.pollMs)
    ? Math.max(250, options.pollMs)
    : REFRESH_PROGRESS_POLL_MS;

  let intervalHandle = null;
  let stopped = false;
  let inFlight = false;
  let lastProgress = "";

  async function tick(force = false) {
    if ((!force && stopped) || inFlight) {
      return;
    }

    inFlight = true;
    try {
      const progress = await readProgress();
      if (!progress) {
        return;
      }

      const serialized = JSON.stringify(progress);
      if (!force && serialized === lastProgress) {
        return;
      }

      lastProgress = serialized;
      await onProgress(progress);
    } finally {
      inFlight = false;
    }
  }

  async function reportError(err) {
    try {
      await onError(err);
    } catch {
      // Deliberately suppress telemetry callback failures.
    }
  }

  intervalHandle = setInterval(() => {
    tick(false).catch((err) => {
      reportError(err);
    });
  }, pollMs);

  tick(false).catch((err) => {
    reportError(err);
  });

  return async () => {
    stopped = true;
    if (intervalHandle) {
      clearInterval(intervalHandle);
      intervalHandle = null;
    }
    await tick(true).catch((err) => {
      reportError(err);
    });
  };
}

function toRefreshJobPayload(job) {
  const checkpoint =
    job?.checkpoint && typeof job.checkpoint === "object" ? job.checkpoint : {};
  const completedSteps = Array.isArray(checkpoint.completedSteps)
    ? checkpoint.completedSteps
    : [];
  const downloadProgress = normalizeDownloadProgress(
    checkpoint.downloadProgress,
  );
  let scope = {};
  if (checkpoint.scope && typeof checkpoint.scope === "object") {
    scope = checkpoint.scope;
  } else if (job?.runContext?.scope && typeof job.runContext.scope === "object") {
    scope = job.runContext.scope;
  }

  return {
    job_id: job.jobId,
    job_type: job.jobType,
    status: mapRefreshJobStatus(job.status),
    raw_status: job.status,
    step: typeof checkpoint.step === "string" ? checkpoint.step : null,
    step_label:
      typeof checkpoint.stepLabel === "string" ? checkpoint.stepLabel : null,
    progress: {
      completed_steps: completedSteps.length,
      total_steps: REFRESH_PIPELINE_STEP_COUNT,
    },
    download_progress: downloadProgress,
    scope,
    checkpoint,
    attempt: job.attempt || 0,
    error_code: job.errorCode || null,
    error_message: job.errorMessage || null,
    started_at: job.startedAt || null,
    ended_at: job.endedAt || null,
  };
}

async function updateRefreshCheckpoint(updateCheckpoint, state, patch) {
  const next = {
    ...state,
    ...patch,
    updatedAt: new Date().toISOString(),
  };
  await updateCheckpoint(next);
  return next;
}

async function runRefreshPipelineSteps(input /* NOSONAR */) {
  const rootDir = input.rootDir || process.cwd();
  const runId = String(input.runId || "").trim() || crypto.randomUUID();
  const scope = normalizeRefreshScope(input.scope || {});
  const updateCheckpoint =
    typeof input.updateCheckpoint === "function"
      ? input.updateCheckpoint
      : async () => {};
  const fetchProgressFile = path.join(
    rootDir,
    "state",
    `qa-refresh-fetch-${runId}.json`,
  );

  await fs.rm(fetchProgressFile, { force: true }).catch(() => {});

  let checkpoint = {};
  let checkpointUpdateLock = Promise.resolve();
  async function patchCheckpoint(patch) {
    checkpointUpdateLock = checkpointUpdateLock.then(async () => {
      checkpoint = await updateRefreshCheckpoint(
        updateCheckpoint,
        checkpoint,
        patch,
      );
    });
    await checkpointUpdateLock;
    return checkpoint;
  }

  await patchCheckpoint({
    status: "running",
    step: "starting",
    stepLabel: "Starting pipeline",
    scope,
    completedSteps: [],
  });

  const fetchArgs = buildFetchArgs(scope);
  const ingestArgs = buildIngestArgs(scope);
  const canonicalArgs = buildCanonicalArgs(scope);
  const queueArgs = buildReviewQueueArgs(scope);

  const steps = [
    {
      id: "fetching_sources",
      label: "Fetching DACH sources",
      run: () =>
        fetchSources({
          rootDir,
          runId: `${runId}-fetch`,
          args: fetchArgs,
          env: {
            FETCH_PROGRESS_FILE: fetchProgressFile,
          },
        }),
      readProgress: async () =>
        normalizeDownloadProgress(
          await readJsonObject(fetchProgressFile, {
            onError: async (err) => {
              process.stderr.write(
                `[qa-refresh] WARN: ${err.message} (errorCode=${err.code})\n`,
              );
            },
          }),
        ),
    },
    {
      id: "ingesting_netex",
      label: "Ingesting NeTEx snapshots",
      run: () =>
        ingestNetex({
          rootDir,
          runId: `${runId}-ingest`,
          args: ingestArgs,
          jobOrchestrationEnabled: false,
        }),
    },
    {
      id: "building_canonical",
      label: "Building canonical stations",
      run: () =>
        buildCanonicalStations({
          rootDir,
          runId: `${runId}-canonical`,
          args: canonicalArgs,
          jobOrchestrationEnabled: false,
        }),
    },
    {
      id: "building_review_queue",
      label: "Building review queue",
      run: () =>
        buildReviewQueue({
          rootDir,
          runId: `${runId}-queue`,
          args: queueArgs,
          jobOrchestrationEnabled: false,
        }),
    },
  ];

  for (const step of steps) {
    await patchCheckpoint({
      step: step.id,
      stepLabel: step.label,
      downloadProgress: null,
    });

    let stopProgressPoller = null;
    if (typeof step.readProgress === "function") {
      stopProgressPoller = createProgressPoller({
        pollMs: REFRESH_PROGRESS_POLL_MS,
        readProgress: step.readProgress,
        onProgress: async (progress) => {
          await patchCheckpoint({
            step: step.id,
            stepLabel: step.label,
            downloadProgress: progress,
          });
        },
        onError: async (err) => {
          process.stderr.write(
            `[qa-refresh] WARN: Progress polling failed for step '${step.id}': ${err.message}\n`,
          );
          await patchCheckpoint({
            progressWarning: {
              step: step.id,
              errorCode: err?.code || "INTERNAL_ERROR",
              message: err?.message || "Progress polling failed",
              at: new Date().toISOString(),
            },
          }).catch(() => {});
        },
      });
    }

    try {
      await step.run();
    } catch (err) {
      if (stopProgressPoller) {
        await stopProgressPoller().catch(() => {});
      }
      const appErr = toAppError(err);
      await patchCheckpoint({
        status: "failed",
        failedStep: step.id,
        errorCode: appErr.code,
        errorMessage: appErr.message,
      }).catch(() => {});
      await fs.rm(fetchProgressFile, { force: true }).catch(() => {});
      throw err;
    }

    if (stopProgressPoller) {
      await stopProgressPoller().catch(() => {});
    }

    await patchCheckpoint({
      completedSteps: [
        ...(Array.isArray(checkpoint.completedSteps)
          ? checkpoint.completedSteps
          : []),
        step.id,
      ],
    });
  }

  await patchCheckpoint({
    status: "completed",
    step: "completed",
    stepLabel: "Pipeline completed",
    completedAt: new Date().toISOString(),
    downloadProgress: null,
  });
  await fs.rm(fetchProgressFile, { force: true }).catch(() => {});

  return {
    ok: true,
    runId,
    scope,
  };
}

async function getDbClient() {
  if (!dbClient) {
    dbClient = createPostgisClient();
    await dbClient.ensureReady();
  }
  return dbClient;
}

function toCleanString(value) {
  return String(value || "").trim();
}

function pushUnique(set, value) {
  const clean = toCleanString(value);
  if (!clean) {
    return;
  }
  set.add(clean);
}

function addArrayValues(targetSet, value) {
  if (Array.isArray(value)) {
    for (const item of value) {
      pushUnique(targetSet, item);
    }
  } else {
    pushUnique(targetSet, value);
  }
}

function deriveServiceContextFromRawRows(rows /* NOSONAR */) {
  const lineKeys = [
    "line",
    "route",
    "service",
    "trip",
    "line_code",
    "route_id",
    "service_id",
  ];
  const lineArrayKeys = [
    "lines",
    "routes",
    "services", // NOSONAR
    "trips",
    "line_codes",
    "route_ids",
    "service_ids",
  ];
  const incomingKeys = [
    "origin",
    "from",
    "arrives_from",
    "arrival_from",
    "previous_stop",
    "previousStop",
    "from_stop",
  ];
  const outgoingKeys = [
    "destination",
    "to",
    "towards",
    "headsign",
    "next_stop",
    "nextStop",
    "to_stop",
  ];
  const directionKeys = [
    "direction",
    "travel_direction",
    "direction_ref",
    "directionRef",
  ];

  const lines = new Set();
  const incoming = new Set();
  const outgoing = new Set();

  let sampledRows = 0;
  for (const row of rows) {
    const payload =
      row && typeof row.raw_payload === "object" ? row.raw_payload : null;
    if (!payload) {
      continue;
    }
    sampledRows += 1;

    for (const key of lineKeys) {
      pushUnique(lines, payload[key]);
    }
    for (const key of lineArrayKeys) {
      addArrayValues(lines, payload[key]);
    }

    for (const key of incomingKeys) {
      pushUnique(incoming, payload[key]);
    }
    for (const key of outgoingKeys) {
      pushUnique(outgoing, payload[key]);
    }

    let nestedServiceContext = {};
    if (
      payload.service_context &&
      typeof payload.service_context === "object"
    ) {
      nestedServiceContext = payload.service_context;
    } else if (
      payload.serviceContext &&
      typeof payload.serviceContext === "object"
    ) {
      nestedServiceContext = payload.serviceContext;
    }
    addArrayValues(lines, nestedServiceContext.lines);
    addArrayValues(incoming, nestedServiceContext.incoming);
    addArrayValues(outgoing, nestedServiceContext.outgoing);

    const directionText = directionKeys
      .map((key) => toCleanString(payload[key]).toLowerCase())
      .find(Boolean);
    if (directionText) {
      const label = toCleanString(
        payload.headsign ||
          payload.destination ||
          payload.to ||
          payload.towards ||
          directionText,
      );
      if (
        directionText.includes("inbound") ||
        directionText.includes("incoming")
      ) {
        pushUnique(incoming, label);
      } else if (
        directionText.includes("outbound") ||
        directionText.includes("outgoing")
      ) {
        pushUnique(outgoing, label);
      }
    }
  }

  if (incoming.size === 0 && outgoing.size === 0 && lines.size > 0) {
    for (const line of lines) {
      outgoing.add(`line:${line}`);
    }
  }

  const incomingList = Array.from(incoming).sort((a, b) => a.localeCompare(b));
  const outgoingList = Array.from(outgoing).sort((a, b) => a.localeCompare(b));
  const lineList = Array.from(lines).sort((a, b) => a.localeCompare(b));

  let completenessStatus = "none";
  let completenessNotes =
    "No source payload rows available for this candidate.";
  if (sampledRows > 0 && (incomingList.length > 0 || outgoingList.length > 0)) {
    completenessStatus =
      incomingList.length > 0 && outgoingList.length > 0 ? "full" : "partial";
    completenessNotes =
      "Incoming and outgoing service context derived from canonical source payload fields.";
  } else if (sampledRows > 0 && lineList.length > 0) {
    completenessStatus = "partial";
    completenessNotes =
      "Line context derived from source payload rows; directional fields were incomplete.";
  } else if (sampledRows > 0) {
    completenessStatus = "incomplete";
    completenessNotes =
      "Source rows exist but did not provide directional service keys.";
  }

  return {
    lines: lineList,
    incoming: incomingList,
    outgoing: outgoingList,
    completeness: {
      status: completenessStatus,
      sampled_rows: sampledRows,
      notes: completenessNotes,
    },
  };
}

async function enrichCandidateServiceContext(client, candidates) {
  if (!Array.isArray(candidates) || candidates.length === 0) {
    return candidates || [];
  }

  const stationIds = Array.from(
    new Set(
      candidates
        .map((candidate) => toCleanString(candidate?.canonical_station_id))
        .filter(Boolean),
    ),
  );
  if (stationIds.length === 0) {
    return candidates;
  }

  const rows = await client.queryRows(
    `
    WITH requested AS (
      SELECT value::text AS canonical_station_id
      FROM jsonb_array_elements_text(:'station_ids'::jsonb)
    )
    SELECT
      css.canonical_station_id,
      s.raw_payload
    FROM requested r
    JOIN canonical_station_sources css
      ON css.canonical_station_id = r.canonical_station_id
    LEFT JOIN netex_stops_staging s
      ON s.source_id = css.source_id
     AND s.source_stop_id = css.source_stop_id
     AND s.snapshot_date = css.snapshot_date
    ORDER BY css.canonical_station_id, css.source_id, css.source_stop_id, css.snapshot_date
    `,
    {
      station_ids: JSON.stringify(stationIds),
    },
  );

  const groupedRows = new Map();
  for (const row of rows) {
    const stationId = toCleanString(row?.canonical_station_id);
    if (!stationId) {
      continue;
    }
    if (!groupedRows.has(stationId)) {
      groupedRows.set(stationId, []);
    }
    groupedRows.get(stationId).push(row);
  }

  return candidates.map((candidate) => {
    const stationId = toCleanString(candidate?.canonical_station_id);
    const derived = deriveServiceContextFromRawRows(
      groupedRows.get(stationId) || [],
    );
    const existingContext =
      candidate?.service_context &&
      typeof candidate.service_context === "object"
        ? candidate.service_context
        : {};
    return {
      ...candidate,
      service_context: {
        ...existingContext,
        lines: derived.lines,
        incoming: derived.incoming,
        outgoing: derived.outgoing,
        completeness: derived.completeness,
      },
    };
  });
}

function gatherDecisionStationIds(decision) {
  const stationIds = new Set(decision.selectedStationIds);
  for (const group of decision.groups) {
    for (const stationId of group.memberStationIds) {
      stationIds.add(stationId);
    }
    if (group.targetCanonicalStationId) {
      stationIds.add(group.targetCanonicalStationId);
    }
  }
  for (const target of decision.renameTargets || []) {
    if (target?.canonicalStationId) {
      stationIds.add(target.canonicalStationId);
    }
  }
  return stationIds;
}

async function loadClusterScope(client, clusterId) {
  const cluster = await client.queryOne(
    `
    SELECT c.cluster_id, c.country, c.status
    FROM qa_station_clusters_v2 c
    WHERE c.cluster_id = :'cluster_id'
    `,
    { cluster_id: clusterId },
  );

  if (!cluster) {
    throw new AppError({
      code: "NOT_FOUND",
      statusCode: 404,
      message: "Cluster not found",
    });
  }

  const clusterCandidates = await client.queryRows(
    `
    SELECT
      cc.canonical_station_id,
      cc.segment_context
    FROM qa_station_cluster_candidates_v2 cc
    WHERE cc.cluster_id = :'cluster_id'
    `,
    { cluster_id: clusterId },
  );

  const candidateSet = new Set(
    clusterCandidates
      .map((row) => toCleanString(row?.canonical_station_id))
      .filter(Boolean),
  );
  const stationToSegment = new Map();
  for (const row of clusterCandidates) {
    const stationId = toCleanString(row?.canonical_station_id);
    const segmentId = toCleanString(row?.segment_context?.segment_id);
    if (stationId && segmentId) {
      stationToSegment.set(stationId, segmentId);
    }
  }

  const clusterSegments = await client.queryRows(
    `
    SELECT DISTINCT seg.segment_id
    FROM qa_station_segments_v2 seg
    JOIN qa_station_cluster_candidates_v2 cc
      ON cc.canonical_station_id = seg.canonical_station_id
    WHERE cc.cluster_id = :'cluster_id'
    `,
    { cluster_id: clusterId },
  );

  const segmentSet = new Set(
    clusterSegments
      .map((row) => toCleanString(row?.segment_id))
      .filter(Boolean),
  );

  return {
    cluster,
    candidateSet,
    segmentSet,
    stationToSegment,
  };
}

function validateClusterMembership(clusterId, stationIds, candidateSet) {
  for (const stationId of stationIds) {
    if (!candidateSet.has(stationId)) {
      throw new AppError({
        code: "INVALID_REQUEST",
        statusCode: 400,
        message: `Station '${stationId}' is not part of cluster '${clusterId}'`,
      });
    }
  }
}

function validateSegmentScope(segmentLinks, segmentSet) {
  for (const link of segmentLinks) {
    if (
      !segmentSet.has(link.fromSegmentId) ||
      !segmentSet.has(link.toSegmentId)
    ) {
      throw new AppError({
        code: "INVALID_REQUEST",
        statusCode: 400,
        message: `Walking link segment is outside cluster scope ('${link.fromSegmentId}' -> '${link.toSegmentId}')`,
      });
    }
  }
}

function hashStableId(prefix, input) {
  const digest = crypto
    .createHash("sha1")
    .update(String(input || ""))
    .digest("hex")
    .slice(0, 20);
  return `${prefix}_${digest}`;
}

function inferSectionTypeFromLabel(label) {
  const text = String(label || "")
    .trim()
    .toLowerCase();
  if (!text) {
    return "other";
  }
  if (
    text.includes("main") ||
    text.includes("hbf") ||
    text.includes("hauptbahnhof") ||
    text.includes("rail")
  ) {
    return "main";
  }
  if (
    text.includes("secondary") ||
    text.includes("aux") ||
    text.includes("side")
  ) {
    return "secondary";
  }
  if (
    text.includes("subway") ||
    text.includes("metro") ||
    text.includes("u-bahn") ||
    text.includes("ubahn")
  ) {
    return "subway";
  }
  if (text.includes("bus")) {
    return "bus";
  }
  if (text.includes("tram") || text.includes("streetcar")) {
    return "tram";
  }
  return "other";
}

function buildGroupModelFromDecision( /* NOSONAR */
  clusterId,
  country,
  decision,
  stationToSegment = new Map(),
) {
  if (
    !decision?.operation || // NOSONAR
    decision.operation !== "split" ||
    !Array.isArray(decision.groups) ||
    decision.groups.length < 2
  ) {
    return null;
  }

  const hasExplicitSections = decision.groups.some((group) =>
    Boolean(group?.hasSectionMetadata),
  );
  if (!hasExplicitSections) {
    return null;
  }

  const filteredGroups = decision.groups.filter(
    (group) =>
      Array.isArray(group.memberStationIds) &&
      group.memberStationIds.length > 0,
  );
  if (filteredGroups.length < 2) {
    return null;
  }

  const sectionSignature = filteredGroups
    .map(
      (group) =>
        `${group.sectionType || inferSectionTypeFromLabel(group.groupLabel)}|${group.sectionName || group.groupLabel}|${group.memberStationIds.join(",")}`,
    )
    .join("|");
  const groupId = hashStableId("grp", `${clusterId}|${sectionSignature}`);

  const segmentToSection = new Map();
  const sections = filteredGroups.map((group, index) => {
    const sectionType =
      group.sectionType || inferSectionTypeFromLabel(group.groupLabel);
    const sectionName =
      String(
        group.sectionName || group.groupLabel || `Section ${index + 1}`,
      ).trim() || `Section ${index + 1}`;
    const sectionId = hashStableId(
      "grpsec",
      `${groupId}|${index}|${sectionType}|${sectionName}|${group.memberStationIds.join(",")}`,
    );
    const mappedStation = group.memberStationIds.find((stationId) =>
      stationToSegment.has(stationId),
    );
    const segmentId = mappedStation ? stationToSegment.get(mappedStation) : "";
    if (segmentId) {
      if (!segmentToSection.has(segmentId)) {
        segmentToSection.set(segmentId, sectionId);
      }
    }

    return {
      sectionId,
      sectionType,
      sectionName,
      memberStationIds: group.memberStationIds,
    };
  });

  const links = [];
  const seen = new Set();
  for (const group of filteredGroups) {
    const segmentAction =
      group && typeof group.segmentAction === "object"
        ? group.segmentAction
        : {};
    const walkLinks = Array.isArray(segmentAction.walk_links)
      ? segmentAction.walk_links
      : [];
    for (const link of walkLinks) {
      const fromSegmentId = toCleanString(
        link && (link.from_segment_id || link.fromSegmentId),
      );
      const toSegmentId = toCleanString(
        link && (link.to_segment_id || link.toSegmentId),
      );
      const fromSectionId = segmentToSection.get(fromSegmentId);
      const toSectionId = segmentToSection.get(toSegmentId);
      if (!fromSectionId || !toSectionId || fromSectionId === toSectionId) {
        continue;
      }

      const minWalkMinutes = Math.max(
        0,
        Number.parseInt(
          String(link && (link.min_walk_minutes ?? link.minWalkMinutes ?? 0)),
          10,
        ) || 0,
      );
      const bidirectional = Boolean(link?.bidirectional);
      const key = `${fromSectionId}|${toSectionId}|${minWalkMinutes}|${bidirectional ? "b" : "s"}`;
      if (seen.has(key)) {
        continue;
      }
      seen.add(key);
      const linkMetadata =
        link.metadata && typeof link.metadata === "object"
          ? link.metadata
          : undefined;
      links.push({
        fromSectionId,
        toSectionId,
        minWalkMinutes,
        metadata: {
          ...linkMetadata,
          source: "cluster_decision",
          from_segment_id: fromSegmentId,
          to_segment_id: toSegmentId,
          bidirectional,
        },
        bidirectional,
      });
    }
  }

  return {
    groupId,
    clusterId,
    country,
    displayName: decision.renameTo || "Grouped station",
    requestedBy: decision.requestedBy,
    sections,
    links,
  };
}

function parseListLimit(raw, fallback = 50, max = 200) {
  const parsed = Number.parseInt(String(raw ?? ""), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return Math.min(max, parsed);
}

function normalizeClusterStatusFilter(raw) {
  const value = String(raw || "")
    .trim()
    .toLowerCase();
  if (!value) {
    return "";
  }
  if (!["open", "in_review", "resolved", "dismissed"].includes(value)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message:
        "status must be one of 'open', 'in_review', 'resolved', 'dismissed'",
    });
  }
  return value;
}

function normalizeCuratedStatusFilter(raw) {
  const value = String(raw || "")
    .trim()
    .toLowerCase();
  if (!value) {
    return "";
  }
  if (!["active", "superseded"].includes(value)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "status must be one of 'active', 'superseded'",
    });
  }
  return value;
}

async function getReviewClustersV2(url) {
  const client = await getDbClient();
  const country = normalizeIsoCountry(url.searchParams.get("country"), {
    allowEmpty: true,
  });
  const status = normalizeClusterStatusFilter(url.searchParams.get("status"));
  const scopeTag = String(url.searchParams.get("scope_tag") || "").trim();
  const limit = parseListLimit(url.searchParams.get("limit"), 50, 200);

  const rows = await client.queryRows(
    `
    SELECT
      c.cluster_id,
      c.country,
      c.scope_tag,
      c.scope_as_of,
      c.severity,
      c.status,
      c.candidate_count,
      c.issue_count,
      c.display_name,
      c.display_name_reason,
      c.summary,
      c.updated_at,
      (
        SELECT COALESCE(json_agg(json_build_object(
          'canonical_station_id', cc.canonical_station_id,
          'display_name', cc.display_name,
          'candidate_rank', cc.candidate_rank,
          'latitude', cc.latitude,
          'longitude', cc.longitude
        ) ORDER BY cc.candidate_rank, cc.canonical_station_id), '[]'::json)
        FROM qa_station_cluster_candidates_v2 cc
        WHERE cc.cluster_id = c.cluster_id
      ) AS candidates
    FROM qa_station_clusters_v2 c
    WHERE (NULLIF(:'country', '') IS NULL OR c.country = NULLIF(:'country', '')::char(2))
      AND (NULLIF(:'status', '') IS NULL OR c.status = NULLIF(:'status', ''))
      AND (NULLIF(:'scope_tag', '') IS NULL OR c.scope_tag = NULLIF(:'scope_tag', ''))
    ORDER BY
      CASE c.severity WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
      c.updated_at DESC,
      c.cluster_id ASC
    LIMIT :'limit'::integer
    `,
    {
      country,
      status,
      scope_tag: scopeTag,
      limit,
    },
  );

  return rows;
}

async function getReviewClusterDetailV2(clusterId) {
  const client = await getDbClient();
  const cleanClusterId = String(clusterId || "").trim();
  if (!cleanClusterId) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "cluster_id is required",
    });
  }

  const cluster = await client.queryOne(
    `
    SELECT
      c.cluster_id,
      c.cluster_key,
      c.country,
      c.scope_tag,
      c.scope_as_of,
      c.severity,
      c.status,
      c.candidate_count,
      c.issue_count,
      c.display_name,
      c.display_name_reason,
      c.summary,
      c.resolved_at,
      c.resolved_by,
      c.created_at,
      c.updated_at
    FROM qa_station_clusters_v2 c
    WHERE c.cluster_id = :'cluster_id'
    `,
    { cluster_id: cleanClusterId },
  );

  if (!cluster) {
    throw new AppError({
      code: "NOT_FOUND",
      statusCode: 404,
      message: "Cluster not found",
    });
  }

  const [candidates, evidence, decisions] = await Promise.all([
    client.queryRows(
      `
      SELECT
        cc.cluster_id,
        cc.canonical_station_id,
        cc.candidate_rank,
        cc.display_name,
        cc.naming,
        cc.aliases,
        cc.language_codes,
        cc.latitude,
        cc.longitude,
        cc.provider_labels,
        cc.source_member_count,
        cc.service_context,
        cc.segment_context,
        cc.metadata
      FROM qa_station_cluster_candidates_v2 cc
      WHERE cc.cluster_id = :'cluster_id'
      ORDER BY cc.candidate_rank, cc.canonical_station_id
      `,
      { cluster_id: cleanClusterId },
    ),
    client.queryRows(
      `
      SELECT
        e.evidence_id,
        e.cluster_id,
        e.source_canonical_station_id,
        e.target_canonical_station_id,
        e.evidence_type,
        e.score,
        e.details,
        e.created_at
      FROM qa_station_cluster_evidence_v2 e
      WHERE e.cluster_id = :'cluster_id'
      ORDER BY e.evidence_type, e.source_canonical_station_id, e.target_canonical_station_id, e.evidence_id
      `,
      { cluster_id: cleanClusterId },
    ),
    client.queryRows(
      `
      SELECT
        d.decision_id,
        d.operation,
        d.decision_payload,
        d.line_decision_payload,
        d.note,
        d.requested_by,
        d.applied_to_overrides,
        d.created_at,
        (
          SELECT COALESCE(json_agg(json_build_object(
            'canonical_station_id', m.canonical_station_id,
            'group_label', m.group_label,
            'action', m.action,
            'metadata', m.metadata
          ) ORDER BY m.canonical_station_id, m.group_label, m.action), '[]'::json)
          FROM qa_station_cluster_decision_members_v2 m
          WHERE m.decision_id = d.decision_id
        ) AS members
      FROM qa_station_cluster_decisions_v2 d
      WHERE d.cluster_id = :'cluster_id'
      ORDER BY d.created_at DESC, d.decision_id DESC
      `,
      { cluster_id: cleanClusterId },
    ),
  ]);

  const enrichedCandidates = await enrichCandidateServiceContext(
    client,
    candidates,
  );

  return {
    ...cluster,
    candidates: enrichedCandidates,
    evidence,
    decisions,
    edit_history: [],
  };
}

async function getCuratedStationsV1(url) {
  const client = await getDbClient();
  const country = normalizeIsoCountry(url.searchParams.get("country"), {
    allowEmpty: true,
  });
  const status = normalizeCuratedStatusFilter(url.searchParams.get("status"));
  const clusterId = String(url.searchParams.get("cluster_id") || "").trim();
  const limit = parseListLimit(url.searchParams.get("limit"), 50, 200);

  const rows = await client.queryRows(
    `
    SELECT
      s.curated_station_id,
      s.country,
      s.status,
      s.primary_cluster_id,
      s.latest_decision_id,
      s.derived_operation,
      s.display_name,
      s.naming_reason,
      s.metadata,
      s.created_by,
      s.updated_by,
      s.created_at,
      s.updated_at,
      (
        SELECT COALESCE(json_agg(json_build_object(
          'canonical_station_id', m.canonical_station_id,
          'member_role', m.member_role,
          'member_rank', m.member_rank,
          'contribution', m.contribution
        ) ORDER BY m.member_rank, m.canonical_station_id), '[]'::json)
        FROM qa_curated_station_members_v1 m
        WHERE m.curated_station_id = s.curated_station_id
      ) AS members,
      (
        SELECT COALESCE(json_agg(json_build_object(
          'field_name', p.field_name,
          'field_value', p.field_value,
          'source_kind', p.source_kind,
          'source_ref', p.source_ref,
          'metadata', p.metadata
        ) ORDER BY p.field_name, p.source_kind, p.source_ref), '[]'::json)
        FROM qa_curated_station_field_provenance_v1 p
        WHERE p.curated_station_id = s.curated_station_id
      ) AS field_provenance,
      (
        SELECT COALESCE(json_agg(json_build_object(
          'decision_id', l.decision_id,
          'cluster_id', l.cluster_id,
          'operation', l.operation,
          'created_at', l.created_at
        ) ORDER BY l.created_at DESC, l.decision_id DESC), '[]'::json)
        FROM qa_curated_station_lineage_v1 l
        WHERE l.curated_station_id = s.curated_station_id
      ) AS lineage
    FROM qa_curated_stations_v1 s
    WHERE (NULLIF(:'country', '') IS NULL OR s.country = NULLIF(:'country', '')::char(2))
      AND (NULLIF(:'status', '') IS NULL OR s.status = NULLIF(:'status', ''))
      AND (NULLIF(:'cluster_id', '') IS NULL OR s.primary_cluster_id = NULLIF(:'cluster_id', ''))
    ORDER BY s.updated_at DESC, s.curated_station_id ASC
    LIMIT :'limit'::integer
    `,
    {
      country,
      status,
      cluster_id: clusterId,
      limit,
    },
  );

  return rows;
}

async function getCuratedStationDetailV1(curatedStationId) {
  const client = await getDbClient();
  const cleanCuratedStationId = String(curatedStationId || "").trim();
  if (!cleanCuratedStationId) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "curated_station_id is required",
    });
  }

  const row = await client.queryOne(
    `
    SELECT
      p.curated_station_id,
      p.country,
      p.status,
      p.primary_cluster_id,
      p.latest_decision_id,
      p.derived_operation,
      p.display_name,
      p.naming_reason,
      p.metadata,
      p.created_by,
      p.updated_by,
      p.created_at,
      p.updated_at,
      p.members,
      p.field_provenance,
      p.lineage
    FROM qa_curated_station_projection_v1 p
    WHERE p.curated_station_id = :'curated_station_id'
    `,
    {
      curated_station_id: cleanCuratedStationId,
    },
  );

  if (!row) {
    throw new AppError({
      code: "NOT_FOUND",
      statusCode: 404,
      message: "Curated station not found",
    });
  }

  return row;
}

async function findActiveRefreshJob(jobsRepo) {
  const jobs = await jobsRepo.listRecentByType(REFRESH_JOB_TYPE, 50);
  const active = jobs.filter((job) =>
    ACTIVE_REFRESH_JOB_STATUSES.has(job.status),
  );
  if (active.length === 0) {
    return null;
  }

  // Prefer exposing true in-flight work over queued placeholders.
  const priority = {
    running: 0,
    retry_wait: 1,
    queued: 2,
  };

  active.sort((a, b) => {
    const pa = Number.isFinite(priority[a.status]) ? priority[a.status] : 99;
    const pb = Number.isFinite(priority[b.status]) ? priority[b.status] : 99;
    if (pa !== pb) {
      return pa - pb;
    }
    return String(b.createdAt || "").localeCompare(String(a.createdAt || ""));
  });

  return active[0];
}

function startRefreshWorker(options = {}) {
  const rootDir = options.rootDir || process.cwd();
  const job = options.job;
  const jobsRepo = options.jobsRepo;

  if (!job?.jobId || !jobsRepo) {
    return null;
  }

  const existing = refreshWorkers.get(job.jobId);
  if (existing) {
    return existing;
  }

  const logger =
    options.logger ||
    createPipelineLogger(rootDir, REFRESH_JOB_TYPE, job.jobId);
  const jobOrchestrator = createJobOrchestrator({
    jobsRepo,
    logger,
  });
  const scope = normalizeRefreshScope(job?.runContext?.scope || {});

  const worker = jobOrchestrator
    .runJob({
      jobId: job.jobId,
      jobType: REFRESH_JOB_TYPE,
      idempotencyKey: job.idempotencyKey,
      runContext: {
        ...(job.runContext && typeof job.runContext === "object"
          ? job.runContext
          : {}),
        scope,
      },
      maxAttempts: Number.parseInt(
        process.env.PIPELINE_JOB_MAX_ATTEMPTS || "3",
        10,
      ),
      maxConcurrent: 1,
      execute: async ({ updateCheckpoint }) =>
        runRefreshPipelineSteps({
          rootDir,
          runId: job.jobId,
          scope,
          updateCheckpoint,
        }),
    })
    .catch((err) => {
      const appErr = toAppError(err);
      logger.error("refresh pipeline worker failed", {
        jobId: job.jobId,
        errorCode: appErr.code,
        error: appErr.message,
      });
    })
    .finally(() => {
      refreshWorkers.delete(job.jobId);
    });

  refreshWorkers.set(job.jobId, worker);
  return worker;
}

async function postRefreshJob(body, options = {}) {
  const rootDir = options.rootDir || process.cwd();
  const scope = normalizeRefreshScope(body || {});
  const client = await getDbClient();
  const jobsRepo = createPipelineJobsRepo(client);

  const active = await findActiveRefreshJob(jobsRepo);
  if (active) {
    const payload = toRefreshJobPayload(active);
    if (active.status === "queued" || active.status === "retry_wait") {
      const runningCount = await jobsRepo
        .countRunningByType(REFRESH_JOB_TYPE)
        .catch(() => 1);
      if (runningCount === 0) {
        startRefreshWorker({
          rootDir,
          job: active,
          jobsRepo,
        });
      }
    }
    return {
      accepted: true,
      reused: true,
      in_flight: true,
      job_id: payload.job_id,
      job: payload,
    };
  }

  const jobId = crypto.randomUUID();
  const idempotencyKey = buildIdempotencyKey(REFRESH_JOB_TYPE, [
    jobId,
    scope.country,
    scope.asOf,
    scope.sourceId,
  ]);
  const queuedJob = await jobsRepo.createQueuedJob({
    jobId,
    jobType: REFRESH_JOB_TYPE,
    idempotencyKey,
    runContext: {
      scope,
      requestedAt: new Date().toISOString(),
      source: "qa.refresh",
    },
    checkpoint: {
      status: "queued",
      scope,
      step: "queued",
      stepLabel: "Queued",
      completedSteps: [],
      updatedAt: new Date().toISOString(),
    },
  });

  startRefreshWorker({
    rootDir,
    job: queuedJob,
    jobsRepo,
  });

  const payload = toRefreshJobPayload(queuedJob);
  return {
    accepted: true,
    reused: false,
    in_flight: true,
    job_id: payload.job_id,
    job: payload,
  };
}

async function getRefreshJob(jobId, options = {}) {
  const rootDir = options.rootDir || process.cwd();
  const cleanJobId = String(jobId || "").trim();
  if (!cleanJobId) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "job_id is required",
    });
  }

  const client = await getDbClient();
  const jobsRepo = createPipelineJobsRepo(client);

  let job;
  try {
    job = await jobsRepo.getById(cleanJobId);
  } catch (err) {
    if (
      String(err.message || "")
        .toLowerCase()
        .includes("invalid input syntax for type uuid")
    ) {
      throw new AppError({
        code: "INVALID_REQUEST",
        statusCode: 400,
        message: "job_id must be a valid UUID",
      });
    }
    throw err;
  }

  if (job?.jobType !== REFRESH_JOB_TYPE) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 404,
      message: "Refresh pipeline job not found",
    });
  }

  if (
    (job.status === "queued" || job.status === "retry_wait") &&
    !refreshWorkers.has(job.jobId)
  ) {
    const runningCount = await jobsRepo
      .countRunningByType(REFRESH_JOB_TYPE)
      .catch(() => 1);
    if (runningCount === 0) {
      startRefreshWorker({
        rootDir,
        job,
        jobsRepo,
      });
    }
  }

  return toRefreshJobPayload(job);
}

function persistGroupModel(tx, groupModel) {
  if (
    !groupModel?.groupId ||
    !Array.isArray(groupModel.sections) ||
    groupModel.sections.length === 0
  ) {
    return;
  }

  tx.add(
    `
    UPDATE qa_station_groups_v2
    SET
      is_active = false,
      updated_by = :'requested_by',
      updated_at = now()
    WHERE cluster_id = :'cluster_id'
      AND is_active = true
    `,
    {
      cluster_id: groupModel.clusterId,
      requested_by: groupModel.requestedBy,
    },
  );

  tx.add(
    `
    INSERT INTO qa_station_groups_v2 (
      group_id,
      cluster_id,
      country,
      display_name,
      scope_tag,
      is_active,
      metadata,
      created_by,
      updated_by,
      created_at,
      updated_at
    ) VALUES (
      :'group_id',
      :'cluster_id',
      :'country',
      :'display_name',
      'latest',
      true,
      '{}'::jsonb,
      :'requested_by',
      :'requested_by',
      now(),
      now()
    )
    ON CONFLICT (group_id)
    DO UPDATE SET
      cluster_id = EXCLUDED.cluster_id,
      country = EXCLUDED.country,
      display_name = EXCLUDED.display_name,
      scope_tag = EXCLUDED.scope_tag,
      is_active = true,
      updated_by = EXCLUDED.updated_by,
      updated_at = now()
    `,
    {
      group_id: groupModel.groupId,
      cluster_id: groupModel.clusterId,
      country: groupModel.country,
      display_name: groupModel.displayName,
      requested_by: groupModel.requestedBy,
    },
  );

  tx.add(
    `
    DELETE FROM qa_station_group_sections_v2
    WHERE group_id = :'group_id'
    `,
    {
      group_id: groupModel.groupId,
    },
  );

  for (const section of groupModel.sections) {
    tx.add(
      `
      INSERT INTO qa_station_group_sections_v2 (
        section_id,
        group_id,
        section_type,
        section_name,
        metadata,
        created_by,
        updated_by,
        created_at,
        updated_at
      ) VALUES (
        :'section_id',
        :'group_id',
        :'section_type',
        :'section_name',
        '{}'::jsonb,
        :'requested_by',
        :'requested_by',
        now(),
        now()
      )
      `,
      {
        section_id: section.sectionId,
        group_id: groupModel.groupId,
        section_type: section.sectionType,
        section_name: section.sectionName,
        requested_by: groupModel.requestedBy,
      },
    );

    if (
      Array.isArray(section.memberStationIds) &&
      section.memberStationIds.length > 0
    ) {
      tx.add(
        `
        INSERT INTO qa_station_group_section_members_v2 (
          section_id,
          canonical_station_id,
          created_at
        )
        SELECT
          :'section_id',
          x.canonical_station_id,
          now()
        FROM jsonb_to_recordset(:'members'::jsonb) AS x(
          canonical_station_id text
        )
        `,
        {
          section_id: section.sectionId,
          members: JSON.stringify(
            section.memberStationIds.map((stationId) => ({
              canonical_station_id: stationId,
            })),
          ),
        },
      );
    }
  }

  if (Array.isArray(groupModel.links) && groupModel.links.length > 0) {
    tx.add(
      `
      INSERT INTO qa_station_group_section_links_v2 (
        from_section_id,
        to_section_id,
        min_walk_minutes,
        metadata,
        created_by,
        created_at
      )
      SELECT
        x.from_section_id,
        x.to_section_id,
        GREATEST(0, COALESCE(x.min_walk_minutes, 0)),
        COALESCE(x.metadata, '{}'::jsonb),
        :'requested_by',
        now()
      FROM jsonb_to_recordset(:'links'::jsonb) AS x(
        from_section_id text,
        to_section_id text,
        min_walk_minutes integer,
        metadata jsonb
      )
      WHERE x.from_section_id <> x.to_section_id
      ON CONFLICT (from_section_id, to_section_id)
      DO UPDATE SET
        min_walk_minutes = EXCLUDED.min_walk_minutes,
        metadata = qa_station_group_section_links_v2.metadata || EXCLUDED.metadata
      `,
      {
        requested_by: groupModel.requestedBy,
        links: JSON.stringify(
          groupModel.links.flatMap((link) => {
            const rows = [
              {
                from_section_id: link.fromSectionId,
                to_section_id: link.toSectionId,
                min_walk_minutes: link.minWalkMinutes,
                metadata: link.metadata || {},
              },
            ];
            if (link.bidirectional) {
              const reverseMetadata =
                link.metadata && typeof link.metadata === "object"
                  ? link.metadata
                  : undefined;
              rows.push({
                from_section_id: link.toSectionId,
                to_section_id: link.fromSectionId,
                min_walk_minutes: link.minWalkMinutes,
                metadata: {
                  ...reverseMetadata,
                  bidirectional: true,
                },
              });
            }
            return rows;
          }),
        ),
      },
    );
  }
}

function buildDecisionMembersPayload(decision /* NOSONAR */) {
  const rows = []; // NOSONAR
  const seen = new Set();

  function pushRow(canonicalStationId, groupLabel, action, metadata = {}) {
    const stationId = String(canonicalStationId || "").trim();
    if (!stationId) {
      return;
    }
    const label = String(groupLabel || "").trim();
    const memberAction = String(action || "candidate").trim() || "candidate";
    const key = `${stationId}|${label}|${memberAction}`;
    if (seen.has(key)) {
      return;
    }
    seen.add(key);
    rows.push({
      canonical_station_id: stationId,
      group_label: label,
      action: memberAction,
      metadata: metadata && typeof metadata === "object" ? metadata : {},
    });
  }

  if (decision.groups.length > 0) {
    for (const group of decision.groups) {
      for (const stationId of group.memberStationIds) {
        pushRow(
          stationId,
          group.groupLabel,
          decision.operation === "merge" ? "merge_member" : "candidate",
          {
            rename_to: group.renameTo || null,
            segment_action: group.segmentAction || {},
            line_action: group.lineAction || {},
          },
        );
      }
    }
  }

  if (decision.groups.length === 0 && decision.selectedStationIds.length > 0) {
    if (decision.operation === "merge") {
      for (const stationId of decision.selectedStationIds) {
        pushRow(stationId, "selected", "merge_member");
      }
    } else {
      for (const stationId of decision.selectedStationIds) {
        pushRow(stationId, "selected", "candidate");
      }
    }
  }

  if (rows.length === 0) {
    for (const stationId of decision.selectedStationIds) {
      pushRow(stationId, "selected", "candidate");
    }
  }

  return rows;
}

function buildRenameTargets(decision) {
  const targets = [];
  const seen = new Set();

  for (const target of decision.renameTargets || []) {
    const stationId = toCleanString(target?.canonicalStationId);
    const renameTo = toCleanString(target?.renameTo);
    if (!stationId || !renameTo) {
      continue;
    }
    const key = `${stationId}|${renameTo}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    targets.push({
      canonicalStationId: stationId,
      renameTo,
    });
  }

  return targets;
}

function buildSegmentWalkLinks(decision /* NOSONAR */) {
  const links = []; // NOSONAR
  const seen = new Set();

  for (const group of decision.groups) {
    const segmentAction =
      group && typeof group.segmentAction === "object"
        ? group.segmentAction
        : {};
    let rawLinks = [];
    if (Array.isArray(segmentAction.walk_links)) {
      rawLinks = segmentAction.walk_links;
    } else if (Array.isArray(segmentAction.walkLinks)) {
      rawLinks = segmentAction.walkLinks;
    }

    for (const rawLink of rawLinks) {
      const input = rawLink && typeof rawLink === "object" ? rawLink : {};
      const fromSegmentId = String(
        input.from_segment_id || input.fromSegmentId || "",
      ).trim();
      const toSegmentId = String(
        input.to_segment_id || input.toSegmentId || "",
      ).trim();
      if (!fromSegmentId || !toSegmentId || fromSegmentId === toSegmentId) {
        continue;
      }

      let minWalkMinutes = Number.parseInt(
        input.min_walk_minutes ?? input.minWalkMinutes ?? 0,
        10,
      );
      if (!Number.isFinite(minWalkMinutes) || minWalkMinutes < 0) {
        minWalkMinutes = 0;
      }

      const bidirectional = Boolean(input.bidirectional);
      const metadata =
        input.metadata && typeof input.metadata === "object"
          ? { ...input.metadata }
          : {};
      const baseMetadata = {
        ...metadata,
        group_label: group.groupLabel || "",
        operation: decision.operation,
      };

      const forwardKey = `${fromSegmentId}|${toSegmentId}`;
      if (!seen.has(forwardKey)) {
        seen.add(forwardKey);
        links.push({
          fromSegmentId,
          toSegmentId,
          minWalkMinutes,
          metadata: {
            ...baseMetadata,
            bidirectional,
          },
        });
      }

      if (bidirectional) {
        const reverseKey = `${toSegmentId}|${fromSegmentId}`;
        if (!seen.has(reverseKey)) {
          seen.add(reverseKey);
          links.push({
            fromSegmentId: toSegmentId,
            toSegmentId: fromSegmentId,
            minWalkMinutes,
            metadata: {
              ...baseMetadata,
              bidirectional: true,
            },
          });
        }
      }
    }
  }

  return links;
}

async function postReviewClusterDecisionV2(clusterId, body) {
  const client = await getDbClient();
  const cleanClusterId = String(clusterId || "").trim();
  if (!cleanClusterId) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "cluster_id is required",
    });
  }

  const { cluster, candidateSet, segmentSet, stationToSegment } =
    await loadClusterScope(client, cleanClusterId);

  const decision = normalizeClusterDecision(body);
  const candidateIdsFromDecision = gatherDecisionStationIds(decision);
  validateClusterMembership(
    cleanClusterId,
    candidateIdsFromDecision,
    candidateSet,
  );

  const membersPayload = buildDecisionMembersPayload(decision);
  const renameTargets = buildRenameTargets(decision);
  const segmentWalkLinks = buildSegmentWalkLinks(decision);
  const groupModel = buildGroupModelFromDecision(
    cleanClusterId,
    cluster.country,
    decision,
    stationToSegment,
  );
  const curatedProjection = buildCuratedProjectionRowsV1({
    clusterId: cleanClusterId,
    decision,
  });
  const shouldDismiss = false;
  const shouldResolve = true;
  const appliedToOverrides = false;

  validateSegmentScope(segmentWalkLinks, segmentSet);

  await client.withTransaction(async (tx) => {
    tx.add(
      `
      CREATE TEMP TABLE _decision_ctx ON COMMIT DROP AS
      WITH inserted AS (
        INSERT INTO qa_station_cluster_decisions_v2 (
          cluster_id,
          operation,
          decision_payload,
          line_decision_payload,
          note,
          requested_by,
          applied_to_overrides,
          created_at
        ) VALUES (
          :'cluster_id',
          :'operation',
          :'decision_payload'::jsonb,
          :'line_decision_payload'::jsonb,
          NULLIF(:'note', ''),
          :'requested_by',
          false,
          now()
        )
        RETURNING decision_id
      )
      SELECT decision_id
      FROM inserted
      `,
      {
        cluster_id: cleanClusterId,
        operation: decision.operation,
        decision_payload: JSON.stringify({
          selected_station_ids: decision.selectedStationIds,
          groups: decision.groups,
          rename_to: decision.renameTo || null,
          rename_targets: decision.renameTargets || [],
        }),
        line_decision_payload: JSON.stringify(decision.lineDecisions || {}),
        note: decision.note || "",
        requested_by: decision.requestedBy,
      },
    );

    if (membersPayload.length > 0) {
      tx.add(
        `
        INSERT INTO qa_station_cluster_decision_members_v2 (
          decision_id,
          canonical_station_id,
          group_label,
          action,
          metadata
        )
        SELECT
          (SELECT decision_id FROM _decision_ctx LIMIT 1),
          m.canonical_station_id,
          COALESCE(m.group_label, ''),
          COALESCE(m.action, 'candidate'),
          COALESCE(m.metadata, '{}'::jsonb)
        FROM jsonb_to_recordset(:'members'::jsonb) AS m(
          canonical_station_id text,
          group_label text,
          action text,
          metadata jsonb
        )
        `,
        {
          members: JSON.stringify(membersPayload),
        },
      );
    }

    if (renameTargets.length > 0) {
      tx.add(
        `
        UPDATE qa_station_naming_overrides_v2 o
        SET is_active = false, updated_at = now()
        FROM jsonb_to_recordset(:'rename_targets'::jsonb) AS r(
          canonical_station_id text,
          rename_to text
        )
        WHERE o.canonical_station_id = r.canonical_station_id
          AND o.is_active = true;
        `,
        {
          rename_targets: JSON.stringify(
            renameTargets.map((target) => ({
              canonical_station_id: target.canonicalStationId,
              rename_to: target.renameTo,
            })),
          ),
        },
      );

      tx.add(
        `
        INSERT INTO qa_station_naming_overrides_v2 (
          canonical_station_id,
          locale,
          display_name,
          aliases,
          reason,
          requested_by,
          approved_by,
          is_active,
          created_at,
          updated_at
        )
        SELECT
          r.canonical_station_id,
          'und',
          r.rename_to,
          '[]'::jsonb,
          'Created by curation v2 cluster decision',
          :'requested_by',
          :'requested_by',
          true,
          now(),
          now()
        FROM jsonb_to_recordset(:'rename_targets'::jsonb) AS r(
          canonical_station_id text,
          rename_to text
        )
        WHERE NULLIF(r.rename_to, '') IS NOT NULL;
        `,
        {
          requested_by: decision.requestedBy,
          rename_targets: JSON.stringify(
            renameTargets.map((target) => ({
              canonical_station_id: target.canonicalStationId,
              rename_to: target.renameTo,
            })),
          ),
        },
      );
    }

    if (segmentWalkLinks.length > 0) {
      tx.add(
        `
        INSERT INTO qa_station_segment_links_v2 (
          from_segment_id,
          to_segment_id,
          min_walk_minutes,
          metadata,
          created_by
        )
        SELECT
          l.from_segment_id,
          l.to_segment_id,
          GREATEST(0, COALESCE(l.min_walk_minutes, 0)),
          COALESCE(l.metadata, '{}'::jsonb) || jsonb_build_object(
            'cluster_id', :'cluster_id',
            'decision_id', (SELECT decision_id FROM _decision_ctx LIMIT 1),
            'requested_by', :'requested_by'
          ),
          :'requested_by'
        FROM jsonb_to_recordset(:'segment_walk_links'::jsonb) AS l(
          from_segment_id text,
          to_segment_id text,
          min_walk_minutes integer,
          metadata jsonb
        )
        WHERE l.from_segment_id <> l.to_segment_id
        ON CONFLICT (from_segment_id, to_segment_id)
        DO UPDATE SET
          min_walk_minutes = EXCLUDED.min_walk_minutes,
          metadata = qa_station_segment_links_v2.metadata || EXCLUDED.metadata
        `,
        {
          cluster_id: cleanClusterId,
          requested_by: decision.requestedBy,
          segment_walk_links: JSON.stringify(
            segmentWalkLinks.map((link) => ({
              from_segment_id: link.fromSegmentId,
              to_segment_id: link.toSegmentId,
              min_walk_minutes: link.minWalkMinutes,
              metadata: link.metadata || {},
            })),
          ),
        },
      );
    }

    persistGroupModel(tx, groupModel);
    persistCuratedProjectionV1(tx, {
      clusterId: cleanClusterId,
      country: cluster.country,
      requestedBy: decision.requestedBy,
      decisionPayload: {
        selected_station_ids: decision.selectedStationIds,
        groups: decision.groups,
        rename_to: decision.renameTo || null,
        rename_targets: decision.renameTargets || [],
      },
      entities: curatedProjection.entities,
      members: curatedProjection.members,
      fieldProvenance: curatedProjection.fieldProvenance,
      lineage: curatedProjection.lineage,
    });

    tx.add(
      `
      UPDATE qa_station_cluster_decisions_v2
      SET applied_to_overrides = :'applied_to_overrides'::boolean
      WHERE decision_id = (SELECT decision_id FROM _decision_ctx LIMIT 1)
      `,
      {
        applied_to_overrides: appliedToOverrides ? "true" : "false",
      },
    );

    tx.add(
      `
      UPDATE qa_station_clusters_v2
      SET
        status = CASE
          WHEN :'dismiss' = 'true' THEN 'dismissed'
          WHEN :'resolve' = 'true' THEN 'resolved'
          ELSE 'in_review'
        END,
        resolved_at = CASE
          WHEN :'dismiss' = 'true' OR :'resolve' = 'true' THEN now()
          ELSE resolved_at
        END,
        resolved_by = CASE
          WHEN :'dismiss' = 'true' OR :'resolve' = 'true' THEN :'requested_by'
          ELSE resolved_by
        END,
        updated_at = now()
      WHERE cluster_id = :'cluster_id'
      `,
      {
        cluster_id: cleanClusterId,
        dismiss: shouldDismiss ? "true" : "false",
        resolve: shouldResolve ? "true" : "false",
        requested_by: decision.requestedBy,
      },
    );

    tx.add(
      `
      UPDATE canonical_review_queue q
      SET
        status = CASE WHEN :'dismiss' = 'true' THEN 'dismissed' ELSE 'resolved' END,
        resolved_at = now(),
        resolved_by = :'requested_by',
        resolution_note = COALESCE(NULLIF(:'note', ''), format('Resolved by v2 cluster decision on %s', :'cluster_id')),
        updated_at = now()
      WHERE q.review_item_id IN (
        SELECT link.review_item_id
        FROM qa_station_cluster_queue_items_v2 link
        WHERE link.cluster_id = :'cluster_id'
      )
      AND q.status IN ('open', 'confirmed')
      `,
      {
        cluster_id: cleanClusterId,
        dismiss: shouldDismiss ? "true" : "false",
        requested_by: decision.requestedBy,
        note: decision.note || "",
      },
    );

    tx.add(
      `
      SELECT qa_refresh_station_display_names_v2(:'country')
      `,
      {
        country: cluster.country,
      },
    );
  });

  const latestDecision = await client.queryOne(
    `
    SELECT decision_id, operation, created_at, applied_to_overrides
    FROM qa_station_cluster_decisions_v2
    WHERE cluster_id = :'cluster_id'
      AND requested_by = :'requested_by'
    ORDER BY created_at DESC, decision_id DESC
    LIMIT 1
    `,
    {
      cluster_id: cleanClusterId,
      requested_by: decision.requestedBy,
    },
  );

  return {
    ok: true,
    cluster_id: cleanClusterId,
    decision_id: latestDecision ? latestDecision.decision_id : null,
    operation: decision.operation,
    applied_to_overrides: latestDecision
      ? Boolean(latestDecision.applied_to_overrides)
      : false,
  };
}

module.exports = {
  getReviewClustersV2,
  getReviewClusterDetailV2,
  getCuratedStationsV1,
  getCuratedStationDetailV1,
  postReviewClusterDecisionV2,
  postRefreshJob,
  getRefreshJob,
};
