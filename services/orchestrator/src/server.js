const fs = require("node:fs/promises");
const path = require("node:path");
const http = require("node:http");
const { execFile } = require("node:child_process");
const { promisify } = require("node:util");

const { loadConfig } = require("./config");
const { createLogger } = require("./logger");
const { GtfsSwitcher } = require("./switcher");
const {
  normalizeProfiles,
  resolveProfileArtifact,
} = require("./profile-resolver");
const { checkMotisHealth, queryMotisRoute } = require("./motis");
const { AppError, errorToPayload, toAppError } = require("./core/errors");
const { resolveCorrelationId } = require("./core/ids");
const {
  foldText,
  isFiniteCoordinate,
  parseCsvLine,
  parseLimit,
  pickPreferredStation,
  resolveStationInput: resolveStationInputFromIndex,
  toTaggedStopId,
} = require("./domains/routing/normalization");
const { validateRouteRequestBody } = require("./domains/routing/contracts");
const {
  validateCompileGtfsRequest,
} = require("./domains/export/compile-contracts");
const { MetricsCollector } = require("./core/metrics");

const execFileAsync = promisify(execFile);

const config = loadConfig();
const logger = createLogger(config.switchLogPath, { service: "orchestrator" });
const switcher = new GtfsSwitcher(config, logger);
const metrics = new MetricsCollector();

const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".mjs": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".txt": "text/plain; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webp": "image/webp",
  ".woff": "font/woff",
  ".woff2": "font/woff2",
  ".ttf": "font/ttf",
  ".pbf": "application/x-protobuf",
};

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
  });
  res.end(JSON.stringify(payload));
}

function sendText(
  res,
  statusCode,
  text,
  contentType = "text/plain; charset=utf-8",
) {
  res.writeHead(statusCode, { "content-type": contentType });
  res.end(String(text || ""));
}

function sendError(res, err, options = {}) {
  const includeDetails =
    options.includeDetails === undefined ? true : options.includeDetails;
  const extra =
    options.extra && typeof options.extra === "object" ? options.extra : {};
  const { statusCode, payload } = errorToPayload(err, { includeDetails });
  sendJson(res, statusCode, {
    ...payload,
    ...extra,
  });
}

const stationIndexCache = new Map();

function touchStationCache(profileName, entry) {
  stationIndexCache.delete(profileName);
  stationIndexCache.set(profileName, {
    ...entry,
    cachedAt: entry.cachedAt || Date.now(),
    lastAccessAt: Date.now(),
  });
}

function pruneStationCache() {
  const ttlMs = config.stationIndexCacheTtlMs;
  const maxEntries = config.stationIndexCacheMaxEntries;
  const now = Date.now();

  for (const [key, value] of stationIndexCache.entries()) {
    if (now - (value.cachedAt || 0) > ttlMs) {
      stationIndexCache.delete(key);
    }
  }

  if (stationIndexCache.size <= maxEntries) {
    return;
  }

  const ordered = Array.from(stationIndexCache.entries()).sort(
    (a, b) => (a[1].lastAccessAt || 0) - (b[1].lastAccessAt || 0),
  );
  const removeCount = stationIndexCache.size - maxEntries;
  for (let i = 0; i < removeCount; i += 1) {
    stationIndexCache.delete(ordered[i][0]);
  }
}

async function loadProfilesMap() {
  const raw = await fs.readFile(config.profilesPath, "utf8");
  return normalizeProfiles(JSON.parse(raw));
}

async function resolveProfileZipForQuery(profileName) {
  const active = await switcher.readActiveProfile().catch(() => null);
  if (
    active &&
    active.activeProfile === profileName &&
    typeof active.zipPath === "string" &&
    active.zipPath.trim().length > 0
  ) {
    const absolutePath = path.isAbsolute(active.zipPath)
      ? active.zipPath
      : path.resolve(config.dataDir, "..", active.zipPath);
    const stat = await fs.stat(absolutePath).catch(() => null);
    if (stat?.isFile()) {
      return {
        zipPath: active.zipPath,
        absolutePath,
      };
    }
  }

  const profiles = await loadProfilesMap();
  const profile = profiles[profileName];
  if (!profile) {
    throw new AppError({
      code: "UNKNOWN_PROFILE",
      statusCode: 404,
      message: `Unknown profile '${profileName}'`,
    });
  }

  const resolved = await resolveProfileArtifact(profileName, profile, {
    dataDir: config.dataDir,
    allowMissing: false,
  }).catch((err) => {
    throw toAppError(err, "PROFILE_ARTIFACT_MISSING");
  });

  return resolved;
}

async function getStationIndexForProfile(profileName) {
  const resolved = await resolveProfileZipForQuery(profileName);
  const zipPath = resolved.absolutePath;
  const stat = await fs.stat(zipPath).catch(() => null);
  if (!stat?.isFile()) {
    throw new AppError({
      code: "PROFILE_ARTIFACT_MISSING",
      statusCode: 404,
      message: `GTFS zip not found for profile '${profileName}': ${zipPath}`,
    });
  }

  const signature = `${zipPath}:${stat.size}:${stat.mtimeMs}`;
  const cached = stationIndexCache.get(profileName);
  if (
    cached?.signature === signature &&
    Date.now() - (cached.cachedAt || 0) <= config.stationIndexCacheTtlMs
  ) {
    touchStationCache(profileName, cached);
    return cached;
  }

  const unzip = await execFileAsync("unzip", ["-p", zipPath, "stops.txt"], {
    maxBuffer: 64 * 1024 * 1024,
  }).catch((err) => {
    throw new AppError({
      code: "STATION_INDEX_FAILED",
      statusCode: 500,
      message: `Failed to read stops.txt from ${zipPath}. Ensure 'unzip' exists in orchestrator container.`,
      cause: err,
    });
  });

  const lines = unzip.stdout
    .split(/\r?\n/)
    .filter((line) => line.trim().length > 0);
  if (lines.length < 2) {
    throw new AppError({
      code: "STATION_INDEX_FAILED",
      statusCode: 500,
      message: `stops.txt is empty in ${zipPath}`,
    });
  }

  const header = parseCsvLine(lines[0]);
  const idxName = header.indexOf("stop_name");
  const idxId = header.indexOf("stop_id");
  const idxLat = header.indexOf("stop_lat");
  const idxLon = header.indexOf("stop_lon");
  const idxLocationType = header.indexOf("location_type");
  if (idxName < 0 || idxId < 0) {
    throw new AppError({
      code: "STATION_INDEX_FAILED",
      statusCode: 500,
      message: `stops.txt missing required columns stop_name/stop_id in ${zipPath}`,
    });
  }

  const byValue = new Map();
  const byId = new Map();
  const byNameFold = new Map();
  const byValueFold = new Map();
  const bucketSets = new Map();
  function addBucketValue(bucket, stationIndex) {
    if (!bucket || bucket.length < 3) {
      return;
    }
    const key = bucket.slice(0, 3);
    const set = bucketSets.get(key) || new Set();
    set.add(stationIndex);
    bucketSets.set(key, set);
  }

  for (let i = 1; i < lines.length; i += 1) {
    const row = parseCsvLine(lines[i]);
    const name = (row[idxName] || "").trim();
    const id = (row[idxId] || "").trim();
    const latRaw = idxLat >= 0 ? (row[idxLat] || "").trim() : "";
    const lonRaw = idxLon >= 0 ? (row[idxLon] || "").trim() : "";
    const lat = Number.parseFloat(latRaw);
    const lon = Number.parseFloat(lonRaw);
    const hasCoords = isFiniteCoordinate(lat, lon);
    const locationType =
      idxLocationType >= 0 ? (row[idxLocationType] || "").trim() : "";
    if (!name || !id) {
      continue;
    }

    // Prefer stations / stops, skip entrances/exits.
    if (locationType === "2" || locationType === "3" || locationType === "4") {
      continue;
    }

    const value = `${name} [${id}]`;
    if (byValue.has(value)) {
      continue;
    }

    const station = {
      id,
      name,
      value,
      token: toTaggedStopId(config.motisDatasetTag, id),
      coordinateToken: hasCoords ? `${lat},${lon}` : null,
      lat: hasCoords ? lat : null,
      lon: hasCoords ? lon : null,
      locationType,
      nameFold: foldText(name),
      valueFold: foldText(value),
    };
    byValue.set(value, station);
    byId.set(id, pickPreferredStation(byId.get(id), station));
    byNameFold.set(
      station.nameFold,
      pickPreferredStation(byNameFold.get(station.nameFold), station),
    );
    byValueFold.set(
      station.valueFold,
      pickPreferredStation(byValueFold.get(station.valueFold), station),
    );
  }

  const stations = Array.from(byValue.values()).sort((a, b) =>
    a.name.localeCompare(b.name, "de"),
  );

  for (let i = 0; i < stations.length; i += 1) {
    const station = stations[i];
    addBucketValue(station.nameFold, i);
    addBucketValue(station.valueFold, i);
    addBucketValue(station.id, i);
  }

  const searchBuckets = new Map();
  for (const [key, idsSet] of bucketSets.entries()) {
    searchBuckets.set(key, Array.from(idsSet));
  }

  const result = {
    signature,
    profileName,
    zipPath,
    stations,
    byId,
    byNameFold,
    byValueFold,
    searchBuckets,
    cachedAt: Date.now(),
    lastAccessAt: Date.now(),
  };
  touchStationCache(profileName, result);
  pruneStationCache();
  return result;
}

async function resolveRouteProfileName(status) {
  if (
    status &&
    typeof status.activeProfile === "string" &&
    status.activeProfile.length > 0
  ) {
    return status.activeProfile;
  }
  const profilesWithMeta = await switcher.getProfilesWithMeta();
  if (profilesWithMeta.activeProfile) {
    return profilesWithMeta.activeProfile;
  }
  return profilesWithMeta.profiles[0] ? profilesWithMeta.profiles[0].name : "";
}

async function resolveStationInput(profileName, inputValue) {
  const index = await getStationIndexForProfile(profileName);
  return resolveStationInputFromIndex(
    inputValue,
    index,
    config.motisDatasetTag,
  );
}

async function parseJsonBody(req) {
  const chunks = [];
  let totalBytes = 0;
  for await (const chunk of req) {
    chunks.push(chunk);
    totalBytes += chunk.length;
    if (totalBytes > 1024 * 1024) {
      throw new AppError({
        code: "REQUEST_TOO_LARGE",
        statusCode: 413,
        message: "Request body too large",
      });
    }
  }

  if (chunks.length === 0) {
    return {};
  }

  const raw = Buffer.concat(chunks).toString("utf8");
  try {
    return JSON.parse(raw);
  } catch {
    throw new AppError({
      code: "INVALID_JSON",
      statusCode: 400,
      message: "Invalid JSON body",
    });
  }
}

function validateGraphqlRequestBody(body) {
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "GraphQL body must be a JSON object",
    });
  }

  const query = typeof body.query === "string" ? body.query.trim() : "";
  if (!query) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "GraphQL field 'query' must be a non-empty string",
    });
  }

  if (
    body.variables !== undefined &&
    (body.variables === null ||
      typeof body.variables !== "object" ||
      Array.isArray(body.variables))
  ) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "GraphQL field 'variables' must be an object when provided",
    });
  }

  const operationName =
    typeof body.operationName === "string" ? body.operationName : undefined;

  return {
    query,
    variables: body.variables || undefined,
    operationName,
  };
}

async function serveStatic(_req, res, urlPath) {
  const cleanPath = urlPath === "/" ? "/index.html" : urlPath;
  const normalized = path.normalize(cleanPath).replace(/^(\.\.[/\\])+/, "");
  const relativePath = normalized.replace(/^[/\\]+/, "");
  const filePath = path.resolve(config.frontendDir, relativePath);
  const frontendBase = path.resolve(config.frontendDir);
  const relativeToBase = path.relative(frontendBase, filePath);

  if (relativeToBase.startsWith("..") || path.isAbsolute(relativeToBase)) {
    sendJson(res, 403, { error: "Forbidden path" });
    return;
  }

  try {
    const data = await fs.readFile(filePath);
    const ext = path.extname(filePath).toLowerCase();
    const contentType = MIME_TYPES[ext] || "application/octet-stream";
    res.writeHead(200, { "content-type": contentType });
    res.end(data);
  } catch (err) {
    if (err.code === "ENOENT") {
      sendJson(res, 404, { error: "Not found" });
      return;
    }
    sendJson(res, 500, { error: `Failed to read static file: ${err.message}` });
  }
}

async function handleApi(req, res, url, requestLogger) {
  if (req.method === "GET" && url.pathname === "/metrics") {
    sendText(
      res,
      200,
      metrics.renderPrometheus(),
      "text/plain; version=0.0.4; charset=utf-8",
    );
    return;
  }

  // ---> NEW GRAPHQL ENDPOINT <---
  if (req.method === "POST" && url.pathname === "/api/graphql") {
    const { graphql } = require("graphql");
    const { schema } = require("./graphql/schema");
    const { rootValue } = require("./graphql/resolvers");
    const requestBody = validateGraphqlRequestBody(await parseJsonBody(req));

    try {
      const response = await graphql({
        schema,
        source: requestBody.query,
        rootValue,
        variableValues: requestBody.variables,
        operationName: requestBody.operationName,
      });
      sendJson(res, 200, response);
    } catch (e) {
      sendJson(res, 500, { errors: [e.message] });
    }
    return;
  }

  if (req.method === "GET" && url.pathname === "/api/qa/v2/clusters") {
    const { getReviewClustersV2 } = require("./domains/qa/api");
    const data = await getReviewClustersV2(url);
    sendJson(res, 200, data);
    return;
  }

  if (req.method === "GET" && url.pathname === "/api/qa/v2/curated-stations") {
    const { getCuratedStationsV1 } = require("./domains/qa/api");
    const data = await getCuratedStationsV1(url);
    sendJson(res, 200, data);
    return;
  }

  if (req.method === "GET") {
    const clusterDetailMatch = url.pathname.match(
      /^\/api\/qa\/v2\/clusters\/([^/]+)$/,
    );
    if (clusterDetailMatch) {
      const { getReviewClusterDetailV2 } = require("./domains/qa/api");
      const clusterId = decodeURIComponent(clusterDetailMatch[1]);
      const data = await getReviewClusterDetailV2(clusterId);
      sendJson(res, 200, data);
      return;
    }

    const curatedDetailMatch = url.pathname.match(
      /^\/api\/qa\/v2\/curated-stations\/([^/]+)$/,
    );
    if (curatedDetailMatch) {
      const { getCuratedStationDetailV1 } = require("./domains/qa/api");
      const curatedStationId = decodeURIComponent(curatedDetailMatch[1]);
      const data = await getCuratedStationDetailV1(curatedStationId);
      sendJson(res, 200, data);
      return;
    }
  }

  if (req.method === "POST") {
    const clusterDecisionMatch = url.pathname.match(
      /^\/api\/qa\/v2\/clusters\/([^/]+)\/decisions$/,
    );
    if (clusterDecisionMatch) {
      const body = await parseJsonBody(req);
      const { postReviewClusterDecisionV2 } = require("./domains/qa/api");
      const clusterId = decodeURIComponent(clusterDecisionMatch[1]);
      const data = await postReviewClusterDecisionV2(clusterId, body);
      sendJson(res, 200, data);
      return;
    }
  }

  if (req.method === "POST" && url.pathname === "/api/qa/jobs/refresh") {
    const body = await parseJsonBody(req);

    // Connect to Temporal
    const { Connection, Client } = require("@temporalio/client");
    const connection = await Connection.connect({
      address: process.env.TEMPORAL_ADDRESS || "localhost:7233",
    });
    const client = new Client({ connection });

    // Convert body args into bash args for the backwards compatible script wrapper
    const refreshArgs = [];
    if (body.country) refreshArgs.push("--country", body.country);
    if (body.asOf) refreshArgs.push("--as-of", body.asOf);
    if (body.sourceId) refreshArgs.push("--source-id", body.sourceId);
    if (body.dryRun) refreshArgs.push("--dry-run");
    if (body.skipMigrate) refreshArgs.push("--skip-migrate");

    // Trigger the Workflow
    const workflowId = `review-pipeline-${Date.now()}`;
    const handle = await client.workflow.start("stationReviewPipeline", {
      taskQueue: "review-pipeline",
      workflowId,
      args: [{ skipMigrate: !!body.skipMigrate, refreshArgs }],
    });

    sendJson(res, 202, {
      message: "Temporal Workflow Accepted",
      workflowId: handle.workflowId,
    });
    return;
  }

  if (req.method === "GET") {
    const match = url.pathname.match(/^\/api\/qa\/jobs\/([^/]+)$/);
    if (match) {
      const { getRefreshJob } = require("./domains/qa/api");
      const jobId = decodeURIComponent(match[1]);
      const result = await getRefreshJob(jobId, {
        rootDir: process.cwd(),
      });
      sendJson(res, 200, result);
      return;
    }
  }

  if (req.method === "GET" && url.pathname === "/api/gtfs/profiles") {
    const payload = await switcher.getProfilesWithMeta();
    sendJson(res, 200, payload);
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/gtfs/compile") {
    const body = await parseJsonBody(req);
    const compileArgs = validateCompileGtfsRequest(body);

    const { Connection, Client } = require("@temporalio/client");
    const connection = await Connection.connect({
      address: process.env.TEMPORAL_ADDRESS || "localhost:7233",
    });
    const client = new Client({ connection });

    const workflowId = `gtfs-compile-${compileArgs.tier}-${Date.now()}`;
    const handle = await client.workflow.start("compileGtfsArtifact", {
      taskQueue: "review-pipeline",
      workflowId,
      args: [compileArgs],
    });

    sendJson(res, 202, {
      message: "Temporal Workflow Accepted",
      workflowId: handle.workflowId,
      request: compileArgs,
    });
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/gtfs/activate") {
    const body = await parseJsonBody(req);
    if (!body.profile || typeof body.profile !== "string") {
      throw new AppError({
        code: "INVALID_REQUEST",
        statusCode: 400,
        message: "Missing required field: profile",
      });
    }

    const result = await switcher.start(body.profile);
    const statusCode = result.noop ? 200 : 202;
    sendJson(res, statusCode, result);
    return;
  }

  if (req.method === "GET" && url.pathname === "/api/gtfs/status") {
    const status = await switcher.getStatus();
    sendJson(res, 200, status);
    return;
  }

  if (req.method === "GET" && url.pathname === "/api/gtfs/stations") {
    const query = (url.searchParams.get("q") || "").trim();
    const limit = parseLimit(url.searchParams.get("limit"), 50, 1, 200);
    const requestedProfile = (url.searchParams.get("profile") || "").trim();

    const profilesWithMeta = await switcher.getProfilesWithMeta();
    const profileName =
      requestedProfile ||
      profilesWithMeta.activeProfile ||
      (profilesWithMeta.profiles[0] ? profilesWithMeta.profiles[0].name : "");

    if (!profileName) {
      sendJson(res, 404, {
        error: "No GTFS profile available for station lookup",
      });
      return;
    }

    const index = await getStationIndexForProfile(profileName);
    const qFold = foldText(query);
    const bucketKey = qFold.length >= 3 ? qFold.slice(0, 3) : "";
    const bucketRows =
      bucketKey && index.searchBuckets?.has(bucketKey)
        ? index.searchBuckets.get(bucketKey).map((idx) => index.stations[idx])
        : index.stations;

    const filtered = qFold
      ? bucketRows.filter(
          (station) =>
            station.nameFold.includes(qFold) ||
            station.valueFold.includes(qFold) ||
            station.id.includes(query),
        )
      : index.stations;

    sendJson(res, 200, {
      profile: profileName,
      query,
      total: filtered.length,
      stations: filtered.slice(0, limit).map((station) => ({
        id: station.id,
        name: station.name,
        value: station.value,
        token: station.token,
        lat: station.lat,
        lon: station.lon,
      })),
    });
    return;
  }

  if (req.method === "GET" && url.pathname === "/health") {
    const motis = await checkMotisHealth(config);
    const motisDataDir = path.dirname(config.motisActiveGtfsPath);
    const motisData = {
      configExists: false,
      activeGtfsExists: false,
    };

    try {
      const configStat = await fs.stat(path.join(motisDataDir, "config.yml"));
      motisData.configExists = configStat.isFile();
    } catch {}

    try {
      const gtfsStat = await fs.stat(config.motisActiveGtfsPath);
      motisData.activeGtfsExists = gtfsStat.isFile();
    } catch {}

    sendJson(res, 200, {
      status: "ok",
      service: "orchestrator",
      motisReady: motis.ok,
      motisStatusCode: motis.status,
      motisData,
      checkedAt: new Date().toISOString(),
    });
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/routes") {
    const body = await parseJsonBody(req);
    const status = await switcher.getStatus();

    if (status.state !== "ready") {
      throw new AppError({
        code: "ROUTE_NOT_READY",
        statusCode: 409,
        message:
          "MOTIS is not ready. Route search disabled while switching/importing/restarting.",
        details: {
          state: status.state,
          message: status.message,
        },
      });
    }

    const validated = validateRouteRequestBody(body);
    const origin = validated.origin;
    const destination = validated.destination;
    const datetime = validated.datetime;

    const routeProfile = await resolveRouteProfileName(status);
    let originResolved = {
      input: origin,
      resolved: origin,
      strategy: "raw",
      matched: null,
    };
    let destinationResolved = {
      input: destination,
      resolved: destination,
      strategy: "raw",
      matched: null,
    };

    if (routeProfile) {
      try {
        [originResolved, destinationResolved] = await Promise.all([
          resolveStationInput(routeProfile, origin),
          resolveStationInput(routeProfile, destination),
        ]);
      } catch (err) {
        requestLogger.warn(
          "Failed to resolve station inputs, forwarding raw values to MOTIS",
          {
            step: "route_resolve",
            profile: routeProfile,
            error: err.message,
          },
        );
      }
    }

    const routeRequest = {
      origin: originResolved.resolved,
      destination: destinationResolved.resolved,
      datetime,
    };

    const routeResponse = await queryMotisRoute(config, routeRequest);
    if (!routeResponse.ok) {
      throw new AppError({
        code: "MOTIS_UNAVAILABLE",
        statusCode: routeResponse.status || 502,
        message: "MOTIS route query failed",
        details: {
          motisMethod: routeResponse.routeMethod,
          motisPath: routeResponse.routePath,
          motisResponse: routeResponse.body,
          motisAttempts: (routeResponse.routeAttempts || []).slice(0, 30),
          routeRequestResolved: {
            profile: routeProfile || null,
            origin: originResolved,
            destination: destinationResolved,
            datetime,
          },
        },
      });
    }

    sendJson(res, 200, {
      ok: true,
      profile: status.activeProfile || null,
      motisMethod: routeResponse.routeMethod,
      motisPath: routeResponse.routePath,
      motisAttempts: (routeResponse.routeAttempts || []).slice(0, 30),
      routeRequestResolved: {
        profile: routeProfile || null,
        origin: originResolved,
        destination: destinationResolved,
        datetime,
      },
      route: routeResponse.body,
    });
    return;
  }

  // --- Task 5.1: MVT Tile Endpoint -------------------------------------------
  // GET /api/tiles/:z/:x/:y.pbf
  // Serves dynamic Mapbox Vector Tiles from PostGIS using ST_AsMVT.
  if (req.method === "GET") {
    const tileMatch = url.pathname.match(
      /^\/api\/tiles\/([^/]+)\/([^/]+)\/([^/]+)\.pbf$/,
    );
    if (tileMatch) {
      const z = Number.parseInt(tileMatch[1], 10);
      const x = Number.parseInt(tileMatch[2], 10);
      const y = Number.parseInt(tileMatch[3], 10);
      const { serveMvtTile } = require("./domains/qa/mvt");
      const { createPostgisClient } = require("./data/postgis/client");

      // Lazily initialise DB client for tile requests
      if (!handleApi._tileDbClient) {
        handleApi._tileDbClient = createPostgisClient();
        await handleApi._tileDbClient.ensureReady();
      }
      const tileBuffer = await serveMvtTile(handleApi._tileDbClient, {
        z,
        x,
        y,
      });
      res.writeHead(200, {
        "content-type": "application/x-protobuf",
        "content-length": tileBuffer.length,
        "cache-control": "public, max-age=60",
        "access-control-allow-origin": "*",
      });
      res.end(tileBuffer);
      return;
    }
  }

  throw new AppError({
    code: "INVALID_REQUEST",
    statusCode: 404,
    message: "Unknown API endpoint",
  });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
  const correlationId = resolveCorrelationId(req.headers || {});
  const startedAtNs = process.hrtime.bigint();
  res.setHeader("x-correlation-id", correlationId);
  const requestLogger = logger.child({
    correlationId,
    method: req.method,
    path: url.pathname,
  });

  res.on("finish", () => {
    const elapsedMs = Number(process.hrtime.bigint() - startedAtNs) / 1_000_000;
    if (config.metricsEnabled) {
      metrics.inc("orchestrator_http_requests_total", {
        method: req.method || "UNKNOWN",
        path: url.pathname,
        status: String(res.statusCode || 0),
      });
      metrics.observe(
        "orchestrator_http_request_duration_ms",
        {
          method: req.method || "UNKNOWN",
          path: url.pathname,
        },
        elapsedMs,
      );
      metrics.set(
        "orchestrator_station_cache_entries",
        {},
        stationIndexCache.size,
      );
    }

    requestLogger.info("request completed", {
      statusCode: res.statusCode || 0,
      latencyMs: Number(elapsedMs.toFixed(3)),
    });
  });

  try {
    if (
      url.pathname.startsWith("/api/") ||
      url.pathname === "/health" ||
      url.pathname === "/metrics"
    ) {
      await handleApi(req, res, url, requestLogger);
      return;
    }

    await serveStatic(req, res, url.pathname);
  } catch (err) {
    const appErr = toAppError(err);
    requestLogger.error("Unhandled request error", {
      step: "request",
      error: appErr.message,
      errorCode: appErr.code,
    });
    sendError(res, appErr, {
      includeDetails: false,
      extra:
        appErr.details && typeof appErr.details === "object"
          ? appErr.details
          : {},
    });
  }
});

async function ensureBootstrapFiles() {
  await fs.mkdir(config.stateDir, { recursive: true });
  await fs.mkdir(config.configDir, { recursive: true });

  const motisDataDir = path.dirname(config.motisActiveGtfsPath);
  const startupChecks = [
    {
      file: path.join(motisDataDir, "config.yml"),
      hint: "Run scripts/init-motis.sh --profile <name> to generate MOTIS config/import data.",
    },
    {
      file: config.motisActiveGtfsPath,
      hint: "Run scripts/init-motis.sh --profile <name> to prepare active GTFS input.",
    },
    {
      file: path.join(config.frontendDir, "index.html"),
      hint: "Build frontend assets first: (cd frontend && npm ci && npm run build), or rebuild the orchestrator image.",
    },
  ];

  for (const check of startupChecks) {
    try {
      const stat = await fs.stat(check.file);
      if (!stat.isFile()) {
        logger.warn(
          "MOTIS startup prerequisite path exists but is not a file",
          {
            step: "startup_check",
            file: check.file,
            hint: check.hint,
          },
        );
      }
    } catch {
      logger.warn("MOTIS startup prerequisite file missing", {
        step: "startup_check",
        file: check.file,
        hint: check.hint,
      });
    }
  }
}

function startServer() {
  return ensureBootstrapFiles()
    .then(() => {
      server.listen(config.port, () => {
        logger.info("Orchestrator started", {
          step: "startup",
          port: config.port,
          motisBaseUrl: config.motisBaseUrl,
          configDir: config.configDir,
          stateDir: config.stateDir,
          dataDir: config.dataDir,
          frontendDir: config.frontendDir,
        });
      });
    })
    .catch((err) => {
      logger.error("Failed to bootstrap orchestrator", {
        step: "startup",
        error: err.message,
      });
      process.exitCode = 1;
    });
}

void startServer();
