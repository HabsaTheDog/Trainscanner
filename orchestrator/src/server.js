const fs = require('node:fs/promises');
const path = require('node:path');
const http = require('node:http');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');

const { loadConfig } = require('./config');
const { createLogger } = require('./logger');
const { GtfsSwitcher } = require('./switcher');
const { normalizeProfiles, resolveProfileArtifact } = require('./profile-resolver');
const { checkMotisHealth, queryMotisRoute } = require('./motis');
const { AppError, errorToPayload, toAppError } = require('./core/errors');
const { resolveCorrelationId } = require('./core/ids');
const { resolveStationInput: resolveStationInputFromIndex } = require('./domains/routing/normalization');
const { validateRouteRequestBody } = require('./domains/routing/contracts');
const { MetricsCollector } = require('./core/metrics');

const execFileAsync = promisify(execFile);

const config = loadConfig();
const logger = createLogger(config.switchLogPath, { service: 'orchestrator' });
const switcher = new GtfsSwitcher(config, logger);
const metrics = new MetricsCollector();

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.txt': 'text/plain; charset=utf-8'
};

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, { 'content-type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(payload));
}

function sendText(res, statusCode, text, contentType = 'text/plain; charset=utf-8') {
  res.writeHead(statusCode, { 'content-type': contentType });
  res.end(String(text || ''));
}

function sendError(res, err, options = {}) {
  const includeDetails = options.includeDetails !== undefined ? options.includeDetails : true;
  const extra = options.extra && typeof options.extra === 'object' ? options.extra : {};
  const { statusCode, payload } = errorToPayload(err, { includeDetails });
  sendJson(res, statusCode, {
    ...payload,
    ...extra
  });
}

function foldText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/\p{Diacritic}/gu, '')
    .toLowerCase()
    .trim();
}

function parseLimit(raw, fallback, min, max) {
  const n = Number.parseInt(raw, 10);
  if (!Number.isFinite(n)) {
    return fallback;
  }
  return Math.min(max, Math.max(min, n));
}

function parseCsvLine(line) {
  const result = [];
  let current = '';
  let quoted = false;

  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      const next = line[i + 1];
      if (quoted && next === '"') {
        current += '"';
        i += 1;
      } else {
        quoted = !quoted;
      }
      continue;
    }
    if (ch === ',' && !quoted) {
      result.push(current);
      current = '';
      continue;
    }
    current += ch;
  }
  result.push(current);
  return result;
}

function isFiniteCoordinate(lat, lon) {
  return Number.isFinite(lat) && Number.isFinite(lon) && Math.abs(lat) <= 90 && Math.abs(lon) <= 180;
}

function toTaggedStopId(tag, stopId) {
  const cleanTag = String(tag || '').trim();
  const cleanId = String(stopId || '').trim();
  if (!cleanId) {
    return '';
  }
  if (!cleanTag) {
    return cleanId;
  }
  if (cleanId.startsWith(`${cleanTag}_`)) {
    return cleanId;
  }
  return `${cleanTag}_${cleanId}`;
}

function stationRank(station) {
  let rank = 0;
  if (station.locationType === '1') {
    rank += 100;
  }
  if (station.locationType === '' || station.locationType === '0') {
    rank += 20;
  }
  if (station.token) {
    rank += 10;
  }
  return rank;
}

function pickPreferredStation(current, candidate) {
  if (!current) {
    return candidate;
  }
  const currentRank = stationRank(current);
  const candidateRank = stationRank(candidate);
  if (candidateRank !== currentRank) {
    return candidateRank > currentRank ? candidate : current;
  }
  return candidate.id.localeCompare(current.id) < 0 ? candidate : current;
}

const stationIndexCache = new Map();

function touchStationCache(profileName, entry) {
  stationIndexCache.delete(profileName);
  stationIndexCache.set(profileName, {
    ...entry,
    cachedAt: entry.cachedAt || Date.now(),
    lastAccessAt: Date.now()
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
    (a, b) => (a[1].lastAccessAt || 0) - (b[1].lastAccessAt || 0)
  );
  const removeCount = stationIndexCache.size - maxEntries;
  for (let i = 0; i < removeCount; i += 1) {
    stationIndexCache.delete(ordered[i][0]);
  }
}

async function loadProfilesMap() {
  const raw = await fs.readFile(config.profilesPath, 'utf8');
  return normalizeProfiles(JSON.parse(raw));
}

async function resolveProfileZipForQuery(profileName) {
  const active = await switcher.readActiveProfile().catch(() => null);
  if (
    active &&
    active.activeProfile === profileName &&
    typeof active.zipPath === 'string' &&
    active.zipPath.trim().length > 0
  ) {
    const absolutePath = path.isAbsolute(active.zipPath)
      ? active.zipPath
      : path.resolve(config.dataDir, '..', active.zipPath);
    const stat = await fs.stat(absolutePath).catch(() => null);
    if (stat && stat.isFile()) {
      return {
        zipPath: active.zipPath,
        absolutePath
      };
    }
  }

  const profiles = await loadProfilesMap();
  const profile = profiles[profileName];
  if (!profile) {
    throw new AppError({
      code: 'UNKNOWN_PROFILE',
      statusCode: 404,
      message: `Unknown profile '${profileName}'`
    });
  }

  const resolved = await resolveProfileArtifact(profileName, profile, {
    dataDir: config.dataDir,
    allowMissing: false
  }).catch((err) => {
    throw toAppError(err, 'PROFILE_ARTIFACT_MISSING');
  });

  return resolved;
}

async function getStationIndexForProfile(profileName) {
  const resolved = await resolveProfileZipForQuery(profileName);
  const zipPath = resolved.absolutePath;
  const stat = await fs.stat(zipPath).catch(() => null);
  if (!stat || !stat.isFile()) {
    throw new AppError({
      code: 'PROFILE_ARTIFACT_MISSING',
      statusCode: 404,
      message: `GTFS zip not found for profile '${profileName}': ${zipPath}`
    });
  }

  const signature = `${zipPath}:${stat.size}:${stat.mtimeMs}`;
  const cached = stationIndexCache.get(profileName);
  if (cached && cached.signature === signature && Date.now() - (cached.cachedAt || 0) <= config.stationIndexCacheTtlMs) {
    touchStationCache(profileName, cached);
    return cached;
  }

  const unzip = await execFileAsync('unzip', ['-p', zipPath, 'stops.txt'], {
    maxBuffer: 64 * 1024 * 1024
  }).catch((err) => {
    throw new AppError({
      code: 'STATION_INDEX_FAILED',
      statusCode: 500,
      message: `Failed to read stops.txt from ${zipPath}. Ensure 'unzip' exists in orchestrator container.`,
      cause: err
    });
  });

  const lines = unzip.stdout.split(/\r?\n/).filter((line) => line.trim().length > 0);
  if (lines.length < 2) {
    throw new AppError({
      code: 'STATION_INDEX_FAILED',
      statusCode: 500,
      message: `stops.txt is empty in ${zipPath}`
    });
  }

  const header = parseCsvLine(lines[0]);
  const idxName = header.indexOf('stop_name');
  const idxId = header.indexOf('stop_id');
  const idxLat = header.indexOf('stop_lat');
  const idxLon = header.indexOf('stop_lon');
  const idxLocationType = header.indexOf('location_type');
  if (idxName < 0 || idxId < 0) {
    throw new AppError({
      code: 'STATION_INDEX_FAILED',
      statusCode: 500,
      message: `stops.txt missing required columns stop_name/stop_id in ${zipPath}`
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
    const name = (row[idxName] || '').trim();
    const id = (row[idxId] || '').trim();
    const latRaw = idxLat >= 0 ? (row[idxLat] || '').trim() : '';
    const lonRaw = idxLon >= 0 ? (row[idxLon] || '').trim() : '';
    const lat = Number.parseFloat(latRaw);
    const lon = Number.parseFloat(lonRaw);
    const hasCoords = isFiniteCoordinate(lat, lon);
    const locationType = idxLocationType >= 0 ? (row[idxLocationType] || '').trim() : '';
    if (!name || !id) {
      continue;
    }

    // Prefer stations / stops, skip entrances/exits.
    if (locationType === '2' || locationType === '3' || locationType === '4') {
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
      valueFold: foldText(value)
    };
    byValue.set(value, station);
    byId.set(id, pickPreferredStation(byId.get(id), station));
    byNameFold.set(station.nameFold, pickPreferredStation(byNameFold.get(station.nameFold), station));
    byValueFold.set(station.valueFold, pickPreferredStation(byValueFold.get(station.valueFold), station));
  }

  const stations = Array.from(byValue.values()).sort((a, b) => a.name.localeCompare(b.name, 'de'));

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
    lastAccessAt: Date.now()
  };
  touchStationCache(profileName, result);
  pruneStationCache();
  return result;
}

async function resolveRouteProfileName(status) {
  if (status && typeof status.activeProfile === 'string' && status.activeProfile.length > 0) {
    return status.activeProfile;
  }
  const profilesWithMeta = await switcher.getProfilesWithMeta();
  if (profilesWithMeta.activeProfile) {
    return profilesWithMeta.activeProfile;
  }
  return profilesWithMeta.profiles[0] ? profilesWithMeta.profiles[0].name : '';
}

async function resolveStationInput(profileName, inputValue) {
  const index = await getStationIndexForProfile(profileName);
  return resolveStationInputFromIndex(inputValue, index, config.motisDatasetTag);
}


async function parseJsonBody(req) {
  const chunks = [];
  let totalBytes = 0;
  for await (const chunk of req) {
    chunks.push(chunk);
    totalBytes += chunk.length;
    if (totalBytes > 1024 * 1024) {
      throw new AppError({
        code: 'REQUEST_TOO_LARGE',
        statusCode: 413,
        message: 'Request body too large'
      });
    }
  }

  if (chunks.length === 0) {
    return {};
  }

  const raw = Buffer.concat(chunks).toString('utf8');
  try {
    return JSON.parse(raw);
  } catch {
    throw new AppError({
      code: 'INVALID_JSON',
      statusCode: 400,
      message: 'Invalid JSON body'
    });
  }
}

async function serveStatic(req, res, urlPath) {
  const cleanPath = urlPath === '/' ? '/index.html' : urlPath;
  const normalized = path.normalize(cleanPath).replace(/^(\.\.[/\\])+/, '');
  const relativePath = normalized.replace(/^[/\\]+/, '');
  const filePath = path.resolve(config.frontendDir, relativePath);
  const frontendBase = path.resolve(config.frontendDir);
  const relativeToBase = path.relative(frontendBase, filePath);

  if (relativeToBase.startsWith('..') || path.isAbsolute(relativeToBase)) {
    sendJson(res, 403, { error: 'Forbidden path' });
    return;
  }

  try {
    const data = await fs.readFile(filePath);
    const ext = path.extname(filePath).toLowerCase();
    const contentType = MIME_TYPES[ext] || 'application/octet-stream';
    res.writeHead(200, { 'content-type': contentType });
    res.end(data);
  } catch (err) {
    if (err.code === 'ENOENT') {
      sendJson(res, 404, { error: 'Not found' });
      return;
    }
    sendJson(res, 500, { error: `Failed to read static file: ${err.message}` });
  }
}

async function handleApi(req, res, url, requestLogger) {
  if (req.method === 'GET' && url.pathname === '/metrics') {
    sendText(res, 200, metrics.renderPrometheus(), 'text/plain; version=0.0.4; charset=utf-8');
    return;
  }

  if (req.method === 'GET' && url.pathname === '/api/qa/queue') {
    const { getReviewQueue } = require('./domains/qa/api');
    const data = await getReviewQueue(url);
    sendJson(res, 200, data);
    return;
  }

  if (req.method === 'POST' && url.pathname === '/api/qa/overrides') {
    const body = await parseJsonBody(req);
    const { postOverride } = require('./domains/qa/api');
    const result = await postOverride(body);
    sendJson(res, 200, result);
    return;
  }

  if (req.method === 'GET' && url.pathname === '/api/gtfs/profiles') {
    const payload = await switcher.getProfilesWithMeta();
    sendJson(res, 200, payload);
    return;
  }

  if (req.method === 'POST' && url.pathname === '/api/gtfs/activate') {
    const body = await parseJsonBody(req);
    if (!body.profile || typeof body.profile !== 'string') {
      throw new AppError({
        code: 'INVALID_REQUEST',
        statusCode: 400,
        message: 'Missing required field: profile'
      });
    }

    const result = await switcher.start(body.profile);
    const statusCode = result.noop ? 200 : 202;
    sendJson(res, statusCode, result);
    return;
  }

  if (req.method === 'GET' && url.pathname === '/api/gtfs/status') {
    const status = await switcher.getStatus();
    sendJson(res, 200, status);
    return;
  }

  if (req.method === 'GET' && url.pathname === '/api/gtfs/stations') {
    const query = (url.searchParams.get('q') || '').trim();
    const limit = parseLimit(url.searchParams.get('limit'), 50, 1, 200);
    const requestedProfile = (url.searchParams.get('profile') || '').trim();

    const profilesWithMeta = await switcher.getProfilesWithMeta();
    const profileName =
      requestedProfile ||
      profilesWithMeta.activeProfile ||
      (profilesWithMeta.profiles[0] ? profilesWithMeta.profiles[0].name : '');

    if (!profileName) {
      sendJson(res, 404, {
        error: 'No GTFS profile available for station lookup'
      });
      return;
    }

    const index = await getStationIndexForProfile(profileName);
    const qFold = foldText(query);
    const bucketKey = qFold.length >= 3 ? qFold.slice(0, 3) : '';
    const bucketRows = bucketKey && index.searchBuckets && index.searchBuckets.has(bucketKey)
      ? index.searchBuckets.get(bucketKey).map((idx) => index.stations[idx])
      : index.stations;

    const filtered = qFold
      ? bucketRows.filter(
        (station) =>
          station.nameFold.includes(qFold) ||
          station.valueFold.includes(qFold) ||
          station.id.includes(query)
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
        lon: station.lon
      }))
    });
    return;
  }

  if (req.method === 'GET' && url.pathname === '/health') {
    const motis = await checkMotisHealth(config);
    const motisDataDir = path.dirname(config.motisActiveGtfsPath);
    const motisData = {
      configExists: false,
      activeGtfsExists: false
    };

    try {
      const configStat = await fs.stat(path.join(motisDataDir, 'config.yml'));
      motisData.configExists = configStat.isFile();
    } catch { }

    try {
      const gtfsStat = await fs.stat(config.motisActiveGtfsPath);
      motisData.activeGtfsExists = gtfsStat.isFile();
    } catch { }

    sendJson(res, 200, {
      status: 'ok',
      service: 'orchestrator',
      motisReady: motis.ok,
      motisStatusCode: motis.status,
      motisData,
      checkedAt: new Date().toISOString()
    });
    return;
  }

  if (req.method === 'POST' && url.pathname === '/api/routes') {
    const body = await parseJsonBody(req);
    const status = await switcher.getStatus();

    if (status.state !== 'ready') {
      throw new AppError({
        code: 'ROUTE_NOT_READY',
        statusCode: 409,
        message: 'MOTIS is not ready. Route search disabled while switching/importing/restarting.',
        details: {
          state: status.state,
          message: status.message
        }
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
      strategy: 'raw',
      matched: null
    };
    let destinationResolved = {
      input: destination,
      resolved: destination,
      strategy: 'raw',
      matched: null
    };

    if (routeProfile) {
      try {
        [originResolved, destinationResolved] = await Promise.all([
          resolveStationInput(routeProfile, origin),
          resolveStationInput(routeProfile, destination)
        ]);
      } catch (err) {
        requestLogger.warn('Failed to resolve station inputs, forwarding raw values to MOTIS', {
          step: 'route_resolve',
          profile: routeProfile,
          error: err.message
        });
      }
    }

    const routeRequest = {
      origin: originResolved.resolved,
      destination: destinationResolved.resolved,
      datetime
    };

    const routeResponse = await queryMotisRoute(config, routeRequest);
    if (!routeResponse.ok) {
      throw new AppError({
        code: 'MOTIS_UNAVAILABLE',
        statusCode: routeResponse.status || 502,
        message: 'MOTIS route query failed',
        details: {
          motisMethod: routeResponse.routeMethod,
          motisPath: routeResponse.routePath,
          motisResponse: routeResponse.body,
          motisAttempts: (routeResponse.routeAttempts || []).slice(0, 30),
          routeRequestResolved: {
            profile: routeProfile || null,
            origin: originResolved,
            destination: destinationResolved,
            datetime
          }
        }
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
        datetime
      },
      route: routeResponse.body
    });
    return;
  }

  throw new AppError({
    code: 'INVALID_REQUEST',
    statusCode: 404,
    message: 'Unknown API endpoint'
  });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const correlationId = resolveCorrelationId(req.headers || {});
  const startedAtNs = process.hrtime.bigint();
  res.setHeader('x-correlation-id', correlationId);
  const requestLogger = logger.child({
    correlationId,
    method: req.method,
    path: url.pathname
  });

  res.on('finish', () => {
    const elapsedMs = Number(process.hrtime.bigint() - startedAtNs) / 1_000_000;
    if (config.metricsEnabled) {
      metrics.inc('orchestrator_http_requests_total', {
        method: req.method || 'UNKNOWN',
        path: url.pathname,
        status: String(res.statusCode || 0)
      });
      metrics.observe('orchestrator_http_request_duration_ms', {
        method: req.method || 'UNKNOWN',
        path: url.pathname
      }, elapsedMs);
      metrics.set('orchestrator_station_cache_entries', {}, stationIndexCache.size);
    }

    requestLogger.info('request completed', {
      statusCode: res.statusCode || 0,
      latencyMs: Number(elapsedMs.toFixed(3))
    });
  });

  try {
    if (url.pathname.startsWith('/api/') || url.pathname === '/health' || url.pathname === '/metrics') {
      await handleApi(req, res, url, requestLogger);
      return;
    }

    await serveStatic(req, res, url.pathname);
  } catch (err) {
    const appErr = toAppError(err);
    requestLogger.error('Unhandled request error', {
      step: 'request',
      error: appErr.message,
      errorCode: appErr.code
    });
    sendError(res, appErr, {
      includeDetails: false,
      extra: appErr.details && typeof appErr.details === 'object' ? appErr.details : {}
    });
  }
});

async function ensureBootstrapFiles() {
  await fs.mkdir(config.stateDir, { recursive: true });
  await fs.mkdir(config.configDir, { recursive: true });

  const motisDataDir = path.dirname(config.motisActiveGtfsPath);
  const startupChecks = [
    {
      file: path.join(motisDataDir, 'config.yml'),
      hint: 'Run scripts/init-motis.sh --profile <name> to generate MOTIS config/import data.'
    },
    {
      file: config.motisActiveGtfsPath,
      hint: 'Run scripts/init-motis.sh --profile <name> to prepare active GTFS input.'
    }
  ];

  for (const check of startupChecks) {
    try {
      const stat = await fs.stat(check.file);
      if (!stat.isFile()) {
        logger.warn('MOTIS startup prerequisite path exists but is not a file', {
          step: 'startup_check',
          file: check.file,
          hint: check.hint
        });
      }
    } catch {
      logger.warn('MOTIS startup prerequisite file missing', {
        step: 'startup_check',
        file: check.file,
        hint: check.hint
      });
    }
  }
}

ensureBootstrapFiles()
  .then(() => {
    server.listen(config.port, () => {
      logger.info('Orchestrator started', {
        step: 'startup',
        port: config.port,
        motisBaseUrl: config.motisBaseUrl,
        configDir: config.configDir,
        stateDir: config.stateDir,
        dataDir: config.dataDir,
        frontendDir: config.frontendDir
      });
    });
  })
  .catch((err) => {
    logger.error('Failed to bootstrap orchestrator', {
      step: 'startup',
      error: err.message
    });
    process.exitCode = 1;
  });
