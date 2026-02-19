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

const execFileAsync = promisify(execFile);

const config = loadConfig();
const logger = createLogger(config.switchLogPath);
const switcher = new GtfsSwitcher(config, logger);

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

function isValidDate(value) {
  return value instanceof Date && !Number.isNaN(value.getTime());
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

function parseCoordinateToken(value) {
  const input = String(value || '').trim();
  if (!input) {
    return null;
  }

  const match = input.match(/^(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)$/);
  if (!match) {
    return null;
  }

  const lat = Number.parseFloat(match[1]);
  const lon = Number.parseFloat(match[2]);
  if (!isFiniteCoordinate(lat, lon)) {
    return null;
  }
  return `${lat},${lon}`;
}

function parseBracketId(value) {
  const input = String(value || '').trim();
  const match = input.match(/\[(.+?)\]\s*$/);
  if (!match) {
    return null;
  }
  return match[1].trim() || null;
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
    throw Object.assign(new Error(`Unknown profile '${profileName}'`), { statusCode: 404 });
  }

  const resolved = await resolveProfileArtifact(profileName, profile, {
    dataDir: config.dataDir,
    allowMissing: false
  }).catch((err) => {
    throw Object.assign(new Error(err.message), { statusCode: 404 });
  });

  return resolved;
}

async function getStationIndexForProfile(profileName) {
  const resolved = await resolveProfileZipForQuery(profileName);
  const zipPath = resolved.absolutePath;
  const stat = await fs.stat(zipPath).catch(() => null);
  if (!stat || !stat.isFile()) {
    throw Object.assign(new Error(`GTFS zip not found for profile '${profileName}': ${zipPath}`), { statusCode: 404 });
  }

  const signature = `${zipPath}:${stat.size}:${stat.mtimeMs}`;
  const cached = stationIndexCache.get(profileName);
  if (cached && cached.signature === signature) {
    return cached;
  }

  const unzip = await execFileAsync('unzip', ['-p', zipPath, 'stops.txt'], {
    maxBuffer: 64 * 1024 * 1024
  }).catch((err) => {
    throw Object.assign(
      new Error(`Failed to read stops.txt from ${zipPath}. Ensure 'unzip' exists in orchestrator container.`),
      { cause: err, statusCode: 500 }
    );
  });

  const lines = unzip.stdout.split(/\r?\n/).filter((line) => line.trim().length > 0);
  if (lines.length < 2) {
    throw Object.assign(new Error(`stops.txt is empty in ${zipPath}`), { statusCode: 500 });
  }

  const header = parseCsvLine(lines[0]);
  const idxName = header.indexOf('stop_name');
  const idxId = header.indexOf('stop_id');
  const idxLat = header.indexOf('stop_lat');
  const idxLon = header.indexOf('stop_lon');
  const idxLocationType = header.indexOf('location_type');
  if (idxName < 0 || idxId < 0) {
    throw Object.assign(new Error(`stops.txt missing required columns stop_name/stop_id in ${zipPath}`), {
      statusCode: 500
    });
  }

  const byValue = new Map();
  const byId = new Map();
  const byNameFold = new Map();
  const byValueFold = new Map();
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
  const result = { signature, profileName, zipPath, stations, byId, byNameFold, byValueFold };
  stationIndexCache.set(profileName, result);
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
  const input = String(inputValue || '').trim();
  if (!input) {
    return {
      input,
      resolved: input,
      strategy: 'empty',
      matched: null
    };
  }

  const coordinate = parseCoordinateToken(input);
  if (coordinate) {
    return {
      input,
      resolved: coordinate,
      strategy: 'coordinates',
      matched: null
    };
  }

  if (input.startsWith(`${config.motisDatasetTag}_`)) {
    return {
      input,
      resolved: input,
      strategy: 'tagged_stop_id',
      matched: null
    };
  }

  const index = await getStationIndexForProfile(profileName);
  const folded = foldText(input);
  const bracketId = parseBracketId(input);
  const idCandidate = !bracketId && /^\S+$/.test(input) ? input : null;
  const station =
    (bracketId ? index.byId.get(bracketId) : null) ||
    (idCandidate ? index.byId.get(idCandidate) : null) ||
    index.byValueFold.get(folded) ||
    index.byNameFold.get(folded) ||
    null;

  if (station && station.token) {
    return {
      input,
      resolved: station.token,
      strategy: 'station_lookup',
      matched: {
        id: station.id,
        name: station.name,
        value: station.value,
        token: station.token,
        coordinateToken: station.coordinateToken
      }
    };
  }

  if (bracketId) {
    return {
      input,
      resolved: toTaggedStopId(config.motisDatasetTag, bracketId),
      strategy: 'bracket_id',
      matched: null
    };
  }

  if (idCandidate && /^\d+$/.test(idCandidate)) {
    return {
      input,
      resolved: toTaggedStopId(config.motisDatasetTag, idCandidate),
      strategy: 'numeric_stop_id',
      matched: null
    };
  }

  return {
    input,
    resolved: input,
    strategy: 'raw',
    matched: station
      ? {
          id: station.id,
          name: station.name,
          value: station.value,
          token: station.token,
          coordinateToken: station.coordinateToken
        }
      : null
  };
}


async function parseJsonBody(req) {
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(chunk);
    if (Buffer.concat(chunks).length > 1024 * 1024) {
      throw Object.assign(new Error('Request body too large'), { statusCode: 413 });
    }
  }

  if (chunks.length === 0) {
    return {};
  }

  const raw = Buffer.concat(chunks).toString('utf8');
  try {
    return JSON.parse(raw);
  } catch {
    throw Object.assign(new Error('Invalid JSON body'), { statusCode: 400 });
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

async function handleApi(req, res, url) {
  if (req.method === 'GET' && url.pathname === '/api/gtfs/profiles') {
    const payload = await switcher.getProfilesWithMeta();
    sendJson(res, 200, payload);
    return;
  }

  if (req.method === 'POST' && url.pathname === '/api/gtfs/activate') {
    const body = await parseJsonBody(req);
    if (!body.profile || typeof body.profile !== 'string') {
      sendJson(res, 400, { error: 'Missing required field: profile' });
      return;
    }

    try {
      await switcher.start(body.profile);
      sendJson(res, 202, {
        accepted: true,
        profile: body.profile,
        message: `Profile switch to '${body.profile}' started`
      });
    } catch (err) {
      sendJson(res, err.statusCode || 500, { error: err.message });
    }
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
    const filtered = qFold
      ? index.stations.filter(
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
    } catch {}

    try {
      const gtfsStat = await fs.stat(config.motisActiveGtfsPath);
      motisData.activeGtfsExists = gtfsStat.isFile();
    } catch {}

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
      sendJson(res, 409, {
        error: 'MOTIS is not ready. Route search disabled while switching/importing/restarting.',
        state: status.state,
        message: status.message
      });
      return;
    }

    const origin = body.origin;
    const destination = body.destination;
    const datetime = body.datetime;

    if (!origin || !destination || !datetime) {
      sendJson(res, 400, {
        error: 'Required fields: origin, destination, datetime'
      });
      return;
    }

    const requestDate = new Date(datetime);
    if (!isValidDate(requestDate)) {
      sendJson(res, 400, {
        error: 'Invalid datetime. Use ISO-8601 format, e.g. 2026-02-19T12:00:00Z.'
      });
      return;
    }

    const now = new Date();
    const min = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
    const max = new Date(now.getTime() + 400 * 24 * 60 * 60 * 1000);
    if (requestDate < min || requestDate > max) {
      sendJson(res, 400, {
        error: 'Datetime outside supported range. Pick a time between now - 30 days and the next 400 days.',
        requestDatetime: requestDate.toISOString(),
        min: min.toISOString(),
        max: max.toISOString()
      });
      return;
    }

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
        logger.warn('Failed to resolve station inputs, forwarding raw values to MOTIS', {
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
      sendJson(res, routeResponse.status || 502, {
        error: 'MOTIS route query failed',
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
      });
      return;
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

  sendJson(res, 404, { error: 'Unknown API endpoint' });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

  try {
    if (url.pathname.startsWith('/api/') || url.pathname === '/health') {
      await handleApi(req, res, url);
      return;
    }

    await serveStatic(req, res, url.pathname);
  } catch (err) {
    logger.error('Unhandled request error', {
      step: 'request',
      method: req.method,
      path: url.pathname,
      error: err.message
    });
    sendJson(res, err.statusCode || 500, { error: err.message });
  }
});

async function ensureBootstrapFiles() {
  await fs.mkdir(config.stateDir, { recursive: true });
  await fs.mkdir(config.configDir, { recursive: true });

  try {
    await fs.access(config.switchStatusPath);
  } catch {
    await fs.writeFile(
      config.switchStatusPath,
      JSON.stringify(
        {
          state: 'idle',
          activeProfile: null,
          message: 'No switch executed yet',
          updatedAt: new Date().toISOString(),
          error: null
        },
        null,
        2
      ) + '\n',
      'utf8'
    );
  }

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
