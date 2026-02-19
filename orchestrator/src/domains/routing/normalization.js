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

function resolveStationInput(inputValue, profileIndex, datasetTag) {
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

  if (input.startsWith(`${datasetTag}_`)) {
    return {
      input,
      resolved: input,
      strategy: 'tagged_stop_id',
      matched: null
    };
  }

  const folded = foldText(input);
  const bracketId = parseBracketId(input);
  const idCandidate = !bracketId && /^\S+$/.test(input) ? input : null;
  const station =
    (bracketId ? profileIndex.byId.get(bracketId) : null) ||
    (idCandidate ? profileIndex.byId.get(idCandidate) : null) ||
    profileIndex.byValueFold.get(folded) ||
    profileIndex.byNameFold.get(folded) ||
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
      resolved: toTaggedStopId(datasetTag, bracketId),
      strategy: 'bracket_id',
      matched: null
    };
  }

  if (idCandidate && /^\d+$/.test(idCandidate)) {
    return {
      input,
      resolved: toTaggedStopId(datasetTag, idCandidate),
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

module.exports = {
  foldText,
  parseLimit,
  parseCsvLine,
  isFiniteCoordinate,
  parseCoordinateToken,
  parseBracketId,
  pickPreferredStation,
  resolveStationInput,
  toTaggedStopId
};
