export function createBoundsState() {
  return {
    minLat: Infinity,
    maxLat: -Infinity,
    minLon: Infinity,
    maxLon: -Infinity,
    count: 0,
  };
}

export function decodePolyline(encoded, precision) {
  const coords = [];
  if (!encoded || typeof encoded !== "string") {
    return coords;
  }

  const factor = 10 ** (Number.isFinite(precision) ? precision : 5);
  let index = 0;
  let lat = 0;
  let lon = 0;

  while (index < encoded.length) {
    let result = 0;
    let shift = 0;
    let byte = 0;

    do {
      byte = (encoded.codePointAt(index) ?? 63) - 63;
      index += 1;
      result |= (byte & 0x1f) << shift;
      shift += 5;
    } while (byte >= 0x20 && index < encoded.length + 1);

    lat += result & 1 ? ~(result >> 1) : result >> 1;

    result = 0;
    shift = 0;
    do {
      byte = (encoded.codePointAt(index) ?? 63) - 63;
      index += 1;
      result |= (byte & 0x1f) << shift;
      shift += 5;
    } while (byte >= 0x20 && index < encoded.length + 1);

    lon += result & 1 ? ~(result >> 1) : result >> 1;
    coords.push([lon / factor, lat / factor]);
  }

  return coords;
}

export function updateBounds(bounds, lon, lat) {
  if (!Number.isFinite(lon) || !Number.isFinite(lat)) {
    return bounds;
  }
  bounds.minLat = Math.min(bounds.minLat, lat);
  bounds.maxLat = Math.max(bounds.maxLat, lat);
  bounds.minLon = Math.min(bounds.minLon, lon);
  bounds.maxLon = Math.max(bounds.maxLon, lon);
  bounds.count += 1;
  return bounds;
}

export function legLineCoordinates(leg) {
  if (leg?.legGeometry?.points) {
    try {
      const precision = Number(leg.legGeometry.precision);
      const decoded = decodePolyline(leg.legGeometry.points, precision);
      if (decoded.length >= 2) {
        return decoded;
      }
    } catch {
      return fallbackLegCoordinates(leg);
    }
  }

  return fallbackLegCoordinates(leg);
}

function fallbackLegCoordinates(leg) {
  if (
    leg?.from &&
    leg.to &&
    Number.isFinite(leg.from.lat) &&
    Number.isFinite(leg.from.lon) &&
    Number.isFinite(leg.to.lat) &&
    Number.isFinite(leg.to.lon)
  ) {
    return [
      [leg.from.lon, leg.from.lat],
      [leg.to.lon, leg.to.lat],
    ];
  }

  return [];
}

export function pickPrimaryRoute(route) {
  const itineraries = Array.isArray(route?.itineraries)
    ? route.itineraries
    : [];
  if (itineraries.length > 0) {
    return itineraries[0];
  }

  const direct = Array.isArray(route?.direct) ? route.direct : [];
  return direct.length > 0 ? direct[0] : null;
}
