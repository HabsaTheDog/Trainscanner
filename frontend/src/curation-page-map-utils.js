export const COORDINATE_BUCKET_PRECISION = 7;
export const BASE_MARKER_SIZE = 24;
export const MARKER_SELECTION_RING_SIZE = 3;
export const STACK_GROUP_DISTANCE = 24;

function coordinateBucketKey(lat, lon) {
  return `${Number(lat).toFixed(COORDINATE_BUCKET_PRECISION)}:${Number(lon).toFixed(COORDINATE_BUCKET_PRECISION)}`;
}

function groupItemsByCoordinate(items) {
  const groups = new Map();
  for (const item of items) {
    const key = coordinateBucketKey(item.lat, item.lon);
    const existing = groups.get(key);
    if (existing) {
      existing.items.push(item);
      continue;
    }
    groups.set(key, { key, lat: item.lat, lon: item.lon, items: [item] });
  }
  return Array.from(groups.values());
}

function findProjectedStackGroup(stackGroups, point) {
  for (const group of stackGroups) {
    if (
      Math.hypot(group.screenX - point.x, group.screenY - point.y) <=
      STACK_GROUP_DISTANCE
    ) {
      return group;
    }
  }
  return null;
}

function updateProjectedGroup(group, point, item) {
  group.items.push(item);
  const itemCount = group.items.length;
  group.screenX = (group.screenX * (itemCount - 1) + point.x) / itemCount;
  group.screenY = (group.screenY * (itemCount - 1) + point.y) / itemCount;
  group.anchorLat = (group.anchorLat * (itemCount - 1) + item.lat) / itemCount;
  group.anchorLon = (group.anchorLon * (itemCount - 1) + item.lon) / itemCount;
}

function createProjectedGroup(point, item) {
  return {
    items: [item],
    screenX: point.x,
    screenY: point.y,
    anchorLat: item.lat,
    anchorLon: item.lon,
  };
}

function approximatePeerDistance(candidate, item) {
  return Math.abs(
    Number(candidate.candidate?.candidate_rank || 9999) -
      Number(item.candidate?.candidate_rank || 9999),
  );
}

function normalizeDisplayName(value) {
  return String(value || "")
    .trim()
    .toLowerCase();
}

function buildApproximateItem(item, peers) {
  if (peers.length === 0) {
    return { ...item, approx: false };
  }

  const sampledPeers = peers.slice(0, 2);
  const lat =
    sampledPeers.reduce((sum, peer) => sum + Number(peer.lat), 0) /
    sampledPeers.length;
  const lon =
    sampledPeers.reduce((sum, peer) => sum + Number(peer.lon), 0) /
    sampledPeers.length;

  return {
    ...item,
    lat,
    lon,
    approx: true,
  };
}

export function buildMarkerOverlapLayout(map, items) {
  if (!map) {
    return new Map();
  }

  const layout = new Map();
  const projectedGroups = [];

  for (const group of groupItemsByCoordinate(items)) {
    for (const item of group.items) {
      const point = map.project([item.lon, item.lat]);
      const existingGroup =
        findProjectedStackGroup(projectedGroups, point) ||
        createProjectedGroup(point, item);

      if (projectedGroups.includes(existingGroup)) {
        updateProjectedGroup(existingGroup, point, item);
      } else {
        projectedGroups.push(existingGroup);
      }
    }
  }

  for (const group of projectedGroups) {
    for (const [stackIndex, item] of group.items.entries()) {
      const sizeMultiplier = Math.max(1, group.items.length - stackIndex);
      layout.set(item.ref, {
        stackIndex,
        stackSize: group.items.length,
        sizeMultiplier,
        markerSize: BASE_MARKER_SIZE * sizeMultiplier,
        zIndex: 2000 + stackIndex,
        aLat: group.anchorLat,
        aLon: group.anchorLon,
      });
    }
  }

  return layout;
}

export function buildMappableItems(items) {
  const rows = Array.isArray(items) ? items : [];

  return rows
    .map((item) => {
      if (Number.isFinite(item.lat) && Number.isFinite(item.lon)) {
        return { ...item, approx: false };
      }

      const displayName = normalizeDisplayName(item.display_name);
      const peers = rows
        .filter((candidate) => {
          return (
            candidate.ref !== item.ref &&
            Number.isFinite(candidate.lat) &&
            Number.isFinite(candidate.lon) &&
            normalizeDisplayName(candidate.display_name) === displayName
          );
        })
        .sort((left, right) => {
          return (
            approximatePeerDistance(left, item) -
            approximatePeerDistance(right, item)
          );
        });

      return buildApproximateItem(item, peers);
    })
    .filter((item) => Number.isFinite(item.lat) && Number.isFinite(item.lon));
}
