const BRACKET_ID_PATTERN = /\[(.+?)\]\s*$/;

export function pretty(payload) {
  return JSON.stringify(payload, null, 2);
}

export function formatTime(value) {
  if (!value) {
    return "--:--";
  }
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) {
    return String(value);
  }
  return dt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

export function formatDateTime(value) {
  if (!value) {
    return "-";
  }
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) {
    return String(value);
  }
  return dt.toLocaleString();
}

export function durationToText(seconds) {
  const total = Number(seconds || 0);
  if (!Number.isFinite(total) || total <= 0) {
    return "0m";
  }
  const hours = Math.floor(total / 3600);
  const minutes = Math.round((total % 3600) / 60);
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  return `${minutes}m`;
}

export function parseBracketId(value) {
  const input = String(value || "").trim();
  const match = BRACKET_ID_PATTERN.exec(input);
  return match ? match[1].trim() : "";
}

async function readJsonPayload(response) {
  try {
    return await response.json();
  } catch {
    return null;
  }
}

export async function fetchJson(url, options) {
  const response = await fetch(url, options);
  const data = await readJsonPayload(response);
  if (!response.ok) {
    const err = new Error(data?.error || `Request failed (${response.status})`);
    err.payload = data || {};
    err.status = response.status;
    throw err;
  }

  if (data == null) {
    const err = new Error("Invalid JSON response");
    err.payload = {};
    err.status = response.status;
    throw err;
  }

  return data;
}

export function protomapsStyleUrl(globalObject = globalThis) {
  const key = String(globalObject.PROTOMAPS_API_KEY || "").trim();
  if (!key) {
    return null;
  }
  return `https://api.protomaps.com/styles/v4/light/en.json?key=${encodeURIComponent(key)}`;
}

export function mapStyleUrl(globalObject = globalThis) {
  const explicit = String(globalObject.MAP_STYLE_URL || "").trim();
  if (explicit) {
    return explicit;
  }

  const proto = protomapsStyleUrl(globalObject);
  if (proto) {
    return proto;
  }

  return "https://tiles.openfreemap.org/styles/liberty";
}
