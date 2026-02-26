const crypto = require("node:crypto");

function sanitizeId(value) {
  return String(value || "")
    .trim()
    .replaceAll(/[^A-Za-z0-9._:-]/g, "")
    .slice(0, 120);
}

function generateId(prefix) {
  const core =
    typeof crypto.randomUUID === "function"
      ? crypto.randomUUID()
      : `${Date.now()}-${crypto.randomBytes(4).toString("hex")}`;
  if (!prefix) {
    return core;
  }
  return `${sanitizeId(prefix) || "id"}-${core}`;
}

function resolveCorrelationId(headers, headerName = "x-correlation-id") {
  const key = String(headerName || "x-correlation-id").toLowerCase();
  const candidate = headers?.[key] ? sanitizeId(headers[key]) : "";
  return candidate || generateId("req");
}

module.exports = {
  generateId,
  resolveCorrelationId,
  sanitizeId,
};
