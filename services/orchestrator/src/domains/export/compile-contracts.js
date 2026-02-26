const { AppError } = require("../../core/errors");

const VALID_TIERS = new Set(["high-speed", "regional", "local", "all"]);
const VALID_COUNTRIES = new Set(["DE", "AT", "CH"]);

function todayUtcIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function normalizeTier(value) {
  const raw = String(value || "all")
    .trim()
    .toLowerCase();
  if (raw === "high_speed" || raw === "highspeed") {
    return "high-speed";
  }
  return raw;
}

function isIsoDate(value) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value || "")) {
    return false;
  }
  const parsed = new Date(`${value}T00:00:00Z`);
  return (
    !Number.isNaN(parsed.getTime()) &&
    parsed.toISOString().slice(0, 10) === value
  );
}

function invalidRequest(message) {
  throw new AppError({
    code: "INVALID_REQUEST",
    statusCode: 400,
    message,
  });
}

function readOptionalField(body, fieldName) {
  if (body[fieldName] === undefined) {
    return { provided: false, value: "" };
  }
  return { provided: true, value: String(body[fieldName]).trim() };
}

function readRequiredNonEmptyOptional(body, fieldName) {
  const field = readOptionalField(body, fieldName);
  if (field.provided && !field.value) {
    invalidRequest(
      `Field '${fieldName}' must be a non-empty string when provided`,
    );
  }
  return field.value || undefined;
}

function validateCompileGtfsRequest(body) {
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    invalidRequest("Compile request body must be a JSON object");
  }

  const profileRaw =
    body.profile === undefined
      ? "canonical_runtime"
      : String(body.profile).trim();
  if (!profileRaw) {
    invalidRequest("Field 'profile' must be a non-empty string when provided");
  }

  const tier = normalizeTier(body.tier);
  if (!VALID_TIERS.has(tier)) {
    invalidRequest("Field 'tier' must be one of: high-speed, regional, local, all");
  }

  const asOfRaw =
    body.asOf === undefined || body.asOf === null || body.asOf === ""
      ? todayUtcIsoDate()
      : String(body.asOf).trim();
  if (!isIsoDate(asOfRaw)) {
    invalidRequest("Field 'asOf' must use YYYY-MM-DD format");
  }

  const countryRaw = String(body.country || "")
    .trim()
    .toUpperCase();
  if (countryRaw && !VALID_COUNTRIES.has(countryRaw)) {
    invalidRequest("Field 'country' must be one of: DE, AT, CH");
  }

  const outputDir = readRequiredNonEmptyOptional(body, "outputDir");
  const outputZip = readRequiredNonEmptyOptional(body, "outputZip");
  const summaryJson = readRequiredNonEmptyOptional(body, "summaryJson");

  return {
    profile: profileRaw,
    tier,
    asOf: asOfRaw,
    country: countryRaw || undefined,
    outputDir,
    outputZip,
    summaryJson,
  };
}

module.exports = {
  VALID_TIERS,
  validateCompileGtfsRequest,
};
