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

function validateCompileGtfsRequest(body) {
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "Compile request body must be a JSON object",
    });
  }

  const profileRaw =
    body.profile === undefined
      ? "canonical_runtime"
      : String(body.profile).trim();
  if (!profileRaw) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "Field 'profile' must be a non-empty string when provided",
    });
  }

  const tier = normalizeTier(body.tier);
  if (!VALID_TIERS.has(tier)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "Field 'tier' must be one of: high-speed, regional, local, all",
    });
  }

  const asOfRaw =
    body.asOf === undefined || body.asOf === null || body.asOf === ""
      ? todayUtcIsoDate()
      : String(body.asOf).trim();
  if (!isIsoDate(asOfRaw)) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "Field 'asOf' must use YYYY-MM-DD format",
    });
  }

  let country;
  if (
    body.country !== undefined &&
    body.country !== null &&
    body.country !== ""
  ) {
    country = String(body.country).trim().toUpperCase();
    if (!VALID_COUNTRIES.has(country)) {
      throw new AppError({
        code: "INVALID_REQUEST",
        statusCode: 400,
        message: "Field 'country' must be one of: DE, AT, CH",
      });
    }
  }

  const outputDir =
    body.outputDir === undefined ? "" : String(body.outputDir).trim();
  const outputZip =
    body.outputZip === undefined ? "" : String(body.outputZip).trim();
  const summaryJson =
    body.summaryJson === undefined ? "" : String(body.summaryJson).trim();

  if (body.outputDir !== undefined && !outputDir) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "Field 'outputDir' must be a non-empty string when provided",
    });
  }
  if (body.outputZip !== undefined && !outputZip) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "Field 'outputZip' must be a non-empty string when provided",
    });
  }
  if (body.summaryJson !== undefined && !summaryJson) {
    throw new AppError({
      code: "INVALID_REQUEST",
      statusCode: 400,
      message: "Field 'summaryJson' must be a non-empty string when provided",
    });
  }

  return {
    profile: profileRaw,
    tier,
    asOf: asOfRaw,
    country: country || undefined,
    outputDir: outputDir || undefined,
    outputZip: outputZip || undefined,
    summaryJson: summaryJson || undefined,
  };
}

module.exports = {
  VALID_TIERS,
  validateCompileGtfsRequest,
};
