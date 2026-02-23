#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");

function parseArgs(argv) {
  const args = {
    topN: 5,
    country: "",
    output: "",
    ojp: "",
    motis: "",
    rules: "",
  };

  for (let i = 2; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    switch (key) {
      case "--ojp":
        args.ojp = next || "";
        i += 1;
        break;
      case "--motis":
        args.motis = next || "";
        i += 1;
        break;
      case "--rules":
        args.rules = next || "";
        i += 1;
        break;
      case "--top-n":
        args.topN = Number(next);
        i += 1;
        break;
      case "--country":
        args.country = next || "";
        i += 1;
        break;
      case "--output":
        args.output = next || "";
        i += 1;
        break;
      case "-h":
      case "--help":
        printUsage();
        process.exit(0);
        break;
      default:
        throw new Error(`Unknown argument: ${key}`);
    }
  }

  if (!args.ojp || !args.motis || !args.rules) {
    throw new Error(
      "Missing required args. Provide --ojp, --motis, and --rules.",
    );
  }
  if (!Number.isInteger(args.topN) || args.topN <= 0) {
    throw new Error("--top-n must be a positive integer");
  }

  return args;
}

function printUsage() {
  process.stdout.write(
    `Usage: node scripts/data/stitch-prototype.js --ojp FILE --motis FILE --rules FILE [options]\n\n`,
  );
  process.stdout.write(`Options:\n`);
  process.stdout.write(
    `  --top-n N       Keep top N stitched itineraries (default: 5)\n`,
  );
  process.stdout.write(
    `  --country C     Restrict to one country (DE|AT|CH)\n`,
  );
  process.stdout.write(`  --output FILE   Write full report to file\n`);
}

function readJson(filePath) {
  try {
    const raw = fs.readFileSync(filePath, "utf8");
    return JSON.parse(raw);
  } catch (err) {
    throw new Error(`Failed to parse JSON '${filePath}': ${err.message}`);
  }
}

function toIso(ts) {
  if (!ts) return "";
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return "";
  return d.toISOString();
}

function minutesBetween(startIso, endIso) {
  const start = new Date(startIso).getTime();
  const end = new Date(endIso).getTime();
  if (Number.isNaN(start) || Number.isNaN(end)) return null;
  return Math.round((end - start) / 60000);
}

function firstNonEmpty(obj, keys) {
  for (const key of keys) {
    if (
      obj[key] !== undefined &&
      obj[key] !== null &&
      String(obj[key]).trim() !== ""
    ) {
      return String(obj[key]);
    }
  }
  return "";
}

function normalizeFeederSegments(raw) {
  const source = Array.isArray(raw)
    ? raw
    : raw.feederSegments || raw.segments || raw.journeys || [];

  return source
    .map((entry, idx) => {
      const role =
        firstNonEmpty(entry, ["role", "segmentRole"]).toLowerCase() || "access";
      return {
        segmentId:
          firstNonEmpty(entry, ["segmentId", "id"]) || `ojp-seg-${idx + 1}`,
        role,
        country: firstNonEmpty(entry, ["country"]),
        providerId: firstNonEmpty(entry, ["providerId", "provider"]),
        originCanonicalStationId: firstNonEmpty(entry, [
          "originCanonicalStationId",
          "originStationId",
          "fromCanonicalStationId",
        ]),
        destinationCanonicalStationId: firstNonEmpty(entry, [
          "destinationCanonicalStationId",
          "destinationStationId",
          "toCanonicalStationId",
        ]),
        transferHub: firstNonEmpty(entry, ["transferHub", "hub"]),
        departureTime: toIso(
          firstNonEmpty(entry, [
            "departureTime",
            "departure",
            "departureDateTime",
          ]),
        ),
        arrivalTime: toIso(
          firstNonEmpty(entry, ["arrivalTime", "arrival", "arrivalDateTime"]),
        ),
        raw: entry,
      };
    })
    .filter(
      (seg) =>
        seg.originCanonicalStationId &&
        seg.destinationCanonicalStationId &&
        seg.departureTime &&
        seg.arrivalTime,
    );
}

function normalizeBackboneSegments(raw) {
  const source = Array.isArray(raw)
    ? raw
    : raw.backboneItineraries || raw.backbone || raw.connections || [];

  return source
    .map((entry, idx) => ({
      itineraryId:
        firstNonEmpty(entry, ["itineraryId", "tripId", "id"]) ||
        `motis-backbone-${idx + 1}`,
      country: firstNonEmpty(entry, ["country"]),
      originCanonicalStationId: firstNonEmpty(entry, [
        "originCanonicalStationId",
        "originStationId",
        "fromCanonicalStationId",
      ]),
      destinationCanonicalStationId: firstNonEmpty(entry, [
        "destinationCanonicalStationId",
        "destinationStationId",
        "toCanonicalStationId",
      ]),
      transferHubFrom: firstNonEmpty(entry, ["transferHubFrom", "originHub"]),
      transferHubTo: firstNonEmpty(entry, ["transferHubTo", "destinationHub"]),
      departureTime: toIso(
        firstNonEmpty(entry, [
          "departureTime",
          "departure",
          "departureDateTime",
        ]),
      ),
      arrivalTime: toIso(
        firstNonEmpty(entry, ["arrivalTime", "arrival", "arrivalDateTime"]),
      ),
      baseScore: Number(entry.baseScore || 0),
      raw: entry,
    }))
    .filter(
      (seg) =>
        seg.originCanonicalStationId &&
        seg.destinationCanonicalStationId &&
        seg.departureTime &&
        seg.arrivalTime,
    );
}

function normalizeRules(raw) {
  const source = Array.isArray(raw) ? raw : raw.rules || [];
  return source.map((rule) => ({
    ruleId: Number(rule.rule_id || rule.ruleId || 0),
    ruleScope: String(rule.rule_scope || rule.ruleScope || "").toLowerCase(),
    country: String(rule.country || ""),
    canonicalStationId: String(
      rule.canonical_station_id || rule.canonicalStationId || "",
    ),
    hubName: String(rule.hub_name || rule.hubName || ""),
    minTransferMinutes: Number(
      rule.min_transfer_minutes ?? rule.minTransferMinutes ?? 0,
    ),
    longWaitMinutes: Number(
      rule.long_wait_minutes ?? rule.longWaitMinutes ?? 45,
    ),
    priority: Number(rule.priority ?? 100),
    sourceReference: String(
      rule.source_reference || rule.sourceReference || "",
    ),
  }));
}

function pickRule(rules, country, stationId, hubName) {
  const scoped = rules
    .filter((r) => !country || !r.country || r.country === country)
    .sort((a, b) => {
      if (a.priority !== b.priority) return a.priority - b.priority;
      return a.ruleId - b.ruleId;
    });

  const stationRule = scoped.find(
    (r) =>
      r.ruleScope === "station" &&
      stationId &&
      r.canonicalStationId === stationId,
  );
  if (stationRule) return stationRule;

  const hubRule = scoped.find(
    (r) => r.ruleScope === "hub" && hubName && r.hubName === hubName,
  );
  if (hubRule) return hubRule;

  const countryDefault = scoped.find((r) => r.ruleScope === "country_default");
  if (countryDefault) return countryDefault;

  return {
    ruleId: 0,
    ruleScope: "fallback_default",
    country,
    canonicalStationId: "",
    hubName: "",
    minTransferMinutes: 10,
    longWaitMinutes: 60,
    priority: 9999,
    sourceReference: "stitch-prototype-default",
  };
}

function isAccessRole(role) {
  return role !== "egress" && role !== "post";
}

function isEgressRole(role) {
  return role !== "access" && role !== "pre";
}

function buildItineraries(feeders, backbones, rules, topN, countryFilter) {
  const filteredFeeders = countryFilter
    ? feeders.filter((f) => !f.country || f.country === countryFilter)
    : feeders;

  const filteredBackbones = countryFilter
    ? backbones.filter((b) => !b.country || b.country === countryFilter)
    : backbones;

  const stitched = [];

  for (const backbone of filteredBackbones) {
    const accessCandidates = filteredFeeders.filter(
      (seg) =>
        isAccessRole(seg.role) &&
        seg.destinationCanonicalStationId === backbone.originCanonicalStationId,
    );

    const egressCandidates = filteredFeeders.filter(
      (seg) =>
        isEgressRole(seg.role) &&
        seg.originCanonicalStationId === backbone.destinationCanonicalStationId,
    );

    const accessPool = accessCandidates.length > 0 ? accessCandidates : [null];
    const egressPool = egressCandidates.length > 0 ? egressCandidates : [null];

    for (const access of accessPool) {
      for (const egress of egressPool) {
        const startTime = access
          ? access.departureTime
          : backbone.departureTime;
        const endTime = egress ? egress.arrivalTime : backbone.arrivalTime;

        const originRule = pickRule(
          rules,
          backbone.country || countryFilter,
          backbone.originCanonicalStationId,
          backbone.transferHubFrom || (access ? access.transferHub : ""),
        );

        const destinationRule = pickRule(
          rules,
          backbone.country || countryFilter,
          backbone.destinationCanonicalStationId,
          backbone.transferHubTo || (egress ? egress.transferHub : ""),
        );

        const transferInMinutes = access
          ? minutesBetween(access.arrivalTime, backbone.departureTime)
          : null;
        const transferOutMinutes = egress
          ? minutesBetween(backbone.arrivalTime, egress.departureTime)
          : null;

        const tightIn =
          transferInMinutes !== null &&
          transferInMinutes < originRule.minTransferMinutes;
        const tightOut =
          transferOutMinutes !== null &&
          transferOutMinutes < destinationRule.minTransferMinutes;
        const longIn =
          transferInMinutes !== null &&
          transferInMinutes > originRule.longWaitMinutes;
        const longOut =
          transferOutMinutes !== null &&
          transferOutMinutes > destinationRule.longWaitMinutes;

        const timeOrderBroken =
          (transferInMinutes !== null && transferInMinutes < 0) ||
          (transferOutMinutes !== null && transferOutMinutes < 0);

        const totalDurationMinutes = minutesBetween(startTime, endTime);
        const waitPenalty =
          (transferInMinutes && transferInMinutes > 0 ? transferInMinutes : 0) +
          (transferOutMinutes && transferOutMinutes > 0
            ? transferOutMinutes
            : 0);

        const riskPenalty =
          (tightIn || tightOut ? 100 : 0) +
          (longIn || longOut ? 20 : 0) +
          (timeOrderBroken ? 200 : 0);

        const rankScore =
          (totalDurationMinutes ?? 999999) +
          waitPenalty +
          riskPenalty +
          backbone.baseScore;

        stitched.push({
          itineraryId: `${backbone.itineraryId}|${access ? access.segmentId : "no-access"}|${egress ? egress.segmentId : "no-egress"}`,
          country: backbone.country || countryFilter || "",
          score: rankScore,
          totalDurationMinutes,
          flags: {
            tight_connection: tightIn || tightOut,
            long_wait: longIn || longOut,
            invalid_time_order: timeOrderBroken,
          },
          transferChecks: {
            inbound: access
              ? {
                  stationId: backbone.originCanonicalStationId,
                  transferMinutes: transferInMinutes,
                  requiredMinMinutes: originRule.minTransferMinutes,
                  longWaitMinutes: originRule.longWaitMinutes,
                  ruleScope: originRule.ruleScope,
                  ruleId: originRule.ruleId,
                  sourceReference: originRule.sourceReference,
                }
              : null,
            outbound: egress
              ? {
                  stationId: backbone.destinationCanonicalStationId,
                  transferMinutes: transferOutMinutes,
                  requiredMinMinutes: destinationRule.minTransferMinutes,
                  longWaitMinutes: destinationRule.longWaitMinutes,
                  ruleScope: destinationRule.ruleScope,
                  ruleId: destinationRule.ruleId,
                  sourceReference: destinationRule.sourceReference,
                }
              : null,
          },
          stitchedSegments: {
            access: access,
            backbone: backbone,
            egress: egress,
          },
        });
      }
    }
  }

  stitched.sort((a, b) => {
    if (a.score !== b.score) return a.score - b.score;
    const aDuration = a.totalDurationMinutes ?? Number.MAX_SAFE_INTEGER;
    const bDuration = b.totalDurationMinutes ?? Number.MAX_SAFE_INTEGER;
    return aDuration - bDuration;
  });

  return stitched.slice(0, topN);
}

function main() {
  const args = parseArgs(process.argv);
  const ojpRaw = readJson(args.ojp);
  const motisRaw = readJson(args.motis);
  const rulesRaw = readJson(args.rules);

  const feeders = normalizeFeederSegments(ojpRaw);
  const backbones = normalizeBackboneSegments(motisRaw);
  const rules = normalizeRules(rulesRaw);

  if (feeders.length === 0) {
    throw new Error(
      "No usable feeder segments found in OJP input. Expected feederSegments/segments with canonical station ids and timestamps.",
    );
  }
  if (backbones.length === 0) {
    throw new Error(
      "No usable backbone segments found in MOTIS input. Expected backbone/backboneItineraries with canonical station ids and timestamps.",
    );
  }

  const ranked = buildItineraries(
    feeders,
    backbones,
    rules,
    args.topN,
    args.country,
  );

  const report = {
    generatedAt: new Date().toISOString(),
    filters: {
      country: args.country || null,
      topN: args.topN,
    },
    inputSummary: {
      feederSegments: feeders.length,
      backboneSegments: backbones.length,
      transferRules: rules.length,
    },
    rankedItineraries: ranked,
  };

  if (args.output) {
    const outputDir = path.dirname(args.output);
    fs.mkdirSync(outputDir, { recursive: true });
    fs.writeFileSync(
      args.output,
      `${JSON.stringify(report, null, 2)}\n`,
      "utf8",
    );
  }

  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}

try {
  main();
} catch (err) {
  process.stderr.write(`[stitch-prototype] ERROR: ${err.message}\n`);
  process.exit(1);
}
