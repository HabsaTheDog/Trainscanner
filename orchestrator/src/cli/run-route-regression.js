#!/usr/bin/env node
const _fs = require("node:fs/promises");
const path = require("node:path");
const {
  loadRouteCaseFile,
  loadBaseline,
  writeQaReport,
} = require("../domains/qa/contracts");

function parseArgs(argv) {
  const args = {
    apiUrl: "http://localhost:3000",
    casesFile: "",
    baselinesDir: "",
    reportDir: path.resolve(process.cwd(), "reports/qa"),
    failOnDiff: true,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--api-url") {
      args.apiUrl = argv[i + 1] || args.apiUrl;
      i += 1;
      continue;
    }
    if (arg === "--cases") {
      args.casesFile = argv[i + 1] || "";
      i += 1;
      continue;
    }
    if (arg === "--baselines-dir") {
      args.baselinesDir = argv[i + 1] || "";
      i += 1;
      continue;
    }
    if (arg === "--report-dir") {
      args.reportDir = argv[i + 1] || args.reportDir;
      i += 1;
      continue;
    }
    if (arg === "--no-fail-on-diff") {
      args.failOnDiff = false;
      continue;
    }
    if (arg === "-h" || arg === "--help") {
      printHelp();
      process.exit(0);
    }
    throw new Error(`Unknown argument: ${arg}`);
  }

  if (!args.casesFile) {
    throw new Error("Missing --cases <path>");
  }
  if (!args.baselinesDir) {
    throw new Error("Missing --baselines-dir <path>");
  }

  args.apiUrl = args.apiUrl.replace(/\/$/, "");
  return args;
}

function printHelp() {
  process.stdout.write(
    "Usage: node orchestrator/src/cli/run-route-regression.js --cases <path> --baselines-dir <dir> [options]\n",
  );
  process.stdout.write("Options:\n");
  process.stdout.write(
    "  --api-url <url>           API base URL (default: http://localhost:3000)\n",
  );
  process.stdout.write("  --cases <path>            Route cases file\n");
  process.stdout.write("  --baselines-dir <path>    Baselines directory\n");
  process.stdout.write(
    "  --report-dir <path>       Report output directory (default: reports/qa)\n",
  );
  process.stdout.write(
    "  --no-fail-on-diff         Exit 0 even when baseline mismatches exist\n",
  );
}

async function postJson(url, payload) {
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  const text = await response.text();
  let parsed;
  try {
    parsed = text ? JSON.parse(text) : {};
  } catch {
    parsed = { raw: text };
  }

  return {
    status: response.status,
    body: parsed,
  };
}

function normalizeRouteResponse(caseDef, response) {
  const body = response.body || {};
  const route = body.route || {};
  const itineraries = Array.isArray(route.itineraries)
    ? route.itineraries.length
    : 0;
  const direct = Array.isArray(route.direct) ? route.direct.length : 0;
  const attempts = Array.isArray(body.motisAttempts)
    ? body.motisAttempts.length
    : 0;

  return {
    caseId: caseDef.id,
    status: response.status,
    errorCode: body.errorCode || null,
    itineraryCount: itineraries,
    directCount: direct,
    motisAttemptCount: attempts,
    originStrategy: body.routeRequestResolved?.origin
      ? body.routeRequestResolved.origin.strategy || null
      : null,
    destinationStrategy: body.routeRequestResolved?.destination
      ? body.routeRequestResolved.destination.strategy || null
      : null,
  };
}

async function runCase(apiUrl, caseDef) {
  const response = await postJson(`${apiUrl}/api/routes`, {
    origin: caseDef.origin,
    destination: caseDef.destination,
    datetime: caseDef.datetime,
  });

  return {
    request: {
      origin: caseDef.origin,
      destination: caseDef.destination,
      datetime: caseDef.datetime,
    },
    response: normalizeRouteResponse(caseDef, response),
  };
}

async function readBaselineForCase(caseDef, baselinesDir) {
  const baselinePath = path.join(baselinesDir, `${caseDef.id}.json`);
  const baseline = await loadBaseline(baselinePath);
  return {
    baselinePath,
    baseline,
  };
}

function deepEqual(a, b) {
  return JSON.stringify(a) === JSON.stringify(b);
}

async function run() {
  const args = parseArgs(process.argv.slice(2));
  const routeCases = await loadRouteCaseFile(path.resolve(args.casesFile));

  const caseResults = [];
  for (const caseDef of routeCases.cases) {
    const { baselinePath, baseline } = await readBaselineForCase(
      caseDef,
      path.resolve(args.baselinesDir),
    );
    const execution = await runCase(args.apiUrl, caseDef);
    const match = deepEqual(execution.response, baseline.expected);

    caseResults.push({
      caseId: caseDef.id,
      label: caseDef.label || "",
      baselinePath,
      expected: baseline.expected,
      actual: execution.response,
      pass: match,
    });
  }

  const failed = caseResults.filter((entry) => !entry.pass);
  const report = {
    generatedAt: new Date().toISOString(),
    apiUrl: args.apiUrl,
    casesFile: path.resolve(args.casesFile),
    baselinesDir: path.resolve(args.baselinesDir),
    summary: {
      total: caseResults.length,
      passed: caseResults.length - failed.length,
      failed: failed.length,
    },
    cases: caseResults,
  };

  const reportFile = `route-regression-${new Date().toISOString().replace(/[:.]/g, "-")}.json`;
  const reportPath = await writeQaReport(args.reportDir, reportFile, report);

  process.stdout.write(`[route-regression] report=${reportPath}\n`);
  process.stdout.write(
    `[route-regression] total=${report.summary.total} passed=${report.summary.passed} failed=${report.summary.failed}\n`,
  );

  if (failed.length > 0) {
    for (const fail of failed) {
      process.stdout.write(`[route-regression] FAIL ${fail.caseId}\n`);
    }
    if (args.failOnDiff) {
      process.exit(1);
    }
  }
}

run().catch((err) => {
  process.stderr.write(`[route-regression] ERROR: ${err.message}\n`);
  process.exit(1);
});
