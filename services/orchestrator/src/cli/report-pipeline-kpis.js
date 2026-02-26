#!/usr/bin/env node
const path = require("node:path");

const { createPostgisClient } = require("../data/postgis/client");
const {
  createPipelineJobsRepo,
} = require("../data/postgis/repositories/pipeline-jobs-repo");
const {
  buildKpiPayload,
  writeKpiReport,
} = require("../domains/qa/pipeline-kpis");
const { printCliError } = require("./pipeline-common");

function parseArgs(argv = []) {
  const args = {
    rootDir: process.cwd(),
    windowHours: 24,
    reportDir: "",
    jobType: "",
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--root") {
      args.rootDir = path.resolve(argv[i + 1] || args.rootDir);
      i += 1;
      continue;
    }
    if (arg === "--window-hours") {
      args.windowHours = Number.parseInt(argv[i + 1] || "24", 10);
      i += 1;
      continue;
    }
    if (arg === "--report-dir") {
      args.reportDir = argv[i + 1] || "";
      i += 1;
      continue;
    }
    if (arg === "--job-type") {
      args.jobType = argv[i + 1] || "";
      i += 1;
      continue;
    }
    if (arg === "-h" || arg === "--help") {
      process.stdout.write(
        "Usage: node services/orchestrator/src/cli/report-pipeline-kpis.js [options]\n",
      );
      process.stdout.write(
        "  --root <path>          Repo root (default: cwd)\n",
      );
      process.stdout.write(
        "  --window-hours <n>     Rolling KPI window in hours (default: 24)\n",
      );
      process.stdout.write(
        "  --job-type <type>      Optional pipeline job type filter\n",
      );
      process.stdout.write(
        "  --report-dir <path>    Output dir (default: <root>/reports/qa)\n",
      );
      process.exit(0);
    }
    throw new Error(`Unknown argument: ${arg}`);
  }

  if (!Number.isFinite(args.windowHours) || args.windowHours <= 0) {
    throw new Error("--window-hours must be a positive integer");
  }

  args.reportDir = args.reportDir
    ? path.resolve(args.reportDir)
    : path.join(args.rootDir, "reports", "qa");

  return args;
}

async function run() {
  const args = parseArgs(process.argv.slice(2));
  const client = createPostgisClient({ rootDir: args.rootDir });
  await client.ensureReady();

  const jobsRepo = createPipelineJobsRepo(client);
  const jobs = await jobsRepo.listRecentByType(args.jobType || "", 1000);
  const payload = buildKpiPayload(jobs, {
    windowHours: args.windowHours,
  });

  const reportPath = await writeKpiReport(args.reportDir, {
    ...payload,
    jobType: args.jobType || null,
  });

  process.stdout.write(`${reportPath}\n`);
}

run().catch((err) => {
  printCliError("pipeline-kpis", err, "Pipeline KPI report generation failed");
  process.exit(1);
});
