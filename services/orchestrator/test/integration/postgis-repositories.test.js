const test = require("node:test");
const assert = require("node:assert/strict");
const { execFileSync, spawnSync } = require("node:child_process");
const path = require("node:path");
const crypto = require("node:crypto");

const { createPostgisClient } = require("../../src/data/postgis/client");
const {
  createImportRunsRepo,
} = require("../../src/data/postgis/repositories/import-runs-repo");
const {
  createPipelineJobsRepo,
} = require("../../src/data/postgis/repositories/pipeline-jobs-repo");

const hasDocker =
  spawnSync("bash", ["-lc", "command -v docker >/dev/null 2>&1"]).status === 0;
const shouldRun = hasDocker && process.env.ENABLE_POSTGIS_TESTS === "1";

test(
  "postgis repositories operate against local docker-compose postgis service",
  { skip: !shouldRun },
  async () => {
    const repoRoot = path.resolve(__dirname, "../../..");

    execFileSync(
      "bash",
      [path.join(repoRoot, "scripts", "data", "db-bootstrap.sh"), "--quiet"],
      {
        cwd: repoRoot,
        stdio: "inherit",
        env: {
          ...process.env,
          CANONICAL_DB_MODE: "docker-compose",
          CANONICAL_DB_DOCKER_PROFILE: "dach-data",
          CANONICAL_DB_DOCKER_SERVICE: "postgis",
        },
      },
    );

    const client = createPostgisClient({
      rootDir: repoRoot,
      env: {
        ...process.env,
        CANONICAL_DB_MODE: "docker-compose",
        CANONICAL_DB_DOCKER_PROFILE: "dach-data",
        CANONICAL_DB_DOCKER_SERVICE: "postgis",
      },
    });

    await client.ensureReady();

    const importRunsRepo = createImportRunsRepo(client);
    const jobsRepo = createPipelineJobsRepo(client);

    const runId = crypto.randomUUID();
    const run = await importRunsRepo.createRun({
      runId,
      pipeline: "netex_ingest",
      status: "running",
      sourceId: "integration_source",
      country: "DE",
      snapshotDate: "2026-02-19",
    });

    assert.equal(run.runId, runId);
    assert.equal(run.pipeline, "netex_ingest");

    const succeeded = await importRunsRepo.markSucceeded({
      runId,
      stats: { loadedRows: 12 },
    });
    assert.equal(succeeded.status, "succeeded");

    const jobId = crypto.randomUUID();
    const createdJob = await jobsRepo.createQueuedJob({
      jobId,
      jobType: "integration.job",
      idempotencyKey: `integration-${Date.now()}`,
      runContext: { scope: "integration" },
    });
    assert.equal(createdJob.jobId, jobId);
    assert.equal(createdJob.status, "queued");

    const runningJob = await jobsRepo.markRunning({ jobId, attempt: 1 });
    assert.equal(runningJob.status, "running");

    const doneJob = await jobsRepo.markSucceeded({
      jobId,
      resultContext: { ok: true },
    });
    assert.equal(doneJob.status, "succeeded");

    const limitedType = "integration.job.claim";
    const limitedJobA = crypto.randomUUID();
    const limitedJobB = crypto.randomUUID();

    await jobsRepo.createQueuedJob({
      jobId: limitedJobA,
      jobType: limitedType,
      idempotencyKey: `integration-claim-a-${Date.now()}`,
      runContext: {},
    });

    await jobsRepo.createQueuedJob({
      jobId: limitedJobB,
      jobType: limitedType,
      idempotencyKey: `integration-claim-b-${Date.now()}`,
      runContext: {},
    });

    const runningA = await jobsRepo.claimRunning({
      jobId: limitedJobA,
      jobType: limitedType,
      attempt: 1,
      maxConcurrent: 1,
    });
    assert.equal(runningA.status, "running");

    const blockedB = await jobsRepo.claimRunning({
      jobId: limitedJobB,
      jobType: limitedType,
      attempt: 1,
      maxConcurrent: 1,
    });
    assert.equal(blockedB, null);

    await jobsRepo.markSucceeded({
      jobId: limitedJobA,
      resultContext: { ok: true },
    });

    const runningB = await jobsRepo.claimRunning({
      jobId: limitedJobB,
      jobType: limitedType,
      attempt: 1,
      maxConcurrent: 1,
    });
    assert.equal(runningB.status, "running");

    const multiType = "integration.job.claim.multi";
    const multiJobA = crypto.randomUUID();
    const multiJobB = crypto.randomUUID();
    const multiJobC = crypto.randomUUID();

    await jobsRepo.createQueuedJob({
      jobId: multiJobA,
      jobType: multiType,
      idempotencyKey: `integration-multi-a-${Date.now()}`,
      runContext: {},
    });
    await jobsRepo.createQueuedJob({
      jobId: multiJobB,
      jobType: multiType,
      idempotencyKey: `integration-multi-b-${Date.now()}`,
      runContext: {},
    });
    await jobsRepo.createQueuedJob({
      jobId: multiJobC,
      jobType: multiType,
      idempotencyKey: `integration-multi-c-${Date.now()}`,
      runContext: {},
    });

    const claimedA = await jobsRepo.claimRunning({
      jobId: multiJobA,
      jobType: multiType,
      attempt: 1,
      maxConcurrent: 2,
    });
    const claimedB = await jobsRepo.claimRunning({
      jobId: multiJobB,
      jobType: multiType,
      attempt: 1,
      maxConcurrent: 2,
    });
    const blockedC = await jobsRepo.claimRunning({
      jobId: multiJobC,
      jobType: multiType,
      attempt: 1,
      maxConcurrent: 2,
    });

    assert.equal(claimedA.status, "running");
    assert.equal(claimedB.status, "running");
    assert.equal(blockedC, null);
  },
);
