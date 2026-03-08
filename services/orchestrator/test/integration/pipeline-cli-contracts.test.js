const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { execFile } = require("node:child_process");
const { promisify } = require("node:util");

const execFileAsync = promisify(execFile);

const cliCases = [
  {
    file: "fetch-sources.js",
    expectedUsage: /Usage: scripts\/data\/fetch-sources\.sh/,
  },
  {
    file: "verify-sources.js",
    expectedUsage: /Usage: scripts\/data\/verify-sources\.sh/,
  },
  {
    file: "ingest-netex.js",
    expectedUsage: /Usage: scripts\/data\/ingest-netex\.sh/,
  },
  {
    file: "build-global-stations.js",
    expectedUsage: /Usage: scripts\/data\/build-global-stations\.sh/,
  },
  {
    file: "build-global-merge-queue.js",
    expectedUsage: /Usage: scripts\/data\/build-global-merge-queue\.sh/,
  },
  {
    file: "report-review-queue.js",
    expectedUsage: /Usage: scripts\/data\/report-review-queue\.sh/,
  },
  {
    file: "refresh-station-review.js",
    expectedUsage: /Usage: scripts\/data\/refresh-station-review\.sh/,
  },
];

for (const cliCase of cliCases) {
  test(`${cliCase.file} exposes script wrapper --help contract`, async () => {
    const orchestratorRoot = path.resolve(__dirname, "../..");
    const repoRoot = path.resolve(orchestratorRoot, "../..");
    const cliPath = path.join(orchestratorRoot, "src", "cli", cliCase.file);

    const result = await execFileAsync(
      process.execPath,
      [cliPath, "--root", repoRoot, "--help"],
      {
        cwd: repoRoot,
      },
    );

    assert.match(result.stdout, cliCase.expectedUsage);
  });
}

test("pipeline CLI returns machine-readable error payload for invalid wrapper args", async () => {
  const orchestratorRoot = path.resolve(__dirname, "../..");
  const repoRoot = path.resolve(orchestratorRoot, "../..");
  const cliPath = path.join(orchestratorRoot, "src", "cli", "fetch-sources.js");

  await assert.rejects(
    execFileAsync(process.execPath, [cliPath, "--root"], {
      cwd: repoRoot,
    }),
    (err) => {
      assert.match(err.stderr, /errorCode=INVALID_REQUEST/);
      assert.match(err.stderr, /"errorCode":"INVALID_REQUEST"/);
      return true;
    },
  );
});

test("refresh station review CLI validates selected steps", async () => {
  const orchestratorRoot = path.resolve(__dirname, "../..");
  const repoRoot = path.resolve(orchestratorRoot, "../..");
  const cliPath = path.join(
    orchestratorRoot,
    "src",
    "cli",
    "refresh-station-review.js",
  );

  await assert.rejects(
    execFileAsync(
      process.execPath,
      [cliPath, "--root", repoRoot, "--only", "bad-step"],
      {
        cwd: repoRoot,
      },
    ),
    (err) => {
      assert.match(err.stderr, /errorCode=INVALID_REQUEST/);
      assert.match(
        err.stderr,
        /must be one of fetch\|ingest\|global-stations\|merge-queue/,
      );
      return true;
    },
  );
});
