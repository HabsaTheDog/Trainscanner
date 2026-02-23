const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const path = require("node:path");
const { execFile } = require("node:child_process");
const { promisify } = require("node:util");

const {
  createGtfsZip,
  httpJson,
  mkTempDir,
  startHttpServer,
  startNodeProcess,
  stopHttpServer,
  waitFor,
  writeJson,
} = require("../helpers/test-utils");

const execFileAsync = promisify(execFile);

function jsonResponse(res, statusCode, payload) {
  res.writeHead(statusCode, { "content-type": "application/json" });
  res.end(JSON.stringify(payload));
}

test("e2e profile activation and route smoke/regression", async (t) => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const temp = await mkTempDir("switch-route-e2e-");

  const configDir = path.join(temp, "config");
  const stateDir = path.join(temp, "state");
  const dataDir = path.join(temp, "data");
  const frontendDir = path.join(temp, "frontend");
  const gtfsZipPath = path.join(dataDir, "gtfs", "test-profile.zip");

  await fs.mkdir(frontendDir, { recursive: true });
  await fs.writeFile(
    path.join(frontendDir, "index.html"),
    "<html><body>ok</body></html>\n",
    "utf8",
  );

  await createGtfsZip(gtfsZipPath);

  await writeJson(path.join(configDir, "gtfs-profiles.json"), {
    profiles: {
      test_profile: {
        zipPath: path.relative(temp, gtfsZipPath).split(path.sep).join("/"),
        description: "e2e fixture profile",
      },
    },
  });

  const motisServer = await startHttpServer((req, res) => {
    const url = new URL(req.url, "http://localhost");
    if (url.pathname === "/health") {
      jsonResponse(res, 200, { ok: true });
      return;
    }

    if (url.pathname === "/api/v5/plan") {
      const from =
        url.searchParams.get("fromPlace") || url.searchParams.get("from") || "";
      const to =
        url.searchParams.get("toPlace") || url.searchParams.get("to") || "";
      if (!from || !to) {
        jsonResponse(res, 200, { ok: true, probe: true });
        return;
      }

      jsonResponse(res, 200, {
        itineraries: [
          {
            id: "itinerary_1",
            duration: 600,
          },
        ],
        direct: [],
      });
      return;
    }

    jsonResponse(res, 404, { error: "not found" });
  });
  t.after(async () => {
    await stopHttpServer(motisServer.server);
  });

  const probeServer = await startHttpServer((_req, res) => {
    res.writeHead(204);
    res.end();
  });
  const orchestratorPort = probeServer.port;
  await stopHttpServer(probeServer.server);

  const orchestrator = startNodeProcess(
    path.join(repoRoot, "orchestrator", "src", "server.js"),
    {
      cwd: repoRoot,
      env: {
        PORT: String(orchestratorPort),
        CONFIG_DIR: configDir,
        STATE_DIR: stateDir,
        DATA_DIR: dataDir,
        FRONTEND_DIR: frontendDir,
        MOTIS_BASE_URL: motisServer.baseUrl,
        MOTIS_RESTART_MODE: "none",
        MOTIS_ROUTE_PATH: "/api/v5/plan",
        MOTIS_HEALTH_PATH: "/health",
        MOTIS_REQUEST_TIMEOUT_MS: "5000",
        // Isolate test from shared PostGIS-backed system_state to avoid cross-test contamination.
        CANONICAL_DB_MODE: "direct",
        CANONICAL_DB_HOST: "127.0.0.1",
        CANONICAL_DB_PORT: "1",
        CANONICAL_DB_CONNECT_TIMEOUT_SEC: "1",
      },
    },
  );
  t.after(async () => {
    await orchestrator.stop();
  });

  const apiUrl = `http://127.0.0.1:${orchestratorPort}`;

  await waitFor(
    async () => {
      try {
        const health = await httpJson(`${apiUrl}/health`);
        if (health.status === 200) {
          return health;
        }
        return null;
      } catch {
        return null;
      }
    },
    { timeoutMs: 15000, intervalMs: 200 },
  );

  const activate = await httpJson(`${apiUrl}/api/gtfs/activate`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify({ profile: "test_profile" }),
  });
  assert.ok([200, 202].includes(activate.status));
  assert.equal(Boolean(activate.body.accepted || activate.body.noop), true);
  assert.ok(activate.body.runId || activate.body.noop);

  await waitFor(
    async () => {
      const status = await httpJson(`${apiUrl}/api/gtfs/status`);
      if (status.body.state === "ready") {
        return status;
      }
      if (status.body.state === "failed") {
        const logs = orchestrator.getLogs();
        throw new Error(
          `switch failed: ${JSON.stringify(status.body)}\n${logs.stderr}\n${logs.stdout}`,
        );
      }
      return null;
    },
    { timeoutMs: 15000, intervalMs: 200 },
  );

  const taggedRoute = await waitFor(
    async () => {
      const response = await httpJson(`${apiUrl}/api/routes`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({
          origin: "active-gtfs_1001",
          destination: "active-gtfs_1002",
          datetime: "2026-02-20T08:00:00Z",
        }),
      });

      if (response.status === 409) {
        return null;
      }
      return response;
    },
    { timeoutMs: 10000, intervalMs: 200 },
  );

  assert.equal(taggedRoute.status, 200);
  assert.equal(
    taggedRoute.body.routeRequestResolved.origin.strategy,
    "tagged_stop_id",
  );
  assert.equal(
    taggedRoute.body.routeRequestResolved.destination.strategy,
    "tagged_stop_id",
  );

  const lookupRoute = await httpJson(`${apiUrl}/api/routes`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify({
      origin: "Alpha Station [1001]",
      destination: "Beta Station [1002]",
      datetime: "2026-02-20T08:00:00Z",
    }),
  });

  assert.equal(lookupRoute.status, 200);
  assert.equal(
    lookupRoute.body.routeRequestResolved.origin.strategy,
    "station_lookup",
  );
  assert.equal(
    lookupRoute.body.routeRequestResolved.destination.strategy,
    "station_lookup",
  );

  await execFileAsync(
    process.execPath,
    [
      path.join(
        repoRoot,
        "orchestrator",
        "src",
        "cli",
        "run-route-regression.js",
      ),
      "--api-url",
      apiUrl,
      "--cases",
      path.join(repoRoot, "tests", "routes", "regression_cases.json"),
      "--baselines-dir",
      path.join(repoRoot, "tests", "routes", "baselines"),
      "--report-dir",
      path.join(repoRoot, "reports", "qa"),
    ],
    {
      cwd: repoRoot,
    },
  );
});
