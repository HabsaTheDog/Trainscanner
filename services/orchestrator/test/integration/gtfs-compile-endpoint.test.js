const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const path = require("node:path");

const {
  httpJson,
  mkTempDir,
  startHttpServer,
  startNodeProcess,
  stopHttpServer,
  waitFor,
  writeJson,
} = require("../helpers/test-utils");

function jsonResponse(res, statusCode, payload) {
  res.writeHead(statusCode, { "content-type": "application/json" });
  res.end(JSON.stringify(payload));
}

test("POST /api/gtfs/compile validates request payload", async (t) => {
  const servicesRoot = path.resolve(__dirname, "../../..");
  const repoRoot = path.resolve(servicesRoot, "..");
  const temp = await mkTempDir("gtfs-compile-endpoint-");

  const configDir = path.join(temp, "config");
  const stateDir = path.join(temp, "state");
  const dataDir = path.join(temp, "data");
  const frontendDir = path.join(temp, "frontend");

  await fs.mkdir(frontendDir, { recursive: true });
  await fs.writeFile(
    path.join(frontendDir, "index.html"),
    "<html><body>compile-endpoint</body></html>\n",
    "utf8",
  );
  await writeJson(path.join(configDir, "gtfs-profiles.json"), {
    profiles: {
      dummy: {
        zipPath: "data/gtfs/dummy.zip",
      },
    },
  });

  const motisServer = await startHttpServer((req, res) => {
    if (req.url.startsWith("/health")) {
      jsonResponse(res, 200, { ok: true });
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
  const port = probeServer.port;
  await stopHttpServer(probeServer.server);

  const orchestrator = startNodeProcess(
    path.join(servicesRoot, "orchestrator", "src", "server.js"),
    {
      cwd: repoRoot,
      env: {
        PORT: String(port),
        CONFIG_DIR: configDir,
        STATE_DIR: stateDir,
        DATA_DIR: dataDir,
        FRONTEND_DIR: frontendDir,
        METRICS_ENABLED: "true",
        MOTIS_BASE_URL: motisServer.baseUrl,
        MOTIS_RESTART_MODE: "none",
        MOTIS_ROUTE_PATH: "/api/v5/plan",
        MOTIS_HEALTH_PATH: "/health",
        MOTIS_REQUEST_TIMEOUT_MS: "5000",
      },
    },
  );
  t.after(async () => {
    await orchestrator.stop();
  });

  const apiUrl = `http://127.0.0.1:${port}`;
  await waitFor(
    async () => {
      try {
        const res = await fetch(`${apiUrl}/health`);
        return res.status === 200 ? true : null;
      } catch {
        return null;
      }
    },
    { timeoutMs: 15000, intervalMs: 200 },
  );

  const response = await httpJson(`${apiUrl}/api/gtfs/compile`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      profile: "sample_profile",
      asOf: "2026-02-20",
      tier: "invalid-tier",
    }),
  });

  assert.equal(response.status, 400);
  assert.equal(response.body.errorCode, "INVALID_REQUEST");
  assert.match(response.body.error || "", /Field 'tier' must be one of/);
});
