const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const path = require('node:path');

const { mkTempDir, startHttpServer, startNodeProcess, stopHttpServer, waitFor, writeJson } = require('../helpers/test-utils');

function jsonResponse(res, statusCode, payload) {
  res.writeHead(statusCode, { 'content-type': 'application/json' });
  res.end(JSON.stringify(payload));
}

test('orchestrator exposes prometheus metrics at /metrics', async (t) => {
  const repoRoot = path.resolve(__dirname, '../../..');
  const temp = await mkTempDir('metrics-endpoint-');

  const configDir = path.join(temp, 'config');
  const stateDir = path.join(temp, 'state');
  const dataDir = path.join(temp, 'data');
  const frontendDir = path.join(temp, 'frontend');

  await fs.mkdir(frontendDir, { recursive: true });
  await fs.writeFile(path.join(frontendDir, 'index.html'), '<html><body>metrics</body></html>\n', 'utf8');
  await writeJson(path.join(configDir, 'gtfs-profiles.json'), {
    profiles: {
      dummy: {
        zipPath: 'data/gtfs/dummy.zip'
      }
    }
  });

  const motisServer = await startHttpServer((req, res) => {
    if (req.url.startsWith('/health')) {
      jsonResponse(res, 200, { ok: true });
      return;
    }
    jsonResponse(res, 404, { error: 'not found' });
  });
  t.after(async () => {
    await stopHttpServer(motisServer.server);
  });

  const probeServer = await startHttpServer((req, res) => {
    res.writeHead(204);
    res.end();
  });
  const port = probeServer.port;
  await stopHttpServer(probeServer.server);

  const orchestrator = startNodeProcess(path.join(repoRoot, 'orchestrator', 'src', 'server.js'), {
    cwd: repoRoot,
    env: {
      PORT: String(port),
      CONFIG_DIR: configDir,
      STATE_DIR: stateDir,
      DATA_DIR: dataDir,
      FRONTEND_DIR: frontendDir,
      METRICS_ENABLED: 'true',
      MOTIS_BASE_URL: motisServer.baseUrl,
      MOTIS_RESTART_MODE: 'none',
      MOTIS_ROUTE_PATH: '/api/v5/plan',
      MOTIS_HEALTH_PATH: '/health',
      MOTIS_REQUEST_TIMEOUT_MS: '5000'
    }
  });
  t.after(async () => {
    await orchestrator.stop();
  });

  const apiUrl = `http://127.0.0.1:${port}`;

  await waitFor(async () => {
    try {
      const res = await fetch(`${apiUrl}/health`);
      return res.status === 200 ? true : null;
    } catch {
      return null;
    }
  }, { timeoutMs: 15000, intervalMs: 200 });

  // Generate one additional measured request before scraping.
  await fetch(`${apiUrl}/health`);

  const metricsRes = await fetch(`${apiUrl}/metrics`);
  assert.equal(metricsRes.status, 200);
  assert.match(metricsRes.headers.get('content-type') || '', /text\/plain/);

  const body = await metricsRes.text();
  assert.match(body, /orchestrator_http_requests_total\{/);
  assert.match(body, /orchestrator_http_request_duration_ms_count\{/);
  assert.match(body, /path="\/health"/);
});
