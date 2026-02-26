const fs = require("node:fs/promises");
const fsSync = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const http = require("node:http");
const { spawn } = require("node:child_process");

async function mkTempDir(prefix = "trainscanner-test-") {
  return fs.mkdtemp(path.join(os.tmpdir(), prefix));
}

async function waitFor(checkFn, options = {}) {
  const timeoutMs = options.timeoutMs || 10000;
  const intervalMs = options.intervalMs || 100;
  const started = Date.now();

  while (Date.now() - started < timeoutMs) {
    const result = await checkFn();
    if (result) {
      return result;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }

  throw new Error(`Timed out after ${timeoutMs}ms`);
}

async function writeJson(filePath, payload) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
}

function startHttpServer(handler) {
  const server = http.createServer(handler);
  return new Promise((resolve, reject) => {
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      resolve({
        server,
        port: address.port,
        baseUrl: `http://127.0.0.1:${address.port}`,
      });
    });
  });
}

async function stopHttpServer(server) {
  if (!server) {
    return;
  }
  await new Promise((resolve) => server.close(() => resolve()));
}

function startNodeProcess(scriptPath, options = {}) {
  const envOverrides =
    options.env && typeof options.env === "object" ? options.env : null;
  const child = spawn(process.execPath, [scriptPath], {
    cwd: options.cwd,
    env: envOverrides ? { ...process.env, ...envOverrides } : process.env,
    stdio: ["ignore", "pipe", "pipe"],
  });

  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => {
    stdout += chunk.toString("utf8");
  });
  child.stderr.on("data", (chunk) => {
    stderr += chunk.toString("utf8");
  });

  return {
    child,
    getLogs() {
      return { stdout, stderr };
    },
    async stop() {
      if (child.exitCode !== null) {
        return;
      }
      child.kill("SIGTERM");
      await new Promise((resolve) => {
        child.once("exit", () => resolve());
      });
    },
  };
}

async function httpJson(url, options = {}) {
  const response = await fetch(url, options);
  const text = await response.text();
  let body = null;
  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      body = { raw: text };
    }
  }
  return {
    status: response.status,
    headers: response.headers,
    body,
  };
}

async function createGtfsZip(zipPath) {
  const dir = path.dirname(zipPath);
  await fs.mkdir(dir, { recursive: true });
  const script = String.raw`
import csv
import io
import zipfile

files = {
  'agency.txt': [
    ['agency_id','agency_name','agency_url','agency_timezone','agency_lang'],
    ['agency_de','Test Agency','https://example.invalid','Europe/Berlin','de']
  ],
  'stops.txt': [
    ['stop_id','stop_name','stop_lat','stop_lon','location_type','parent_station'],
    ['1001','Alpha Station','48.100000','11.500000','0',''],
    ['1002','Beta Station','48.200000','11.600000','0','']
  ],
  'routes.txt': [
    ['route_id','agency_id','route_short_name','route_long_name','route_type'],
    ['r1','agency_de','T1','Test Route','2']
  ],
  'trips.txt': [
    ['route_id','service_id','trip_id','trip_headsign'],
    ['r1','svc1','trip1','Alpha -> Beta']
  ],
  'stop_times.txt': [
    ['trip_id','arrival_time','departure_time','stop_id','stop_sequence'],
    ['trip1','08:00:00','08:00:00','1001','1'],
    ['trip1','08:10:00','08:10:00','1002','2']
  ],
  'calendar.txt': [
    ['service_id','monday','tuesday','wednesday','thursday','friday','saturday','sunday','start_date','end_date'],
    ['svc1','1','1','1','1','1','1','1','20240101','20351231']
  ]
}

with zipfile.ZipFile(r'''${zipPath}''', 'w') as zf:
  for name in ['agency.txt','stops.txt','routes.txt','trips.txt','stop_times.txt','calendar.txt']:
    rows = files[name]
    out = io.StringIO()
    writer = csv.writer(out, lineterminator='\\\\n')
    writer.writerows(rows)
    zf.writestr(name, out.getvalue())
`;
  await new Promise((resolve, reject) => {
    const proc = spawn("python3", ["-c", script], {
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stderr = "";
    proc.stderr.on("data", (chunk) => {
      stderr += chunk.toString("utf8");
    });
    proc.on("exit", (code) => {
      if (code === 0 && fsSync.existsSync(zipPath)) {
        resolve();
      } else {
        reject(new Error(`Failed to create GTFS zip: ${stderr}`));
      }
    });
  });
}

module.exports = {
  createGtfsZip,
  httpJson,
  mkTempDir,
  startHttpServer,
  startNodeProcess,
  stopHttpServer,
  waitFor,
  writeJson,
};
