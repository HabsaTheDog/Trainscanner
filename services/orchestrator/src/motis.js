const http = require("node:http");

function joinUrl(base, pathname) {
  if (pathname.startsWith("/")) {
    return `${base}${pathname}`;
  }
  return `${base}/${pathname}`;
}

function uniqueList(values) {
  const seen = new Set();
  const out = [];
  for (const value of values) {
    if (!seen.has(value)) {
      seen.add(value);
      out.push(value);
    }
  }
  return out;
}

function parseStationInput(value) {
  const input = String(value || "").trim();
  const bracketMatch = /\[(.+?)\]\s*$/.exec(input);
  if (bracketMatch) {
    return bracketMatch[1].trim();
  }
  return input;
}

function stationCandidates(value) {
  const input = String(value || "").trim();
  const out = [];
  const bracketMatch = /^(.*?)\s*\[(.+?)\]\s*$/.exec(input);
  if (bracketMatch) {
    const name = bracketMatch[1].trim();
    const id = bracketMatch[2].trim();
    if (name) {
      out.push(name);
    }
    if (id) {
      out.push(id);
    }
  } else if (input) {
    out.push(parseStationInput(input));
  }
  if (input) {
    out.push(input);
  }
  return uniqueList(out.filter((x) => x.length > 0));
}

function responseText(response) {
  const raw = response.body?.raw || "";
  const message = response.body?.message || "";
  const error = response.body?.error || "";
  return `${raw} ${message} ${error}`.trim();
}

function isEndpointNotFound(response) {
  if (response.status === 404) {
    return true;
  }
  const text = responseText(response);
  return /not found/i.test(text);
}

async function requestJson(url, options = {}, timeoutMs = 10000) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  const headers = options.headers
    ? {
        "content-type": "application/json",
        ...options.headers,
      }
    : { "content-type": "application/json" };

  try {
    const response = await fetch(url, {
      ...options,
      signal: ctrl.signal,
      headers,
    });

    const text = await response.text();
    let parsed;

    if (text.length > 0) {
      try {
        parsed = JSON.parse(text);
      } catch {
        parsed = { raw: text };
      }
    } else {
      parsed = {};
    }

    return {
      ok: response.ok,
      status: response.status,
      body: parsed,
    };
  } finally {
    clearTimeout(timer);
  }
}

async function checkMotisHealth(config) {
  const url = joinUrl(config.motisBaseUrl, config.motisHealthPath);

  try {
    const response = await requestJson(
      url,
      { method: "GET" },
      config.motisRequestTimeoutMs,
    );
    if (
      !response.ok &&
      response.status === 404 &&
      config.motisHealthAccept404
    ) {
      return {
        ok: true,
        status: response.status,
        body: {
          message:
            "Configured MOTIS health endpoint returned 404, treating service as reachable/ready",
          originalBody: response.body,
        },
      };
    }

    return {
      ok: response.ok,
      status: response.status,
      body: response.body,
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      body: { error: err.message },
    };
  }
}

function dockerApiRequest(
  socketPath,
  path,
  method = "POST",
  timeoutMs = 10000,
) {
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        socketPath,
        path,
        method,
        timeout: timeoutMs,
      },
      (res) => {
        const chunks = [];
        res.on("data", (chunk) => chunks.push(chunk));
        res.on("end", () => {
          resolve({
            statusCode: res.statusCode || 0,
            body: Buffer.concat(chunks).toString("utf8"),
          });
        });
      },
    );

    req.on("timeout", () => {
      req.destroy(new Error("docker API request timeout"));
    });
    req.on("error", reject);
    req.end();
  });
}

function parseDockerApiVersion(raw) {
  if (typeof raw !== "string") {
    return null;
  }
  const m = /^(\d+)\.(\d+)$/.exec(raw);
  if (!m) {
    return null;
  }
  return { major: Number.parseInt(m[1], 10), minor: Number.parseInt(m[2], 10) };
}

function compareDockerApiVersion(a, b) {
  if (a.major !== b.major) {
    return a.major - b.major;
  }
  return a.minor - b.minor;
}

async function resolveDockerApiVersion(config) {
  if (config.motisDockerApiVersion && config.motisDockerApiVersion !== "auto") {
    return config.motisDockerApiVersion;
  }

  try {
    const response = await dockerApiRequest(
      config.motisDockerSocketPath,
      "/version",
      "GET",
      config.motisRequestTimeoutMs,
    );

    if (response.statusCode < 200 || response.statusCode >= 300) {
      return "v1.44";
    }

    const payload = JSON.parse(response.body || "{}");
    const rawVersion =
      typeof payload.ApiVersion === "string" ? payload.ApiVersion : "1.44";
    const parsed = parseDockerApiVersion(rawVersion);
    const min = parseDockerApiVersion("1.44");

    if (!parsed || !min) {
      return "v1.44";
    }

    if (compareDockerApiVersion(parsed, min) < 0) {
      return "v1.44";
    }

    return `v${parsed.major}.${parsed.minor}`;
  } catch {
    return "v1.44";
  }
}

async function restartMotisContainer(config) {
  if (config.motisRestartMode === "none") {
    return { skipped: true };
  }

  if (config.motisRestartMode !== "docker") {
    throw new Error(
      `Unsupported MOTIS_RESTART_MODE: ${config.motisRestartMode}`,
    );
  }

  const containerName = encodeURIComponent(config.motisContainerName);
  const apiVersion = await resolveDockerApiVersion(config);
  const apiPath = `/${apiVersion}/containers/${containerName}/restart?t=${config.motisRestartTimeoutSec}`;
  const response = await dockerApiRequest(
    config.motisDockerSocketPath,
    apiPath,
    "POST",
    config.motisRequestTimeoutMs,
  );

  if (response.statusCode !== 204) {
    throw new Error(
      `Failed to restart MOTIS container. Docker API status=${response.statusCode} body=${response.body}`,
    );
  }

  return { skipped: false };
}

async function waitForMotisReady(config, logger) {
  if (config.motisSkipHealthcheck) {
    logger.warn(
      "Skipping MOTIS health checks because MOTIS_SKIP_HEALTHCHECK=true",
      {
        step: "health_poll",
      },
    );
    return {
      ok: true,
      health: {
        ok: true,
        status: 200,
        body: { skipped: true, reason: "MOTIS_SKIP_HEALTHCHECK" },
      },
    };
  }

  const start = Date.now();
  let lastHealth = { ok: false, status: 0, body: { message: "not checked" } };

  while (Date.now() - start < config.motisReadyTimeoutMs) {
    lastHealth = await checkMotisHealth(config);
    if (lastHealth.ok) {
      return { ok: true, health: lastHealth };
    }

    logger.info("MOTIS health check not ready yet", {
      step: "health_poll",
      statusCode: lastHealth.status,
      details: lastHealth.body,
    });

    await new Promise((resolve) =>
      setTimeout(resolve, config.motisHealthPollIntervalMs),
    );
  }

  return { ok: false, health: lastHealth };
}

async function queryMotisRoute(config, payload) {
  const originCandidates = stationCandidates(payload.origin);
  const destinationCandidates = stationCandidates(payload.destination);
  const datetime = String(payload.datetime || "").trim();

  const defaultPlanPaths = [
    "/api/v5/plan",
    "/v5/plan",
    "/api/v4/plan",
    "/v4/plan",
    "/api/v3/plan",
    "/v3/plan",
    "/api/v2/plan",
    "/v2/plan",
    "/api/v1/plan",
    "/v1/plan",
    "/api/plan",
    "/plan",
  ];

  const configuredPath = config.motisRoutePath.startsWith("/")
    ? config.motisRoutePath
    : `/${config.motisRoutePath}`;
  const planPaths = uniqueList([configuredPath, ...defaultPlanPaths]);

  const attempts = [];
  let firstNonNotFound = null;
  let firstRouteNotFound = null;

  for (const planPath of planPaths) {
    // Probe endpoint existence without params.
    const probeUrl = new URL(joinUrl(config.motisBaseUrl, planPath));
    const probe = await requestJson(
      probeUrl.toString(),
      { method: "GET" },
      config.motisRequestTimeoutMs,
    );
    attempts.push({
      method: "GET",
      path: planPath,
      params: {},
      status: probe.status,
      probe: true,
    });

    // Missing endpoint: skip to next path.
    if (probe.status === 404 && isEndpointNotFound(probe)) {
      continue;
    }

    const queryVariants = [
      // MOTIS instance in this stack responds to camelCase place params.
      (from, to, t) => ({
        fromPlace: from,
        toPlace: to,
        time: t,
        detailedTransfers: true,
      }),
      (from, to, t) => ({
        fromPlace: from,
        toPlace: to,
        time: t,
        detailed_transfers: true,
      }),
      // Alternate styles for compatibility across builds.
      (from, to, t) => ({
        from_place: from,
        to_place: to,
        time: t,
        detailed_transfers: true,
      }),
      (from, to, t) => ({
        from: from,
        to: to,
        time: t,
        detailed_transfers: true,
      }),
      (from, to, t) => ({
        from: from,
        to: to,
        departure_time: t,
        detailed_transfers: true,
      }),
      (from, to, t) => ({
        from: from,
        to: to,
        departureTime: t,
        detailed_transfers: true,
      }),
    ];

    for (const origin of originCandidates) {
      for (const destination of destinationCandidates) {
        for (const buildParams of queryVariants) {
          const params = buildParams(origin, destination, datetime);
          const url = new URL(joinUrl(config.motisBaseUrl, planPath));
          for (const [key, value] of Object.entries(params)) {
            url.searchParams.set(key, value);
          }

          const response = await requestJson(
            url.toString(),
            { method: "GET" },
            config.motisRequestTimeoutMs,
          );
          attempts.push({
            method: "GET",
            path: planPath,
            params,
            status: response.status,
          });

          if (response.ok) {
            return {
              ...response,
              routePath: planPath,
              routeMethod: "GET",
              routeAttempts: attempts,
            };
          }

          if (response.status === 404 && isEndpointNotFound(response)) {
            // Endpoint exists (probe succeeded), so this is likely "no route found".
            if (!firstRouteNotFound) {
              firstRouteNotFound = {
                ...response,
                routePath: planPath,
                routeMethod: "GET",
              };
            }
            continue;
          }

          if (!firstNonNotFound) {
            firstNonNotFound = {
              ...response,
              routePath: planPath,
              routeMethod: "GET",
            };
          }
        }
      }
    }
  }

  if (firstRouteNotFound) {
    return {
      ...firstRouteNotFound,
      routeAttempts: attempts,
    };
  }

  if (firstNonNotFound) {
    return {
      ...firstNonNotFound,
      routeAttempts: attempts,
    };
  }

  return {
    ok: false,
    status: 404,
    body: {
      error: "No known MOTIS routing endpoint responded",
      attempts,
    },
    routePath: configuredPath,
    routeMethod: "GET",
    routeAttempts: attempts,
  };
}

module.exports = {
  checkMotisHealth,
  restartMotisContainer,
  waitForMotisReady,
  queryMotisRoute,
};
