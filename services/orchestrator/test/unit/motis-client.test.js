const test = require("node:test");
const assert = require("node:assert/strict");

const {
  _internal: {
    buildPlanPaths,
    buildRouteRequestParamCandidates,
    classifyFallbackRouteResponse,
    compareDockerApiVersion,
    isEndpointNotFound,
    parseDockerApiVersion,
    parseStationInput,
    responseText,
    stationCandidates,
    toConfiguredPlanPath,
    uniqueList,
  },
} = require("../../src/motis-client");

test("stationCandidates expands bracketed station inputs without duplicates", () => {
  assert.equal(parseStationInput("Berlin Hbf [8011160]"), "8011160");
  assert.deepEqual(stationCandidates("Berlin Hbf [8011160]"), [
    "Berlin Hbf",
    "8011160",
    "Berlin Hbf [8011160]",
  ]);
  assert.deepEqual(uniqueList(["a", "a", "b"]), ["a", "b"]);
});

test("responseText and endpoint detection classify not-found variants", () => {
  assert.equal(
    responseText({
      body: { raw: "path", message: "not found", error: "missing" },
    }),
    "path not found missing",
  );
  assert.equal(isEndpointNotFound({ status: 404, body: {} }), true);
  assert.equal(
    isEndpointNotFound({
      status: 400,
      body: { message: "Route endpoint not found" },
    }),
    true,
  );
  assert.equal(
    isEndpointNotFound({ status: 500, body: { message: "upstream failed" } }),
    false,
  );
});

test("docker api version parsing and comparison normalize unsupported versions", () => {
  assert.deepEqual(parseDockerApiVersion("1.44"), { major: 1, minor: 44 });
  assert.equal(parseDockerApiVersion("v1.44"), null);
  assert.ok(
    compareDockerApiVersion({ major: 1, minor: 45 }, { major: 1, minor: 44 }) >
      0,
  );
});

test("plan path helpers prioritize configured routes and avoid duplicates", () => {
  assert.equal(
    toConfiguredPlanPath({ motisRoutePath: "api/custom-plan" }),
    "/api/custom-plan",
  );
  assert.deepEqual(buildPlanPaths({ motisRoutePath: "/v5/plan" }).slice(0, 3), [
    "/v5/plan",
    "/api/v5/plan",
    "/api/v4/plan",
  ]);
});

test("route parameter candidates cover query style variants", () => {
  const candidates = buildRouteRequestParamCandidates(
    ["origin"],
    ["destination"],
    "2026-03-11T10:00",
  );
  assert.equal(candidates.length, 6);
  assert.deepEqual(candidates[0], {
    fromPlace: "origin",
    toPlace: "destination",
    time: "2026-03-11T10:00",
    detailedTransfers: true,
  });
});

test("fallback route classification distinguishes endpoint misses from route misses", () => {
  assert.deepEqual(
    classifyFallbackRouteResponse(
      { ok: false, status: 404, body: { error: "not found" } },
      "/v5/plan",
    ),
    {
      routeNotFound: {
        ok: false,
        status: 404,
        body: { error: "not found" },
        routePath: "/v5/plan",
        routeMethod: "GET",
      },
      nonNotFound: null,
    },
  );

  assert.deepEqual(
    classifyFallbackRouteResponse(
      { ok: false, status: 500, body: { error: "boom" } },
      "/v5/plan",
    ),
    {
      routeNotFound: null,
      nonNotFound: {
        ok: false,
        status: 500,
        body: { error: "boom" },
        routePath: "/v5/plan",
        routeMethod: "GET",
      },
    },
  );
});
