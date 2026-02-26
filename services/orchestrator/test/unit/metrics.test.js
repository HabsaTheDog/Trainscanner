const test = require("node:test");
const assert = require("node:assert/strict");

const { MetricsCollector, labelsText } = require("../../src/core/metrics");

test("labelsText escapes Prometheus label values", () => {
  const rendered = labelsText({
    path: '/api/"routes"',
    note: "line1\nline2",
    slash: "a\\b",
  });

  assert.equal(
    rendered,
    String.raw`{note="line1\nline2",path="/api/\"routes\"",slash="a\\b"}`,
  );
});

test("MetricsCollector aggregates counters, gauges, and histograms", () => {
  const metrics = new MetricsCollector();

  metrics.inc("orchestrator_http_requests_total", {
    method: "GET",
    path: "/health",
  });
  metrics.inc(
    "orchestrator_http_requests_total",
    { method: "GET", path: "/health" },
    2,
  );
  metrics.set("orchestrator_station_cache_entries", {}, 7);
  metrics.observe(
    "orchestrator_http_request_duration_ms",
    { path: "/health" },
    10,
  );
  metrics.observe(
    "orchestrator_http_request_duration_ms",
    { path: "/health" },
    25,
  );

  const output = metrics.renderPrometheus();

  assert.match(
    output,
    /orchestrator_http_requests_total\{method="GET",path="\/health"\} 3/,
  );
  assert.match(output, /orchestrator_station_cache_entries 7/);
  assert.match(
    output,
    /orchestrator_http_request_duration_ms_count\{path="\/health"\} 2/,
  );
  assert.match(
    output,
    /orchestrator_http_request_duration_ms_sum\{path="\/health"\} 35/,
  );
  assert.match(
    output,
    /orchestrator_http_request_duration_ms_min\{path="\/health"\} 10/,
  );
  assert.match(
    output,
    /orchestrator_http_request_duration_ms_max\{path="\/health"\} 25/,
  );
});
