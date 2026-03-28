const test = require("node:test");
const assert = require("node:assert/strict");

const {
  _internal: { aggregateBenchmarks, buildStepArgs, parseArgs, selectSteps },
} = require("../../src/cli/benchmark-station-review");

test("parseArgs applies benchmark defaults and validates scopes", () => {
  const parsed = parseArgs([
    "--country",
    "DE",
    "--as-of",
    "2026-03-13",
    "--skip-step",
    "reference-data",
  ]);

  assert.equal(parsed.country, "DE");
  assert.equal(parsed.asOf, "2026-03-13");
  assert.equal(parsed.runs, 3);
  assert.equal(parsed.warmupRuns, 1);
  assert.equal(parsed.fromStep, "stop-topology");
  assert.equal(parsed.toStep, "merge-queue");
  assert.deepEqual(parsed.skipSteps, ["reference-data"]);
});

test("selectSteps returns the requested range", () => {
  const steps = selectSteps({
    fromStep: "reference-data",
    toStep: "merge-queue",
    skipSteps: [],
  });

  assert.deepEqual(steps, [
    "reference-data",
    "qa-network-projection",
    "merge-queue",
  ]);
});

test("selectSteps excludes skipped steps inside the requested range", () => {
  const steps = selectSteps({
    fromStep: "global-stations",
    toStep: "merge-queue",
    skipSteps: ["reference-data"],
  });

  assert.deepEqual(steps, [
    "global-stations",
    "qa-network-projection",
    "merge-queue",
  ]);
});

test("buildStepArgs passes country through to merge-queue benchmarks", () => {
  const args = buildStepArgs(
    {
      country: "DE",
      asOf: "2026-03-13",
      sourceId: "db-source",
    },
    "merge-queue",
  );

  assert.deepEqual(args, ["--as-of", "2026-03-13", "--country", "DE"]);
});

test("buildStepArgs passes source and country through to qa-network-projection benchmarks", () => {
  const args = buildStepArgs(
    {
      country: "DE",
      asOf: "2026-03-13",
      sourceId: "db-source",
    },
    "qa-network-projection",
  );

  assert.deepEqual(args, [
    "--as-of",
    "2026-03-13",
    "--country",
    "DE",
    "--source-id",
    "db-source",
  ]);
});

test("aggregateBenchmarks summarizes measured runs and slow phases", () => {
  const aggregate = aggregateBenchmarks([
    {
      runType: "warmup",
      totalDurationMs: 1000,
      resourceSummary: {
        averageCpuPercent: 10,
        peakRssKb: 200,
      },
      steps: [],
    },
    {
      runType: "measured",
      totalDurationMs: 4000,
      resourceSummary: {
        averageCpuPercent: 50,
        peakRssKb: 1200,
      },
      steps: [
        {
          stepId: "global-stations",
          durationMs: 2500,
          metrics: {
            phases: [
              {
                name: "loading_latest_stop_places",
                durationMs: 1000,
              },
              {
                name: "writing_global_stations",
                durationMs: 800,
              },
            ],
          },
          summary: {
            sourceRows: 10,
          },
        },
      ],
    },
    {
      runType: "measured",
      totalDurationMs: 5000,
      resourceSummary: {
        averageCpuPercent: 60,
        peakRssKb: 1600,
      },
      steps: [
        {
          stepId: "global-stations",
          durationMs: 3000,
          metrics: {
            phases: [
              {
                name: "loading_latest_stop_places",
                durationMs: 1200,
              },
              {
                name: "writing_global_stations",
                durationMs: 900,
              },
            ],
          },
          summary: {
            sourceRows: 12,
          },
        },
      ],
    },
  ]);

  assert.equal(aggregate.measuredRuns, 2);
  assert.equal(aggregate.warmupRuns, 1);
  assert.equal(aggregate.medianTotalDurationMs, 4500);
  assert.equal(aggregate.stepMedians["global-stations"], 2750);
  assert.equal(
    aggregate.phaseMedians["global-stations:loading_latest_stop_places"],
    1100,
  );
  assert.equal(aggregate.topSlowPhases.length, 2);
  assert.deepEqual(aggregate.outputCounts["global-stations"], {
    sourceRows: 12,
  });
});
