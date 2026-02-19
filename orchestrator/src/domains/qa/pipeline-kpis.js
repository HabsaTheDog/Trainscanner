const fs = require('node:fs/promises');
const path = require('node:path');

function parseIso(value) {
  if (!value) {
    return null;
  }
  const ts = Date.parse(value);
  return Number.isFinite(ts) ? ts : null;
}

function buildKpiPayload(jobs, options = {}) {
  const windowHours = Number.isFinite(options.windowHours) ? options.windowHours : 24;
  const now = Date.now();
  const cutoff = now - windowHours * 60 * 60 * 1000;

  const scoped = (jobs || []).filter((job) => {
    const startedAt = parseIso(job.startedAt);
    const createdAt = parseIso(job.createdAt);
    const ts = startedAt || createdAt || now;
    return ts >= cutoff;
  });

  const total = scoped.length;
  const succeeded = scoped.filter((job) => job.status === 'succeeded').length;
  const failed = scoped.filter((job) => job.status === 'failed').length;

  const completed = scoped.filter((job) => job.status === 'succeeded' || job.status === 'failed');
  const durationsMs = completed
    .map((job) => {
      const start = parseIso(job.startedAt);
      const end = parseIso(job.endedAt);
      if (start === null || end === null || end < start) {
        return null;
      }
      return end - start;
    })
    .filter((value) => Number.isFinite(value));

  const durationAvgMs =
    durationsMs.length > 0
      ? durationsMs.reduce((sum, value) => sum + value, 0) / durationsMs.length
      : 0;

  const durationP95Ms =
    durationsMs.length > 0
      ? [...durationsMs].sort((a, b) => a - b)[Math.max(0, Math.floor(durationsMs.length * 0.95) - 1)]
      : 0;

  const throughputPerHour = windowHours > 0 ? succeeded / windowHours : 0;
  const failureRatePercent = total > 0 ? (failed / total) * 100 : 0;

  return {
    generatedAt: new Date(now).toISOString(),
    windowHours,
    totalJobs: total,
    succeededJobs: succeeded,
    failedJobs: failed,
    throughputPerHour,
    failureRatePercent,
    durationAvgMs,
    durationP95Ms
  };
}

async function writeKpiReport(reportDir, payload) {
  await fs.mkdir(reportDir, { recursive: true });
  const reportPath = path.join(reportDir, 'pipeline-kpis.json');
  await fs.writeFile(reportPath, JSON.stringify(payload, null, 2) + '\n', 'utf8');
  return reportPath;
}

module.exports = {
  buildKpiPayload,
  writeKpiReport
};
