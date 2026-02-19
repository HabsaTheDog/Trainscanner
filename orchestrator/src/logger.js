const fs = require('node:fs');
const path = require('node:path');

function normalizeMeta(meta) {
  if (!meta || typeof meta !== 'object') {
    return {};
  }

  const next = {};
  for (const [key, value] of Object.entries(meta)) {
    if (value instanceof Error) {
      next[key] = {
        name: value.name,
        message: value.message
      };
      continue;
    }
    next[key] = value;
  }
  return next;
}

function createLogger(logFilePath, baseMeta = {}) {
  const rootMeta = normalizeMeta(baseMeta);

  function log(level, message, meta = {}) {
    const mergedMeta = {
      ...rootMeta,
      ...normalizeMeta(meta)
    };

    const entry = {
      ts: new Date().toISOString(),
      level,
      message,
      service: mergedMeta.service || null,
      runId: mergedMeta.runId || null,
      correlationId: mergedMeta.correlationId || null,
      errorCode: mergedMeta.errorCode || null,
      latencyMs:
        Number.isFinite(mergedMeta.latencyMs) || Number.isFinite(mergedMeta.latency)
          ? Number(mergedMeta.latencyMs ?? mergedMeta.latency)
          : null,
      ...mergedMeta
    };
    const line = JSON.stringify(entry);
    if (level === 'error') {
      console.error(line);
    } else {
      console.log(line);
    }

    try {
      fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
      fs.appendFileSync(logFilePath, line + '\n', 'utf8');
    } catch (err) {
      console.error(JSON.stringify({
        ts: new Date().toISOString(),
        level: 'error',
        message: 'failed to write log file',
        error: err.message
      }));
    }
  }

  const logger = {
    info: (message, meta) => log('info', message, meta),
    warn: (message, meta) => log('warn', message, meta),
    error: (message, meta) => log('error', message, meta),
    child(meta = {}) {
      return createLogger(logFilePath, {
        ...rootMeta,
        ...normalizeMeta(meta)
      });
    }
  };

  return logger;
}

module.exports = { createLogger };
