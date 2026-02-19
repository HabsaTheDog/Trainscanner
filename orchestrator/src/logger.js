const fs = require('node:fs');
const path = require('node:path');

function createLogger(logFilePath) {
  function log(level, message, meta = {}) {
    const entry = {
      ts: new Date().toISOString(),
      level,
      message,
      ...meta
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

  return {
    info: (message, meta) => log('info', message, meta),
    warn: (message, meta) => log('warn', message, meta),
    error: (message, meta) => log('error', message, meta)
  };
}

module.exports = { createLogger };
