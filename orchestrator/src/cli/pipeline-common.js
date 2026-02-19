const path = require('node:path');

const { AppError, toAppError } = require('../core/errors');

function parsePipelineCliArgs(argv = []) {
  const args = Array.isArray(argv) ? argv : [];
  let rootDir = process.cwd();
  let runId = '';
  const passthroughArgs = [];

  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];

    if (arg === '--root') {
      const value = args[i + 1];
      if (!value) {
        throw new AppError({
          code: 'INVALID_REQUEST',
          message: 'Missing value for --root'
        });
      }
      rootDir = path.resolve(value);
      i += 1;
      continue;
    }

    if (arg === '--run-id') {
      const value = args[i + 1];
      if (!value) {
        throw new AppError({
          code: 'INVALID_REQUEST',
          message: 'Missing value for --run-id'
        });
      }
      runId = String(value).trim();
      i += 1;
      continue;
    }

    passthroughArgs.push(arg);
  }

  return {
    rootDir,
    runId,
    passthroughArgs
  };
}

function formatCliError(err, fallbackMessage = 'Pipeline command failed') {
  const appErr = toAppError(err, 'INTERNAL_ERROR', fallbackMessage);
  const details = appErr.details && typeof appErr.details === 'object' ? appErr.details : {};

  return {
    message: appErr.message,
    errorCode: appErr.code,
    runId: typeof details.runId === 'string' ? details.runId : ''
  };
}

function printCliError(prefix, err, fallbackMessage) {
  const payload = formatCliError(err, fallbackMessage);
  const runIdSuffix = payload.runId ? ` runId=${payload.runId}` : '';

  process.stderr.write(
    `[${prefix}] ERROR: ${payload.message} (errorCode=${payload.errorCode}${runIdSuffix})\n`
  );
  process.stderr.write(`${JSON.stringify(payload)}\n`);
}

module.exports = {
  formatCliError,
  parsePipelineCliArgs,
  printCliError
};
