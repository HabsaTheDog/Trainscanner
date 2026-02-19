const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const path = require('node:path');

const { AppError } = require('../../core/errors');

const execFileAsync = promisify(execFile);

function commandExists(cmd) {
  return execFileAsync('bash', ['-lc', `command -v ${cmd}`], { maxBuffer: 1024 * 1024 })
    .then(() => true)
    .catch(() => false);
}

function parseBool(value, fallback) {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }
  return String(value).toLowerCase() === 'true';
}

function toInt(value, fallback) {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }
  const parsed = Number.parseInt(String(value), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function resolveConnectionConfig(options = {}) {
  const env = { ...process.env, ...(options.env || {}) };

  const rootDir = path.resolve(options.rootDir || process.cwd());
  const mode = String(options.mode || env.CANONICAL_DB_MODE || 'auto').trim();

  const config = {
    rootDir,
    mode,
    url: env.CANONICAL_DB_URL || env.DATABASE_URL || '',
    host: env.CANONICAL_DB_HOST || env.PGHOST || 'localhost',
    port: env.CANONICAL_DB_PORT || env.PGPORT || '5432',
    user: env.CANONICAL_DB_USER || env.PGUSER || 'trainscanner',
    database: env.CANONICAL_DB_NAME || env.PGDATABASE || 'trainscanner',
    password: env.CANONICAL_DB_PASSWORD || env.PGPASSWORD || 'trainscanner',
    dockerProfile: env.CANONICAL_DB_DOCKER_PROFILE || 'dach-data',
    dockerService: env.CANONICAL_DB_DOCKER_SERVICE || 'postgis',
    readyTimeoutSec: toInt(options.readyTimeoutSec || env.CANONICAL_DB_READY_TIMEOUT_SEC, 90),
    connectTimeoutSec: toInt(options.connectTimeoutSec || env.CANONICAL_DB_CONNECT_TIMEOUT_SEC, 3),
    maxBuffer: toInt(options.maxBuffer, 64 * 1024 * 1024),
    useDockerTty: parseBool(options.useDockerTty, false)
  };

  config.hasExplicitDirectTarget = Boolean(
    env.CANONICAL_DB_URL ||
      env.DATABASE_URL ||
      env.CANONICAL_DB_HOST ||
      env.CANONICAL_DB_PORT ||
      env.CANONICAL_DB_USER ||
      env.CANONICAL_DB_NAME ||
      env.PGHOST ||
      env.PGPORT ||
      env.PGUSER ||
      env.PGDATABASE
  );

  return config;
}

function buildVariableArgs(params = {}) {
  const args = ['-v', 'ON_ERROR_STOP=1'];
  for (const [key, value] of Object.entries(params || {})) {
    args.push('-v', `${key}=${value === undefined || value === null ? '' : String(value)}`);
  }
  return args;
}

function parseRowsFromJsonOutput(stdout) {
  const text = String(stdout || '').trim();
  if (!text) {
    return [];
  }

  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  if (lines.length === 0) {
    return [];
  }

  const last = lines[lines.length - 1];
  try {
    const parsed = JSON.parse(last);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

async function runExecFile(command, args, options = {}) {
  try {
    return await execFileAsync(command, args, {
      cwd: options.cwd,
      env: options.env,
      maxBuffer: options.maxBuffer || 64 * 1024 * 1024
    });
  } catch (err) {
    const stderr = (err && err.stderr ? String(err.stderr) : '').trim();
    const stdout = (err && err.stdout ? String(err.stdout) : '').trim();

    throw new AppError({
      code: 'INTERNAL_ERROR',
      message: stderr || stdout || err.message || `Command failed: ${command}`,
      details: {
        command,
        args,
        stderr,
        stdout
      },
      cause: err
    });
  }
}

function createPostgisClient(options = {}) {
  const config = resolveConnectionConfig(options);

  let resolvedMode = '';

  async function buildDirectPsqlArgs(baseArgs = []) {
    if (config.url) {
      return [config.url, ...baseArgs];
    }
    return [
      '-h',
      config.host,
      '-p',
      String(config.port),
      '-U',
      config.user,
      '-d',
      config.database,
      ...baseArgs
    ];
  }

  async function probeDirectConnection() {
    const baseArgs = ['-At', '-c', 'SELECT 1;'];
    const args = await buildDirectPsqlArgs(baseArgs);
    try {
      await runExecFile('psql', args, {
        cwd: config.rootDir,
        env: {
          ...process.env,
          PGPASSWORD: config.password,
          PGCONNECT_TIMEOUT: String(config.connectTimeoutSec)
        },
        maxBuffer: config.maxBuffer
      });
      return true;
    } catch {
      return false;
    }
  }

  async function resolveMode() {
    if (resolvedMode) {
      return resolvedMode;
    }

    if (config.mode === 'direct' || config.mode === 'docker-compose') {
      resolvedMode = config.mode;
      return resolvedMode;
    }

    if (config.mode !== 'auto') {
      throw new AppError({
        code: 'INVALID_CONFIG',
        message: `Invalid CANONICAL_DB_MODE '${config.mode}' (expected auto, direct, docker-compose)`
      });
    }

    const hasPsql = await commandExists('psql');
    if (hasPsql) {
      const directOk = await probeDirectConnection();
      if (directOk) {
        resolvedMode = 'direct';
        return resolvedMode;
      }

      if (config.hasExplicitDirectTarget) {
        throw new AppError({
          code: 'INVALID_CONFIG',
          message:
            'Auto DB mode detected explicit direct DB config, but direct connection probe failed. Fix connectivity or set CANONICAL_DB_MODE=docker-compose.'
        });
      }
    } else if (config.hasExplicitDirectTarget) {
      throw new AppError({
        code: 'INVALID_CONFIG',
        message:
          'Auto DB mode detected explicit direct DB config, but psql is not installed. Install psql or set CANONICAL_DB_MODE=docker-compose.'
      });
    }

    const hasDocker = await commandExists('docker');
    if (!hasDocker) {
      throw new AppError({
        code: 'INVALID_CONFIG',
        message: 'docker is required for CANONICAL_DB_MODE=docker-compose'
      });
    }

    resolvedMode = 'docker-compose';
    return resolvedMode;
  }

  async function runPsql(baseArgs = [], extraOptions = {}) {
    const mode = await resolveMode();
    if (mode === 'docker-compose') {
      const args = [
        'compose',
        '--profile',
        config.dockerProfile,
        'exec',
        ...(config.useDockerTty ? [] : ['-T']),
        config.dockerService,
        'psql',
        '-U',
        config.user,
        '-d',
        config.database,
        ...baseArgs
      ];
      return runExecFile('docker', args, {
        cwd: config.rootDir,
        env: process.env,
        maxBuffer: extraOptions.maxBuffer || config.maxBuffer
      });
    }

    const args = await buildDirectPsqlArgs(baseArgs);
    return runExecFile('psql', args, {
      cwd: config.rootDir,
      env: {
        ...process.env,
        PGPASSWORD: config.password,
        PGCONNECT_TIMEOUT: String(config.connectTimeoutSec)
      },
      maxBuffer: extraOptions.maxBuffer || config.maxBuffer
    });
  }

  async function ensureReady() {
    const started = Date.now();

    if ((await resolveMode()) === 'docker-compose') {
      await runExecFile(
        'docker',
        ['compose', '--profile', config.dockerProfile, 'up', '-d', config.dockerService],
        {
          cwd: config.rootDir,
          env: process.env,
          maxBuffer: config.maxBuffer
        }
      );
    }

    while (Date.now() - started < config.readyTimeoutSec * 1000) {
      try {
        await runPsql(['-At', '-v', 'ON_ERROR_STOP=1', '-c', 'SELECT 1;']);
        return;
      } catch {
        await new Promise((resolve) => setTimeout(resolve, 2000));
      }
    }

    throw new AppError({
      code: 'INTERNAL_ERROR',
      message: `Database did not become ready within ${config.readyTimeoutSec}s`
    });
  }

  async function runSql(sql, params = {}, options = {}) {
    const query = String(sql || '').trim();
    if (!query) {
      throw new AppError({
        code: 'INVALID_REQUEST',
        message: 'SQL query is required'
      });
    }

    const args = [
      '-X',
      '-q',
      ...(options.at === false ? [] : ['-At']),
      ...buildVariableArgs(params),
      '-c',
      query
    ];

    return runPsql(args, { maxBuffer: options.maxBuffer });
  }

  async function runScript(sql, params = {}, options = {}) {
    const query = String(sql || '').trim();
    if (!query) {
      throw new AppError({
        code: 'INVALID_REQUEST',
        message: 'SQL script is required'
      });
    }

    const args = ['-X', ...(options.at === false ? [] : ['-At']), ...buildVariableArgs(params), '-c', query];
    return runPsql(args, { maxBuffer: options.maxBuffer });
  }

  async function exec(sql, params = {}, options = {}) {
    const result = await runSql(sql, params, { ...options, at: options.at === undefined ? false : options.at });
    return {
      stdout: String(result.stdout || ''),
      stderr: String(result.stderr || '')
    };
  }

  async function queryRows(sql, params = {}, options = {}) {
    const wrapped = `WITH __q AS (${sql}) SELECT COALESCE(json_agg(__q), '[]'::json)::text FROM __q;`;
    const result = await runSql(wrapped, params, { ...options, at: true });
    return parseRowsFromJsonOutput(result.stdout);
  }

  async function queryOne(sql, params = {}, options = {}) {
    const rows = await queryRows(sql, params, options);
    return rows[0] || null;
  }

  async function withTransaction(sqlOrBuilder, params = {}, options = {}) {
    if (typeof sqlOrBuilder === 'string') {
      const script = `BEGIN;\n${sqlOrBuilder}\nCOMMIT;`;
      return exec(script, params, options);
    }

    if (typeof sqlOrBuilder === 'function') {
      const statements = [];
      const statementParams = {};
      let statementIdx = 0;

      const tx = {
        add(sql, localParams = {}) {
          const keyPrefix = `tx_${statementIdx}`;
          statementIdx += 1;

          let rewritten = String(sql || '');
          for (const [key, value] of Object.entries(localParams || {})) {
            const scopedKey = `${keyPrefix}_${key}`;
            rewritten = rewritten.replace(new RegExp(`:'${key}'`, 'g'), `:'${scopedKey}'`);
            rewritten = rewritten.replace(new RegExp(`:${key}(?![A-Za-z0-9_])`, 'g'), `:${scopedKey}`);
            statementParams[scopedKey] = value;
          }

          statements.push(rewritten.trim().replace(/;+\s*$/, '') + ';');
        }
      };

      await sqlOrBuilder(tx);
      if (statements.length === 0) {
        return { stdout: '', stderr: '' };
      }

      const script = ['BEGIN;', ...statements, 'COMMIT;'].join('\n');
      return exec(script, { ...(params || {}), ...statementParams }, options);
    }

    throw new AppError({
      code: 'INVALID_REQUEST',
      message: 'withTransaction expects a SQL string or builder callback'
    });
  }

  async function copyCsvFromFile(csvFilePath, copyTarget) {
    const target = String(copyTarget || '').trim();
    if (!target) {
      throw new AppError({
        code: 'INVALID_REQUEST',
        message: 'copy target is required'
      });
    }

    const mode = await resolveMode();
    const copyCommand = `\\copy ${target} FROM STDIN WITH (FORMAT csv, HEADER true)`;

    if (mode === 'docker-compose') {
      await runExecFile(
        'bash',
        [
          '-lc',
          `cat ${JSON.stringify(csvFilePath)} | docker compose --profile ${JSON.stringify(
            config.dockerProfile
          )} exec -T ${JSON.stringify(config.dockerService)} psql -v ON_ERROR_STOP=1 -U ${JSON.stringify(
            config.user
          )} -d ${JSON.stringify(config.database)} -c ${JSON.stringify(copyCommand)}`
        ],
        {
          cwd: config.rootDir,
          env: process.env,
          maxBuffer: config.maxBuffer
        }
      );
      return;
    }

    if (config.url) {
      await runExecFile(
        'bash',
        [
          '-lc',
          `cat ${JSON.stringify(csvFilePath)} | PGPASSWORD=${JSON.stringify(config.password)} psql ${JSON.stringify(
            config.url
          )} -v ON_ERROR_STOP=1 -c ${JSON.stringify(copyCommand)}`
        ],
        {
          cwd: config.rootDir,
          env: {
            ...process.env,
            PGPASSWORD: config.password
          },
          maxBuffer: config.maxBuffer
        }
      );
      return;
    }

    await runExecFile(
      'bash',
      [
        '-lc',
        `cat ${JSON.stringify(csvFilePath)} | PGPASSWORD=${JSON.stringify(
          config.password
        )} psql -h ${JSON.stringify(config.host)} -p ${JSON.stringify(config.port)} -U ${JSON.stringify(
          config.user
        )} -d ${JSON.stringify(config.database)} -v ON_ERROR_STOP=1 -c ${JSON.stringify(copyCommand)}`
      ],
      {
        cwd: config.rootDir,
        env: {
          ...process.env,
          PGPASSWORD: config.password
        },
        maxBuffer: config.maxBuffer
      }
    );
  }

  return {
    config,
    resolveMode,
    ensureReady,
    runSql,
    runScript,
    exec,
    queryRows,
    queryOne,
    withTransaction,
    copyCsvFromFile
  };
}

module.exports = {
  createPostgisClient,
  parseRowsFromJsonOutput,
  resolveConnectionConfig
};
