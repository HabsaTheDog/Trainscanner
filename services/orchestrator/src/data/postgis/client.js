const { Pool } = require("pg");
const fs = require("node:fs");
const { pipeline } = require("node:stream/promises");
const { from: copyFrom } = require("pg-copy-streams");
const path = require("node:path");

const { AppError } = require("../../core/errors");

function parseBool(value, fallback) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }
  return String(value).toLowerCase() === "true";
}

function toInt(value, fallback) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }
  const parsed = Number.parseInt(String(value), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function resolveConnectionConfig(options = {}) {
  const env = { ...process.env, ...options.env };

  const rootDir = path.resolve(options.rootDir || process.cwd());
  const mode = String(options.mode || env.CANONICAL_DB_MODE || "auto").trim();

  const config = {
    rootDir,
    mode,
    url: env.CANONICAL_DB_URL || env.DATABASE_URL || "",
    host: env.CANONICAL_DB_HOST || env.PGHOST || "localhost",
    port: env.CANONICAL_DB_PORT || env.PGPORT || "5432",
    user: env.CANONICAL_DB_USER || env.PGUSER || "trainscanner",
    database: env.CANONICAL_DB_NAME || env.PGDATABASE || "trainscanner",
    password: env.CANONICAL_DB_PASSWORD || env.PGPASSWORD || "trainscanner",
    dockerProfile: env.CANONICAL_DB_DOCKER_PROFILE || "dach-data",
    dockerService: env.CANONICAL_DB_DOCKER_SERVICE || "postgis",
    readyTimeoutSec: toInt(
      options.readyTimeoutSec || env.CANONICAL_DB_READY_TIMEOUT_SEC,
      90,
    ),
    connectTimeoutSec: toInt(
      options.connectTimeoutSec || env.CANONICAL_DB_CONNECT_TIMEOUT_SEC,
      3,
    ),
    maxBuffer: toInt(options.maxBuffer, 64 * 1024 * 1024),
    useDockerTty: parseBool(options.useDockerTty, false),
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
      env.PGDATABASE,
  );

  return config;
}

function escapeRegex(value) {
  return String(value).replaceAll(/[.*+?^${}()|[\]\\]/g, String.raw`\$&`);
}

function toSqlLiteral(value) {
  if (value === null || value === undefined) {
    return "NULL";
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new AppError({
        code: "INVALID_REQUEST",
        message: "SQL numeric parameter must be finite",
      });
    }
    return String(value);
  }
  if (typeof value === "boolean") {
    return value ? "TRUE" : "FALSE";
  }
  if (value instanceof Date) {
    return `'${value.toISOString().replaceAll("'", "''")}'`;
  }
  if (typeof value === "object") {
    return `'${JSON.stringify(value).replaceAll("'", "''")}'`;
  }
  return `'${String(value).replaceAll("'", "''")}'`;
}

function interpolateSqlLiterals(sql, params = {}) {
  let query = String(sql || "");
  const entries = Object.entries(params);
  entries.sort((a, b) => b[0].length - a[0].length);

  for (const [key, value] of entries) {
    const token = new RegExp(`:'${escapeRegex(key)}'`, "g");
    if (token.test(query)) {
      query = query.replace(token, toSqlLiteral(value));
    }
  }

  const unresolved = query.match(/:'[A-Za-z_]\w*'/g);
  if (unresolved && unresolved.length > 0) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: `Missing SQL params for placeholders: ${Array.from(new Set(unresolved)).join(", ")}`,
    });
  }

  return query;
}

function scriptResultToStdout(result) {
  const candidate = Array.isArray(result)
    ? [...result]
        .reverse()
        .find((item) => Array.isArray(item?.rows) && item.rows.length > 0) ||
      result.at(-1)
    : result;

  if (
    !candidate ||
    !Array.isArray(candidate.rows) ||
    candidate.rows.length === 0
  ) {
    return "";
  }

  return candidate.rows
    .map((row) => {
      const values = Object.values(row || {});
      if (values.length === 1) {
        return values[0] === undefined || values[0] === null
          ? ""
          : String(values[0]);
      }
      return JSON.stringify(row);
    })
    .join("\n");
}

/**
 * Converts named parameters (:'paramName') into Postgres positional parameters ($1, $2)
 * and returns the parameterized SQL string and the ordered array of values.
 */
function interpolateSqlParams(sql, params = {}) {
  let query = String(sql || "");
  const values = [];
  let paramIndex = 1;

  const entries = Object.entries(params);

  // Sort entries by length descending to prevent partial matches (e.g. replacing :id inside :idx)
  entries.sort((a, b) => b[0].length - a[0].length);

  for (const [key, value] of entries) {
    const token = new RegExp(`:'${escapeRegex(key)}'`, "g");
    if (token.test(query)) {
      query = query.replace(token, `$${paramIndex}`);
      values.push(value);
      paramIndex++;
    }
  }

  const unresolved = query.match(/:'[A-Za-z_]\w*'/g);
  if (unresolved && unresolved.length > 0) {
    throw new AppError({
      code: "INVALID_REQUEST",
      message: `Missing SQL params for placeholders: ${Array.from(new Set(unresolved)).join(", ")}`,
    });
  }

  return { query, values };
}

async function resolveMode() {
  // Legacy support: mode is always resolved to 'direct' now that we use pg.Pool
  return "direct";
}

function createPostgisClient(options = {}) {
  const config = resolveConnectionConfig(options);

  const poolConfig = config.url
    ? {
        connectionString: config.url,
        connectionTimeoutMillis: config.connectTimeoutSec * 1000,
      }
    : {
        host: config.host,
        port: Number.parseInt(config.port, 10),
        user: config.user,
        database: config.database,
        password: config.password,
        connectionTimeoutMillis: config.connectTimeoutSec * 1000,
      };

  const pool = new Pool(poolConfig);

  pool.on("error", (err) => {
    console.error("Unexpected error on idle database client", err);
  });

  async function ensureReady() {
    const started = Date.now();
    let lastError = null;

    while (Date.now() - started < config.readyTimeoutSec * 1000) {
      try {
        const client = await pool.connect();
        await client.query("SELECT 1;");
        client.release();
        return;
      } catch (err) {
        lastError = err;
        await new Promise((resolve) => setTimeout(resolve, 2000));
      }
    }

    throw new AppError({
      code: "INTERNAL_ERROR",
      message: `Database did not become ready within ${config.readyTimeoutSec}s`,
      cause: lastError || undefined,
    });
  }

  async function runSql(sql, params = {}) {
    const q = String(sql || "").trim();
    if (!q) {
      throw new AppError({
        code: "INVALID_REQUEST",
        message: "SQL query is required",
      });
    }
    const { query, values } = interpolateSqlParams(q, params);

    try {
      const result = await pool.query(query, values);
      return result;
    } catch (err) {
      throw new AppError({
        code: "INTERNAL_ERROR",
        message: err.message,
        details: { query, values },
        cause: err,
      });
    }
  }

  async function runScript(sql, params = {}) {
    const q = String(sql || "").trim();
    if (!q) {
      throw new AppError({
        code: "INVALID_REQUEST",
        message: "SQL script is required",
      });
    }
    const query = interpolateSqlLiterals(q, params);

    try {
      const result = await pool.query(query);
      return {
        stdout: scriptResultToStdout(result),
        stderr: "",
      };
    } catch (err) {
      throw new AppError({
        code: "INTERNAL_ERROR",
        message: err.message,
        details: { query },
        cause: err,
      });
    }
  }

  async function exec(sql, params = {}) {
    try {
      await runSql(sql, params);
      return { stdout: "OK", stderr: "" };
    } catch (err) {
      return { stdout: "", stderr: err.message };
    }
  }

  async function queryRows(sql, params = {}) {
    const result = await runSql(sql, params);
    return result.rows || [];
  }

  async function queryOne(sql, params = {}) {
    const rows = await queryRows(sql, params);
    return rows[0] || null;
  }

  async function withTransaction(sqlOrBuilder, params = {}) {
    if (typeof sqlOrBuilder === "string") {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        const { query, values } = interpolateSqlParams(sqlOrBuilder, params);
        await client.query(query, values);
        await client.query("COMMIT");
        return { stdout: "OK", stderr: "" };
      } catch (err) {
        await client.query("ROLLBACK");
        throw new AppError({
          code: "INTERNAL_ERROR",
          message: err.message,
          cause: err,
        });
      } finally {
        client.release();
      }
    }

    if (typeof sqlOrBuilder === "function") {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");

        const statements = [];

        const tx = {
          add(sql, localParams = {}) {
            statements.push({ sql, localParams });
          },
        };

        await sqlOrBuilder(tx);

        for (const stmt of statements) {
          const mergedParams = {
            ...params,
            ...stmt.localParams,
          };
          const { query, values } = interpolateSqlParams(
            stmt.sql,
            mergedParams,
          );
          await client.query(query, values);
        }

        await client.query("COMMIT");
        return { stdout: "OK", stderr: "" };
      } catch (err) {
        await client.query("ROLLBACK");
        throw new AppError({
          code: "INTERNAL_ERROR",
          message: err.message,
          cause: err,
        });
      } finally {
        client.release();
      }
    }

    throw new AppError({
      code: "INVALID_REQUEST",
      message: "withTransaction expects a SQL string or builder callback",
    });
  }

  async function copyCsvFromFile(csvFilePath, copyTarget) {
    const target = String(copyTarget || "").trim();
    if (!target) {
      throw new AppError({
        code: "INVALID_REQUEST",
        message: "copy target is required",
      });
    }

    const client = await pool.connect();
    try {
      const stream = client.query(
        copyFrom(`COPY ${target} FROM STDIN WITH (FORMAT csv, HEADER true)`),
      );
      const fileStream = fs.createReadStream(csvFilePath);
      await pipeline(fileStream, stream);
    } catch (err) {
      throw new AppError({
        code: "INTERNAL_ERROR",
        message: err.message,
        cause: err,
      });
    } finally {
      client.release();
    }
  }

  async function end() {
    await pool.end();
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
    copyCsvFromFile,
    end,
  };
}

module.exports = {
  createPostgisClient,
  interpolateSqlParams,
  resolveConnectionConfig,
};
