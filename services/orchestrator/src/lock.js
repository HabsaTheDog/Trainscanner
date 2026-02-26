const fs = require("node:fs/promises");

function isProcessAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 0) {
    return false;
  }
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    if (err && err.code === "EPERM") {
      return true;
    }
    return false;
  }
}

async function readLockPayload(lockPath) {
  try {
    const raw = await fs.readFile(lockPath, "utf8");
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function isStaleLock(payload, staleMs) {
  const pidAlive = isProcessAlive(Number.parseInt(payload?.pid, 10));
  if (!pidAlive) {
    return true;
  }

  if (!Number.isFinite(staleMs) || staleMs <= 0) {
    return false;
  }

  const createdAt = Date.parse(payload?.createdAt);
  if (!Number.isFinite(createdAt)) {
    return false;
  }

  return Date.now() - createdAt > staleMs;
}

async function acquireLock(lockPath, options = {}) {
  const staleMs = Number.parseInt(options.staleMs, 10);
  const logger = options.logger;
  const payload = JSON.stringify(
    { pid: process.pid, createdAt: new Date().toISOString() },
    null,
    2,
  );
  let handle;

  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      handle = await fs.open(lockPath, "wx", 0o644);
      await handle.writeFile(payload, "utf8");
      break;
    } catch (err) {
      if (err.code !== "EEXIST") {
        throw err;
      }

      const existing = await readLockPayload(lockPath);
      if (!existing || !isStaleLock(existing, staleMs)) {
        return null;
      }

      await fs.unlink(lockPath).catch((unlinkErr) => {
        if (unlinkErr.code !== "ENOENT") {
          throw unlinkErr;
        }
      });

      if (logger && typeof logger.info === "function") {
        logger.info("Removed stale switch lock", {
          step: "lock_stale_cleanup",
          stalePid: existing.pid || null,
          staleCreatedAt: existing.createdAt || null,
        });
      }
    }
  }

  if (!handle) {
    return null;
  }

  return {
    async release() {
      try {
        await handle.close();
      } finally {
        await fs.unlink(lockPath).catch((err) => {
          if (err.code !== "ENOENT") {
            throw err;
          }
        });
      }
    },
  };
}

module.exports = { acquireLock };
