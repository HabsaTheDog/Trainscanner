const fs = require('node:fs/promises');

async function acquireLock(lockPath) {
  const payload = JSON.stringify({ pid: process.pid, createdAt: new Date().toISOString() }, null, 2);
  let handle;

  try {
    handle = await fs.open(lockPath, 'wx', 0o644);
    await handle.writeFile(payload, 'utf8');
  } catch (err) {
    if (err.code === 'EEXIST') {
      return null;
    }
    throw err;
  }

  return {
    async release() {
      try {
        await handle.close();
      } finally {
        await fs.unlink(lockPath).catch((err) => {
          if (err.code !== 'ENOENT') {
            throw err;
          }
        });
      }
    }
  };
}

module.exports = { acquireLock };
