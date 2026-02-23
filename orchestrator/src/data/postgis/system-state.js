const { createPostgisClient } = require("./client");

let dbClient = null;

function getDbClient() {
  if (!dbClient) {
    dbClient = createPostgisClient();
  }
  return dbClient;
}

/**
 * Retrieves a state value by key from the system_state table.
 * @param {string} key
 * @param {any} fallback
 * @returns {Promise<any>}
 */
async function getSystemState(key, fallback = null) {
  try {
    const client = getDbClient();
    const row = await client.queryOne(
      `SELECT value FROM system_state WHERE key = :'key'`,
      { key },
    );
    if (row?.value) {
      return row.value;
    }
    return fallback;
  } catch (_err) {
    // If table doesn't exist yet or connection fails, return fallback.
    // This makes it graceful.
    return fallback;
  }
}

/**
 * Sets a state value by key in the system_state table.
 * @param {string} key
 * @param {any} value
 */
async function setSystemState(key, value) {
  const client = getDbClient();
  const valueJson = JSON.stringify(value);

  await client.runSql(
    `INSERT INTO system_state (key, value) 
     VALUES (:'key', :'value'::jsonb) 
     ON CONFLICT (key) DO UPDATE 
     SET value = :'value'::jsonb, updated_at = CURRENT_TIMESTAMP`,
    { key, value: valueJson },
  );
}

module.exports = {
  getSystemState,
  setSystemState,
};
