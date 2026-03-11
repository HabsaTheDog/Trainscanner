const { createPostgisClient } = require("./src/data/postgis/client");

async function main() {
  const client = createPostgisClient();
  await client.ensureReady();

  console.log("--- Import Runs ---");
  const runs = await client.queryRows(
    "SELECT pipeline, country, status, error_message, started_at FROM import_runs ORDER BY started_at DESC LIMIT 20;",
  );
  console.table(runs);

  console.log("--- Raw Snapshots ---");
  const snaps = await client.queryRows(
    "SELECT source_id, country, snapshot_date FROM raw_snapshots ORDER BY snapshot_date DESC LIMIT 10;",
  );
  console.table(snaps);

  await client.end();
}

void (async () => {
  try {
    await main();
  } catch (error) {
    console.error(error);
    process.exitCode = 1;
  }
})();
