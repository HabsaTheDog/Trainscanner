function createNetexStopsRepo(client) {
  return {
    async deleteBySourceSnapshot(scope) {
      await client.exec(
        `
          DELETE FROM netex_stops_staging
          WHERE source_id = :'source_id'
            AND snapshot_date = :'snapshot_date'::date;
        `,
        {
          source_id: scope.sourceId,
          snapshot_date: scope.snapshotDate,
        },
      );
    },

    async countByImportRun(runId) {
      const row = await client.queryOne(
        `
          SELECT COUNT(*)::integer AS row_count
          FROM netex_stops_staging
          WHERE import_run_id = :'run_id'::uuid;
        `,
        { run_id: runId },
      );

      return row
        ? Number.parseInt(String(row.row_count || row.rowCount || 0), 10)
        : 0;
    },

    async copyCsv(csvPath) {
      await client.copyCsvFromFile(
        csvPath,
        `netex_stops_staging (
          import_run_id,
          source_id,
          country,
          provider_slug,
          snapshot_date,
          manifest_sha256,
          source_stop_id,
          source_parent_stop_id,
          stop_name,
          latitude,
          longitude,
          public_code,
          private_code,
          hard_id,
          source_file,
          raw_payload
        )`,
      );
    },
  };
}

module.exports = {
  createNetexStopsRepo,
};
