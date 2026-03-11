function createNetexStopsRepo(client) {
  return {
    async deleteBySourceSnapshot(scope) {
      await client.exec(
        `
          DELETE FROM raw_provider_stop_points rsp
          USING provider_datasets d
          WHERE d.source_id = :'source_id'
            AND d.snapshot_date = :'snapshot_date'::date
            AND rsp.dataset_id = d.dataset_id;
        `,
        {
          source_id: scope.sourceId,
          snapshot_date: scope.snapshotDate,
        },
      );

      await client.exec(
        `
          DELETE FROM raw_provider_stop_places rsp
          USING provider_datasets d
          WHERE d.source_id = :'source_id'
            AND d.snapshot_date = :'snapshot_date'::date
            AND rsp.dataset_id = d.dataset_id;
        `,
        {
          source_id: scope.sourceId,
          snapshot_date: scope.snapshotDate,
        },
      );
    },

    async countStopPlacesByDataset(datasetId) {
      const row = await client.queryOne(
        `
          SELECT COUNT(*)::integer AS row_count
          FROM raw_provider_stop_places
          WHERE dataset_id = :'dataset_id'::bigint;
        `,
        { dataset_id: String(datasetId) },
      );

      return row
        ? Number.parseInt(String(row.row_count || row.rowCount || 0), 10)
        : 0;
    },

    async copyStopPlacesCsv(csvPath) {
      await client.copyCsvFromFile(
        csvPath,
        `raw_provider_stop_places (
          stop_place_id,
          dataset_id,
          source_id,
          provider_stop_place_ref,
          country,
          stop_name,
          latitude,
          longitude,
          parent_stop_place_ref,
          topographic_place_ref,
          public_code,
          private_code,
          hard_id,
          raw_payload
        )`,
      );
    },

    async copyStopPointsCsv(csvPath) {
      await client.copyCsvFromFile(
        csvPath,
        `raw_provider_stop_points (
          stop_point_id,
          dataset_id,
          source_id,
          provider_stop_point_ref,
          provider_stop_place_ref,
          stop_place_id,
          country,
          stop_name,
          latitude,
          longitude,
          topographic_place_ref,
          platform_code,
          track_code,
          raw_payload
        )`,
      );
    },
  };
}

module.exports = {
  createNetexStopsRepo,
};
