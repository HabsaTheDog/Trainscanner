const { createPostgisClient } = require('../../data/postgis/client');
const { AppError } = require('../../core/errors');

let dbClient = null;

async function getDbClient() {
  if (!dbClient) {
    dbClient = createPostgisClient();
    await dbClient.ensureReady();
  }
  return dbClient;
}

async function getReviewQueue(url) {
  const client = await getDbClient();
  const country = (url.searchParams.get('country') || '').trim().toUpperCase();

  // Validate country is strictly a 2-letter code to prevent injection
  const validCountry = /^[A-Z]{2}$/.test(country) ? country : '';

  const countryFilter = validCountry
    ? `AND q.country = '${validCountry}'`
    : '';

  const sql = `
    SELECT
      q.review_item_id,
      q.issue_key,
      q.country,
      q.canonical_station_id,
      q.issue_type,
      q.severity,
      q.status,
      q.details,
      q.created_at,
      CASE 
        WHEN q.canonical_station_id IS NOT NULL THEN (
          SELECT json_agg(json_build_object(
            'source_id', s.source_id,
            'source_stop_id', s.source_stop_id,
            'stop_name', s.stop_name,
            'latitude', s.latitude,
            'longitude', s.longitude
          ))
          FROM canonical_station_sources css
          JOIN netex_stops_staging s ON s.source_id = css.source_id AND s.source_stop_id = css.source_stop_id AND s.snapshot_date = css.snapshot_date
          WHERE css.canonical_station_id = q.canonical_station_id
        )
        ELSE NULL
      END as members,
      CASE
        WHEN q.details ? 'canonicalStationIds' THEN (
          SELECT json_agg(json_build_object(
            'canonical_station_id', cs.canonical_station_id,
            'canonical_name', cs.canonical_name,
            'latitude', cs.latitude,
            'longitude', cs.longitude
          ))
          FROM canonical_stations cs
          WHERE cs.canonical_station_id IN (
            SELECT value FROM jsonb_array_elements_text(q.details->'canonicalStationIds') AS value
          )
        )
        ELSE NULL
      END as related_stations
    FROM canonical_review_queue q
    WHERE q.status IN ('open', 'confirmed')
      ${countryFilter}
    ORDER BY
      CASE q.severity WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
      q.last_detected_at DESC,
      q.review_item_id DESC
    LIMIT 50
  `;

  const rows = await client.queryRows(sql);
  return rows;
}

async function postOverride(body) {
  const client = await getDbClient();
  const { review_item_id, operation, new_canonical_name, operation_payload } = body;

  if (!review_item_id || !operation) {
    throw new AppError({
      code: 'INVALID_REQUEST',
      statusCode: 400,
      message: 'review_item_id and operation are required'
    });
  }

  const queueItem = await client.queryOne(
    `SELECT * FROM canonical_review_queue WHERE review_item_id = :'review_item_id'::bigint`,
    { review_item_id }
  );

  if (!queueItem) {
    throw new AppError({
      code: 'NOT_FOUND',
      statusCode: 404,
      message: 'Review item not found'
    });
  }

  const country = queueItem.country;

  await client.withTransaction(async (tx) => {
    if (operation === 'keep_separate') {
      // Dismiss the issue
      tx.add(`
        UPDATE canonical_review_queue 
        SET status = 'dismissed', resolved_at = now(), resolved_by = :'user', resolution_note = 'Dismissed via curation tool'
        WHERE review_item_id = :'review_item_id'::bigint
      `, {
        review_item_id,
        user: 'curation_tool'
      });
    } else if (operation === 'merge') {
      const source_id = operation_payload?.source_canonical_station_id;
      const target_id = operation_payload?.target_canonical_station_id || queueItem.canonical_station_id;

      if (source_id && target_id && source_id !== target_id) {
        tx.add(`
          INSERT INTO canonical_station_overrides (
            operation, status, country, source_canonical_station_id, target_canonical_station_id, created_via, requested_by
          ) VALUES (
            'merge', 'approved', :'country', :'source_id', :'target_id', 'script', :'user'
          )
        `, {
          country,
          source_id,
          target_id,
          user: 'curation_tool'
        });
      }

      tx.add(`
        UPDATE canonical_review_queue 
        SET status = 'resolved', resolved_at = now(), resolved_by = :'user', resolution_note = 'Merged via curation tool'
        WHERE review_item_id = :'review_item_id'::bigint
      `, {
        review_item_id,
        user: 'curation_tool'
      });
    } else if (operation === 'rename') {
      const target_id = queueItem.canonical_station_id || operation_payload?.target_canonical_station_id;

      tx.add(`
        INSERT INTO canonical_station_overrides (
          operation, status, country, target_canonical_station_id, new_canonical_name, created_via, requested_by
        ) VALUES (
          'rename', 'approved', :'country', :'target_id', :'new_name', 'script', :'user'
        )
      `, {
        country,
        target_id,
        new_name: new_canonical_name,
        user: 'curation_tool'
      });

      tx.add(`
        UPDATE canonical_review_queue 
        SET status = 'resolved', resolved_at = now(), resolved_by = :'user', resolution_note = 'Renamed via curation tool'
        WHERE review_item_id = :'review_item_id'::bigint
      `, {
        review_item_id,
        user: 'curation_tool'
      });
    } else {
      throw new AppError({
        code: 'INVALID_REQUEST',
        statusCode: 400,
        message: 'Invalid operation'
      });
    }
  });

  return { ok: true };
}

module.exports = {
  getReviewQueue,
  postOverride
};
