/**
 * mvt.js - Dynamic MapVector Tile (MVT) serving via PostGIS ST_AsMVT.
 *
 * Exposes serveMvtTile(client, { z, x, y }) which returns a Buffer containing
 * a Mapbox Vector Tile (.pbf) with two layers:
 *   - "global_stations" from global_stations
 *   - "raw_stop_places" from raw_provider_stop_places
 */

const TILE_EXTENT = 4096;

/**
 * Validate that z/x/y are within acceptable ranges.
 * @param {number} z
 * @param {number} x
 * @param {number} y
 */
function validateTileCoords(z, x, y) {
  if (!Number.isInteger(z) || z < 0 || z > 20) {
    throw Object.assign(new Error("Tile z must be an integer 0–20"), {
      code: "INVALID_REQUEST",
      statusCode: 400,
    });
  }
  const maxCoord = 2 ** z;
  if (!Number.isInteger(x) || x < 0 || x >= maxCoord) {
    throw Object.assign(new Error(`Tile x out of range for z=${z}`), {
      code: "INVALID_REQUEST",
      statusCode: 400,
    });
  }
  if (!Number.isInteger(y) || y < 0 || y >= maxCoord) {
    throw Object.assign(new Error(`Tile y out of range for z=${y}`), {
      code: "INVALID_REQUEST",
      statusCode: 400,
    });
  }
}

async function serveMvtTile(client, { z, x, y }) {
  validateTileCoords(z, x, y);

  const globalStationsResult = await client.queryOne(
    `
    SELECT ST_AsMVT(q.*, 'global_stations', ${TILE_EXTENT}, 'geom') AS tile
    FROM (
      SELECT
        gs.global_station_id,
        gs.display_name,
        gs.country,
        gs.station_kind,
        ST_AsMVTGeom(
          gs.geom,
          ST_TileEnvelope(:z, :x, :y),
          ${TILE_EXTENT},
          256,
          true
        ) AS geom
      FROM global_stations gs
      WHERE gs.is_active = true
        AND gs.geom IS NOT NULL
        AND gs.geom && ST_TileEnvelope(:z, :x, :y)
    ) q
    WHERE q.geom IS NOT NULL
    `,
    { z, x, y },
  );

  const rawStopPlacesResult = await client.queryOne(
    `
    SELECT ST_AsMVT(q.*, 'raw_stop_places', ${TILE_EXTENT}, 'geom') AS tile
    FROM (
      SELECT
        rp.stop_place_id,
        rp.provider_stop_place_ref,
        rp.stop_name,
        rp.source_id,
        rp.country,
        rp.dataset_id,
        ST_AsMVTGeom(
          rp.geom,
          ST_TileEnvelope(:z, :x, :y),
          ${TILE_EXTENT},
          256,
          true
        ) AS geom
      FROM raw_provider_stop_places rp
      WHERE rp.geom IS NOT NULL
        AND rp.geom && ST_TileEnvelope(:z, :x, :y)
    ) q
    WHERE q.geom IS NOT NULL
    `,
    { z, x, y },
  );

  const globalStationsBuf = globalStationsResult?.tile
    ? Buffer.from(globalStationsResult.tile, "hex")
    : Buffer.alloc(0);

  const rawStopPlacesBuf = rawStopPlacesResult?.tile
    ? Buffer.from(rawStopPlacesResult.tile, "hex")
    : Buffer.alloc(0);

  return Buffer.concat([globalStationsBuf, rawStopPlacesBuf]);
}

module.exports = { serveMvtTile, validateTileCoords };
