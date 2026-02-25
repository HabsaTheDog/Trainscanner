"use strict";

/**
 * mvt.js  –  Task 5.1: Dynamic MapVector Tile (MVT) serving via PostGIS ST_AsMVT.
 *
 * Exposes serveMvtTile(client, { z, x, y }) which returns a Buffer containing
 * a Mapbox Vector Tile (.pbf) with two layers:
 *   - "canonical"  from canonical_stations
 *   - "staging"    from netex_stops_staging  (may be empty if table is vacant)
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
        throw Object.assign(new Error("Tile z must be an integer 0–20"), { code: "INVALID_REQUEST", statusCode: 400 });
    }
    const maxCoord = 2 ** z;
    if (!Number.isInteger(x) || x < 0 || x >= maxCoord) {
        throw Object.assign(new Error(`Tile x out of range for z=${z}`), { code: "INVALID_REQUEST", statusCode: 400 });
    }
    if (!Number.isInteger(y) || y < 0 || y >= maxCoord) {
        throw Object.assign(new Error(`Tile y out of range for z=${y}`), { code: "INVALID_REQUEST", statusCode: 400 });
    }
}

/**
 * Fetch and concatenate MVT data for the requested tile.
 *
 * The SQL uses:
 *   ST_TileEnvelope(z, x, y)  – computes the Web Mercator bbox for the tile
 *   ST_AsMVTGeom(geom, envelope) – clips + scales geometry to tile coordinates
 *   ST_AsMVT(...)  – serialises to protobuf binary
 *
 * We execute two queries and concatenate the raw tile bytes.
 * Mapbox GL / MapLibre will de-multiplex the layers automatically.
 *
 * @param {object} client  PostGIS client (createPostgisClient())
 * @param {{ z: number, x: number, y: number }} coords
 * @returns {Promise<Buffer>}
 */
async function serveMvtTile(client, { z, x, y }) {
    validateTileCoords(z, x, y);

    // Layer 1: canonical stations
    const canonicalResult = await client.queryOne(
        `
    SELECT ST_AsMVT(q.*, 'canonical', ${TILE_EXTENT}, 'geom') AS tile
    FROM (
      SELECT
        cs.canonical_station_id,
        cs.canonical_name,
        cs.country,
        cs.member_count,
        ST_AsMVTGeom(
          cs.geom,
          ST_TileEnvelope(:z, :x, :y),
          ${TILE_EXTENT},
          256,
          true
        ) AS geom
      FROM canonical_stations cs
      WHERE cs.geom IS NOT NULL
        AND cs.geom && ST_TileEnvelope(:z, :x, :y)
    ) q
    WHERE q.geom IS NOT NULL
    `,
        { z, x, y },
    );

    // Layer 2: staging stops (NeTEx raw ingested stops)
    const stagingResult = await client.queryOne(
        `
    SELECT ST_AsMVT(q.*, 'staging', ${TILE_EXTENT}, 'geom') AS tile
    FROM (
      SELECT
        s.source_stop_id,
        s.stop_name,
        s.source_id,
        ST_AsMVTGeom(
          s.geom,
          ST_TileEnvelope(:z, :x, :y),
          ${TILE_EXTENT},
          256,
          true
        ) AS geom
      FROM netex_stops_staging s
      WHERE s.geom IS NOT NULL
        AND s.geom && ST_TileEnvelope(:z, :x, :y)
    ) q
    WHERE q.geom IS NOT NULL
    `,
        { z, x, y },
    );

    const canonicalBuf = canonicalResult && canonicalResult.tile
        ? Buffer.from(canonicalResult.tile, "hex")
        : Buffer.alloc(0);

    const stagingBuf = stagingResult && stagingResult.tile
        ? Buffer.from(stagingResult.tile, "hex")
        : Buffer.alloc(0);

    // Concatenate both layers into one .pbf blob
    return Buffer.concat([canonicalBuf, stagingBuf]);
}

module.exports = { serveMvtTile, validateTileCoords };
