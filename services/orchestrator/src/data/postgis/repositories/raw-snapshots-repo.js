const { validateOrThrow } = require("../../../core/schema");

const RAW_SNAPSHOT_SCHEMA = {
  type: "object",
  required: [
    "sourceId",
    "country",
    "format",
    "snapshotDate",
    "manifestPath",
    "fileName",
  ],
  properties: {
    sourceId: { type: "string", minLength: 1 },
    country: { type: "string", pattern: /^[A-Z]{2}$/ },
    providerSlug: { type: "string", minLength: 1 },
    format: { type: "string", minLength: 1 },
    snapshotDate: { type: "string", pattern: /^\d{4}-\d{2}-\d{2}$/ },
    manifestPath: { type: "string", minLength: 1 },
    manifestSha256: { type: "string" },
    manifest: { type: "object" },
    resolvedDownloadUrl: { type: "string" },
    fileName: { type: "string", minLength: 1 },
    fileSizeBytes: { type: "integer", minimum: 0 },
    retrievalTimestamp: { type: "string" },
    detectedVersionOrDate: { type: "string" },
    requestedAsOf: { type: "string" },
  },
  additionalProperties: true,
};

function normalizeRow(row) {
  if (!row) {
    return null;
  }

  let fileSizeBytes = null;
  if (Number.isFinite(row.file_size_bytes)) {
    fileSizeBytes = row.file_size_bytes;
  } else if (Number.isFinite(row.fileSizeBytes)) {
    fileSizeBytes = row.fileSizeBytes;
  }

  const out = {
    sourceId: row.source_id || row.sourceid || row.sourceId,
    country: row.country,
    providerSlug: row.provider_slug || row.providerslug || row.providerSlug,
    format: row.format,
    snapshotDate: row.snapshot_date || row.snapshotdate || row.snapshotDate,
    manifestPath: row.manifest_path || row.manifestpath || row.manifestPath,
    manifestSha256:
      row.manifest_sha256 || row.manifestsha256 || row.manifestSha256 || null,
    manifest:
      row.manifest && typeof row.manifest === "object" ? row.manifest : {},
    resolvedDownloadUrl:
      row.resolved_download_url ||
      row.resolveddownloadurl ||
      row.resolvedDownloadUrl ||
      null,
    fileName: row.file_name || row.filename || row.fileName,
    fileSizeBytes,
    retrievalTimestamp:
      row.retrieval_timestamp ||
      row.retrievaltimestamp ||
      row.retrievalTimestamp ||
      null,
    detectedVersionOrDate:
      row.detected_version_or_date ||
      row.detectedversionordate ||
      row.detectedVersionOrDate ||
      null,
    requestedAsOf:
      row.requested_as_of || row.requestedasof || row.requestedAsOf || null,
  };

  if (out.fileSizeBytes === null || Number.isNaN(out.fileSizeBytes)) {
    delete out.fileSizeBytes;
  }

  validateOrThrow(out, RAW_SNAPSHOT_SCHEMA, {
    code: "INVALID_CONFIG",
    message: "Invalid raw snapshot row returned from repository",
  });

  return {
    ...out,
    fileSizeBytes: out.fileSizeBytes === undefined ? null : out.fileSizeBytes,
  };
}

function createRawSnapshotsRepo(client) {
  return {
    async upsertSnapshot(input) {
      const row = await client.queryOne(
        `
          INSERT INTO raw_snapshots (
            source_id,
            country,
            provider_slug,
            format,
            snapshot_date,
            manifest_path,
            manifest_sha256,
            manifest,
            resolved_download_url,
            file_name,
            file_size_bytes,
            retrieval_timestamp,
            detected_version_or_date,
            requested_as_of,
            updated_at
          )
          VALUES (
            :'source_id',
            :'country'::char(2),
            :'provider_slug',
            :'format',
            :'snapshot_date'::date,
            :'manifest_path',
            NULLIF(:'manifest_sha256', ''),
            NULLIF(:'manifest_json', '')::jsonb,
            NULLIF(:'resolved_download_url', ''),
            :'file_name',
            NULLIF(:'file_size_bytes', '')::bigint,
            NULLIF(:'retrieval_timestamp', '')::timestamptz,
            NULLIF(:'detected_version_or_date', ''),
            NULLIF(:'requested_as_of', '')::date,
            now()
          )
          ON CONFLICT (source_id, snapshot_date)
          DO UPDATE SET
            country = EXCLUDED.country,
            provider_slug = EXCLUDED.provider_slug,
            format = EXCLUDED.format,
            manifest_path = EXCLUDED.manifest_path,
            manifest_sha256 = EXCLUDED.manifest_sha256,
            manifest = EXCLUDED.manifest,
            resolved_download_url = EXCLUDED.resolved_download_url,
            file_name = EXCLUDED.file_name,
            file_size_bytes = EXCLUDED.file_size_bytes,
            retrieval_timestamp = EXCLUDED.retrieval_timestamp,
            detected_version_or_date = EXCLUDED.detected_version_or_date,
            requested_as_of = EXCLUDED.requested_as_of,
            updated_at = now()
          RETURNING
            source_id,
            country::text,
            provider_slug,
            format,
            to_char(snapshot_date, 'YYYY-MM-DD') AS snapshot_date,
            manifest_path,
            manifest_sha256,
            manifest,
            resolved_download_url,
            file_name,
            file_size_bytes,
            to_char(retrieval_timestamp, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS retrieval_timestamp,
            detected_version_or_date,
            to_char(requested_as_of, 'YYYY-MM-DD') AS requested_as_of;
        `,
        {
          source_id: input.sourceId,
          country: input.country,
          provider_slug: input.providerSlug,
          format: input.format || "netex",
          snapshot_date: input.snapshotDate,
          manifest_path: input.manifestPath,
          manifest_sha256: input.manifestSha256 || "",
          manifest_json: JSON.stringify(input.manifest || {}),
          resolved_download_url: input.resolvedDownloadUrl || "",
          file_name: input.fileName,
          file_size_bytes:
            input.fileSizeBytes === undefined || input.fileSizeBytes === null
              ? ""
              : String(input.fileSizeBytes),
          retrieval_timestamp: input.retrievalTimestamp || "",
          detected_version_or_date: input.detectedVersionOrDate || "",
          requested_as_of: input.requestedAsOf || "",
        },
      );

      return normalizeRow(row);
    },

    async listLatestSnapshots(scope = {}) {
      const rows = await client.queryRows(
        `
          WITH selected AS (
            SELECT
              source_id,
              country,
              MAX(snapshot_date) AS snapshot_date
            FROM raw_snapshots
            WHERE format = NULLIF(:'format', '')
              AND (NULLIF(:'country', '') IS NULL OR country = NULLIF(:'country', '')::char(2))
              AND (NULLIF(:'source_id', '') IS NULL OR source_id = NULLIF(:'source_id', ''))
              AND (NULLIF(:'as_of', '') IS NULL OR snapshot_date <= NULLIF(:'as_of', '')::date)
            GROUP BY source_id, country
          )
          SELECT
            rs.source_id,
            rs.country::text,
            rs.provider_slug,
            rs.format,
            to_char(rs.snapshot_date, 'YYYY-MM-DD') AS snapshot_date,
            rs.manifest_path,
            rs.manifest_sha256,
            rs.manifest,
            rs.resolved_download_url,
            rs.file_name,
            rs.file_size_bytes,
            to_char(rs.retrieval_timestamp, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS retrieval_timestamp,
            rs.detected_version_or_date,
            to_char(rs.requested_as_of, 'YYYY-MM-DD') AS requested_as_of
          FROM raw_snapshots rs
          JOIN selected s
            ON s.source_id = rs.source_id
           AND s.snapshot_date = rs.snapshot_date
          ORDER BY rs.country, rs.source_id;
        `,
        {
          format: scope.format || "netex",
          country: scope.country || "",
          source_id: scope.sourceId || "",
          as_of: scope.asOf || "",
        },
      );

      return rows.map((row) => normalizeRow(row));
    },
  };
}

module.exports = {
  createRawSnapshotsRepo,
  normalizeRow,
};
