const ENSURE_QA_MERGE_ELIGIBLE_PAIRS_SQL = `
CREATE TABLE IF NOT EXISTS qa_merge_eligible_pairs (
  scope_key text NOT NULL,
  scope_tag text NOT NULL DEFAULT 'latest',
  scope_country iso_country_code,
  scope_as_of date,
  merge_cluster_id text NOT NULL REFERENCES qa_merge_clusters(merge_cluster_id) ON DELETE CASCADE,
  source_global_station_id text NOT NULL REFERENCES global_stations(global_station_id) ON DELETE CASCADE,
  target_global_station_id text NOT NULL REFERENCES global_stations(global_station_id) ON DELETE CASCADE,
  seed_reasons jsonb NOT NULL DEFAULT '[]'::jsonb,
  exact_name boolean NOT NULL DEFAULT false,
  loose_similarity numeric(8,4),
  planar_distance_meters numeric(12,2),
  distance_meters numeric(12,2),
  provider_overlap_count integer NOT NULL DEFAULT 0 CHECK (provider_overlap_count >= 0),
  route_overlap_count integer NOT NULL DEFAULT 0 CHECK (route_overlap_count >= 0),
  adjacent_overlap_count integer NOT NULL DEFAULT 0 CHECK (adjacent_overlap_count >= 0),
  generic_name_pair boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (scope_key, source_global_station_id, target_global_station_id),
  CHECK (source_global_station_id < target_global_station_id)
);

CREATE INDEX IF NOT EXISTS idx_qa_merge_eligible_pairs_scope
  ON qa_merge_eligible_pairs (scope_tag, scope_country, merge_cluster_id);
`;

async function ensureQaMergeEligiblePairsTable(client) {
  if (client && typeof client.exec === "function") {
    await client.exec(ENSURE_QA_MERGE_ELIGIBLE_PAIRS_SQL);
    return;
  }
  if (client && typeof client.runSql === "function") {
    await client.runSql(ENSURE_QA_MERGE_ELIGIBLE_PAIRS_SQL);
    return;
  }
  throw new Error("Client does not support exec or runSql for schema setup");
}

module.exports = {
  ENSURE_QA_MERGE_ELIGIBLE_PAIRS_SQL,
  ensureQaMergeEligiblePairsTable,
};
