-- Migration 013: AI confidence columns for QA Low-Confidence Queue
-- Adds ai_confidence + ai_suggested_action to evidence and decisions tables.
-- Also ensures a partial index exists for fast < 0.90 queue queries.

-- 1. Add AI confidence score column to cluster evidence
ALTER TABLE qa_station_cluster_evidence_v2
  ADD COLUMN IF NOT EXISTS ai_confidence numeric(5,4)
    CHECK (ai_confidence IS NULL OR (ai_confidence >= 0 AND ai_confidence <= 1));

-- 2. Add AI suggested action to evidence (e.g. 'approve', 'reject', 'review')
ALTER TABLE qa_station_cluster_evidence_v2
  ADD COLUMN IF NOT EXISTS ai_suggested_action text;

-- 3. Record what confidence score was at decision time (for audit trail)
ALTER TABLE qa_station_cluster_decisions_v2
  ADD COLUMN IF NOT EXISTS ai_confidence numeric(5,4)
    CHECK (ai_confidence IS NULL OR (ai_confidence >= 0 AND ai_confidence <= 1));

-- 4. Fast partial index for the Low-Confidence Queue (confidence < 0.90)
CREATE INDEX IF NOT EXISTS idx_qa_cluster_evidence_low_confidence
  ON qa_station_cluster_evidence_v2 (cluster_id, ai_confidence ASC)
  WHERE ai_confidence IS NOT NULL AND ai_confidence < 0.90;

-- 5. Guard: ensure canonical_stations.geom has required spatial index
--    (migration 001 creates it, but this is idempotent)
CREATE INDEX IF NOT EXISTS idx_canonical_stations_geom
  ON canonical_stations USING gist (geom)
  WHERE geom IS NOT NULL;

-- 6. Spatial index on netex_stops_staging for MVT tile slicing
CREATE INDEX IF NOT EXISTS idx_netex_stops_staging_geom
  ON netex_stops_staging USING gist (geom)
  WHERE geom IS NOT NULL;
