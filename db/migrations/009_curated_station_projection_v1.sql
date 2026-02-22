-- Additive curated-station projection layer.
--
-- Purpose:
-- - Keep raw/canonical ingestion data immutable and reviewable.
-- - Materialize reviewer-confirmed station entities separately from source candidates.
-- - Preserve field-level provenance and decision lineage for explainability.

CREATE TABLE IF NOT EXISTS qa_curated_stations_v1 (
  curated_station_id text PRIMARY KEY,
  country char(2) NOT NULL CHECK (country ~ '^[A-Z]{2}$'),
  status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'superseded')),
  primary_cluster_id text REFERENCES qa_station_clusters_v2(cluster_id) ON DELETE SET NULL,
  latest_decision_id bigint REFERENCES qa_station_cluster_decisions_v2(decision_id) ON DELETE SET NULL,
  derived_operation text NOT NULL CHECK (derived_operation IN ('merge', 'split', 'rename', 'keep_separate')),
  display_name text NOT NULL,
  naming_reason text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_by text NOT NULL DEFAULT current_user,
  updated_by text NOT NULL DEFAULT current_user,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (display_name <> '')
);

CREATE INDEX IF NOT EXISTS idx_qa_curated_stations_v1_scope
  ON qa_curated_stations_v1 (country, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_qa_curated_stations_v1_cluster
  ON qa_curated_stations_v1 (primary_cluster_id, status, updated_at DESC);

CREATE TABLE IF NOT EXISTS qa_curated_station_members_v1 (
  curated_station_id text NOT NULL REFERENCES qa_curated_stations_v1(curated_station_id) ON DELETE CASCADE,
  canonical_station_id text NOT NULL REFERENCES canonical_stations(canonical_station_id) ON DELETE RESTRICT,
  member_role text NOT NULL DEFAULT 'member' CHECK (member_role IN ('primary', 'member')),
  member_rank integer NOT NULL DEFAULT 1 CHECK (member_rank > 0),
  contribution jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (curated_station_id, canonical_station_id)
);

CREATE INDEX IF NOT EXISTS idx_qa_curated_station_members_v1_station
  ON qa_curated_station_members_v1 (canonical_station_id, curated_station_id);

CREATE TABLE IF NOT EXISTS qa_curated_station_lineage_v1 (
  lineage_id bigserial PRIMARY KEY,
  curated_station_id text NOT NULL REFERENCES qa_curated_stations_v1(curated_station_id) ON DELETE CASCADE,
  decision_id bigint NOT NULL REFERENCES qa_station_cluster_decisions_v2(decision_id) ON DELETE CASCADE,
  cluster_id text REFERENCES qa_station_clusters_v2(cluster_id) ON DELETE SET NULL,
  operation text NOT NULL CHECK (operation IN ('merge', 'split', 'rename', 'keep_separate')),
  decision_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (curated_station_id, decision_id)
);

CREATE INDEX IF NOT EXISTS idx_qa_curated_station_lineage_v1_cluster
  ON qa_curated_station_lineage_v1 (cluster_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_qa_curated_station_lineage_v1_decision
  ON qa_curated_station_lineage_v1 (decision_id);

CREATE TABLE IF NOT EXISTS qa_curated_station_field_provenance_v1 (
  curated_station_id text NOT NULL REFERENCES qa_curated_stations_v1(curated_station_id) ON DELETE CASCADE,
  field_name text NOT NULL,
  field_value text,
  source_kind text NOT NULL CHECK (source_kind IN ('manual_decision', 'canonical_candidate', 'derived')),
  source_ref text NOT NULL DEFAULT '',
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (field_name <> ''),
  PRIMARY KEY (curated_station_id, field_name, source_kind, source_ref)
);

CREATE INDEX IF NOT EXISTS idx_qa_curated_station_field_provenance_v1_lookup
  ON qa_curated_station_field_provenance_v1 (field_name, source_kind, created_at DESC);

CREATE OR REPLACE VIEW qa_curated_station_projection_v1 AS
SELECT
  s.curated_station_id,
  s.country,
  s.status,
  s.primary_cluster_id,
  s.latest_decision_id,
  s.derived_operation,
  s.display_name,
  s.naming_reason,
  s.metadata,
  s.created_by,
  s.updated_by,
  s.created_at,
  s.updated_at,
  (
    SELECT COALESCE(json_agg(json_build_object(
      'canonical_station_id', m.canonical_station_id,
      'member_role', m.member_role,
      'member_rank', m.member_rank,
      'contribution', m.contribution
    ) ORDER BY m.member_rank, m.canonical_station_id), '[]'::json)
    FROM qa_curated_station_members_v1 m
    WHERE m.curated_station_id = s.curated_station_id
  ) AS members,
  (
    SELECT COALESCE(json_agg(json_build_object(
      'field_name', p.field_name,
      'field_value', p.field_value,
      'source_kind', p.source_kind,
      'source_ref', p.source_ref,
      'metadata', p.metadata
    ) ORDER BY p.field_name, p.source_kind, p.source_ref), '[]'::json)
    FROM qa_curated_station_field_provenance_v1 p
    WHERE p.curated_station_id = s.curated_station_id
  ) AS field_provenance,
  (
    SELECT COALESCE(json_agg(json_build_object(
      'decision_id', l.decision_id,
      'cluster_id', l.cluster_id,
      'operation', l.operation,
      'created_at', l.created_at
    ) ORDER BY l.created_at DESC, l.decision_id DESC), '[]'::json)
    FROM qa_curated_station_lineage_v1 l
    WHERE l.curated_station_id = s.curated_station_id
  ) AS lineage
FROM qa_curated_stations_v1 s;
