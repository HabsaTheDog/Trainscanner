-- V2 Capabilities: Tombstoning and Spatial Partitioning

-- 1. Add Tombstoning columns to canonical_stations
ALTER TABLE canonical_stations ADD COLUMN IF NOT EXISTS is_deleted boolean NOT NULL DEFAULT false;
ALTER TABLE canonical_stations ADD COLUMN IF NOT EXISTS deleted_at timestamptz;

-- 2. Add Tombstoning columns to qa_station_clusters_v2
ALTER TABLE qa_station_clusters_v2 ADD COLUMN IF NOT EXISTS is_deleted boolean NOT NULL DEFAULT false;
ALTER TABLE qa_station_clusters_v2 ADD COLUMN IF NOT EXISTS deleted_at timestamptz;

-- 3. Partitioning: Renaming old tables to legacy to prepare partitions
ALTER TABLE canonical_stations RENAME TO canonical_stations_legacy;
ALTER TABLE netex_stops_staging RENAME TO netex_stops_staging_legacy;

-- 4. Create new Partitioned Table for canonical_stations
CREATE TABLE canonical_stations (
  canonical_station_id text,
  canonical_name text NOT NULL,
  normalized_name text NOT NULL,
  country char(2) NOT NULL CHECK (country IN ('DE', 'AT', 'CH')),
  latitude double precision,
  longitude double precision,
  geom geometry(Point, 4326),
  match_method text NOT NULL CHECK (match_method IN ('hard_id', 'name_geo', 'name_only')),
  member_count integer NOT NULL,
  first_seen_snapshot_date date,
  last_seen_snapshot_date date,
  last_built_run_id uuid, -- no foreign key in partitioned tables
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (country, canonical_station_id)
) PARTITION BY LIST (country);

CREATE TABLE canonical_stations_de PARTITION OF canonical_stations FOR VALUES IN ('DE');
CREATE TABLE canonical_stations_at PARTITION OF canonical_stations FOR VALUES IN ('AT');
CREATE TABLE canonical_stations_ch PARTITION OF canonical_stations FOR VALUES IN ('CH');

CREATE INDEX idx_canonical_stations_name ON canonical_stations (country, normalized_name);
CREATE INDEX idx_canonical_stations_geom ON canonical_stations USING gist (geom);

-- Migration of existing data
INSERT INTO canonical_stations (
  canonical_station_id, canonical_name, normalized_name, country, latitude, longitude, geom,
  match_method, member_count, first_seen_snapshot_date, last_seen_snapshot_date,
  last_built_run_id, created_at, updated_at
)
SELECT 
  canonical_station_id, canonical_name, normalized_name, country, latitude, longitude, geom,
  match_method, member_count, first_seen_snapshot_date, last_seen_snapshot_date,
  last_built_run_id, created_at, updated_at
FROM canonical_stations_legacy;

-- Drop foreign keys pointing to old table before dropping
ALTER TABLE qa_station_naming_overrides_v2 DROP CONSTRAINT qa_station_naming_overrides_v2_canonical_station_id_fkey;
ALTER TABLE qa_station_display_names_v2 DROP CONSTRAINT qa_station_display_names_v2_canonical_station_id_fkey;
ALTER TABLE canonical_station_sources DROP CONSTRAINT canonical_station_sources_canonical_station_id_fkey;
ALTER TABLE qa_station_segments_v2 DROP CONSTRAINT qa_station_segments_v2_canonical_station_id_fkey;
ALTER TABLE qa_station_cluster_candidates_v2 DROP CONSTRAINT qa_station_cluster_candidates_v2_canonical_stat_fkey;
ALTER TABLE qa_station_cluster_evidence_v2 DROP CONSTRAINT qa_station_cluster_evidence_v2_source_canonical_s_fkey;
ALTER TABLE qa_station_cluster_evidence_v2 DROP CONSTRAINT qa_station_cluster_evidence_v2_target_canonical_s_fkey;
ALTER TABLE qa_station_cluster_decision_members_v2 DROP CONSTRAINT qa_station_cluster_decision_members_v2_canonical_s_fkey;

DROP TABLE canonical_stations_legacy;

-- 5. Create new Partitioned Table for netex_stops_staging
CREATE TABLE netex_stops_staging (
  staging_id bigserial,
  import_run_id uuid NOT NULL,
  source_id text NOT NULL,
  country char(2) NOT NULL CHECK (country IN ('DE', 'AT', 'CH')),
  provider_slug text NOT NULL,
  snapshot_date date NOT NULL,
  manifest_sha256 text,
  source_stop_id text NOT NULL,
  source_parent_stop_id text,
  stop_name text NOT NULL,
  normalized_name text,
  latitude double precision,
  longitude double precision,
  geom geometry(Point, 4326),
  public_code text,
  private_code text,
  hard_id text,
  source_file text,
  raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (country, staging_id)
) PARTITION BY LIST (country);

CREATE TABLE netex_stops_staging_de PARTITION OF netex_stops_staging FOR VALUES IN ('DE');
CREATE TABLE netex_stops_staging_at PARTITION OF netex_stops_staging FOR VALUES IN ('AT');
CREATE TABLE netex_stops_staging_ch PARTITION OF netex_stops_staging FOR VALUES IN ('CH');

CREATE UNIQUE INDEX idx_netex_stops_uniq ON netex_stops_staging (country, source_id, snapshot_date, source_stop_id);
CREATE INDEX idx_netex_stops_staging_geom ON netex_stops_staging USING gist (geom);
CREATE INDEX idx_netex_stops_name ON netex_stops_staging (country, normalized_name);
CREATE INDEX idx_netex_stops_hard_id ON netex_stops_staging (country, hard_id) WHERE hard_id IS NOT NULL;

INSERT INTO netex_stops_staging (
  staging_id, import_run_id, source_id, country, provider_slug, snapshot_date, manifest_sha256,
  source_stop_id, source_parent_stop_id, stop_name, normalized_name, latitude, longitude, geom,
  public_code, private_code, hard_id, source_file, raw_payload, inserted_at, updated_at
)
SELECT 
  staging_id, import_run_id, source_id, country, provider_slug, snapshot_date, manifest_sha256,
  source_stop_id, source_parent_stop_id, stop_name, normalized_name, latitude, longitude, geom,
  public_code, private_code, hard_id, source_file, raw_payload, inserted_at, updated_at
FROM netex_stops_staging_legacy;

DROP TABLE netex_stops_staging_legacy;
