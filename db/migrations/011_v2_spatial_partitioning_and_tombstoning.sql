-- V2 Capabilities: Tombstoning and Spatial Partitioning
--
-- This migration is intentionally resilient to partial previous runs.
-- It is wrapped in a transaction so fresh executions are atomic.

BEGIN;

-- 1. Add Tombstoning columns to canonical_stations
ALTER TABLE IF EXISTS canonical_stations
  ADD COLUMN IF NOT EXISTS is_deleted boolean NOT NULL DEFAULT false;
ALTER TABLE IF EXISTS canonical_stations
  ADD COLUMN IF NOT EXISTS deleted_at timestamptz;

-- 2. Add Tombstoning columns to qa_station_clusters_v2
ALTER TABLE qa_station_clusters_v2
  ADD COLUMN IF NOT EXISTS is_deleted boolean NOT NULL DEFAULT false;
ALTER TABLE qa_station_clusters_v2
  ADD COLUMN IF NOT EXISTS deleted_at timestamptz;

-- 3. Rename old base tables to *_legacy only when needed.
DO $$
DECLARE
  v_canonical regclass := to_regclass('public.canonical_stations');
  v_staging regclass := to_regclass('public.netex_stops_staging');
BEGIN
  IF to_regclass('canonical_stations_legacy') IS NULL
     AND v_canonical IS NOT NULL
     AND NOT EXISTS (
       SELECT 1
       FROM pg_partitioned_table p
       WHERE p.partrelid = v_canonical
     )
  THEN
    EXECUTE 'ALTER TABLE canonical_stations RENAME TO canonical_stations_legacy';
  END IF;

  IF to_regclass('public.netex_stops_staging_legacy') IS NULL
     AND v_staging IS NOT NULL
     AND NOT EXISTS (
       SELECT 1
       FROM pg_partitioned_table p
       WHERE p.partrelid = v_staging
     )
  THEN
    EXECUTE 'ALTER TABLE netex_stops_staging RENAME TO netex_stops_staging_legacy';
  END IF;
END $$;

-- 4. Create partitioned canonical_stations.
CREATE TABLE IF NOT EXISTS canonical_stations (
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

CREATE TABLE IF NOT EXISTS canonical_stations_de
  PARTITION OF canonical_stations FOR VALUES IN ('DE');
CREATE TABLE IF NOT EXISTS canonical_stations_at
  PARTITION OF canonical_stations FOR VALUES IN ('AT');
CREATE TABLE IF NOT EXISTS canonical_stations_ch
  PARTITION OF canonical_stations FOR VALUES IN ('CH');

CREATE INDEX IF NOT EXISTS idx_canonical_stations_name
  ON canonical_stations (country, normalized_name);
CREATE INDEX IF NOT EXISTS idx_canonical_stations_geom_partitioned
  ON canonical_stations USING gist (geom);

-- Copy existing rows if legacy table exists.
DO $$
BEGIN
  IF to_regclass('canonical_stations_legacy') IS NOT NULL THEN
    INSERT INTO canonical_stations (
      canonical_station_id,
      canonical_name,
      normalized_name,
      country,
      latitude,
      longitude,
      geom,
      match_method,
      member_count,
      first_seen_snapshot_date,
      last_seen_snapshot_date,
      last_built_run_id,
      is_deleted,
      deleted_at,
      created_at,
      updated_at
    )
    SELECT
      canonical_station_id,
      canonical_name,
      normalized_name,
      country,
      latitude,
      longitude,
      geom,
      match_method,
      member_count,
      first_seen_snapshot_date,
      last_seen_snapshot_date,
      last_built_run_id,
      COALESCE(is_deleted, false),
      deleted_at,
      created_at,
      updated_at
    FROM canonical_stations_legacy
    ON CONFLICT (country, canonical_station_id) DO NOTHING;
  END IF;
END $$;

-- Drop all foreign keys that still point at legacy canonical table.
DO $$
DECLARE
  r record;
  v_canonical_legacy regclass := to_regclass('canonical_stations_legacy');
BEGIN
  IF v_canonical_legacy IS NOT NULL THEN
    FOR r IN
      SELECT conrelid::regclass AS table_name, conname
      FROM pg_constraint
      WHERE contype = 'f'
        AND confrelid = v_canonical_legacy
    LOOP
      EXECUTE format(
        'ALTER TABLE %s DROP CONSTRAINT IF EXISTS %I',
        r.table_name,
        r.conname
      );
    END LOOP;
  END IF;
END $$;

DROP TABLE IF EXISTS canonical_stations_legacy;

-- 5. Create partitioned netex_stops_staging.
CREATE TABLE IF NOT EXISTS netex_stops_staging (
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

CREATE TABLE IF NOT EXISTS netex_stops_staging_de
  PARTITION OF netex_stops_staging FOR VALUES IN ('DE');
CREATE TABLE IF NOT EXISTS netex_stops_staging_at
  PARTITION OF netex_stops_staging FOR VALUES IN ('AT');
CREATE TABLE IF NOT EXISTS netex_stops_staging_ch
  PARTITION OF netex_stops_staging FOR VALUES IN ('CH');

CREATE UNIQUE INDEX IF NOT EXISTS idx_netex_stops_uniq
  ON netex_stops_staging (country, source_id, snapshot_date, source_stop_id);
CREATE INDEX IF NOT EXISTS idx_netex_stops_staging_geom
  ON netex_stops_staging USING gist (geom);
CREATE INDEX IF NOT EXISTS idx_netex_stops_name
  ON netex_stops_staging (country, normalized_name);
CREATE INDEX IF NOT EXISTS idx_netex_stops_hard_id
  ON netex_stops_staging (country, hard_id)
  WHERE hard_id IS NOT NULL;

DO $$
BEGIN
  IF to_regclass('public.netex_stops_staging_legacy') IS NOT NULL THEN
    INSERT INTO netex_stops_staging (
      staging_id,
      import_run_id,
      source_id,
      country,
      provider_slug,
      snapshot_date,
      manifest_sha256,
      source_stop_id,
      source_parent_stop_id,
      stop_name,
      normalized_name,
      latitude,
      longitude,
      geom,
      public_code,
      private_code,
      hard_id,
      source_file,
      raw_payload,
      inserted_at,
      updated_at
    )
    SELECT
      staging_id,
      import_run_id,
      source_id,
      country,
      provider_slug,
      snapshot_date,
      manifest_sha256,
      source_stop_id,
      source_parent_stop_id,
      stop_name,
      normalized_name,
      latitude,
      longitude,
      geom,
      public_code,
      private_code,
      hard_id,
      source_file,
      raw_payload,
      inserted_at,
      updated_at
    FROM netex_stops_staging_legacy
    ON CONFLICT (country, staging_id) DO NOTHING;
  END IF;
END $$;

DROP TABLE IF EXISTS netex_stops_staging_legacy;

COMMIT;
