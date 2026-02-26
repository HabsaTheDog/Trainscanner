-- V2 Geographic Grid Partitioning
--
-- Replaces country-list partitions with geospatial grid partitions so writes
-- and reads scale beyond fixed country shards.
--
-- Note: this repository uses forward-only migrations. A manual rollback recipe
-- is provided at the end of this file.

BEGIN;

ALTER TABLE canonical_stations RENAME TO canonical_stations_country_partitioned_legacy;
ALTER TABLE netex_stops_staging RENAME TO netex_stops_staging_country_partitioned_legacy;

CREATE OR REPLACE FUNCTION compute_geo_grid_id(
  p_country text,
  p_latitude double precision,
  p_longitude double precision,
  p_geom geometry DEFAULT NULL
)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN p_geom IS NOT NULL THEN
      'g'
      || lpad(floor(ST_Y(p_geom) + 90)::text, 3, '0')
      || '_'
      || lpad(floor(ST_X(p_geom) + 180)::text, 3, '0')
    WHEN p_longitude BETWEEN -180 AND 180 AND p_latitude BETWEEN -90 AND 90 THEN
      'g'
      || lpad(floor(p_latitude + 90)::text, 3, '0')
      || '_'
      || lpad(floor(p_longitude + 180)::text, 3, '0')
    ELSE 'zzz' || lower(btrim(COALESCE(p_country, '')))
  END;
$$;

-- Keep distributed/partitioned tables free of direct FK dependencies.
DO $$
DECLARE
  r record;
BEGIN
  FOR r IN
    SELECT conrelid::regclass AS table_name, conname
    FROM pg_constraint
    WHERE contype = 'f'
      AND confrelid IN (
        'canonical_stations_country_partitioned_legacy'::regclass,
        'netex_stops_staging_country_partitioned_legacy'::regclass
      )
  LOOP
    EXECUTE format(
      'ALTER TABLE %s DROP CONSTRAINT IF EXISTS %I',
      r.table_name,
      r.conname
    );
  END LOOP;
END $$;

CREATE TABLE canonical_stations (
  canonical_station_id text NOT NULL,
  canonical_name text NOT NULL,
  normalized_name text NOT NULL,
  country char(2) NOT NULL CHECK (country ~ '^[A-Z]{2}$'),
  latitude double precision,
  longitude double precision,
  geom geometry(Point, 4326),
  grid_id text NOT NULL,
  match_method text NOT NULL CHECK (match_method IN ('hard_id', 'name_geo', 'name_only')),
  member_count integer NOT NULL,
  first_seen_snapshot_date date,
  last_seen_snapshot_date date,
  last_built_run_id uuid,
  is_deleted boolean NOT NULL DEFAULT false,
  deleted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT canonical_stations_grid_pkey PRIMARY KEY (grid_id, canonical_station_id)
) PARTITION BY HASH (grid_id);

DO $$
DECLARE
  i integer;
BEGIN
  FOR i IN 0..31 LOOP
    EXECUTE format(
      'CREATE TABLE canonical_stations_p%s PARTITION OF canonical_stations FOR VALUES WITH (MODULUS 32, REMAINDER %s)',
      lpad(i::text, 2, '0'),
      i
    );
  END LOOP;
END $$;

CREATE INDEX idx_canonical_stations_grid_country_name
  ON canonical_stations (country, normalized_name);
CREATE INDEX idx_canonical_stations_grid_lookup
  ON canonical_stations (canonical_station_id, country);
CREATE INDEX idx_canonical_stations_grid_geom
  ON canonical_stations USING gist (geom);

INSERT INTO canonical_stations (
  canonical_station_id,
  canonical_name,
  normalized_name,
  country,
  latitude,
  longitude,
  geom,
  grid_id,
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
  compute_geo_grid_id(country::text, latitude, longitude, geom) AS grid_id,
  match_method,
  member_count,
  first_seen_snapshot_date,
  last_seen_snapshot_date,
  last_built_run_id,
  is_deleted,
  deleted_at,
  created_at,
  updated_at
FROM canonical_stations_country_partitioned_legacy;

CREATE SEQUENCE netex_stops_staging_grid_seq AS bigint;

CREATE TABLE netex_stops_staging (
  staging_id bigint NOT NULL DEFAULT nextval('netex_stops_staging_grid_seq'),
  import_run_id uuid NOT NULL,
  source_id text NOT NULL,
  country char(2) NOT NULL CHECK (country ~ '^[A-Z]{2}$'),
  provider_slug text NOT NULL,
  snapshot_date date NOT NULL,
  manifest_sha256 text,
  source_stop_id text NOT NULL,
  source_parent_stop_id text,
  stop_name text NOT NULL,
  normalized_name text GENERATED ALWAYS AS (normalize_station_name(stop_name)) STORED,
  latitude double precision,
  longitude double precision,
  geom geometry(Point, 4326) GENERATED ALWAYS AS (
    CASE
      WHEN longitude BETWEEN -180 AND 180 AND latitude BETWEEN -90 AND 90
        THEN ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
      ELSE NULL
    END
  ) STORED,
  grid_id text NOT NULL,
  public_code text,
  private_code text,
  hard_id text,
  source_file text,
  raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT netex_stops_staging_grid_pkey PRIMARY KEY (grid_id, staging_id)
) PARTITION BY HASH (grid_id);

ALTER SEQUENCE netex_stops_staging_grid_seq
  OWNED BY netex_stops_staging.staging_id;

DO $$
DECLARE
  i integer;
BEGIN
  FOR i IN 0..31 LOOP
    EXECUTE format(
      'CREATE TABLE netex_stops_staging_p%s PARTITION OF netex_stops_staging FOR VALUES WITH (MODULUS 32, REMAINDER %s)',
      lpad(i::text, 2, '0'),
      i
    );
  END LOOP;
END $$;

CREATE UNIQUE INDEX idx_netex_stops_staging_grid_uniq
  ON netex_stops_staging (grid_id, source_id, snapshot_date, source_stop_id);
CREATE INDEX idx_netex_stops_staging_grid_geom
  ON netex_stops_staging USING gist (geom);
CREATE INDEX idx_netex_stops_staging_grid_name
  ON netex_stops_staging (country, normalized_name);
CREATE INDEX idx_netex_stops_staging_grid_lookup
  ON netex_stops_staging (source_id, snapshot_date DESC);
CREATE INDEX idx_netex_stops_staging_grid_hard_id
  ON netex_stops_staging (country, hard_id)
  WHERE hard_id IS NOT NULL;

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
  latitude,
  longitude,
  grid_id,
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
  latitude,
  longitude,
  compute_geo_grid_id(country::text, latitude, longitude, NULL::geometry) AS grid_id,
  public_code,
  private_code,
  hard_id,
  source_file,
  raw_payload,
  inserted_at,
  updated_at
FROM netex_stops_staging_country_partitioned_legacy;

SELECT setval(
  'netex_stops_staging_grid_seq',
  COALESCE((SELECT MAX(staging_id) FROM netex_stops_staging), 1),
  true
);

DROP TABLE canonical_stations_country_partitioned_legacy;
DROP TABLE netex_stops_staging_country_partitioned_legacy;

-- Safety net in case any migration/user SQL recreated FKs to partitioned tables.
DO $$
DECLARE
  r record;
BEGIN
  FOR r IN
    SELECT conrelid::regclass AS table_name, conname
    FROM pg_constraint
    WHERE contype = 'f'
      AND confrelid IN (
        'canonical_stations'::regclass,
        'netex_stops_staging'::regclass
      )
  LOOP
    EXECUTE format(
      'ALTER TABLE %s DROP CONSTRAINT IF EXISTS %I',
      r.table_name,
      r.conname
    );
  END LOOP;
END $$;

COMMIT;
-- Manual down migration recipe (forward-only runner; execute manually if needed):
-- 1) Rename current tables to *_grid_legacy.
-- 2) Recreate LIST(country) partitioned tables with PK (country, canonical_station_id)
--    and PK (country, staging_id).
-- 3) Copy rows from *_grid_legacy into recreated tables.
-- 4) Recreate country-based indexes and unique constraint
--    (country, source_id, snapshot_date, source_stop_id).
-- 5) Drop *_grid_legacy tables.
