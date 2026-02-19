CREATE EXTENSION IF NOT EXISTS postgis;

CREATE OR REPLACE FUNCTION normalize_station_name(input_name text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT trim(regexp_replace(lower(coalesce(input_name, '')), '[^[:alnum:]]+', ' ', 'g'));
$$;

CREATE TABLE IF NOT EXISTS import_runs (
  run_id uuid PRIMARY KEY,
  pipeline text NOT NULL CHECK (pipeline IN ('netex_ingest', 'canonical_build')),
  status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
  source_id text,
  country char(2) CHECK (country IN ('DE', 'AT', 'CH')),
  snapshot_date date,
  started_at timestamptz NOT NULL DEFAULT now(),
  ended_at timestamptz,
  stats jsonb NOT NULL DEFAULT '{}'::jsonb,
  error_message text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_import_runs_pipeline_started
  ON import_runs (pipeline, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_import_runs_status_started
  ON import_runs (status, started_at DESC);

CREATE TABLE IF NOT EXISTS raw_snapshots (
  source_id text NOT NULL,
  country char(2) NOT NULL CHECK (country IN ('DE', 'AT', 'CH')),
  provider_slug text NOT NULL,
  format text NOT NULL CHECK (format = 'netex'),
  snapshot_date date NOT NULL,
  manifest_path text NOT NULL,
  manifest_sha256 text,
  manifest jsonb NOT NULL DEFAULT '{}'::jsonb,
  resolved_download_url text,
  file_name text NOT NULL,
  file_size_bytes bigint,
  retrieval_timestamp timestamptz,
  detected_version_or_date text,
  requested_as_of date,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_raw_snapshots_country_snapshot
  ON raw_snapshots (country, snapshot_date DESC);

CREATE TABLE IF NOT EXISTS netex_stops_staging (
  staging_id bigserial PRIMARY KEY,
  import_run_id uuid NOT NULL REFERENCES import_runs(run_id),
  source_id text NOT NULL,
  country char(2) NOT NULL CHECK (country IN ('DE', 'AT', 'CH')),
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
      WHEN longitude BETWEEN -180 AND 180 AND latitude BETWEEN -90 AND 90 THEN ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
      ELSE NULL
    END
  ) STORED,
  public_code text,
  private_code text,
  hard_id text,
  source_file text,
  raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source_id, snapshot_date, source_stop_id)
);

CREATE INDEX IF NOT EXISTS idx_netex_stops_source_snapshot
  ON netex_stops_staging (source_id, snapshot_date DESC);

CREATE INDEX IF NOT EXISTS idx_netex_stops_country_name
  ON netex_stops_staging (country, normalized_name);

CREATE INDEX IF NOT EXISTS idx_netex_stops_hard_id
  ON netex_stops_staging (country, hard_id)
  WHERE hard_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_netex_stops_geom
  ON netex_stops_staging USING gist (geom)
  WHERE geom IS NOT NULL;

CREATE TABLE IF NOT EXISTS canonical_stations (
  canonical_station_id text PRIMARY KEY,
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
  last_built_run_id uuid REFERENCES import_runs(run_id),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_canonical_stations_country_name
  ON canonical_stations (country, normalized_name);

CREATE INDEX IF NOT EXISTS idx_canonical_stations_geom
  ON canonical_stations USING gist (geom)
  WHERE geom IS NOT NULL;

CREATE TABLE IF NOT EXISTS canonical_station_sources (
  canonical_station_id text NOT NULL REFERENCES canonical_stations(canonical_station_id) ON DELETE CASCADE,
  source_id text NOT NULL,
  source_stop_id text NOT NULL,
  country char(2) NOT NULL CHECK (country IN ('DE', 'AT', 'CH')),
  snapshot_date date NOT NULL,
  match_method text NOT NULL CHECK (match_method IN ('hard_id', 'name_geo', 'name_only')),
  hard_id text,
  import_run_id uuid NOT NULL REFERENCES import_runs(run_id),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source_id, source_stop_id)
);

CREATE INDEX IF NOT EXISTS idx_canonical_station_sources_canonical
  ON canonical_station_sources (canonical_station_id);

CREATE INDEX IF NOT EXISTS idx_canonical_station_sources_country
  ON canonical_station_sources (country, source_id);
