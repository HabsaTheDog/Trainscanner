\set ON_ERROR_STOP on

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'iso_country_code') THEN
    CREATE DOMAIN iso_country_code AS char(2)
      CHECK (VALUE ~ '^[A-Z]{2}$');
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'provider_feed_format') THEN
    CREATE TYPE provider_feed_format AS ENUM ('netex', 'gtfs');
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_type WHERE typname = 'qa_merge_decision_operation'
  ) THEN
    CREATE TYPE qa_merge_decision_operation AS ENUM (
      'merge',
      'split',
      'group',
      'keep_separate',
      'rename',
      'reopen_workspace',
      'resolve_workspace',
      'dismiss_workspace'
    );
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_type WHERE typname = 'qa_merge_decision_member_action'
  ) THEN
    CREATE TYPE qa_merge_decision_member_action AS ENUM (
      'candidate',
      'merge_member',
      'group_member',
      'separate',
      'rename_target'
    );
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_type WHERE typname = 'qa_merge_workspace_action'
  ) THEN
    CREATE TYPE qa_merge_workspace_action AS ENUM (
      'save',
      'undo',
      'reset',
      'resolve',
      'dismiss'
    );
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_type WHERE typname = 'pipeline_job_status'
  ) THEN
    CREATE TYPE pipeline_job_status AS ENUM (
      'queued',
      'running',
      'retry_wait',
      'succeeded',
      'failed'
    );
  END IF;
END $$;

CREATE OR REPLACE FUNCTION normalize_station_name(input_name text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT trim(regexp_replace(lower(coalesce(input_name, '')), '[^[:alnum:]]+', ' ', 'g'));
$$;

CREATE OR REPLACE FUNCTION qa_loose_station_name(input_name text)
RETURNS text
LANGUAGE sql
STABLE
AS $$
  SELECT trim(
    regexp_replace(
      regexp_replace(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(
                lower(unaccent(coalesce(input_name, ''))),
                '\b(abzw|abzw\.)\b',
                ' abzweig ',
                'g'
              ),
              '\b(bhf|bf)\b',
              ' bahnhof ',
              'g'
            ),
            '\b(hbf)\b',
            ' hauptbahnhof ',
            'g'
          ),
          '\b(str|str\.)\b',
          ' strasse ',
          'g'
        ),
        '[^[:alnum:]]+',
        ' ',
        'g'
      ),
      '\s+',
      ' ',
      'g'
    )
  );
$$;

-- Operational runtime tables used by orchestrator job/state services.

CREATE TABLE IF NOT EXISTS pipeline_jobs (
  job_id uuid PRIMARY KEY,
  job_type text NOT NULL,
  idempotency_key text NOT NULL,
  status pipeline_job_status NOT NULL,
  attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
  run_context jsonb NOT NULL DEFAULT jsonb_build_object(),
  checkpoint jsonb NOT NULL DEFAULT jsonb_build_object(),
  result_context jsonb NOT NULL DEFAULT jsonb_build_object(),
  started_at timestamptz,
  ended_at timestamptz,
  error_code text,
  error_message text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (job_type, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_status_type
  ON pipeline_jobs (status, job_type, updated_at DESC);

CREATE TABLE IF NOT EXISTS system_state (
  key text PRIMARY KEY,
  value jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO system_state (key, value)
VALUES
  ('active_gtfs', '{"activeProfile": "pan_europe_runtime", "zipPath": "", "sourceType": "runtime", "runtime": null, "activatedAt": null}'::jsonb),
  ('gtfs_switch_status', '{"state": "ready", "activeProfile": "pan_europe_runtime", "message": "Pan-European runtime ready", "error": null, "requestedProfile": "pan_europe_runtime"}'::jsonb)
ON CONFLICT (key) DO NOTHING;

-- Optional ingest bookkeeping (kept for orchestration visibility).

CREATE TABLE IF NOT EXISTS import_runs (
  run_id uuid PRIMARY KEY,
  pipeline text NOT NULL CHECK (pipeline IN ('source_fetch', 'netex_ingest', 'global_build', 'qa_merge_build')),
  status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
  source_id text,
  country iso_country_code,
  snapshot_date date,
  started_at timestamptz NOT NULL DEFAULT now(),
  ended_at timestamptz,
  stats jsonb NOT NULL DEFAULT jsonb_build_object(),
  error_message text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_import_runs_pipeline_started
  ON import_runs (pipeline, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_import_runs_status_started
  ON import_runs (status, started_at DESC);

CREATE TABLE IF NOT EXISTS raw_snapshots (
  source_id text NOT NULL,
  country iso_country_code NOT NULL,
  provider_slug text NOT NULL,
  format provider_feed_format NOT NULL,
  snapshot_date date NOT NULL,
  manifest_path text NOT NULL,
  manifest_sha256 text,
  manifest jsonb NOT NULL DEFAULT jsonb_build_object(),
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

-- Pan-European canonical model.

CREATE TABLE IF NOT EXISTS provider_datasets (
  dataset_id bigserial PRIMARY KEY,
  source_id text NOT NULL,
  provider_slug text NOT NULL,
  country iso_country_code,
  format provider_feed_format NOT NULL DEFAULT 'netex',
  snapshot_date date NOT NULL,
  manifest_path text,
  manifest_sha256 text,
  manifest jsonb NOT NULL DEFAULT jsonb_build_object(),
  raw_archive_path text,
  ingestion_status text NOT NULL DEFAULT 'pending' CHECK (ingestion_status IN ('pending', 'ingested', 'failed')),
  ingestion_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_provider_datasets_country_snapshot
  ON provider_datasets (country, snapshot_date DESC);

CREATE TABLE IF NOT EXISTS raw_provider_stop_places (
  stop_place_id text PRIMARY KEY,
  dataset_id bigint NOT NULL REFERENCES provider_datasets(dataset_id) ON DELETE CASCADE,
  source_id text NOT NULL,
  provider_stop_place_ref text NOT NULL,
  country iso_country_code,
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
  parent_stop_place_ref text,
  topographic_place_ref text,
  public_code text,
  private_code text,
  hard_id text,
  raw_payload jsonb NOT NULL DEFAULT jsonb_build_object(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source_id, provider_stop_place_ref, dataset_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_stop_places_dataset
  ON raw_provider_stop_places (dataset_id, source_id);

CREATE INDEX IF NOT EXISTS idx_raw_stop_places_lookup
  ON raw_provider_stop_places (source_id, provider_stop_place_ref);

CREATE INDEX IF NOT EXISTS idx_raw_stop_places_country_name
  ON raw_provider_stop_places (country, normalized_name);

CREATE INDEX IF NOT EXISTS idx_raw_stop_places_hard_id
  ON raw_provider_stop_places (hard_id)
  WHERE hard_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_raw_stop_places_geom
  ON raw_provider_stop_places USING gist (geom)
  WHERE geom IS NOT NULL;

CREATE TABLE IF NOT EXISTS raw_provider_stop_points (
  stop_point_id text PRIMARY KEY,
  dataset_id bigint NOT NULL REFERENCES provider_datasets(dataset_id) ON DELETE CASCADE,
  source_id text NOT NULL,
  provider_stop_point_ref text NOT NULL,
  provider_stop_place_ref text,
  stop_place_id text REFERENCES raw_provider_stop_places(stop_place_id) ON DELETE SET NULL,
  country iso_country_code,
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
  topographic_place_ref text,
  platform_code text,
  track_code text,
  raw_payload jsonb NOT NULL DEFAULT jsonb_build_object(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source_id, provider_stop_point_ref, dataset_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_stop_points_dataset
  ON raw_provider_stop_points (dataset_id, source_id);

CREATE INDEX IF NOT EXISTS idx_raw_stop_points_lookup
  ON raw_provider_stop_points (source_id, provider_stop_point_ref);

CREATE INDEX IF NOT EXISTS idx_raw_stop_points_country_name
  ON raw_provider_stop_points (country, normalized_name);

CREATE INDEX IF NOT EXISTS idx_raw_stop_points_geom
  ON raw_provider_stop_points USING gist (geom)
  WHERE geom IS NOT NULL;

CREATE TABLE IF NOT EXISTS global_stations (
  global_station_id text PRIMARY KEY,
  display_name text NOT NULL,
  normalized_name text NOT NULL,
  country iso_country_code,
  latitude double precision,
  longitude double precision,
  geom geometry(Point, 4326),
  station_kind text NOT NULL DEFAULT 'station',
  confidence_score numeric(5,4),
  is_active boolean NOT NULL DEFAULT true,
  metadata jsonb NOT NULL DEFAULT jsonb_build_object(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (display_name <> '')
);

CREATE INDEX IF NOT EXISTS idx_global_stations_name
  ON global_stations (normalized_name, country);

CREATE INDEX IF NOT EXISTS idx_global_stations_active_country_name
  ON global_stations (country, normalized_name, global_station_id)
  WHERE is_active = true;

CREATE INDEX IF NOT EXISTS idx_global_stations_geom
  ON global_stations USING gist (geom)
  WHERE geom IS NOT NULL;

CREATE TABLE IF NOT EXISTS global_stop_points (
  global_stop_point_id text PRIMARY KEY,
  global_station_id text NOT NULL REFERENCES global_stations(global_station_id) ON DELETE CASCADE,
  display_name text NOT NULL,
  normalized_name text NOT NULL,
  country iso_country_code,
  latitude double precision,
  longitude double precision,
  geom geometry(Point, 4326),
  stop_point_kind text NOT NULL DEFAULT 'platform',
  is_active boolean NOT NULL DEFAULT true,
  metadata jsonb NOT NULL DEFAULT jsonb_build_object(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (display_name <> '')
);

CREATE INDEX IF NOT EXISTS idx_global_stop_points_station
  ON global_stop_points (global_station_id, normalized_name);

CREATE INDEX IF NOT EXISTS idx_global_stop_points_active_station
  ON global_stop_points (global_station_id, global_stop_point_id)
  WHERE is_active = true;

CREATE INDEX IF NOT EXISTS idx_global_stop_points_geom
  ON global_stop_points USING gist (geom)
  WHERE geom IS NOT NULL;

CREATE TABLE IF NOT EXISTS provider_global_station_mappings (
  mapping_id bigserial PRIMARY KEY,
  source_id text NOT NULL,
  provider_stop_place_ref text NOT NULL,
  global_station_id text NOT NULL REFERENCES global_stations(global_station_id) ON DELETE CASCADE,
  confidence_score numeric(5,4),
  mapping_method text NOT NULL DEFAULT 'heuristic',
  is_active boolean NOT NULL DEFAULT true,
  valid_from date,
  valid_to date,
  metadata jsonb NOT NULL DEFAULT jsonb_build_object(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_provider_global_station_mapping_active
  ON provider_global_station_mappings (source_id, provider_stop_place_ref)
  WHERE is_active = true;

CREATE INDEX IF NOT EXISTS idx_provider_global_station_mapping_station
  ON provider_global_station_mappings (global_station_id, is_active);

CREATE INDEX IF NOT EXISTS idx_provider_global_station_mapping_active_cover
  ON provider_global_station_mappings (global_station_id)
  INCLUDE (source_id, provider_stop_place_ref)
  WHERE is_active = true;

CREATE TABLE IF NOT EXISTS provider_global_stop_point_mappings (
  mapping_id bigserial PRIMARY KEY,
  source_id text NOT NULL,
  provider_stop_point_ref text NOT NULL,
  global_stop_point_id text NOT NULL REFERENCES global_stop_points(global_stop_point_id) ON DELETE CASCADE,
  confidence_score numeric(5,4),
  mapping_method text NOT NULL DEFAULT 'heuristic',
  is_active boolean NOT NULL DEFAULT true,
  valid_from date,
  valid_to date,
  metadata jsonb NOT NULL DEFAULT jsonb_build_object(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from)
);

ALTER TABLE provider_global_stop_point_mappings
  SET (parallel_workers = 4);

CREATE UNIQUE INDEX IF NOT EXISTS idx_provider_global_stop_point_mapping_active
  ON provider_global_stop_point_mappings (source_id, provider_stop_point_ref)
  WHERE is_active = true;

CREATE INDEX IF NOT EXISTS idx_provider_global_stop_point_mapping_active_cover
  ON provider_global_stop_point_mappings (source_id, provider_stop_point_ref)
  INCLUDE (global_stop_point_id)
  WHERE is_active = true;

CREATE INDEX IF NOT EXISTS idx_provider_global_stop_point_mapping_stop_point
  ON provider_global_stop_point_mappings (global_stop_point_id, is_active);

CREATE TABLE IF NOT EXISTS timetable_trips (
  trip_fact_id text PRIMARY KEY,
  dataset_id bigint REFERENCES provider_datasets(dataset_id) ON DELETE SET NULL,
  source_id text NOT NULL,
  provider_trip_ref text NOT NULL,
  service_id text,
  route_id text,
  route_short_name text,
  route_long_name text,
  trip_headsign text,
  transport_mode text,
  trip_start_date date,
  trip_end_date date,
  raw_payload jsonb NOT NULL DEFAULT jsonb_build_object(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source_id, provider_trip_ref, dataset_id)
);

ALTER TABLE timetable_trips
  SET (parallel_workers = 4);

CREATE INDEX IF NOT EXISTS idx_timetable_trips_source
  ON timetable_trips (source_id, provider_trip_ref);

CREATE INDEX IF NOT EXISTS idx_timetable_trips_source_trip_fact
  ON timetable_trips (source_id, trip_fact_id);

CREATE INDEX IF NOT EXISTS idx_timetable_trips_dataset_trip_fact
  ON timetable_trips (dataset_id, trip_fact_id);

CREATE INDEX IF NOT EXISTS idx_timetable_trips_active_window
  ON timetable_trips (
    source_id,
    COALESCE(trip_start_date, DATE '1900-01-01'),
    COALESCE(trip_end_date, DATE '2999-12-31'),
    trip_fact_id
  );

CREATE TABLE IF NOT EXISTS timetable_trip_stop_times (
  trip_fact_id text NOT NULL REFERENCES timetable_trips(trip_fact_id) ON DELETE CASCADE,
  stop_sequence integer NOT NULL CHECK (stop_sequence > 0),
  global_stop_point_id text REFERENCES global_stop_points(global_stop_point_id) ON DELETE SET NULL,
  arrival_time text,
  departure_time text,
  pickup_type integer NOT NULL DEFAULT 0 CHECK (pickup_type >= 0),
  drop_off_type integer NOT NULL DEFAULT 0 CHECK (drop_off_type >= 0),
  metadata jsonb NOT NULL DEFAULT jsonb_build_object(),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (trip_fact_id, stop_sequence)
);

ALTER TABLE timetable_trip_stop_times
  SET (parallel_workers = 8);

CREATE INDEX IF NOT EXISTS idx_timetable_stop_times_stop_point
  ON timetable_trip_stop_times (global_stop_point_id, stop_sequence);

CREATE INDEX IF NOT EXISTS idx_timetable_stop_times_trip_provider_point_ref
  ON timetable_trip_stop_times (
    trip_fact_id,
    (COALESCE(metadata ->> 'provider_stop_point_ref', ''))
  );

CREATE TABLE IF NOT EXISTS transfer_edges (
  transfer_edge_id bigserial PRIMARY KEY,
  from_global_stop_point_id text NOT NULL REFERENCES global_stop_points(global_stop_point_id) ON DELETE CASCADE,
  to_global_stop_point_id text NOT NULL REFERENCES global_stop_points(global_stop_point_id) ON DELETE CASCADE,
  min_transfer_seconds integer NOT NULL DEFAULT 0 CHECK (min_transfer_seconds >= 0),
  transfer_type smallint NOT NULL DEFAULT 2 CHECK (transfer_type BETWEEN 0 AND 3),
  is_bidirectional boolean NOT NULL DEFAULT false,
  metadata jsonb NOT NULL DEFAULT jsonb_build_object(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (from_global_stop_point_id <> to_global_stop_point_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_transfer_edges_unique
  ON transfer_edges (from_global_stop_point_id, to_global_stop_point_id);

CREATE TABLE IF NOT EXISTS qa_merge_clusters (
  merge_cluster_id text PRIMARY KEY,
  cluster_key text NOT NULL UNIQUE,
  status text NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'in_review', 'resolved', 'dismissed')),
  severity text NOT NULL DEFAULT 'medium' CHECK (severity IN ('low', 'medium', 'high')),
  scope_tag text NOT NULL DEFAULT 'latest',
  scope_as_of date,
  display_name text,
  summary jsonb NOT NULL DEFAULT jsonb_build_object(),
  country_tags text[] NOT NULL DEFAULT ARRAY[]::text[],
  candidate_count integer NOT NULL DEFAULT 0 CHECK (candidate_count >= 0),
  issue_count integer NOT NULL DEFAULT 0 CHECK (issue_count >= 0),
  resolved_at timestamptz,
  resolved_by text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_qa_merge_clusters_scope
  ON qa_merge_clusters (scope_tag, status, severity, updated_at DESC);

CREATE TABLE IF NOT EXISTS qa_merge_cluster_candidates (
  merge_cluster_id text NOT NULL REFERENCES qa_merge_clusters(merge_cluster_id) ON DELETE CASCADE,
  global_station_id text NOT NULL REFERENCES global_stations(global_station_id) ON DELETE CASCADE,
  candidate_rank integer NOT NULL CHECK (candidate_rank > 0),
  display_name text NOT NULL,
  latitude double precision,
  longitude double precision,
  country iso_country_code,
  provider_labels jsonb NOT NULL DEFAULT '[]'::jsonb,
  metadata jsonb NOT NULL DEFAULT jsonb_build_object(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (merge_cluster_id, global_station_id)
);

CREATE INDEX IF NOT EXISTS idx_qa_merge_cluster_candidates_rank
  ON qa_merge_cluster_candidates (merge_cluster_id, candidate_rank);

CREATE TABLE IF NOT EXISTS qa_merge_cluster_evidence (
  evidence_id bigserial PRIMARY KEY,
  merge_cluster_id text NOT NULL REFERENCES qa_merge_clusters(merge_cluster_id) ON DELETE CASCADE,
  source_global_station_id text NOT NULL REFERENCES global_stations(global_station_id) ON DELETE CASCADE,
  target_global_station_id text NOT NULL REFERENCES global_stations(global_station_id) ON DELETE CASCADE,
  evidence_type text NOT NULL,
  status text,
  score numeric(8,4),
  raw_value numeric(12,4),
  details jsonb NOT NULL DEFAULT jsonb_build_object(),
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (source_global_station_id <> target_global_station_id)
);

ALTER TABLE qa_merge_cluster_evidence
  ADD COLUMN IF NOT EXISTS status text;

ALTER TABLE qa_merge_cluster_evidence
  ADD COLUMN IF NOT EXISTS raw_value numeric(12,4);

CREATE INDEX IF NOT EXISTS idx_qa_merge_cluster_evidence_cluster
  ON qa_merge_cluster_evidence (merge_cluster_id, evidence_type);

CREATE TABLE IF NOT EXISTS qa_merge_decisions (
  decision_id bigserial PRIMARY KEY,
  merge_cluster_id text NOT NULL REFERENCES qa_merge_clusters(merge_cluster_id) ON DELETE CASCADE,
  operation qa_merge_decision_operation NOT NULL,
  decision_payload jsonb NOT NULL DEFAULT jsonb_build_object(),
  note text,
  requested_by text NOT NULL DEFAULT current_user,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_qa_merge_decisions_cluster
  ON qa_merge_decisions (merge_cluster_id, created_at DESC);

CREATE TABLE IF NOT EXISTS qa_merge_decision_members (
  decision_id bigint NOT NULL REFERENCES qa_merge_decisions(decision_id) ON DELETE CASCADE,
  global_station_id text NOT NULL REFERENCES global_stations(global_station_id) ON DELETE CASCADE,
  action qa_merge_decision_member_action NOT NULL DEFAULT 'candidate',
  group_label text NOT NULL DEFAULT '',
  metadata jsonb NOT NULL DEFAULT jsonb_build_object(),
  PRIMARY KEY (decision_id, global_station_id, action, group_label)
);

CREATE TABLE IF NOT EXISTS qa_merge_cluster_workspaces (
  merge_cluster_id text PRIMARY KEY REFERENCES qa_merge_clusters(merge_cluster_id) ON DELETE CASCADE,
  version integer NOT NULL DEFAULT 1 CHECK (version > 0),
  workspace_payload jsonb NOT NULL DEFAULT jsonb_build_object(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  updated_by text NOT NULL DEFAULT current_user
);

CREATE INDEX IF NOT EXISTS idx_qa_merge_cluster_workspaces_updated
  ON qa_merge_cluster_workspaces (updated_at DESC);

CREATE TABLE IF NOT EXISTS qa_merge_cluster_workspace_versions (
  merge_cluster_id text NOT NULL REFERENCES qa_merge_clusters(merge_cluster_id) ON DELETE CASCADE,
  version integer NOT NULL CHECK (version > 0),
  workspace_payload jsonb NOT NULL DEFAULT jsonb_build_object(),
  action qa_merge_workspace_action NOT NULL DEFAULT 'save',
  updated_at timestamptz NOT NULL DEFAULT now(),
  updated_by text NOT NULL DEFAULT current_user,
  PRIMARY KEY (merge_cluster_id, version)
);

CREATE INDEX IF NOT EXISTS idx_qa_merge_cluster_workspace_versions_history
  ON qa_merge_cluster_workspace_versions (merge_cluster_id, updated_at DESC, version DESC);
