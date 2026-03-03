\set ON_ERROR_STOP on

-- Baseline schema for a clean bootstrap from scratch.
-- Flattened from the legacy incremental SQL fragments.

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

-- Initial definitions commented out because they conflict with later partitioned definitions
/*
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
*/

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

CREATE TABLE IF NOT EXISTS canonical_review_queue (
  review_item_id bigserial PRIMARY KEY,
  issue_key text NOT NULL UNIQUE,
  country char(2) NOT NULL CHECK (country IN ('DE', 'AT', 'CH')),
  canonical_station_id text,
  issue_type text NOT NULL CHECK (
    issue_type IN (
      'name_only_cluster',
      'suspicious_geo_spread',
      'duplicate_hard_id',
      'duplicate_normalized_name'
    )
  ),
  severity text NOT NULL DEFAULT 'medium' CHECK (severity IN ('low', 'medium', 'high')),
  detected_as_of date,
  status text NOT NULL DEFAULT 'open' CHECK (
    status IN ('open', 'confirmed', 'dismissed', 'resolved', 'auto_resolved')
  ),
  detected_count integer NOT NULL DEFAULT 1 CHECK (detected_count >= 1),
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  provenance_source text NOT NULL DEFAULT 'build-review-queue.sh',
  provenance_run_tag text,
  provenance_note text,
  first_detected_at timestamptz NOT NULL DEFAULT now(),
  last_detected_at timestamptz NOT NULL DEFAULT now(),
  resolved_at timestamptz,
  resolved_by text,
  resolution_note text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    resolved_at IS NULL
    OR status IN ('dismissed', 'resolved', 'auto_resolved')
  )
);

CREATE INDEX IF NOT EXISTS idx_canonical_review_queue_status_country
  ON canonical_review_queue (status, country, last_detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_canonical_review_queue_issue_type
  ON canonical_review_queue (issue_type, country);

CREATE INDEX IF NOT EXISTS idx_canonical_review_queue_canonical
  ON canonical_review_queue (canonical_station_id)
  WHERE canonical_station_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS canonical_station_overrides (
  override_id bigserial PRIMARY KEY,
  operation text NOT NULL CHECK (operation IN ('merge', 'split', 'rename')),
  status text NOT NULL DEFAULT 'draft' CHECK (
    status IN ('draft', 'approved', 'applied', 'failed', 'rejected')
  ),
  country char(2) NOT NULL CHECK (country IN ('DE', 'AT', 'CH')),
  source_canonical_station_id text,
  target_canonical_station_id text,
  source_id text,
  source_stop_id text,
  new_canonical_name text,
  reason text,
  external_ref text,
  operation_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  requested_by text NOT NULL DEFAULT current_user,
  approved_by text,
  applied_by text,
  created_via text NOT NULL DEFAULT 'manual_sql' CHECK (
    created_via IN ('manual_sql', 'csv_import', 'script')
  ),
  created_from_file text,
  created_from_line integer CHECK (created_from_line IS NULL OR created_from_line > 0),
  requested_at timestamptz NOT NULL DEFAULT now(),
  approved_at timestamptz,
  applied_at timestamptz,
  applied_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (operation = 'rename' AND target_canonical_station_id IS NOT NULL AND NULLIF(new_canonical_name, '') IS NOT NULL)
    OR (operation = 'merge' AND source_canonical_station_id IS NOT NULL AND target_canonical_station_id IS NOT NULL AND source_canonical_station_id <> target_canonical_station_id)
    OR (operation = 'split' AND source_canonical_station_id IS NOT NULL AND source_id IS NOT NULL AND source_stop_id IS NOT NULL)
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_canonical_station_overrides_external_ref
  ON canonical_station_overrides (external_ref)
  WHERE external_ref IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_canonical_station_overrides_status_country
  ON canonical_station_overrides (status, country, override_id);

CREATE INDEX IF NOT EXISTS idx_canonical_station_overrides_source
  ON canonical_station_overrides (source_canonical_station_id)
  WHERE source_canonical_station_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_canonical_station_overrides_target
  ON canonical_station_overrides (target_canonical_station_id)
  WHERE target_canonical_station_id IS NOT NULL;

CREATE OR REPLACE FUNCTION station_rule_scope_country_default()
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT 'country_default';
$$;

CREATE TABLE IF NOT EXISTS station_transfer_rules (
  rule_id bigserial PRIMARY KEY,
  rule_scope text NOT NULL CHECK (rule_scope ~ '^(country_default|hub|station)$'),
  country char(2) NOT NULL CHECK (country IN ('DE', 'AT', 'CH')),
  canonical_station_id text,
  hub_name text,
  min_transfer_minutes integer NOT NULL CHECK (min_transfer_minutes >= 0),
  long_wait_minutes integer NOT NULL DEFAULT 45 CHECK (long_wait_minutes >= 0),
  priority integer NOT NULL DEFAULT 100 CHECK (priority >= 0),
  effective_from date,
  effective_to date,
  is_active boolean NOT NULL DEFAULT true,
  source_reference text,
  notes text,
  created_by text NOT NULL DEFAULT current_user,
  updated_by text NOT NULL DEFAULT current_user,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (effective_to IS NULL OR effective_from IS NULL OR effective_to >= effective_from),
  CHECK (
    (rule_scope = station_rule_scope_country_default() AND canonical_station_id IS NULL AND hub_name IS NULL)
    OR (rule_scope = 'hub' AND canonical_station_id IS NULL AND NULLIF(hub_name, '') IS NOT NULL)
    OR (rule_scope = 'station' AND canonical_station_id IS NOT NULL AND hub_name IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_station_transfer_rules_active_country
  ON station_transfer_rules (country, is_active, priority, rule_id);

CREATE INDEX IF NOT EXISTS idx_station_transfer_rules_station
  ON station_transfer_rules (country, canonical_station_id, is_active)
  WHERE canonical_station_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_station_transfer_rules_hub
  ON station_transfer_rules (country, hub_name, is_active)
  WHERE hub_name IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_station_transfer_rules_country_default_active
  ON station_transfer_rules (country, rule_scope)
  WHERE rule_scope = station_rule_scope_country_default() AND is_active = true AND effective_to IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_station_transfer_rules_hub_active
  ON station_transfer_rules (country, hub_name)
  WHERE rule_scope = 'hub' AND is_active = true AND effective_to IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_station_transfer_rules_station_active
  ON station_transfer_rules (country, canonical_station_id)
  WHERE rule_scope = 'station' AND is_active = true AND effective_to IS NULL;

INSERT INTO station_transfer_rules (
  rule_scope,
  country,
  min_transfer_minutes,
  long_wait_minutes,
  priority,
  source_reference,
  notes
)
SELECT
  station_rule_scope_country_default(),
  v.country,
  8,
  45,
  1000,
  'mvp_seed',
  'Seed default transfer/wait thresholds for stitching prototype.'
FROM (VALUES ('DE'::char(2)), ('AT'::char(2)), ('CH'::char(2))) AS v(country)
  WHERE NOT EXISTS (
    SELECT 1
    FROM station_transfer_rules r
    WHERE r.rule_scope = station_rule_scope_country_default()
      AND r.country = v.country
      AND r.is_active
      AND r.effective_to IS NULL
  );

CREATE TABLE IF NOT EXISTS ojp_stop_refs (
  ojp_stop_ref_id bigserial PRIMARY KEY,
  canonical_station_id text NOT NULL REFERENCES canonical_stations(canonical_station_id) ON DELETE CASCADE,
  country char(2) NOT NULL CHECK (country IN ('DE', 'AT', 'CH')),
  provider_id text NOT NULL,
  ojp_stop_ref text NOT NULL,
  ojp_stop_name text,
  is_primary boolean NOT NULL DEFAULT false,
  confidence_score numeric(5,4) CHECK (confidence_score IS NULL OR (confidence_score >= 0 AND confidence_score <= 1)),
  source text NOT NULL DEFAULT 'manual',
  source_reference text,
  valid_from date,
  valid_to date,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_by text NOT NULL DEFAULT current_user,
  updated_by text NOT NULL DEFAULT current_user,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ojp_stop_refs_provider_ref
  ON ojp_stop_refs (country, provider_id, ojp_stop_ref);

CREATE INDEX IF NOT EXISTS idx_ojp_stop_refs_canonical
  ON ojp_stop_refs (canonical_station_id, country, provider_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ojp_stop_refs_primary_per_provider
  ON ojp_stop_refs (canonical_station_id, country, provider_id)
  WHERE is_primary = true;

CREATE OR REPLACE FUNCTION refresh_canonical_station(p_station_id text)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  WITH agg AS (
    SELECT
      css.canonical_station_id,
      MIN(css.country) AS country,
      MIN(s.stop_name) FILTER (WHERE s.stop_name IS NOT NULL AND btrim(s.stop_name) <> '') AS canonical_name,
      AVG(s.latitude) FILTER (WHERE s.latitude IS NOT NULL) AS latitude,
      AVG(s.longitude) FILTER (WHERE s.longitude IS NOT NULL) AS longitude,
      CASE
        WHEN COUNT(*) FILTER (WHERE s.geom IS NOT NULL) > 0 THEN ST_SetSRID(ST_Centroid(ST_Collect(s.geom)), 4326)
        ELSE NULL
      END AS geom,
      CASE
        WHEN COUNT(*) FILTER (WHERE css.hard_id IS NOT NULL AND btrim(css.hard_id) <> '') > 0 THEN 'hard_id'
        WHEN COUNT(*) FILTER (WHERE s.geom IS NOT NULL) > 0 THEN 'name_geo'
        ELSE 'name_only'
      END AS match_method,
      COUNT(*)::integer AS member_count,
      MIN(css.snapshot_date) AS first_seen_snapshot_date,
      MAX(css.snapshot_date) AS last_seen_snapshot_date
    FROM canonical_station_sources css
    LEFT JOIN netex_stops_staging s
      ON s.source_id = css.source_id
     AND s.source_stop_id = css.source_stop_id
     AND s.snapshot_date = css.snapshot_date
    WHERE css.canonical_station_id = p_station_id
    GROUP BY css.canonical_station_id
  )
  UPDATE canonical_stations cs
  SET
    canonical_name = COALESCE(a.canonical_name, cs.canonical_name),
    normalized_name = normalize_station_name(COALESCE(a.canonical_name, cs.canonical_name)),
    country = a.country,
    latitude = a.latitude,
    longitude = a.longitude,
    geom = a.geom,
    match_method = a.match_method,
    member_count = a.member_count,
    first_seen_snapshot_date = a.first_seen_snapshot_date,
    last_seen_snapshot_date = a.last_seen_snapshot_date,
    updated_at = now()
  FROM agg a
  WHERE cs.canonical_station_id = a.canonical_station_id;

  IF (
    SELECT COUNT(*)
    FROM canonical_station_sources css
    WHERE css.canonical_station_id = p_station_id
  ) = 0 THEN
    DELETE FROM canonical_stations
    WHERE canonical_station_id = p_station_id;
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION assert_true(p_condition boolean, p_message text)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  IF NOT COALESCE(p_condition, false) THEN
    RAISE EXCEPTION '%', p_message;
  END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS pipeline_jobs (
  job_id uuid PRIMARY KEY,
  job_type text NOT NULL,
  idempotency_key text NOT NULL,
  status text NOT NULL CHECK (status IN ('queued', 'running', 'retry_wait', 'succeeded', 'failed')),
  attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
  started_at timestamptz,
  ended_at timestamptz,
  error_code text,
  error_message text,
  run_context jsonb NOT NULL DEFAULT '{}'::jsonb,
  checkpoint jsonb NOT NULL DEFAULT '{}'::jsonb,
  result_context jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (job_type, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_type_status_created
  ON pipeline_jobs (job_type, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_status_updated
  ON pipeline_jobs (status, updated_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_jobs_one_running_per_type
  ON pipeline_jobs (job_type)
  WHERE status = 'running';

DROP INDEX IF EXISTS idx_pipeline_jobs_one_running_per_type;

CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_running_by_type
  ON pipeline_jobs (job_type, started_at DESC)
  WHERE status = 'running';

-- Migration to store system state in the database instead of the filesystem.

CREATE TABLE IF NOT EXISTS system_state (
    key VARCHAR(255) PRIMARY KEY,
    value JSONB NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO system_state (key, value) 
VALUES 
  ('active_gtfs', '{"activeProfile": "sample_de", "zipPath": "data/gtfs/de_fv.zip", "sourceType": "static", "runtime": null, "activatedAt": "2026-02-19T17:31:54.964Z"}'::jsonb),
  ('gtfs_switch_status', '{"state": "ready", "activeProfile": "sample_de", "message": "Profile ''sample_de'' activated successfully", "error": null, "requestedProfile": "sample_de", "lastHealth": {"ok": true, "status": 404, "body": {"message": "Configured MOTIS health endpoint returned 404, treating service as reachable/ready", "originalBody": {}}}}'::jsonb)
ON CONFLICT (key) DO NOTHING;

-- Station curation v2: additive schema for cluster-first review, naming auditability,
-- segment-aware modeling, and service/line context scaffolding.

CREATE TABLE IF NOT EXISTS qa_cluster_scoring_config (
  scope_key text PRIMARY KEY,
  config jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION qa_jsonb_is_array(p_value jsonb)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT COALESCE(jsonb_typeof(p_value) = 'array', false);
$$;

CREATE OR REPLACE FUNCTION qa_jsonb_array_or_empty(p_value jsonb)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE WHEN qa_jsonb_is_array(p_value) THEN p_value ELSE '[]'::jsonb END;
$$;

CREATE OR REPLACE FUNCTION qa_is_iso_country_code(p_country text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT char_length(COALESCE(p_country, '')) = 2
    AND p_country = upper(p_country);
$$;

INSERT INTO qa_cluster_scoring_config (scope_key, config)
SELECT
  v.scope_key,
  seed_config.config
FROM (
  VALUES
    ('default'),
    ('DE'),
    ('AT'),
    ('CH')
) AS v(scope_key)
CROSS JOIN (
  SELECT jsonb_build_object(
    'name_similarity_weight', 0.35,
    'distance_weight', 0.25,
    'hard_id_weight', 0.20,
    'provider_overlap_weight', 0.10,
    'service_overlap_weight', 0.10,
    'distance_threshold_meters', 1500,
    'max_cluster_candidates', 40
  ) AS config
) AS seed_config
ON CONFLICT (scope_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS qa_station_naming_overrides (
  naming_override_id bigserial PRIMARY KEY,
  canonical_station_id text NOT NULL REFERENCES canonical_stations(canonical_station_id) ON DELETE CASCADE,
  locale text NOT NULL DEFAULT 'und',
  display_name text NOT NULL,
  aliases jsonb NOT NULL DEFAULT '[]'::jsonb,
  reason text,
  requested_by text NOT NULL DEFAULT current_user,
  approved_by text,
  linked_override_id bigint REFERENCES canonical_station_overrides(override_id) ON DELETE SET NULL,
  is_active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (display_name <> ''),
  CHECK (qa_jsonb_is_array(aliases))
);

CREATE INDEX IF NOT EXISTS idx_qa_station_naming_overrides_station
  ON qa_station_naming_overrides (canonical_station_id, is_active, created_at DESC);

CREATE TABLE IF NOT EXISTS qa_station_display_names (
  canonical_station_id text PRIMARY KEY REFERENCES canonical_stations(canonical_station_id) ON DELETE CASCADE,
  display_name text NOT NULL,
  locale text NOT NULL DEFAULT 'und',
  naming_strategy text NOT NULL,
  naming_reason text,
  source_refs jsonb NOT NULL DEFAULT '{}'::jsonb,
  aliases jsonb NOT NULL DEFAULT '[]'::jsonb,
  evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
  computed_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (display_name <> ''),
  CHECK (qa_jsonb_is_array(aliases))
);

CREATE INDEX IF NOT EXISTS idx_qa_station_display_names_lookup
  ON qa_station_display_names (locale, display_name);

CREATE TABLE IF NOT EXISTS qa_station_complexes (
  complex_id text PRIMARY KEY,
  country char(2) NOT NULL CHECK (qa_is_iso_country_code(country)),
  complex_name text NOT NULL,
  display_name text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_by text NOT NULL DEFAULT current_user,
  updated_by text NOT NULL DEFAULT current_user,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_qa_station_complexes_country
  ON qa_station_complexes (country, complex_name);

CREATE TABLE IF NOT EXISTS qa_station_segments (
  segment_id text PRIMARY KEY,
  complex_id text NOT NULL REFERENCES qa_station_complexes(complex_id) ON DELETE CASCADE,
  canonical_station_id text REFERENCES canonical_stations(canonical_station_id) ON DELETE SET NULL,
  segment_name text NOT NULL,
  segment_type text NOT NULL DEFAULT 'platform_group',
  latitude double precision,
  longitude double precision,
  geom geometry(Point, 4326),
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_by text NOT NULL DEFAULT current_user,
  updated_by text NOT NULL DEFAULT current_user,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_qa_station_segments_complex
  ON qa_station_segments (complex_id, segment_type);

CREATE INDEX IF NOT EXISTS idx_qa_station_segments_canonical
  ON qa_station_segments (canonical_station_id)
  WHERE canonical_station_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_qa_station_segments_geom
  ON qa_station_segments USING gist (geom)
  WHERE geom IS NOT NULL;

CREATE TABLE IF NOT EXISTS qa_station_segment_links (
  segment_link_id bigserial PRIMARY KEY,
  from_segment_id text NOT NULL REFERENCES qa_station_segments(segment_id) ON DELETE CASCADE,
  to_segment_id text NOT NULL REFERENCES qa_station_segments(segment_id) ON DELETE CASCADE,
  min_walk_minutes integer NOT NULL DEFAULT 0 CHECK (min_walk_minutes >= 0),
  transfer_rule_ref bigint REFERENCES station_transfer_rules(rule_id) ON DELETE SET NULL,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_by text NOT NULL DEFAULT current_user,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (from_segment_id <> to_segment_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_qa_station_segment_links_unique
  ON qa_station_segment_links (from_segment_id, to_segment_id);

CREATE TABLE IF NOT EXISTS canonical_line_identities (
  line_identity_id text PRIMARY KEY,
  country char(2) NOT NULL CHECK (qa_is_iso_country_code(country)),
  provider_id text,
  line_code text,
  line_name text,
  transport_mode text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_canonical_line_identities_provider_code
  ON canonical_line_identities (country, COALESCE(provider_id, ''), COALESCE(line_code, ''), COALESCE(line_name, ''));

CREATE TABLE IF NOT EXISTS station_segment_line_links (
  segment_id text NOT NULL REFERENCES qa_station_segments(segment_id) ON DELETE CASCADE,
  line_identity_id text NOT NULL REFERENCES canonical_line_identities(line_identity_id) ON DELETE CASCADE,
  direction text,
  service_context jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_station_segment_line_links_unique
  ON station_segment_line_links (segment_id, line_identity_id, COALESCE(direction, ''));

CREATE TABLE IF NOT EXISTS qa_station_clusters (
  cluster_id text PRIMARY KEY,
  cluster_key text NOT NULL UNIQUE,
  country char(2) NOT NULL CHECK (qa_is_iso_country_code(country)),
  scope_tag text NOT NULL,
  scope_as_of date,
  severity text NOT NULL CHECK (severity ~ '^(low|medium|high)$'),
  status text NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'in_review', 'resolved', 'dismissed')),
  candidate_count integer NOT NULL DEFAULT 0 CHECK (candidate_count >= 0),
  issue_count integer NOT NULL DEFAULT 0 CHECK (issue_count >= 0),
  display_name text,
  display_name_reason jsonb NOT NULL DEFAULT '{}'::jsonb,
  summary jsonb NOT NULL DEFAULT '{}'::jsonb,
  resolved_at timestamptz,
  resolved_by text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_qa_station_clusters_scope
  ON qa_station_clusters (scope_tag, country, status, severity, updated_at DESC);

CREATE TABLE IF NOT EXISTS qa_station_cluster_candidates (
  cluster_id text NOT NULL REFERENCES qa_station_clusters(cluster_id) ON DELETE CASCADE,
  canonical_station_id text NOT NULL REFERENCES canonical_stations(canonical_station_id) ON DELETE CASCADE,
  candidate_rank integer NOT NULL DEFAULT 1 CHECK (candidate_rank > 0),
  display_name text NOT NULL,
  naming jsonb NOT NULL DEFAULT '{}'::jsonb,
  aliases jsonb NOT NULL DEFAULT '[]'::jsonb,
  language_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
  latitude double precision,
  longitude double precision,
  provider_labels jsonb NOT NULL DEFAULT '[]'::jsonb,
  source_member_count integer NOT NULL DEFAULT 0 CHECK (source_member_count >= 0),
  service_context jsonb NOT NULL DEFAULT '{}'::jsonb,
  segment_context jsonb NOT NULL DEFAULT '{}'::jsonb,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (cluster_id, canonical_station_id),
  CHECK (qa_jsonb_is_array(aliases))
);

CREATE INDEX IF NOT EXISTS idx_qa_station_cluster_candidates_rank
  ON qa_station_cluster_candidates (cluster_id, candidate_rank);

CREATE TABLE IF NOT EXISTS qa_station_cluster_evidence (
  evidence_id bigserial PRIMARY KEY,
  cluster_id text NOT NULL REFERENCES qa_station_clusters(cluster_id) ON DELETE CASCADE,
  source_canonical_station_id text NOT NULL REFERENCES canonical_stations(canonical_station_id) ON DELETE CASCADE,
  target_canonical_station_id text NOT NULL REFERENCES canonical_stations(canonical_station_id) ON DELETE CASCADE,
  evidence_type text NOT NULL CHECK (
    evidence_type IN (
      'name_similarity',
      'distance_proximity',
      'hard_id_overlap',
      'provider_overlap',
      'service_overlap',
      'segment_relation'
    )
  ),
  score numeric(8,4),
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (source_canonical_station_id <> target_canonical_station_id)
);

CREATE INDEX IF NOT EXISTS idx_qa_station_cluster_evidence_cluster
  ON qa_station_cluster_evidence (cluster_id, evidence_type);

CREATE TABLE IF NOT EXISTS qa_station_cluster_queue_items (
  cluster_id text NOT NULL REFERENCES qa_station_clusters(cluster_id) ON DELETE CASCADE,
  review_item_id bigint NOT NULL REFERENCES canonical_review_queue(review_item_id) ON DELETE CASCADE,
  linked_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (cluster_id, review_item_id)
);

CREATE INDEX IF NOT EXISTS idx_qa_station_cluster_queue_items_item
  ON qa_station_cluster_queue_items (review_item_id, cluster_id);

CREATE TABLE IF NOT EXISTS qa_station_cluster_decisions (
  decision_id bigserial PRIMARY KEY,
  cluster_id text NOT NULL REFERENCES qa_station_clusters(cluster_id) ON DELETE CASCADE,
  operation text NOT NULL CHECK (operation IN ('merge', 'keep_separate', 'split', 'rename')),
  decision_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  line_decision_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  note text,
  requested_by text NOT NULL DEFAULT current_user,
  applied_to_overrides boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_qa_station_cluster_decisions_cluster
  ON qa_station_cluster_decisions (cluster_id, created_at DESC);

CREATE TABLE IF NOT EXISTS qa_station_cluster_decision_members (
  decision_id bigint NOT NULL REFERENCES qa_station_cluster_decisions(decision_id) ON DELETE CASCADE,
  canonical_station_id text NOT NULL REFERENCES canonical_stations(canonical_station_id) ON DELETE CASCADE,
  group_label text NOT NULL DEFAULT '',
  action text NOT NULL DEFAULT 'candidate' CHECK (action IN ('candidate', 'merge_member', 'separate', 'segment_assign', 'line_assign')),
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (decision_id, canonical_station_id, group_label, action)
);

CREATE OR REPLACE FUNCTION qa_effective_scoring_config(p_country text)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
  SELECT COALESCE(
    (SELECT config FROM qa_cluster_scoring_config WHERE scope_key = COALESCE(NULLIF(upper(p_country), ''), '') LIMIT 1),
    (SELECT config FROM qa_cluster_scoring_config WHERE scope_key = 'default' LIMIT 1),
    '{}'::jsonb
  );
$$;

CREATE OR REPLACE FUNCTION qa_refresh_station_display_names(p_country text DEFAULT NULL)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_count integer := 0;
  v_default_naming_strategy constant text := 'canonical_name';
  v_source_ref_canonical_name_key constant text := v_default_naming_strategy;
  v_source_ref_country_key constant text := 'country';
BEGIN
  INSERT INTO qa_station_display_names (
    canonical_station_id,
    display_name,
    locale,
    naming_strategy,
    naming_reason,
    source_refs,
    aliases,
    evidence,
    computed_at,
    updated_at
  )
  SELECT
    cs.canonical_station_id,
    COALESCE(NULLIF(ov.display_name, ''), cs.canonical_name) AS display_name,
    COALESCE(NULLIF(ov.locale, ''), 'und') AS locale,
    CASE
      WHEN ov.naming_override_id IS NOT NULL THEN 'manual_override'
      ELSE v_default_naming_strategy
    END AS naming_strategy,
    COALESCE(NULLIF(ov.reason, ''), 'Derived from canonical station aggregate') AS naming_reason,
    jsonb_build_object(
      v_source_ref_canonical_name_key, cs.canonical_name,
      v_source_ref_country_key, cs.country,
      'override_id', ov.naming_override_id
    ) AS source_refs,
    COALESCE(
      ov.aliases,
      (
        SELECT COALESCE(to_jsonb(array_agg(n.stop_name ORDER BY n.stop_name)), '[]'::jsonb)
        FROM (
          SELECT DISTINCT s.stop_name
          FROM canonical_station_sources css
          JOIN netex_stops_staging s
            ON s.source_id = css.source_id
           AND s.source_stop_id = css.source_stop_id
           AND s.snapshot_date = css.snapshot_date
          WHERE css.canonical_station_id = cs.canonical_station_id
            AND s.stop_name IS NOT NULL
            AND btrim(s.stop_name) <> ''
            AND s.stop_name IS DISTINCT FROM cs.canonical_name
        ) AS n
      ),
      '[]'::jsonb
    ) AS aliases,
    jsonb_build_object(
      'member_count', cs.member_count,
      'match_method', cs.match_method,
      'sources', (
        SELECT COUNT(*)
        FROM canonical_station_sources css
        WHERE css.canonical_station_id = cs.canonical_station_id
      )
    ) AS evidence,
    now(),
    now()
  FROM canonical_stations cs
  LEFT JOIN LATERAL (
    SELECT o.*
    FROM qa_station_naming_overrides o
    WHERE o.canonical_station_id = cs.canonical_station_id
      AND o.is_active = true
    ORDER BY o.created_at DESC, o.naming_override_id DESC
    LIMIT 1
  ) ov ON true
  WHERE (NULLIF(upper(COALESCE(p_country, '')), '') IS NULL OR cs.country = NULLIF(upper(COALESCE(p_country, '')), '')::char(2))
  ON CONFLICT (canonical_station_id)
  DO UPDATE SET
    display_name = EXCLUDED.display_name,
    locale = EXCLUDED.locale,
    naming_strategy = EXCLUDED.naming_strategy,
    naming_reason = EXCLUDED.naming_reason,
    source_refs = EXCLUDED.source_refs,
    aliases = EXCLUDED.aliases,
    evidence = EXCLUDED.evidence,
    computed_at = now(),
    updated_at = now();

  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END;
$$;

CREATE OR REPLACE FUNCTION qa_rebuild_station_clusters(
  p_country text DEFAULT NULL,
  p_as_of date DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  v_scope_tag text := COALESCE(to_char(p_as_of, 'YYYY-MM-DD'), 'latest');
  v_country text := upper(COALESCE(p_country, ''));
  v_detected integer := 0;
  v_country_key constant text := 'country';
  v_lines_key constant text := 'lines';
  v_default_naming_strategy constant text := 'canonical_name';
  v_source_ref_canonical_name_key constant text := v_default_naming_strategy;
  v_max_cluster_candidates_key constant text := 'max_cluster_candidates';
  v_distance_threshold_meters_key constant text := 'distance_threshold_meters';
BEGIN
  PERFORM qa_refresh_station_display_names(NULLIF(v_country, ''));

  DELETE FROM qa_station_clusters c
  WHERE c.scope_tag = v_scope_tag
    AND (NULLIF(v_country, '') IS NULL OR c.country = NULLIF(v_country, '')::char(2));

  CREATE TEMP TABLE _issue_scope_v2 ON COMMIT DROP AS
  SELECT
    q.review_item_id,
    q.issue_key,
    q.country,
    q.severity,
    q.details,
    q.canonical_station_id
  FROM canonical_review_queue q
  WHERE q.status IN ('open', 'confirmed')
    AND q.provenance_run_tag = v_scope_tag
    AND (NULLIF(v_country, '') IS NULL OR q.country = NULLIF(v_country, '')::char(2));

  IF (SELECT COUNT(*) FROM _issue_scope_v2) = 0 THEN
    RETURN jsonb_build_object(
      'scopeTag', v_scope_tag,
      v_country_key, NULLIF(v_country, ''),
      'clusters', 0,
      'candidates', 0,
      'issues', 0
    );
  END IF;

  CREATE TEMP TABLE _issue_station_map_v2 ON COMMIT DROP AS
  SELECT DISTINCT i.review_item_id, i.issue_key, i.country, i.severity, i.canonical_station_id
  FROM (
    SELECT
      q.review_item_id,
      q.issue_key,
      q.country,
      q.severity,
      NULLIF(q.canonical_station_id, '') AS canonical_station_id
    FROM _issue_scope_v2 q

    UNION ALL

    SELECT
      q.review_item_id,
      q.issue_key,
      q.country,
      q.severity,
      NULLIF(js.value, '') AS canonical_station_id
    FROM _issue_scope_v2 q
    CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(q.details -> 'canonicalStationIds', '[]'::jsonb)) AS js(value)

    UNION ALL

    SELECT
      q.review_item_id,
      q.issue_key,
      q.country,
      q.severity,
      css.canonical_station_id
    FROM _issue_scope_v2 q
    JOIN canonical_station_sources css
      ON css.country = q.country
     AND css.hard_id = NULLIF(q.details ->> 'hardId', '')
    WHERE NULLIF(q.details ->> 'hardId', '') IS NOT NULL

    UNION ALL

    SELECT
      q.review_item_id,
      q.issue_key,
      q.country,
      q.severity,
      cs.canonical_station_id
    FROM _issue_scope_v2 q
    JOIN canonical_stations cs
      ON cs.country = q.country
     AND cs.normalized_name = NULLIF(q.details ->> 'normalizedName', '')
    WHERE NULLIF(q.details ->> 'normalizedName', '') IS NOT NULL
  ) i
  WHERE i.canonical_station_id IS NOT NULL;

  CREATE TEMP TABLE _issue_station_sets_v2 ON COMMIT DROP AS
  SELECT
    m.review_item_id,
    m.issue_key,
    m.country,
    CASE m.severity WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END AS severity_rank,
    ARRAY(
      SELECT DISTINCT x
      FROM unnest(array_agg(m.canonical_station_id)) AS x
      ORDER BY x
    ) AS station_ids
  FROM _issue_station_map_v2 m
  GROUP BY m.review_item_id, m.issue_key, m.country, m.severity;

  CREATE TEMP TABLE _issue_station_sets_expanded_v2 ON COMMIT DROP AS
  SELECT
    s.review_item_id,
    s.issue_key,
    s.country,
    s.severity_rank,
    ARRAY(
      SELECT canonical_station_id
      FROM (
        SELECT DISTINCT x.canonical_station_id
        FROM (
          SELECT unnest(s.station_ids) AS canonical_station_id

          UNION

          SELECT cs2.canonical_station_id
          FROM canonical_stations cs2
          WHERE cs2.country = s.country
            AND EXISTS (
              SELECT 1
              FROM unnest(s.station_ids) seed_id
              JOIN canonical_stations seed
                ON seed.canonical_station_id = seed_id
               AND seed.country = s.country
              WHERE seed.normalized_name = cs2.normalized_name
            )

          UNION

          SELECT css2.canonical_station_id
          FROM canonical_station_sources css2
          WHERE css2.country = s.country
            AND css2.hard_id IS NOT NULL
            AND btrim(css2.hard_id) <> ''
            AND EXISTS (
              SELECT 1
              FROM unnest(s.station_ids) seed_id
              JOIN canonical_station_sources css1
                ON css1.canonical_station_id = seed_id
               AND css1.country = s.country
              WHERE css1.hard_id = css2.hard_id
                AND css1.hard_id IS NOT NULL
                AND btrim(css1.hard_id) <> ''
            )
        ) x
      ) expanded
      ORDER BY canonical_station_id
      LIMIT COALESCE((qa_effective_scoring_config(s.country) ->> v_max_cluster_candidates_key)::integer, 40)
    ) AS station_ids
  FROM _issue_station_sets_v2 s;

  CREATE TEMP TABLE _cluster_base_v2 ON COMMIT DROP AS
  SELECT
    'qacl_' || substr(md5(e.country || '|' || v_scope_tag || '|' || array_to_string(e.station_ids, ',')), 1, 24) AS cluster_id,
    md5(e.country || '|' || v_scope_tag || '|' || array_to_string(e.station_ids, ',')) AS cluster_key,
    e.country,
    e.station_ids,
    array_agg(DISTINCT e.review_item_id ORDER BY e.review_item_id) AS review_item_ids,
    array_agg(DISTINCT e.issue_key ORDER BY e.issue_key) AS issue_keys,
    MAX(e.severity_rank) AS severity_rank,
    COUNT(*)::integer AS issue_count
  FROM _issue_station_sets_expanded_v2 e
  WHERE cardinality(e.station_ids) > 0
  GROUP BY e.country, e.station_ids;

  INSERT INTO qa_station_clusters (
    cluster_id,
    cluster_key,
    country,
    scope_tag,
    scope_as_of,
    severity,
    status,
    candidate_count,
    issue_count,
    display_name,
    display_name_reason,
    summary,
    created_at,
    updated_at
  )
  SELECT
    b.cluster_id,
    b.cluster_key,
    b.country,
    v_scope_tag,
    p_as_of,
    CASE b.severity_rank WHEN 3 THEN 'high' WHEN 2 THEN 'medium' ELSE 'low' END,
    'open',
    cardinality(b.station_ids),
    b.issue_count,
    (
      SELECT COALESCE(dn.display_name, cs.canonical_name)
      FROM unnest(b.station_ids) sid
      LEFT JOIN qa_station_display_names dn
        ON dn.canonical_station_id = sid
      LEFT JOIN canonical_stations cs
        ON cs.canonical_station_id = sid
      ORDER BY COALESCE(dn.display_name, cs.canonical_name), sid
      LIMIT 1
    ),
    jsonb_build_object(
      'strategy', 'highest_ranked_candidate',
      'issue_count', b.issue_count,
      'scope_tag', v_scope_tag
    ),
    jsonb_build_object(
      'review_item_ids', b.review_item_ids,
      'issue_keys', b.issue_keys,
      'scoring_config', qa_effective_scoring_config(b.country)
    ),
    now(),
    now()
  FROM _cluster_base_v2 b
  ON CONFLICT (cluster_id)
  DO UPDATE SET
    cluster_key = EXCLUDED.cluster_key,
    country = EXCLUDED.country,
    scope_tag = EXCLUDED.scope_tag,
    scope_as_of = EXCLUDED.scope_as_of,
    severity = EXCLUDED.severity,
    status = CASE
      WHEN qa_station_clusters.status IN ('resolved', 'dismissed') THEN qa_station_clusters.status
      ELSE EXCLUDED.status
    END,
    candidate_count = EXCLUDED.candidate_count,
    issue_count = EXCLUDED.issue_count,
    display_name = EXCLUDED.display_name,
    display_name_reason = EXCLUDED.display_name_reason,
    summary = EXCLUDED.summary,
    updated_at = now();

  INSERT INTO qa_station_cluster_queue_items (cluster_id, review_item_id, linked_at)
  SELECT b.cluster_id, rid.review_item_id, now()
  FROM _cluster_base_v2 b
  CROSS JOIN LATERAL unnest(b.review_item_ids) AS rid(review_item_id)
  ON CONFLICT (cluster_id, review_item_id) DO NOTHING;

  CREATE TEMP TABLE _cluster_candidate_seed_v2 ON COMMIT DROP AS
  SELECT
    b.cluster_id,
    b.country,
    sid.canonical_station_id
  FROM _cluster_base_v2 b
  CROSS JOIN LATERAL unnest(b.station_ids) AS sid(canonical_station_id);

  INSERT INTO qa_station_complexes (
    complex_id,
    country,
    complex_name,
    display_name,
    metadata,
    created_at,
    updated_at
  )
  SELECT DISTINCT
    'cplx_' || substr(md5(seed.country || '|' || seed.canonical_station_id), 1, 20) AS complex_id,
    seed.country,
    COALESCE(dn.display_name, cs.canonical_name) || ' Complex' AS complex_name,
    COALESCE(dn.display_name, cs.canonical_name) AS display_name,
    jsonb_build_object('source', 'qa_rebuild_station_clusters', 'canonical_station_id', seed.canonical_station_id),
    now(),
    now()
  FROM _cluster_candidate_seed_v2 seed
  JOIN canonical_stations cs
    ON cs.canonical_station_id = seed.canonical_station_id
  LEFT JOIN qa_station_display_names dn
    ON dn.canonical_station_id = seed.canonical_station_id
  ON CONFLICT (complex_id)
  DO UPDATE SET
    complex_name = EXCLUDED.complex_name,
    display_name = EXCLUDED.display_name,
    metadata = EXCLUDED.metadata,
    updated_at = now();

  INSERT INTO qa_station_segments (
    segment_id,
    complex_id,
    canonical_station_id,
    segment_name,
    segment_type,
    latitude,
    longitude,
    geom,
    metadata,
    created_at,
    updated_at
  )
  SELECT DISTINCT
    'seg_' || substr(md5('segment|' || seed.canonical_station_id), 1, 20) AS segment_id,
    'cplx_' || substr(md5(seed.country || '|' || seed.canonical_station_id), 1, 20) AS complex_id,
    seed.canonical_station_id,
    COALESCE(dn.display_name, cs.canonical_name) || ' Main Segment' AS segment_name,
    'station_segment',
    cs.latitude,
    cs.longitude,
    cs.geom,
    jsonb_build_object('source', 'qa_rebuild_station_clusters', 'kind', 'default_segment'),
    now(),
    now()
  FROM _cluster_candidate_seed_v2 seed
  JOIN canonical_stations cs
    ON cs.canonical_station_id = seed.canonical_station_id
  LEFT JOIN qa_station_display_names dn
    ON dn.canonical_station_id = seed.canonical_station_id
  ON CONFLICT (segment_id)
  DO UPDATE SET
    complex_id = EXCLUDED.complex_id,
    canonical_station_id = EXCLUDED.canonical_station_id,
    segment_name = EXCLUDED.segment_name,
    segment_type = EXCLUDED.segment_type,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    geom = EXCLUDED.geom,
    metadata = EXCLUDED.metadata,
    updated_at = now();

  DELETE FROM qa_station_cluster_candidates c
  USING qa_station_clusters cl
  WHERE c.cluster_id = cl.cluster_id
    AND cl.scope_tag = v_scope_tag
    AND (NULLIF(v_country, '') IS NULL OR cl.country = NULLIF(v_country, '')::char(2));

  INSERT INTO qa_station_cluster_candidates (
    cluster_id,
    canonical_station_id,
    candidate_rank,
    display_name,
    naming,
    aliases,
    language_codes,
    latitude,
    longitude,
    provider_labels,
    source_member_count,
    service_context,
    segment_context,
    metadata,
    created_at,
    updated_at
  )
  SELECT
    seed.cluster_id,
    seed.canonical_station_id,
    ROW_NUMBER() OVER (
      PARTITION BY seed.cluster_id
      ORDER BY COALESCE(dn.display_name, cs.canonical_name), seed.canonical_station_id
    ) AS candidate_rank,
    COALESCE(dn.display_name, cs.canonical_name) AS display_name,
    jsonb_build_object(
      'locale', COALESCE(dn.locale, 'und'),
      'strategy', COALESCE(dn.naming_strategy, v_default_naming_strategy),
      'reason', COALESCE(dn.naming_reason, 'Derived from canonical station aggregate'),
      'source_refs', COALESCE(dn.source_refs, '{}'::jsonb)
    ) AS naming,
    COALESCE(dn.aliases, '[]'::jsonb) AS aliases,
    ARRAY(
      SELECT DISTINCT lang
      FROM (
        SELECT NULLIF(s.raw_payload ->> 'language', '') AS lang
        FROM canonical_station_sources css
        LEFT JOIN netex_stops_staging s
          ON s.source_id = css.source_id
         AND s.source_stop_id = css.source_stop_id
         AND s.snapshot_date = css.snapshot_date
        WHERE css.canonical_station_id = seed.canonical_station_id
      ) l
      WHERE lang IS NOT NULL
      ORDER BY lang
    ) AS language_codes,
    cs.latitude,
    cs.longitude,
    (
      SELECT COALESCE(jsonb_agg(p.provider ORDER BY p.provider), '[]'::jsonb)
      FROM (
        SELECT DISTINCT css.source_id AS provider
        FROM canonical_station_sources css
        WHERE css.canonical_station_id = seed.canonical_station_id
      ) p
    ) AS provider_labels,
    (
      SELECT COUNT(*)
      FROM canonical_station_sources css
      WHERE css.canonical_station_id = seed.canonical_station_id
    )::integer AS source_member_count,
    (
      WITH src AS (
        SELECT s.raw_payload
        FROM canonical_station_sources css
        LEFT JOIN netex_stops_staging s
          ON s.source_id = css.source_id
         AND s.source_stop_id = css.source_stop_id
         AND s.snapshot_date = css.snapshot_date
        WHERE css.canonical_station_id = seed.canonical_station_id
      ), lines AS (
        SELECT DISTINCT NULLIF(v.value, '') AS line
        FROM (
          SELECT raw_payload ->> 'line' AS value FROM src
          UNION ALL
          SELECT raw_payload ->> 'route' AS value FROM src
          UNION ALL
          SELECT raw_payload ->> 'service' AS value FROM src
          UNION ALL
          SELECT raw_payload ->> 'trip' AS value FROM src
          UNION ALL
          SELECT jsonb_array_elements_text(qa_jsonb_array_or_empty(raw_payload -> v_lines_key)) FROM src
          UNION ALL
          SELECT jsonb_array_elements_text(qa_jsonb_array_or_empty(raw_payload -> 'routes')) FROM src
          UNION ALL
          SELECT jsonb_array_elements_text(qa_jsonb_array_or_empty(raw_payload -> 'services')) FROM src
          UNION ALL
          SELECT jsonb_array_elements_text(qa_jsonb_array_or_empty(raw_payload -> 'trips')) FROM src
        ) v
        WHERE NULLIF(v.value, '') IS NOT NULL
      )
      SELECT jsonb_build_object(
        v_lines_key, COALESCE((SELECT jsonb_agg(l.line ORDER BY l.line) FROM lines l), '[]'::jsonb),
        'incoming', '[]'::jsonb,
        'outgoing', '[]'::jsonb,
        'completeness', jsonb_build_object(
          'status', CASE
            WHEN (SELECT COUNT(*) FROM src) = 0 THEN 'none'
            WHEN (SELECT COUNT(*) FROM lines) > 0 THEN 'partial'
            ELSE 'incomplete'
          END,
          'sampled_rows', (SELECT COUNT(*) FROM src),
          'notes', CASE
            WHEN (SELECT COUNT(*) FROM src) = 0 THEN 'No source payload rows available for this candidate.'
            WHEN (SELECT COUNT(*) FROM lines) > 0 THEN 'Line/service context extracted from available source payload keys; directionality may be incomplete.'
            ELSE 'Source rows exist but did not expose line/service keys in the expected fields.'
          END
        )
      )
    ) AS service_context,
    (
      SELECT jsonb_build_object(
        'complex_id', seg.complex_id,
        'complex_name', cplx.display_name,
        'segment_id', seg.segment_id,
        'segment_name', seg.segment_name,
        'segment_type', seg.segment_type,
        'walk_links', COALESCE((
          SELECT jsonb_agg(jsonb_build_object(
            'to_segment_id', l.to_segment_id,
            'min_walk_minutes', l.min_walk_minutes
          ) ORDER BY l.to_segment_id)
          FROM qa_station_segment_links l
          WHERE l.from_segment_id = seg.segment_id
        ), '[]'::jsonb)
      )
      FROM qa_station_segments seg
      JOIN qa_station_complexes cplx
        ON cplx.complex_id = seg.complex_id
      WHERE seg.canonical_station_id = seed.canonical_station_id
      ORDER BY seg.segment_id
      LIMIT 1
    ) AS segment_context,
    jsonb_build_object(
      v_source_ref_canonical_name_key, cs.canonical_name,
      'match_method', cs.match_method,
      'member_count', cs.member_count
    ) AS metadata,
    now(),
    now()
  FROM _cluster_candidate_seed_v2 seed
  JOIN canonical_stations cs
    ON cs.canonical_station_id = seed.canonical_station_id
  LEFT JOIN qa_station_display_names dn
    ON dn.canonical_station_id = seed.canonical_station_id;

  DELETE FROM qa_station_cluster_evidence e
  USING qa_station_clusters c
  WHERE e.cluster_id = c.cluster_id
    AND c.scope_tag = v_scope_tag
    AND (NULLIF(v_country, '') IS NULL OR c.country = NULLIF(v_country, '')::char(2));

  INSERT INTO qa_station_cluster_evidence (
    cluster_id,
    source_canonical_station_id,
    target_canonical_station_id,
    evidence_type,
    score,
    details,
    created_at
  )
  SELECT
    a.cluster_id,
    a.canonical_station_id,
    b.canonical_station_id,
    'name_similarity',
    CASE
      WHEN cs1.normalized_name = cs2.normalized_name THEN 1.0
      WHEN cs1.normalized_name LIKE cs2.normalized_name || '%' OR cs2.normalized_name LIKE cs1.normalized_name || '%' THEN 0.75
      ELSE 0.40
    END,
    jsonb_build_object(
      'left', a.display_name,
      'right', b.display_name,
      'normalized_left', cs1.normalized_name,
      'normalized_right', cs2.normalized_name
    ),
    now()
  FROM qa_station_cluster_candidates a
  JOIN qa_station_cluster_candidates b
    ON b.cluster_id = a.cluster_id
   AND b.canonical_station_id > a.canonical_station_id
  JOIN qa_station_clusters cl
    ON cl.cluster_id = a.cluster_id
  JOIN canonical_stations cs1
    ON cs1.canonical_station_id = a.canonical_station_id
  JOIN canonical_stations cs2
    ON cs2.canonical_station_id = b.canonical_station_id
  WHERE cl.scope_tag = v_scope_tag
    AND (NULLIF(v_country, '') IS NULL OR cl.country = NULLIF(v_country, '')::char(2));

  INSERT INTO qa_station_cluster_evidence (
    cluster_id,
    source_canonical_station_id,
    target_canonical_station_id,
    evidence_type,
    score,
    details,
    created_at
  )
  SELECT
    d.cluster_id,
    d.source_station_id,
    d.target_station_id,
    'distance_proximity',
    CASE
      WHEN d.dist_m IS NULL THEN NULL
      ELSE GREATEST(0::numeric, LEAST(1::numeric, 1 - (d.dist_m / GREATEST(1, COALESCE((qa_effective_scoring_config(cl.country) ->> v_distance_threshold_meters_key)::numeric, 1500)))))
    END,
    jsonb_build_object(
      'distance_meters', d.dist_m,
      'threshold_meters', COALESCE((qa_effective_scoring_config(cl.country) ->> v_distance_threshold_meters_key)::integer, 1500)
    ),
    now()
  FROM (
    SELECT
      a.cluster_id,
      a.canonical_station_id AS source_station_id,
      b.canonical_station_id AS target_station_id,
      CASE
        WHEN a.latitude IS NOT NULL AND a.longitude IS NOT NULL AND b.latitude IS NOT NULL AND b.longitude IS NOT NULL THEN
          ST_DistanceSphere(
            ST_SetSRID(ST_MakePoint(a.longitude, a.latitude), 4326),
            ST_SetSRID(ST_MakePoint(b.longitude, b.latitude), 4326)
          )
        ELSE NULL
      END AS dist_m
    FROM qa_station_cluster_candidates a
    JOIN qa_station_cluster_candidates b
      ON b.cluster_id = a.cluster_id
     AND b.canonical_station_id > a.canonical_station_id
  ) d
  JOIN qa_station_clusters cl
    ON cl.cluster_id = d.cluster_id
  WHERE cl.scope_tag = v_scope_tag
    AND (NULLIF(v_country, '') IS NULL OR cl.country = NULLIF(v_country, '')::char(2));

  INSERT INTO qa_station_cluster_evidence (
    cluster_id,
    source_canonical_station_id,
    target_canonical_station_id,
    evidence_type,
    score,
    details,
    created_at
  )
  SELECT
    p.cluster_id,
    p.source_station_id,
    p.target_station_id,
    'hard_id_overlap',
    1.0,
    jsonb_build_object('shared_hard_ids', p.shared_hard_ids),
    now()
  FROM (
    SELECT
      a.cluster_id,
      a.canonical_station_id AS source_station_id,
      b.canonical_station_id AS target_station_id,
      ARRAY(
        SELECT DISTINCT css1.hard_id
        FROM canonical_station_sources css1
        JOIN canonical_station_sources css2
          ON css2.hard_id = css1.hard_id
         AND css2.country = css1.country
        WHERE css1.canonical_station_id = a.canonical_station_id
          AND css2.canonical_station_id = b.canonical_station_id
          AND css1.hard_id IS NOT NULL
          AND btrim(css1.hard_id) <> ''
        ORDER BY css1.hard_id
      ) AS shared_hard_ids
    FROM qa_station_cluster_candidates a
    JOIN qa_station_cluster_candidates b
      ON b.cluster_id = a.cluster_id
     AND b.canonical_station_id > a.canonical_station_id
  ) p
  JOIN qa_station_clusters cl
    ON cl.cluster_id = p.cluster_id
  WHERE cl.scope_tag = v_scope_tag
    AND cardinality(p.shared_hard_ids) > 0
    AND (NULLIF(v_country, '') IS NULL OR cl.country = NULLIF(v_country, '')::char(2));

  INSERT INTO qa_station_cluster_evidence (
    cluster_id,
    source_canonical_station_id,
    target_canonical_station_id,
    evidence_type,
    score,
    details,
    created_at
  )
  SELECT
    p.cluster_id,
    p.source_station_id,
    p.target_station_id,
    'provider_overlap',
    CASE WHEN p.provider_overlap_count > 0 THEN LEAST(1::numeric, p.provider_overlap_count::numeric / 3::numeric) ELSE 0::numeric END,
    jsonb_build_object('shared_providers', p.shared_providers),
    now()
  FROM (
    SELECT
      a.cluster_id,
      a.canonical_station_id AS source_station_id,
      b.canonical_station_id AS target_station_id,
      ARRAY(
        SELECT DISTINCT css1.source_id
        FROM canonical_station_sources css1
        JOIN canonical_station_sources css2
          ON css2.source_id = css1.source_id
        WHERE css1.canonical_station_id = a.canonical_station_id
          AND css2.canonical_station_id = b.canonical_station_id
        ORDER BY css1.source_id
      ) AS shared_providers,
      (
        SELECT COUNT(*)
        FROM (
          SELECT DISTINCT css1.source_id
          FROM canonical_station_sources css1
          JOIN canonical_station_sources css2
            ON css2.source_id = css1.source_id
          WHERE css1.canonical_station_id = a.canonical_station_id
            AND css2.canonical_station_id = b.canonical_station_id
        ) shared
      ) AS provider_overlap_count
    FROM qa_station_cluster_candidates a
    JOIN qa_station_cluster_candidates b
      ON b.cluster_id = a.cluster_id
     AND b.canonical_station_id > a.canonical_station_id
  ) p
  JOIN qa_station_clusters cl
    ON cl.cluster_id = p.cluster_id
  WHERE cl.scope_tag = v_scope_tag
    AND (NULLIF(v_country, '') IS NULL OR cl.country = NULLIF(v_country, '')::char(2));

  INSERT INTO qa_station_cluster_evidence (
    cluster_id,
    source_canonical_station_id,
    target_canonical_station_id,
    evidence_type,
    score,
    details,
    created_at
  )
  SELECT
    p.cluster_id,
    p.source_station_id,
    p.target_station_id,
    'service_overlap',
    CASE WHEN p.overlap_count > 0 THEN LEAST(1::numeric, p.overlap_count::numeric / 5::numeric) ELSE 0::numeric END,
    jsonb_build_object('shared_lines', p.shared_lines),
    now()
  FROM (
    SELECT
      a.cluster_id,
      a.canonical_station_id AS source_station_id,
      b.canonical_station_id AS target_station_id,
      ARRAY(
        SELECT DISTINCT l1.value
        FROM jsonb_array_elements_text(qa_jsonb_array_or_empty(a.service_context -> v_lines_key)) l1(value)
        JOIN jsonb_array_elements_text(qa_jsonb_array_or_empty(b.service_context -> v_lines_key)) l2(value)
          ON l2.value = l1.value
        ORDER BY l1.value
      ) AS shared_lines,
      (
        SELECT COUNT(*)
        FROM (
          SELECT DISTINCT l1.value
          FROM jsonb_array_elements_text(qa_jsonb_array_or_empty(a.service_context -> v_lines_key)) l1(value)
          JOIN jsonb_array_elements_text(qa_jsonb_array_or_empty(b.service_context -> v_lines_key)) l2(value)
            ON l2.value = l1.value
        ) x
      ) AS overlap_count
    FROM qa_station_cluster_candidates a
    JOIN qa_station_cluster_candidates b
      ON b.cluster_id = a.cluster_id
     AND b.canonical_station_id > a.canonical_station_id
  ) p
  JOIN qa_station_clusters cl
    ON cl.cluster_id = p.cluster_id
  WHERE cl.scope_tag = v_scope_tag
    AND (NULLIF(v_country, '') IS NULL OR cl.country = NULLIF(v_country, '')::char(2));

  INSERT INTO qa_station_cluster_evidence (
    cluster_id,
    source_canonical_station_id,
    target_canonical_station_id,
    evidence_type,
    score,
    details,
    created_at
  )
  SELECT
    a.cluster_id,
    a.canonical_station_id,
    b.canonical_station_id,
    'segment_relation',
    CASE
      WHEN seg_a.complex_id = seg_b.complex_id THEN 0.8
      ELSE 0.2
    END,
    jsonb_build_object(
      'left_complex_id', seg_a.complex_id,
      'right_complex_id', seg_b.complex_id,
      'same_complex', seg_a.complex_id = seg_b.complex_id
    ),
    now()
  FROM qa_station_cluster_candidates a
  JOIN qa_station_cluster_candidates b
    ON b.cluster_id = a.cluster_id
   AND b.canonical_station_id > a.canonical_station_id
  JOIN qa_station_segments seg_a
    ON seg_a.canonical_station_id = a.canonical_station_id
  JOIN qa_station_segments seg_b
    ON seg_b.canonical_station_id = b.canonical_station_id
  JOIN qa_station_clusters cl
    ON cl.cluster_id = a.cluster_id
  WHERE cl.scope_tag = v_scope_tag
    AND (NULLIF(v_country, '') IS NULL OR cl.country = NULLIF(v_country, '')::char(2));

  SELECT COUNT(*) INTO v_detected
  FROM qa_station_clusters c
  WHERE c.scope_tag = v_scope_tag
    AND (NULLIF(v_country, '') IS NULL OR c.country = NULLIF(v_country, '')::char(2));

  RETURN jsonb_build_object(
    'scopeTag', v_scope_tag,
    v_country_key, NULLIF(v_country, ''),
    'clusters', v_detected,
    'candidates', (
      SELECT COUNT(*)
      FROM qa_station_cluster_candidates c
      JOIN qa_station_clusters cl ON cl.cluster_id = c.cluster_id
      WHERE cl.scope_tag = v_scope_tag
        AND (NULLIF(v_country, '') IS NULL OR cl.country = NULLIF(v_country, '')::char(2))
    ),
    'issues', (
      SELECT COUNT(*)
      FROM _issue_scope_v2
    )
  );
END;
$$;

-- Additive schema for explicit station group modeling.

CREATE TABLE IF NOT EXISTS qa_station_groups (
  group_id text PRIMARY KEY,
  cluster_id text REFERENCES qa_station_clusters(cluster_id) ON DELETE SET NULL,
  country char(2) NOT NULL CHECK (country ~ '^[A-Z]{2}$'),
  display_name text NOT NULL,
  scope_tag text NOT NULL DEFAULT 'latest',
  is_active boolean NOT NULL DEFAULT true,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_by text NOT NULL DEFAULT current_user,
  updated_by text NOT NULL DEFAULT current_user,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (display_name <> '')
);

CREATE INDEX IF NOT EXISTS idx_qa_station_groups_country_active
  ON qa_station_groups (country, is_active, scope_tag, updated_at DESC);

CREATE TABLE IF NOT EXISTS qa_station_group_sections (
  section_id text PRIMARY KEY,
  group_id text NOT NULL REFERENCES qa_station_groups(group_id) ON DELETE CASCADE,
  section_type text NOT NULL DEFAULT 'other' CHECK (section_type IN ('main', 'secondary', 'subway', 'bus', 'tram', 'other')),
  section_name text NOT NULL,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_by text NOT NULL DEFAULT current_user,
  updated_by text NOT NULL DEFAULT current_user,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (section_name <> '')
);

CREATE INDEX IF NOT EXISTS idx_qa_station_group_sections_group
  ON qa_station_group_sections (group_id, section_type, section_name);

CREATE TABLE IF NOT EXISTS qa_station_group_section_members (
  section_id text NOT NULL REFERENCES qa_station_group_sections(section_id) ON DELETE CASCADE,
  canonical_station_id text NOT NULL REFERENCES canonical_stations(canonical_station_id) ON DELETE CASCADE,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (section_id, canonical_station_id)
);

CREATE INDEX IF NOT EXISTS idx_qa_station_group_section_members_station
  ON qa_station_group_section_members (canonical_station_id, section_id);

CREATE TABLE IF NOT EXISTS qa_station_group_section_links (
  section_link_id bigserial PRIMARY KEY,
  from_section_id text NOT NULL REFERENCES qa_station_group_sections(section_id) ON DELETE CASCADE,
  to_section_id text NOT NULL REFERENCES qa_station_group_sections(section_id) ON DELETE CASCADE,
  min_walk_minutes integer NOT NULL DEFAULT 0 CHECK (min_walk_minutes >= 0),
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_by text NOT NULL DEFAULT current_user,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (from_section_id <> to_section_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_qa_station_group_section_links_unique
  ON qa_station_group_section_links (from_section_id, to_section_id);

-- Additive curated-station projection layer.
--
-- Purpose:
-- - Keep raw/canonical ingestion data immutable and reviewable.
-- - Materialize reviewer-confirmed station entities separately from source candidates.
-- - Preserve field-level provenance and decision lineage for explainability.

CREATE TABLE IF NOT EXISTS qa_curated_stations (
  curated_station_id text PRIMARY KEY,
  country char(2) NOT NULL CHECK (country ~ '^[A-Z]{2}$'),
  status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'superseded')),
  primary_cluster_id text REFERENCES qa_station_clusters(cluster_id) ON DELETE SET NULL,
  latest_decision_id bigint REFERENCES qa_station_cluster_decisions(decision_id) ON DELETE SET NULL,
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

CREATE INDEX IF NOT EXISTS idx_qa_curated_stations_scope
  ON qa_curated_stations (country, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_qa_curated_stations_cluster
  ON qa_curated_stations (primary_cluster_id, status, updated_at DESC);

CREATE TABLE IF NOT EXISTS qa_curated_station_members (
  curated_station_id text NOT NULL REFERENCES qa_curated_stations(curated_station_id) ON DELETE CASCADE,
  canonical_station_id text NOT NULL REFERENCES canonical_stations(canonical_station_id) ON DELETE RESTRICT,
  member_role text NOT NULL DEFAULT 'member' CHECK (member_role IN ('primary', 'member')),
  member_rank integer NOT NULL DEFAULT 1 CHECK (member_rank > 0),
  contribution jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (curated_station_id, canonical_station_id)
);

CREATE INDEX IF NOT EXISTS idx_qa_curated_station_members_station
  ON qa_curated_station_members (canonical_station_id, curated_station_id);

CREATE TABLE IF NOT EXISTS qa_curated_station_lineage (
  lineage_id bigserial PRIMARY KEY,
  curated_station_id text NOT NULL REFERENCES qa_curated_stations(curated_station_id) ON DELETE CASCADE,
  decision_id bigint NOT NULL REFERENCES qa_station_cluster_decisions(decision_id) ON DELETE CASCADE,
  cluster_id text REFERENCES qa_station_clusters(cluster_id) ON DELETE SET NULL,
  operation text NOT NULL CHECK (operation IN ('merge', 'split', 'rename', 'keep_separate')),
  decision_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (curated_station_id, decision_id)
);

CREATE INDEX IF NOT EXISTS idx_qa_curated_station_lineage_cluster
  ON qa_curated_station_lineage (cluster_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_qa_curated_station_lineage_decision
  ON qa_curated_station_lineage (decision_id);

CREATE TABLE IF NOT EXISTS qa_curated_station_field_provenance (
  curated_station_id text NOT NULL REFERENCES qa_curated_stations(curated_station_id) ON DELETE CASCADE,
  field_name text NOT NULL,
  field_value text,
  source_kind text NOT NULL CHECK (source_kind IN ('manual_decision', 'canonical_candidate', 'derived')),
  source_ref text NOT NULL DEFAULT '',
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (field_name <> ''),
  PRIMARY KEY (curated_station_id, field_name, source_kind, source_ref)
);

CREATE INDEX IF NOT EXISTS idx_qa_curated_station_field_provenance_lookup
  ON qa_curated_station_field_provenance (field_name, source_kind, created_at DESC);

CREATE OR REPLACE VIEW qa_curated_station_projection AS
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
    FROM qa_curated_station_members m
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
    FROM qa_curated_station_field_provenance p
    WHERE p.curated_station_id = s.curated_station_id
  ) AS field_provenance,
  (
    SELECT COALESCE(json_agg(json_build_object(
      'decision_id', l.decision_id,
      'cluster_id', l.cluster_id,
      'operation', l.operation,
      'created_at', l.created_at
    ) ORDER BY l.created_at DESC, l.decision_id DESC), '[]'::json)
    FROM qa_curated_station_lineage l
    WHERE l.curated_station_id = s.curated_station_id
  ) AS lineage
FROM qa_curated_stations s;

-- Normalize v2 merge decision member actions to target-free semantics.
--
-- Previous rows used merge_target/merge_source to encode a merge target.
-- V2 merge now creates a new curated entity and does not need a target member role.

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name = 'qa_station_cluster_decision_members'
  ) THEN
    ALTER TABLE qa_station_cluster_decision_members
      DROP CONSTRAINT IF EXISTS qa_station_cluster_decision_members_action_check;

    UPDATE qa_station_cluster_decision_members
    SET action = 'merge_member'
    WHERE action IN ('merge_target', 'merge_source');

    ALTER TABLE qa_station_cluster_decision_members
      ADD CONSTRAINT qa_station_cluster_decision_members_action_check
      CHECK (
        action IN (
          'candidate',
          'merge_member',
          'separate',
          'segment_assign',
          'line_assign'
        )
      );
  END IF;
END $$;

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

-- 2. Add Tombstoning columns to qa_station_clusters
ALTER TABLE qa_station_clusters
  ADD COLUMN IF NOT EXISTS is_deleted boolean NOT NULL DEFAULT false;
ALTER TABLE qa_station_clusters
  ADD COLUMN IF NOT EXISTS deleted_at timestamptz;

-- 3. Rename old base tables to *_legacy only when needed.
DO $$
DECLARE
  v_canonical regclass := to_regclass('public.canonical_stations');
  v_staging regclass := to_regclass('public.netex_stops_staging');
BEGIN
  IF to_regclass('canonical_stations' || '_legacy') IS NULL
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

/*
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
*/

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

/*
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
*/

/*
CREATE UNIQUE INDEX IF NOT EXISTS idx_netex_stops_uniq
  ON netex_stops_staging (country, source_id, snapshot_date, source_stop_id);
CREATE INDEX IF NOT EXISTS idx_netex_stops_staging_geom
  ON netex_stops_staging USING gist (geom);
CREATE INDEX IF NOT EXISTS idx_netex_stops_name
  ON netex_stops_staging (country, normalized_name);
CREATE INDEX IF NOT EXISTS idx_netex_stops_hard_id
  ON netex_stops_staging (country, hard_id)
  WHERE hard_id IS NOT NULL;
*/

/*
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
*/

COMMIT;

-- V2 Geographic Grid Partitioning
--
-- Replaces country-list partitions with geospatial grid partitions so writes
-- and reads scale beyond fixed country shards.
--
-- Note: this repository uses forward-only migrations. A manual rollback recipe
-- is provided at the end of this file.

BEGIN;

DO $$
BEGIN
  IF to_regclass('canonical_stations') IS NOT NULL AND to_regclass('canonical_stations_country_partitioned_legacy') IS NULL THEN
    ALTER TABLE canonical_stations RENAME TO canonical_stations_country_partitioned_legacy;
  END IF;
  IF to_regclass('netex_stops_staging') IS NOT NULL AND to_regclass('netex_stops_staging_country_partitioned_legacy') IS NULL THEN
    ALTER TABLE netex_stops_staging RENAME TO netex_stops_staging_country_partitioned_legacy;
  END IF;
END $$;

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

CREATE TABLE IF NOT EXISTS canonical_stations (
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
  PRIMARY KEY (grid_id, canonical_station_id)
) PARTITION BY HASH (grid_id);

DO $$
DECLARE
  i integer;
BEGIN
  FOR i IN 0..31 LOOP
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS canonical_stations_p%s PARTITION OF canonical_stations FOR VALUES WITH (MODULUS 32, REMAINDER %s)',
      lpad(i::text, 2, '0'),
      i
    );
  END LOOP;
END $$;

CREATE INDEX IF NOT EXISTS idx_canonical_stations_grid_country_name
  ON canonical_stations (country, normalized_name);
CREATE INDEX IF NOT EXISTS idx_canonical_stations_grid_lookup
  ON canonical_stations (canonical_station_id, country);
CREATE INDEX IF NOT EXISTS idx_canonical_stations_grid_geom
  ON canonical_stations USING gist (geom);

DO $$
BEGIN
  IF to_regclass('canonical_stations_country_partitioned_legacy') IS NOT NULL THEN
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
  END IF;
END $$;

CREATE SEQUENCE netex_stops_staging_grid_seq AS bigint;

CREATE TABLE IF NOT EXISTS netex_stops_staging (
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
  PRIMARY KEY (grid_id, staging_id)
) PARTITION BY HASH (grid_id);

ALTER SEQUENCE netex_stops_staging_grid_seq
  OWNED BY netex_stops_staging.staging_id;

DO $$
DECLARE
  i integer;
BEGIN
  FOR i IN 0..31 LOOP
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS netex_stops_staging_p%s PARTITION OF netex_stops_staging FOR VALUES WITH (MODULUS 32, REMAINDER %s)',
      lpad(i::text, 2, '0'),
      i
    );
  END LOOP;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS idx_netex_stops_staging_grid_uniq
  ON netex_stops_staging (grid_id, source_id, snapshot_date, source_stop_id);
CREATE INDEX IF NOT EXISTS idx_netex_stops_staging_grid_geom
  ON netex_stops_staging USING gist (geom);
CREATE INDEX IF NOT EXISTS idx_netex_stops_staging_grid_name
  ON netex_stops_staging (country, normalized_name);
CREATE INDEX IF NOT EXISTS idx_netex_stops_staging_grid_lookup
  ON netex_stops_staging (source_id, snapshot_date DESC);
CREATE INDEX IF NOT EXISTS idx_netex_stops_staging_grid_hard_id
  ON netex_stops_staging (country, hard_id)
  WHERE hard_id IS NOT NULL;

DO $$
BEGIN
  IF to_regclass('netex_stops_staging_country_partitioned_legacy') IS NOT NULL THEN
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
  END IF;
END $$;

SELECT setval(
  'netex_stops_staging_grid_seq',
  COALESCE((SELECT MAX(staging_id) FROM netex_stops_staging), 1),
  true
);

DROP TABLE IF EXISTS canonical_stations_country_partitioned_legacy;
DROP TABLE IF EXISTS netex_stops_staging_country_partitioned_legacy;

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

-- Migration 013: AI confidence columns for QA Low-Confidence Queue
-- Adds ai_confidence + ai_suggested_action to evidence and decisions tables.
-- Also ensures a partial index exists for fast < 0.90 queue queries.

-- 1. Add AI confidence score column to cluster evidence
ALTER TABLE qa_station_cluster_evidence
  ADD COLUMN IF NOT EXISTS ai_confidence numeric(5,4)
    CHECK (ai_confidence IS NULL OR (ai_confidence >= 0 AND ai_confidence <= 1));

-- 2. Add AI suggested action to evidence (e.g. 'approve', 'reject', 'review')
ALTER TABLE qa_station_cluster_evidence
  ADD COLUMN IF NOT EXISTS ai_suggested_action text;

-- 3. Record what confidence score was at decision time (for audit trail)
ALTER TABLE qa_station_cluster_decisions
  ADD COLUMN IF NOT EXISTS ai_confidence numeric(5,4)
    CHECK (ai_confidence IS NULL OR (ai_confidence >= 0 AND ai_confidence <= 1));

-- 4. Fast partial index for the Low-Confidence Queue (confidence < 0.90)
CREATE INDEX IF NOT EXISTS idx_qa_cluster_evidence_low_confidence
  ON qa_station_cluster_evidence (cluster_id, ai_confidence ASC)
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

-- Migration 014: Walk-time overrides for QA mega-hub transfer matrix (Task 5.2)
--
-- The existing station_transfer_rules table already supports hub-scoped rules,
-- but its country CHECK only allows 'DE', 'AT', 'CH'. For pan-European mega-hubs
-- we need to relax that so the QA operator can save walk-times for any hub.
--
-- Strategy: add a nullable hub_id text column to store the stable frontend
-- identifier (e.g. 'paris-cdg') and drop the country NOT NULL + CHECK constraint
-- so we can use 'EU' as a sentinel for cross-border hubs.
--
-- To keep this migration safe and additive we:
--  1. Add hub_id column (nullable, unique within hub scope)
--  2. Add a partial unique index on (hub_id) for hub-scoped rows (the primary
--     deduplication used by the ON CONFLICT clause in ai-queue.js)
--  3. Widen the country CHECK to also accept 'EU'
--  4. Relax the NOT NULL on country for hub-scoped rows via a new CHECK

-- 1. Add stable hub_id column (frontend identifier, e.g. 'frankfurt-hbf')
ALTER TABLE station_transfer_rules
  ADD COLUMN IF NOT EXISTS hub_id text;

-- 2. Unique index on hub_id for hub-scoped rows
--    This is what ai-queue.js uses for ON CONFLICT DO UPDATE
CREATE UNIQUE INDEX IF NOT EXISTS idx_station_transfer_rules_hub_id
  ON station_transfer_rules (hub_id)
  WHERE rule_scope = 'hub'
    AND hub_id IS NOT NULL
    AND is_active = true
    AND effective_to IS NULL;

-- 3. Drop and recreate the country CHECK to also allow 'EU'
--    (PostgreSQL does not support ALTER TABLE ... ALTER CHECK inline)
ALTER TABLE station_transfer_rules
  DROP CONSTRAINT IF EXISTS station_transfer_rules_country_check;

ALTER TABLE station_transfer_rules
  ADD CONSTRAINT station_transfer_rules_country_check
  CHECK (country IN ('DE', 'AT', 'CH', 'EU', 'FR', 'NL', 'BE', 'LU',
                     'CH', 'PL', 'CZ', 'SK', 'HU', 'RO', 'BG', 'GR',
                     'PT', 'ES', 'IT', 'HR', 'RS', 'SI', 'BA', 'ME',
                     'MK', 'AL', 'DK', 'SE', 'NO', 'FI', 'EE', 'LV',
                     'LT', 'GB', 'IE', 'UK'));
