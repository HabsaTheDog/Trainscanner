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

CREATE TABLE IF NOT EXISTS station_transfer_rules (
  rule_id bigserial PRIMARY KEY,
  rule_scope text NOT NULL CHECK (rule_scope IN ('country_default', 'hub', 'station')),
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
    (rule_scope = 'country_default' AND canonical_station_id IS NULL AND hub_name IS NULL)
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
  WHERE rule_scope = 'country_default' AND is_active = true AND effective_to IS NULL;

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
  'country_default',
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
    WHERE r.rule_scope = 'country_default'
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
