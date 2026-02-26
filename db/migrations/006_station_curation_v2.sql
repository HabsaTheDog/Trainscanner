-- Station curation v2: additive schema for cluster-first review, naming auditability,
-- segment-aware modeling, and service/line context scaffolding.

CREATE TABLE IF NOT EXISTS qa_cluster_scoring_config_v2 (
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

INSERT INTO qa_cluster_scoring_config_v2 (scope_key, config)
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

CREATE TABLE IF NOT EXISTS qa_station_naming_overrides_v2 (
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

CREATE INDEX IF NOT EXISTS idx_qa_station_naming_overrides_v2_station
  ON qa_station_naming_overrides_v2 (canonical_station_id, is_active, created_at DESC);

CREATE TABLE IF NOT EXISTS qa_station_display_names_v2 (
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

CREATE INDEX IF NOT EXISTS idx_qa_station_display_names_v2_lookup
  ON qa_station_display_names_v2 (locale, display_name);

CREATE TABLE IF NOT EXISTS qa_station_complexes_v2 (
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

CREATE INDEX IF NOT EXISTS idx_qa_station_complexes_v2_country
  ON qa_station_complexes_v2 (country, complex_name);

CREATE TABLE IF NOT EXISTS qa_station_segments_v2 (
  segment_id text PRIMARY KEY,
  complex_id text NOT NULL REFERENCES qa_station_complexes_v2(complex_id) ON DELETE CASCADE,
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

CREATE INDEX IF NOT EXISTS idx_qa_station_segments_v2_complex
  ON qa_station_segments_v2 (complex_id, segment_type);

CREATE INDEX IF NOT EXISTS idx_qa_station_segments_v2_canonical
  ON qa_station_segments_v2 (canonical_station_id)
  WHERE canonical_station_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_qa_station_segments_v2_geom
  ON qa_station_segments_v2 USING gist (geom)
  WHERE geom IS NOT NULL;

CREATE TABLE IF NOT EXISTS qa_station_segment_links_v2 (
  segment_link_id bigserial PRIMARY KEY,
  from_segment_id text NOT NULL REFERENCES qa_station_segments_v2(segment_id) ON DELETE CASCADE,
  to_segment_id text NOT NULL REFERENCES qa_station_segments_v2(segment_id) ON DELETE CASCADE,
  min_walk_minutes integer NOT NULL DEFAULT 0 CHECK (min_walk_minutes >= 0),
  transfer_rule_ref bigint REFERENCES station_transfer_rules(rule_id) ON DELETE SET NULL,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_by text NOT NULL DEFAULT current_user,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (from_segment_id <> to_segment_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_qa_station_segment_links_v2_unique
  ON qa_station_segment_links_v2 (from_segment_id, to_segment_id);

CREATE TABLE IF NOT EXISTS canonical_line_identities_v2 (
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

CREATE UNIQUE INDEX IF NOT EXISTS idx_canonical_line_identities_v2_provider_code
  ON canonical_line_identities_v2 (country, COALESCE(provider_id, ''), COALESCE(line_code, ''), COALESCE(line_name, ''));

CREATE TABLE IF NOT EXISTS station_segment_line_links_v2 (
  segment_id text NOT NULL REFERENCES qa_station_segments_v2(segment_id) ON DELETE CASCADE,
  line_identity_id text NOT NULL REFERENCES canonical_line_identities_v2(line_identity_id) ON DELETE CASCADE,
  direction text,
  service_context jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_station_segment_line_links_v2_unique
  ON station_segment_line_links_v2 (segment_id, line_identity_id, COALESCE(direction, ''));

CREATE TABLE IF NOT EXISTS qa_station_clusters_v2 (
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

CREATE INDEX IF NOT EXISTS idx_qa_station_clusters_v2_scope
  ON qa_station_clusters_v2 (scope_tag, country, status, severity, updated_at DESC);

CREATE TABLE IF NOT EXISTS qa_station_cluster_candidates_v2 (
  cluster_id text NOT NULL REFERENCES qa_station_clusters_v2(cluster_id) ON DELETE CASCADE,
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

CREATE INDEX IF NOT EXISTS idx_qa_station_cluster_candidates_v2_rank
  ON qa_station_cluster_candidates_v2 (cluster_id, candidate_rank);

CREATE TABLE IF NOT EXISTS qa_station_cluster_evidence_v2 (
  evidence_id bigserial PRIMARY KEY,
  cluster_id text NOT NULL REFERENCES qa_station_clusters_v2(cluster_id) ON DELETE CASCADE,
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

CREATE INDEX IF NOT EXISTS idx_qa_station_cluster_evidence_v2_cluster
  ON qa_station_cluster_evidence_v2 (cluster_id, evidence_type);

CREATE TABLE IF NOT EXISTS qa_station_cluster_queue_items_v2 (
  cluster_id text NOT NULL REFERENCES qa_station_clusters_v2(cluster_id) ON DELETE CASCADE,
  review_item_id bigint NOT NULL REFERENCES canonical_review_queue(review_item_id) ON DELETE CASCADE,
  linked_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (cluster_id, review_item_id)
);

CREATE INDEX IF NOT EXISTS idx_qa_station_cluster_queue_items_v2_item
  ON qa_station_cluster_queue_items_v2 (review_item_id, cluster_id);

CREATE TABLE IF NOT EXISTS qa_station_cluster_decisions_v2 (
  decision_id bigserial PRIMARY KEY,
  cluster_id text NOT NULL REFERENCES qa_station_clusters_v2(cluster_id) ON DELETE CASCADE,
  operation text NOT NULL CHECK (operation IN ('merge', 'keep_separate', 'split', 'rename')),
  decision_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  line_decision_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  note text,
  requested_by text NOT NULL DEFAULT current_user,
  applied_to_overrides boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_qa_station_cluster_decisions_v2_cluster
  ON qa_station_cluster_decisions_v2 (cluster_id, created_at DESC);

CREATE TABLE IF NOT EXISTS qa_station_cluster_decision_members_v2 (
  decision_id bigint NOT NULL REFERENCES qa_station_cluster_decisions_v2(decision_id) ON DELETE CASCADE,
  canonical_station_id text NOT NULL REFERENCES canonical_stations(canonical_station_id) ON DELETE CASCADE,
  group_label text NOT NULL DEFAULT '',
  action text NOT NULL DEFAULT 'candidate' CHECK (action IN ('candidate', 'merge_member', 'separate', 'segment_assign', 'line_assign')),
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (decision_id, canonical_station_id, group_label, action)
);

CREATE OR REPLACE FUNCTION qa_effective_scoring_config_v2(p_country text)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
  SELECT COALESCE(
    (SELECT config FROM qa_cluster_scoring_config_v2 WHERE scope_key = COALESCE(NULLIF(upper(p_country), ''), '') LIMIT 1),
    (SELECT config FROM qa_cluster_scoring_config_v2 WHERE scope_key = 'default' LIMIT 1),
    '{}'::jsonb
  );
$$;

CREATE OR REPLACE FUNCTION qa_refresh_station_display_names_v2(p_country text DEFAULT NULL)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_count integer := 0;
  v_default_naming_strategy constant text := 'canonical_name';
  v_source_ref_canonical_name_key constant text := v_default_naming_strategy;
  v_source_ref_country_key constant text := 'country';
BEGIN
  INSERT INTO qa_station_display_names_v2 (
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
    FROM qa_station_naming_overrides_v2 o
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

CREATE OR REPLACE FUNCTION qa_rebuild_station_clusters_v2(
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
  PERFORM qa_refresh_station_display_names_v2(NULLIF(v_country, ''));

  DELETE FROM qa_station_clusters_v2 c
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
      LIMIT COALESCE((qa_effective_scoring_config_v2(s.country) ->> v_max_cluster_candidates_key)::integer, 40)
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

  INSERT INTO qa_station_clusters_v2 (
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
      LEFT JOIN qa_station_display_names_v2 dn
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
      'scoring_config', qa_effective_scoring_config_v2(b.country)
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
      WHEN qa_station_clusters_v2.status IN ('resolved', 'dismissed') THEN qa_station_clusters_v2.status
      ELSE EXCLUDED.status
    END,
    candidate_count = EXCLUDED.candidate_count,
    issue_count = EXCLUDED.issue_count,
    display_name = EXCLUDED.display_name,
    display_name_reason = EXCLUDED.display_name_reason,
    summary = EXCLUDED.summary,
    updated_at = now();

  INSERT INTO qa_station_cluster_queue_items_v2 (cluster_id, review_item_id, linked_at)
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

  INSERT INTO qa_station_complexes_v2 (
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
    jsonb_build_object('source', 'qa_rebuild_station_clusters_v2', 'canonical_station_id', seed.canonical_station_id),
    now(),
    now()
  FROM _cluster_candidate_seed_v2 seed
  JOIN canonical_stations cs
    ON cs.canonical_station_id = seed.canonical_station_id
  LEFT JOIN qa_station_display_names_v2 dn
    ON dn.canonical_station_id = seed.canonical_station_id
  ON CONFLICT (complex_id)
  DO UPDATE SET
    complex_name = EXCLUDED.complex_name,
    display_name = EXCLUDED.display_name,
    metadata = EXCLUDED.metadata,
    updated_at = now();

  INSERT INTO qa_station_segments_v2 (
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
    jsonb_build_object('source', 'qa_rebuild_station_clusters_v2', 'kind', 'default_segment'),
    now(),
    now()
  FROM _cluster_candidate_seed_v2 seed
  JOIN canonical_stations cs
    ON cs.canonical_station_id = seed.canonical_station_id
  LEFT JOIN qa_station_display_names_v2 dn
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

  DELETE FROM qa_station_cluster_candidates_v2 c
  USING qa_station_clusters_v2 cl
  WHERE c.cluster_id = cl.cluster_id
    AND cl.scope_tag = v_scope_tag
    AND (NULLIF(v_country, '') IS NULL OR cl.country = NULLIF(v_country, '')::char(2));

  INSERT INTO qa_station_cluster_candidates_v2 (
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
          FROM qa_station_segment_links_v2 l
          WHERE l.from_segment_id = seg.segment_id
        ), '[]'::jsonb)
      )
      FROM qa_station_segments_v2 seg
      JOIN qa_station_complexes_v2 cplx
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
  LEFT JOIN qa_station_display_names_v2 dn
    ON dn.canonical_station_id = seed.canonical_station_id;

  DELETE FROM qa_station_cluster_evidence_v2 e
  USING qa_station_clusters_v2 c
  WHERE e.cluster_id = c.cluster_id
    AND c.scope_tag = v_scope_tag
    AND (NULLIF(v_country, '') IS NULL OR c.country = NULLIF(v_country, '')::char(2));

  INSERT INTO qa_station_cluster_evidence_v2 (
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
  FROM qa_station_cluster_candidates_v2 a
  JOIN qa_station_cluster_candidates_v2 b
    ON b.cluster_id = a.cluster_id
   AND b.canonical_station_id > a.canonical_station_id
  JOIN qa_station_clusters_v2 cl
    ON cl.cluster_id = a.cluster_id
  JOIN canonical_stations cs1
    ON cs1.canonical_station_id = a.canonical_station_id
  JOIN canonical_stations cs2
    ON cs2.canonical_station_id = b.canonical_station_id
  WHERE cl.scope_tag = v_scope_tag
    AND (NULLIF(v_country, '') IS NULL OR cl.country = NULLIF(v_country, '')::char(2));

  INSERT INTO qa_station_cluster_evidence_v2 (
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
      ELSE GREATEST(0::numeric, LEAST(1::numeric, 1 - (d.dist_m / GREATEST(1, COALESCE((qa_effective_scoring_config_v2(cl.country) ->> v_distance_threshold_meters_key)::numeric, 1500)))))
    END,
    jsonb_build_object(
      'distance_meters', d.dist_m,
      'threshold_meters', COALESCE((qa_effective_scoring_config_v2(cl.country) ->> v_distance_threshold_meters_key)::integer, 1500)
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
    FROM qa_station_cluster_candidates_v2 a
    JOIN qa_station_cluster_candidates_v2 b
      ON b.cluster_id = a.cluster_id
     AND b.canonical_station_id > a.canonical_station_id
  ) d
  JOIN qa_station_clusters_v2 cl
    ON cl.cluster_id = d.cluster_id
  WHERE cl.scope_tag = v_scope_tag
    AND (NULLIF(v_country, '') IS NULL OR cl.country = NULLIF(v_country, '')::char(2));

  INSERT INTO qa_station_cluster_evidence_v2 (
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
    FROM qa_station_cluster_candidates_v2 a
    JOIN qa_station_cluster_candidates_v2 b
      ON b.cluster_id = a.cluster_id
     AND b.canonical_station_id > a.canonical_station_id
  ) p
  JOIN qa_station_clusters_v2 cl
    ON cl.cluster_id = p.cluster_id
  WHERE cl.scope_tag = v_scope_tag
    AND cardinality(p.shared_hard_ids) > 0
    AND (NULLIF(v_country, '') IS NULL OR cl.country = NULLIF(v_country, '')::char(2));

  INSERT INTO qa_station_cluster_evidence_v2 (
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
    FROM qa_station_cluster_candidates_v2 a
    JOIN qa_station_cluster_candidates_v2 b
      ON b.cluster_id = a.cluster_id
     AND b.canonical_station_id > a.canonical_station_id
  ) p
  JOIN qa_station_clusters_v2 cl
    ON cl.cluster_id = p.cluster_id
  WHERE cl.scope_tag = v_scope_tag
    AND (NULLIF(v_country, '') IS NULL OR cl.country = NULLIF(v_country, '')::char(2));

  INSERT INTO qa_station_cluster_evidence_v2 (
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
    FROM qa_station_cluster_candidates_v2 a
    JOIN qa_station_cluster_candidates_v2 b
      ON b.cluster_id = a.cluster_id
     AND b.canonical_station_id > a.canonical_station_id
  ) p
  JOIN qa_station_clusters_v2 cl
    ON cl.cluster_id = p.cluster_id
  WHERE cl.scope_tag = v_scope_tag
    AND (NULLIF(v_country, '') IS NULL OR cl.country = NULLIF(v_country, '')::char(2));

  INSERT INTO qa_station_cluster_evidence_v2 (
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
  FROM qa_station_cluster_candidates_v2 a
  JOIN qa_station_cluster_candidates_v2 b
    ON b.cluster_id = a.cluster_id
   AND b.canonical_station_id > a.canonical_station_id
  JOIN qa_station_segments_v2 seg_a
    ON seg_a.canonical_station_id = a.canonical_station_id
  JOIN qa_station_segments_v2 seg_b
    ON seg_b.canonical_station_id = b.canonical_station_id
  JOIN qa_station_clusters_v2 cl
    ON cl.cluster_id = a.cluster_id
  WHERE cl.scope_tag = v_scope_tag
    AND (NULLIF(v_country, '') IS NULL OR cl.country = NULLIF(v_country, '')::char(2));

  SELECT COUNT(*) INTO v_detected
  FROM qa_station_clusters_v2 c
  WHERE c.scope_tag = v_scope_tag
    AND (NULLIF(v_country, '') IS NULL OR c.country = NULLIF(v_country, '')::char(2));

  RETURN jsonb_build_object(
    'scopeTag', v_scope_tag,
    v_country_key, NULLIF(v_country, ''),
    'clusters', v_detected,
    'candidates', (
      SELECT COUNT(*)
      FROM qa_station_cluster_candidates_v2 c
      JOIN qa_station_clusters_v2 cl ON cl.cluster_id = c.cluster_id
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
