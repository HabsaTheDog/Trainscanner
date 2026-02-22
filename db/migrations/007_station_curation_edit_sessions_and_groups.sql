-- Additive schema for explicit station group modeling.

CREATE TABLE IF NOT EXISTS qa_station_groups_v2 (
  group_id text PRIMARY KEY,
  cluster_id text REFERENCES qa_station_clusters_v2(cluster_id) ON DELETE SET NULL,
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

CREATE INDEX IF NOT EXISTS idx_qa_station_groups_v2_country_active
  ON qa_station_groups_v2 (country, is_active, scope_tag, updated_at DESC);

CREATE TABLE IF NOT EXISTS qa_station_group_sections_v2 (
  section_id text PRIMARY KEY,
  group_id text NOT NULL REFERENCES qa_station_groups_v2(group_id) ON DELETE CASCADE,
  section_type text NOT NULL DEFAULT 'other' CHECK (section_type IN ('main', 'secondary', 'subway', 'bus', 'tram', 'other')),
  section_name text NOT NULL,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_by text NOT NULL DEFAULT current_user,
  updated_by text NOT NULL DEFAULT current_user,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (section_name <> '')
);

CREATE INDEX IF NOT EXISTS idx_qa_station_group_sections_v2_group
  ON qa_station_group_sections_v2 (group_id, section_type, section_name);

CREATE TABLE IF NOT EXISTS qa_station_group_section_members_v2 (
  section_id text NOT NULL REFERENCES qa_station_group_sections_v2(section_id) ON DELETE CASCADE,
  canonical_station_id text NOT NULL REFERENCES canonical_stations(canonical_station_id) ON DELETE CASCADE,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (section_id, canonical_station_id)
);

CREATE INDEX IF NOT EXISTS idx_qa_station_group_section_members_v2_station
  ON qa_station_group_section_members_v2 (canonical_station_id, section_id);

CREATE TABLE IF NOT EXISTS qa_station_group_section_links_v2 (
  section_link_id bigserial PRIMARY KEY,
  from_section_id text NOT NULL REFERENCES qa_station_group_sections_v2(section_id) ON DELETE CASCADE,
  to_section_id text NOT NULL REFERENCES qa_station_group_sections_v2(section_id) ON DELETE CASCADE,
  min_walk_minutes integer NOT NULL DEFAULT 0 CHECK (min_walk_minutes >= 0),
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_by text NOT NULL DEFAULT current_user,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (from_section_id <> to_section_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_qa_station_group_section_links_v2_unique
  ON qa_station_group_section_links_v2 (from_section_id, to_section_id);
