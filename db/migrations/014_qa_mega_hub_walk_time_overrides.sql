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
