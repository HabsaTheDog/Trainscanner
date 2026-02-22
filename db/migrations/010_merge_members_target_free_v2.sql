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
      AND table_name = 'qa_station_cluster_decision_members_v2'
  ) THEN
    ALTER TABLE qa_station_cluster_decision_members_v2
      DROP CONSTRAINT IF EXISTS qa_station_cluster_decision_members_v2_action_check;

    UPDATE qa_station_cluster_decision_members_v2
    SET action = 'merge_member'
    WHERE action IN ('merge_target', 'merge_source');

    ALTER TABLE qa_station_cluster_decision_members_v2
      ADD CONSTRAINT qa_station_cluster_decision_members_v2_action_check
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
