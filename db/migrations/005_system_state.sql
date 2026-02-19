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
