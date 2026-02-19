# System Prompt: Migrate Application State to PostgreSQL

You are an expert backend developer working on the `Trainscanner` orchestrator project.
Your current task is to migrate the application's simple file-based state management into the existing PostgreSQL database.

## Context

Currently, the `Trainscanner` project stores various pieces of state in the `state/` directory as raw JSON files. 
Specifically, there are:
- `active-gtfs.json`
- `gtfs-switch-status.json`
- Various pipeline logs (`gtfs-switch.log`, `pipeline.log`, `stitch-prototype-report.json`)

As the application grows, relying on the filesystem for generic application state presents concurrency risks and forces the frontend to rely on static files instead of a unified database source of truth.

## Requirements

Please implement a migration that moves the JSON file state into PostgreSQL. 

### 1. Database Schema
Create a new migration file in `orchestrator/db/migrations/` (or wherever migrations are currently stored).
You need to create a simple key-value table utilizing `JSONB`.

```sql
CREATE TABLE IF NOT EXISTS system_state (
    key VARCHAR(255) PRIMARY KEY,
    value JSONB NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

### 2. Default Values Migration
In the same migration (or a seed script), insert default values for the existing states based on the current JSON structures:

**For `active_gtfs`:**
```json
{
  "activeProfile": "sample_de",
  "zipPath": "data/gtfs/de_fv.zip",
  "sourceType": "static",
  "runtime": null,
  "activatedAt": "2026-02-19T17:31:54.964Z"
}
```

**For `gtfs_switch_status`:**
```json
{
  "state": "ready",
  "activeProfile": "sample_de",
  "message": "Profile 'sample_de' activated successfully",
  "updatedAt": "2026-02-19T17:31:57.213Z",
  "error": null,
  "requestedProfile": "sample_de",
  "lastHealth": {
    "ok": true,
    "status": 404,
    "body": {
      "message": "Configured MOTIS health endpoint returned 404, treating service as reachable/ready",
      "originalBody": {}
    }
  }
}
```

Write the SQL to insert these default JSON structures into the `system_state` table:
```sql
INSERT INTO system_state (key, value) 
VALUES 
  ('active_gtfs', '{"activeProfile": "sample_de", "zipPath": "data/gtfs/de_fv.zip", "sourceType": "static", "runtime": null, "activatedAt": "2026-02-19T17:31:54.964Z"}'::jsonb),
  ('gtfs_switch_status', '{"state": "ready", "activeProfile": "sample_de", "message": "Profile ''sample_de'' activated ...", "error": null, "requestedProfile": "sample_de", "lastHealth": {"ok": true, "status": 404}}'::jsonb)
ON CONFLICT (key) DO NOTHING;
```

### 3. Application Code Updates (Orchestrator)
- Locate the code in the `orchestrator` that currently reads from or writes to `state/active-gtfs.json` and `state/gtfs-switch-status.json`.
- Refactor these areas to use the database pool to execute `SELECT`, `INSERT`, and `UPDATE` statements against the new `system_state` table instead of relying on `fs.readFile` and `fs.writeFile`.
  - For example, reading the state: `SELECT value FROM system_state WHERE key = $1`
  - Writing the state: `INSERT INTO system_state (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = CURRENT_TIMESTAMP`
- Ensure that the Express API (if any endpoints currently expose this state) is also updated to fetch from the DB.

### 4. Pipeline Execution Logs
- Ensure the raw text logs (`.log`) remain as standard filesystem logs (or stdout).
- For structured reports like `stitch-prototype-report.json`, consider if this needs its own table (e.g., `pipeline_runs`) or if it should also go into `system_state` under a specific key if it only tracks the *latest* run. Provide a brief recommendation in your implementation response.

## Guidelines
- Ensure that all database interactions use parameterized queries (`$1`, `$2`, etc.) to prevent SQL injection.
- Ensure the Node.js application gracefully handles DB connection errors when falling back to reading/writing state.
- Please start by outlining the specific implementation steps, list the files that will be modified, and provide the fully updated source code.
