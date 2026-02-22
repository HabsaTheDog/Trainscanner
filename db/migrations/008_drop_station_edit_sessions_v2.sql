-- Remove deprecated reviewer draft/edit-session persistence tables.

DROP TABLE IF EXISTS qa_station_edit_session_events_v2;
DROP TABLE IF EXISTS qa_station_edit_sessions_v2;
