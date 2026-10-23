SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Reverting drops every token a replay minted beyond the session's latest,
-- which `data_plane_sessions.token_hash` still holds. Roll the API and
-- internal-api releases back with this migration: a release that writes the
-- table fails its session creates and replays once the table is gone.
DROP TABLE IF EXISTS data_plane_session_tokens;
