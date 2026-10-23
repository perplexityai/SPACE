SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Live attach tokens of a data-plane session, one row per token.
--
-- Replaying a session's create with the same idempotency key mints another
-- token for the same session. Each token stays valid until its own
-- `expires_at`, so a replay no longer invalidates the tokens earlier replays
-- returned. `data_plane_sessions.token_hash` and `token_expires_at` keep
-- naming the most recent token while servers that read only those columns are
-- still deployed; validators consult this table first and fall back to them.
CREATE TABLE IF NOT EXISTS data_plane_session_tokens (
    -- SHA-256 hex digest of the bearer token; the raw token is never stored.
    token_hash text PRIMARY KEY,
    session_id text NOT NULL REFERENCES data_plane_sessions (session_id) ON DELETE CASCADE,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Replays prune and cap a session's tokens by expiry, and the session
-- foreign key's cascade finds its rows through the leading column.
CREATE INDEX IF NOT EXISTS idx_data_plane_session_tokens_session_id_expires_at
    ON data_plane_session_tokens (session_id, expires_at);
