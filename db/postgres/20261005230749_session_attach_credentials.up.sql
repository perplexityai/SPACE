SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE data_plane_session_attach_credentials (
    session_id TEXT NOT NULL REFERENCES data_plane_sessions(session_id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (session_id, token_hash)
);
