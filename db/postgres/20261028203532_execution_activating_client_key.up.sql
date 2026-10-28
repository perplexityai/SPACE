SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- The bounded key of the client that activated each execution: an allowlisted
-- X-Client-ID value or 'unknown'. The API writes it when it inserts a
-- generation's row and renders it as the space.pplx.net/client-id pod label,
-- so a pod rebuilt for the same generation keeps its attribution. A generation
-- inserted with no client in hand copies its predecessor's key.
--
-- Nullable with no default or backfill: rows from before this column, and rows
-- written by older API images, read as unattributed, which the API renders as
-- 'unknown'. Its values come from configuration, so the format is enforced in
-- Rust, not with a CHECK.
ALTER TABLE sandbox_k8s_executions
    ADD COLUMN activating_client_key text;
