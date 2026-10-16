SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '60s';

-- Product policy assertions are separate from native security ownership and
-- ordinary mutable metadata. Absence is disabled; no legacy backfill.
CREATE TABLE sandbox_lifecycle_registrations (
    sandbox_id UUID PRIMARY KEY REFERENCES sandboxes(id) ON DELETE CASCADE,
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    revision BIGINT NOT NULL CHECK (revision > 0),
    product_user_id BIGINT NOT NULL CHECK (product_user_id > 0),
    product_organization_uuid UUID,
    product_session_id TEXT NOT NULL CHECK (
        product_session_id ~ '^[A-Za-z0-9_-]{1,128}$'
    ),
    product_context_uuid UUID
);
CREATE INDEX idx_sandbox_lifecycle_registrations_enabled_id
    ON sandbox_lifecycle_registrations (enabled, sandbox_id);
