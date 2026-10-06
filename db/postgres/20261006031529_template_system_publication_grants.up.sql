SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Pre-dispatch record that a system template build may publish its output.
--
-- System templates (`seed`, `code-runtime`) are built only through the Admin
-- API's system build route, which build-manager then publishes as an ownerless
-- template. Build-manager also publishes tenant builds, so ClusterApi does not
-- let it classify a publication as a system one by omitting the organization:
-- a system publication from a publisher that also holds tenant authority needs
-- one of these rows. Publishers with system authority alone (the deploy-time
-- template publisher) are unaffected.
--
-- One row per system build submission. The API inserts it before dispatching
-- `StartBuild` and deletes it again if dispatch fails. Registering the template
-- requires a fresh row; registering the release consumes one in the same
-- transaction, so each submission publishes at most one release.
--
-- Rows are not deleted when a build fails after dispatch, because the API
-- learns completion only through client polling; freshness is the bound, and
-- the API sweeps rows older than it whenever it inserts a new one.
CREATE TABLE IF NOT EXISTS template_system_publication_grants (
    -- Application-generated UUIDv7 (Uuid::now_v7()), supplied by the
    -- inserting Rust code, not a v4 DB default.
    id            UUID PRIMARY KEY,
    -- Template the system build publishes. No foreign key: the template is
    -- registered in `templates` only when the build publishes.
    template_name TEXT NOT NULL CHECK (BTRIM(template_name) <> ''),
    granted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ClusterApi reads and consumes by template and freshness.
CREATE INDEX IF NOT EXISTS idx_template_system_publication_grants_template
    ON template_system_publication_grants (template_name, granted_at);

-- The API's expiry sweep deletes by age alone.
CREATE INDEX IF NOT EXISTS idx_template_system_publication_grants_granted_at
    ON template_system_publication_grants (granted_at);
