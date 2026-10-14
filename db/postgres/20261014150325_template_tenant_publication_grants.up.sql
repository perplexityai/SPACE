SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Pre-dispatch record that a tenant template build may publish its output.
--
-- Build-manager's publisher identity may assert template ownership, so
-- ClusterApi does not accept the organization a publication names on that
-- authority alone: a tenant publication from such a publisher needs one of
-- these rows, naming the target and the organization the API authorized the
-- build for.
--
-- One row per build submission. The API inserts it before dispatching
-- `StartBuild`, because a build may publish before the API records the
-- response, and deletes it again if dispatch fails. Registering the template
-- requires a fresh row; registering the release consumes it in the same
-- transaction, so each submission publishes at most one release.
--
-- The builder binding follows `template_build_base_grants`. Until the API
-- records the accepting builder the row is unpinned (`builder_node_id IS
-- NULL`) and any registered builder node may use it for one hour; the pin
-- follows the `StartBuild` response within seconds. Once pinned, only that
-- builder may use it, for seven days, so a job that waits in its builder's
-- queue keeps its authorization.
--
-- Rows are not deleted when a build fails after dispatch, because the API
-- learns completion only through client polling; freshness is the bound, and
-- the API sweeps rows older than it whenever it inserts a new one.
CREATE TABLE IF NOT EXISTS template_tenant_publication_grants (
    -- Application-generated UUIDv7 (Uuid::now_v7()), supplied by the
    -- inserting Rust code, not a v4 DB default.
    id              UUID PRIMARY KEY,
    -- Template the build publishes. No foreign key: the template is
    -- registered in `templates` only when the build publishes.
    template_name   TEXT NOT NULL CHECK (BTRIM(template_name) <> ''),
    -- Organization the build was authorized for and that owns its output.
    org_id          UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    -- Builder node that accepted the build, recorded from the `StartBuild`
    -- response. NULL until then. Deleting the node revokes the grant so it
    -- cannot become available to other builders through the unpinned phase.
    builder_node_id UUID REFERENCES nodes(id) ON DELETE CASCADE,
    granted_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- The foreign keys' referencing sides: deletions must find their rows.
CREATE INDEX IF NOT EXISTS idx_template_tenant_publication_grants_org_id
    ON template_tenant_publication_grants (org_id);
CREATE INDEX IF NOT EXISTS idx_template_tenant_publication_grants_builder_node_id
    ON template_tenant_publication_grants (builder_node_id);

-- The API's expiry sweep deletes by age alone.
CREATE INDEX IF NOT EXISTS idx_template_tenant_publication_grants_granted_at
    ON template_tenant_publication_grants (granted_at);
