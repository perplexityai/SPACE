SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Pre-dispatch record that a template build may read its base template.
--
-- The API authorizes a build's base (visibility and ownership against the
-- requesting organization) before it dispatches `StartBuild`, but it learns
-- which builder node took the job only from the response. The builder,
-- meanwhile, resolves and pulls the base through ClusterApi when the job
-- reaches the front of its queue, which may be long after acceptance.
-- ClusterApi scopes release lookups by caller node, so it needs a
-- control-plane fact that exists *before* dispatch and does not name the
-- node: this row. Nothing here widens what a worker may read.
--
-- One row per build submission, so independently accepted builds keep
-- independent authorization: a second build of the same target, on another
-- builder or from another base, never touches the first build's grant.
--
-- A grant has two phases. Until the API records the accepting builder it is
-- unpinned (`builder_node_id IS NULL`) and any registered builder node may
-- resolve the base, but only briefly: the pin follows the `StartBuild`
-- response within seconds. Once pinned, only that builder may use the grant,
-- for as long as its queued job can plausibly still be running; a builder that
-- has already been handed the base learns nothing new from a long pin.
--
-- Rows are not deleted when a build finishes, because the API learns
-- completion only through client polling; freshness is the bound, and the API
-- sweeps rows older than the longest validity whenever it inserts a new one.
CREATE TABLE IF NOT EXISTS template_build_base_grants (
    -- Application-generated UUIDv7 (Uuid::now_v7()), supplied by the
    -- inserting Rust code, not a v4 DB default.
    id                 UUID PRIMARY KEY,
    -- Target template of the build. No foreign key: the target is registered
    -- in `templates` only when the build publishes.
    template_id        TEXT NOT NULL CHECK (BTRIM(template_id) <> ''),
    base_template_name TEXT NOT NULL REFERENCES templates(name) ON DELETE CASCADE,
    -- Organization the base was authorized for; must own an org-specific base.
    org_id             UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    -- Builder node that accepted this build, recorded from the `StartBuild`
    -- response. NULL until then. A deleted node clears the pin rather than
    -- deleting the grant, so the row falls back to the unpinned phase.
    builder_node_id    UUID REFERENCES nodes(id) ON DELETE SET NULL,
    granted_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ClusterApi reads by base template and freshness.
CREATE INDEX IF NOT EXISTS idx_template_build_base_grants_base
    ON template_build_base_grants (base_template_name, granted_at);

-- The foreign key's referencing side: a node deletion must find its pins.
CREATE INDEX IF NOT EXISTS idx_template_build_base_grants_builder_node_id
    ON template_build_base_grants (builder_node_id);

-- The API's expiry sweep deletes by age alone.
CREATE INDEX IF NOT EXISTS idx_template_build_base_grants_granted_at
    ON template_build_base_grants (granted_at);
