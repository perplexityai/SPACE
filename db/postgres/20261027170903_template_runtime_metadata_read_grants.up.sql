SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- A cold create may ask one builder to recover an existing release's runtime
-- defaults. This grants an exact release read, never build or publication
-- authority. A separate table keeps older readers fail-closed during rollout.
CREATE TABLE template_runtime_metadata_read_grants (
    release_id UUID NOT NULL REFERENCES template_releases(id) ON DELETE CASCADE,
    builder_node_id UUID NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    org_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (release_id, builder_node_id)
);

CREATE INDEX idx_template_runtime_metadata_read_grants_builder
    ON template_runtime_metadata_read_grants (builder_node_id);
CREATE INDEX idx_template_runtime_metadata_read_grants_org
    ON template_runtime_metadata_read_grants (org_id);
