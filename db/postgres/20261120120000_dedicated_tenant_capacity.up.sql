SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- A tenant is a child org of the client org whose API key registered it.
-- external_id is the client's own name for the tenant; registration is
-- idempotent on (parent_id, external_id). NULL for orgs that are not tenants.
ALTER TABLE organizations
    ADD COLUMN external_id TEXT;

ALTER TABLE organizations
    ADD CONSTRAINT organizations_external_id_shape_check
    CHECK (external_id IS NULL OR (parent_id IS NOT NULL
        AND char_length(external_id) BETWEEN 1 AND 128));

CREATE UNIQUE INDEX organizations_parent_external_id_key
    ON organizations (parent_id, external_id)
    WHERE external_id IS NOT NULL;

-- Dedicated capacity drawn from a generic, Terraform-owned pool. pool names
-- the pool (an EKS managed node group on AWS); desired_nodes is how many of
-- its nodes SPACE assigns to this group. Both NULL for groups whose nodes are
-- assigned some other way.
ALTER TABLE node_groups
    ADD COLUMN pool TEXT,
    ADD COLUMN desired_nodes INTEGER;

ALTER TABLE node_groups
    ADD CONSTRAINT node_groups_dedicated_capacity_shape_check
    CHECK ((pool IS NULL AND desired_nodes IS NULL)
        OR (pool IS NOT NULL AND desired_nodes IS NOT NULL
            AND org_id IS NOT NULL AND desired_nodes >= 0));
