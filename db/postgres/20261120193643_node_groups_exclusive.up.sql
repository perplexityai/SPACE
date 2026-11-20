SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- An exclusive dedicated node group confines its owning org to dedicated
-- capacity. While an org owns at least one exclusive group, node-group
-- placement drops every generic group from that org's candidate set on create,
-- restore, evacuation and drain, and an empty set fails closed instead of
-- falling back to shared nodes. Use it for orgs whose sandboxes must only run
-- on nodes with org-controlled disk encryption.
--
-- A NOT NULL column with a constant default changes the catalog only; no row
-- is rewritten. Every existing row is false and satisfies the CHECK, so it is
-- added NOT VALID to skip the scan.
ALTER TABLE node_groups
    ADD COLUMN exclusive BOOLEAN NOT NULL DEFAULT FALSE,
    ADD CONSTRAINT node_groups_exclusive_requires_org
        CHECK ((NOT exclusive) OR (org_id IS NOT NULL)) NOT VALID;
