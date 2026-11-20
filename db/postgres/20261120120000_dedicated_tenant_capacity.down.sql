SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE node_groups
    DROP CONSTRAINT node_groups_dedicated_capacity_shape_check,
    DROP COLUMN desired_nodes,
    DROP COLUMN pool;

DROP INDEX organizations_parent_external_id_key;

ALTER TABLE organizations
    DROP CONSTRAINT organizations_external_id_shape_check,
    DROP COLUMN external_id;
