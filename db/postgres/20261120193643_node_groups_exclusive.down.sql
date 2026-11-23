-- Roll back the images that read and write this column, then run this.
-- Dropping it reopens shared capacity to every org that owned an exclusive
-- group.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE node_groups
    DROP CONSTRAINT node_groups_exclusive_requires_org,
    DROP COLUMN exclusive;
