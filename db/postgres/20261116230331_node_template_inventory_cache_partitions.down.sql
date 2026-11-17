-- Roll back the publishers and readers of cache_partitions before removing it.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE node_template_inventory
    DROP COLUMN cache_partitions;
