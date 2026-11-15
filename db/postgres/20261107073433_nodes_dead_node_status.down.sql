-- The value is a report the verb's next tick rewrites, so nothing is lost.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE nodes
    DROP COLUMN dead_node_status;
