SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE nodes
    DROP CONSTRAINT nodes_drain_status_object_check,
    DROP CONSTRAINT nodes_drain_engine_check,
    DROP COLUMN drain_status,
    DROP COLUMN drain_engine;
