SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE nodes
    DROP COLUMN drain_status,
    DROP COLUMN drain_engine;
