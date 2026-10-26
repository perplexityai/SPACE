-- Roll back sort-key readers and writers before removing the column.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Dropping the column drops its index and CHECK.
ALTER TABLE node_runtime_installations
    DROP COLUMN version_sort_key;
