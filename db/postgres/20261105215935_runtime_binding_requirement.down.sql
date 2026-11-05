SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE node_runtime_installations DROP COLUMN requires_environment;
