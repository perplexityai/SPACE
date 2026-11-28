SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE node_runtime_installations
    ADD COLUMN requires_environment BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN node_runtime_installations.requires_environment IS
    'Installation requires an explicit environment binding; not eligible for legacy/unbound placement';
