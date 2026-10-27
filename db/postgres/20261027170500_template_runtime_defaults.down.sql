SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE template_releases
    DROP CONSTRAINT template_release_runtime_defaults_valid,
    DROP COLUMN default_vcpu,
    DROP COLUMN default_memory_mib;
