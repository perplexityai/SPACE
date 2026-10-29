SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE template_releases
    DROP CONSTRAINT template_release_resources_valid,
    DROP COLUMN vcpu,
    DROP COLUMN memory_mib;
