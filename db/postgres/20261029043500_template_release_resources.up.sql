SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE template_releases
    -- Match sandbox resource columns; registration bounds both values to i32.
    -- squawk-ignore prefer-bigint-over-int
    ADD COLUMN vcpu INTEGER,
    -- squawk-ignore prefer-bigint-over-int
    ADD COLUMN memory_mib INTEGER,
    ADD CONSTRAINT template_release_resources_valid CHECK (
        (vcpu IS NULL AND memory_mib IS NULL) OR
        (vcpu IS NOT NULL AND memory_mib IS NOT NULL AND vcpu > 0 AND memory_mib > 0)
    ) NOT VALID;
