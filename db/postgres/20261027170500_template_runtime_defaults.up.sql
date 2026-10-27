SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Match the validated integer resource fields on sandbox and inventory rows.
-- Existing rows receive NULL defaults; avoid scanning the release catalog.
ALTER TABLE template_releases
    -- squawk-ignore prefer-bigint-over-int
    ADD COLUMN default_vcpu INTEGER,
    -- squawk-ignore prefer-bigint-over-int
    ADD COLUMN default_memory_mib INTEGER,
    ADD CONSTRAINT template_release_runtime_defaults_valid CHECK (
        (default_vcpu IS NULL AND default_memory_mib IS NULL)
        OR (default_vcpu IS NOT NULL AND default_memory_mib IS NOT NULL
            AND default_vcpu BETWEEN 1 AND 255 AND default_memory_mib > 0)
    ) NOT VALID;
