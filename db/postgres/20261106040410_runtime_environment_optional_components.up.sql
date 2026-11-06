SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Runtime-neutral definitions need not have a kernel or a persisted-state
-- format. Their immutable reference still covers the complete descriptor.
-- Existing production readers select only environment_ref. No reader assumes
-- these metadata columns are present, and the owning definition makes them
-- optional. The compatibility test pins the unchanged reference-list read.
ALTER TABLE runtime_environments
    -- squawk-ignore ban-drop-not-null
    ALTER COLUMN kernel_version DROP NOT NULL,
    -- squawk-ignore ban-drop-not-null
    ALTER COLUMN state_format_name DROP NOT NULL,
    -- squawk-ignore ban-drop-not-null
    ALTER COLUMN state_format_version DROP NOT NULL,
    ADD CONSTRAINT runtime_environments_state_format_pair_check
        CHECK ((state_format_name IS NULL) = (state_format_version IS NULL)) NOT VALID;
