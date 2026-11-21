SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Fail closed if runtime-neutral rows cannot fit the old schema. Never delete
-- catalog entries or undo operator retirement to make a downgrade succeed.
ALTER TABLE runtime_environments
    ALTER COLUMN kernel_version SET NOT NULL,
    ALTER COLUMN state_format_name SET NOT NULL,
    ALTER COLUMN state_format_version SET NOT NULL,
    DROP CONSTRAINT runtime_environments_state_format_pair_check;
