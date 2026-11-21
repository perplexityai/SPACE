SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Restore the preceding migration's unvalidated constraint state without
-- changing accepted data or dropping enforcement for newly written rows.
ALTER TABLE runtime_environments
    -- squawk-ignore ban-drop-constraint
    DROP CONSTRAINT runtime_environments_state_format_pair_check,
    ADD CONSTRAINT runtime_environments_state_format_pair_check
        CHECK ((state_format_name IS NULL) = (state_format_version IS NULL)) NOT VALID;
