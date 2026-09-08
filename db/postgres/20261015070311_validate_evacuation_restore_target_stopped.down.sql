SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Restore the unvalidated state owned by the preceding migration so rolling
-- back only this migration doesn't leave SQLx's migration state out of sync
-- with the constraint state.
ALTER TABLE sandboxes
    -- squawk-ignore ban-drop-constraint
    DROP CONSTRAINT sandboxes_evacuation_restore_target_check,
    ADD CONSTRAINT sandboxes_evacuation_restore_target_check
        CHECK (evacuation_restore_target IS NULL
               OR evacuation_restore_target IN ('started', 'paused', 'stopped'))
        NOT VALID;
