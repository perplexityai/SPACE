SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Admit `'stopped'` as an evacuation restore target.
--
-- The baseline pins the column to `'started'` and `'paused'`, the two
-- lifecycles the suspend -> restore pipeline services. A drain evacuation of
-- a `stopped` resident publishes that sandbox's disk artifact to S3, tears
-- down its local presence, and re-places it `stopped` on another node. It
-- carries the same marker, so the CHECK has to admit `'stopped'` before any
-- drain code writes it. Nothing writes the value until `EVACUABLE_LIFECYCLES`
-- gains `Stopped`; until then this widening is inert.
--
-- Widening only: every existing row already satisfies the new predicate, so
-- the validation in the paired migration is a formality on the current data
-- rather than a possible failure. It's still split out because VALIDATE
-- CONSTRAINT scans all of `sandboxes`.
--
-- The marker-clearing trigger, `sandboxes_clear_evacuation_restore_target`,
-- is deliberately left alone. It clears the marker only on a lifecycle
-- *change*, so it fits a stopped evacuation that sets the marker while the
-- row stays `stopped` and re-places it through `restoring -> stopped`: the
-- claim survives the statement that writes it, a user takeover moving the row
-- out of `stopped` clears the marker, and the `restoring -> stopped` finalize
-- clears it on completion. The drain change that writes `'stopped'` has to
-- take that shape; a claim that moves the row to another lifecycle means
-- revisiting this trigger.
--
-- The drop is replaced in the same transaction by a strictly wider CHECK, so
-- the only guarantee it removes is that the marker is never `'stopped'`.
ALTER TABLE sandboxes
    -- squawk-ignore ban-drop-constraint
    DROP CONSTRAINT IF EXISTS sandboxes_evacuation_restore_target_check;

ALTER TABLE sandboxes
    ADD CONSTRAINT sandboxes_evacuation_restore_target_check
        CHECK (evacuation_restore_target IS NULL
               OR evacuation_restore_target IN ('started', 'paused', 'stopped'))
        NOT VALID;
