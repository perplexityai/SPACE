SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Narrowing back is only safe once no row carries the `'stopped'` marker, and
-- this migration refuses rather than clearing one. A stopped evacuation marks
-- a row while it's `stopped` and keeps the marker through `restoring`, and
-- placement finalizes a `restoring` row with no marker as `started`, so
-- clearing it would start a sandbox the user stopped. Before rolling back,
-- stop starting stopped evacuations and let the in-flight ones finish while
-- the drain code that services them is still deployed: code without
-- `Stopped` in `EVACUABLE_LIFECYCLES` can't finish one, so a row it left
-- marked would block this migration indefinitely. Then roll back that code,
-- whose `'stopped'` writes would fail against the narrowed CHECK, and only
-- then this migration.
--
-- The first check runs before the ACCESS EXCLUSIVE lock, so a refusal doesn't
-- block traffic. The second, identical one runs after the ALTERs, under that
-- lock, so no marker can be written between the check and the narrowed CHECK;
-- such a row would fail every later update that keeps the marker. A failed
-- check rolls the whole migration back.
--
-- The probe's cost depends on the plan: through
-- `idx_sandboxes_evacuation_restore_target` it reads every marked row, and
-- without that index it scans the table. So the locked probe runs under its
-- own 1s `statement_timeout`, which bounds how long the lock is held however
-- the probe is planned. A timeout fails closed like a refusal.
--
-- The narrowed CHECK is left `NOT VALID`, unlike the baseline's: validating it
-- here would scan under ACCESS EXCLUSIVE, and the second check has already
-- shown that no row violates it.
DO $$
BEGIN
    IF EXISTS (SELECT 1
                 FROM sandboxes
                WHERE evacuation_restore_target = 'stopped') THEN
        RAISE EXCEPTION 'sandboxes carry evacuation_restore_target = '
            '''stopped''; let those evacuations finish before rolling back '
            'this migration';
    END IF;
END;
$$;

ALTER TABLE sandboxes
    -- squawk-ignore ban-drop-constraint
    DROP CONSTRAINT IF EXISTS sandboxes_evacuation_restore_target_check;

ALTER TABLE sandboxes
    ADD CONSTRAINT sandboxes_evacuation_restore_target_check
        CHECK (evacuation_restore_target IS NULL
               OR evacuation_restore_target IN ('started', 'paused'))
        NOT VALID;

SET LOCAL statement_timeout = '1s';

DO $$
BEGIN
    IF EXISTS (SELECT 1
                 FROM sandboxes
                WHERE evacuation_restore_target = 'stopped') THEN
        RAISE EXCEPTION 'sandboxes carry evacuation_restore_target = '
            '''stopped''; let those evacuations finish before rolling back '
            'this migration';
    END IF;
END;
$$;
