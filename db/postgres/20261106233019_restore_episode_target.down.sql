-- Roll back the images that read and write the column before removing it.
-- Down runs once, without the lock retry up has; rerun it if it times out.
--
-- A row holding a value is a restore episode in flight whose landing
-- lifecycle would be lost: the code before this migration lands it in the
-- default lifecycle. So this refuses while any row holds one, rather than
-- dropping it. The probe scans `sandboxes` before the ACCESS EXCLUSIVE lock,
-- so a refusal doesn't block traffic; with the writers rolled back first, no
-- value can appear between it and the drop.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

DO $$
BEGIN
    IF EXISTS (SELECT 1
                 FROM sandboxes
                WHERE restore_episode_target IS NOT NULL) THEN
        RAISE EXCEPTION 'sandboxes carry restore_episode_target; let those '
            'restores finish before rolling back this migration';
    END IF;
END;
$$;

ALTER TABLE sandboxes
    DROP COLUMN restore_episode_target;
