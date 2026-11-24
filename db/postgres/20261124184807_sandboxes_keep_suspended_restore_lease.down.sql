SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Every write that leaves a row outside `restoring` clears its restore lease
-- again. A memory drop's seal still on a `suspended` row stays until the row's
-- next write clears it or it expires. Until then a user restore refuses the
-- row as it refuses any live lease. Once a write clears it, a drop still
-- copying finds its token gone at commit and refuses, and its retry seals the
-- row again, which the next write clears: roll back the images that run the
-- drop with this migration.
CREATE OR REPLACE FUNCTION public.sandboxes_clear_restore_lease() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF LOWER(TRIM(NEW.lifecycle)) <> 'restoring' THEN
        NEW.restore_lease_expires_at := NULL;
        NEW.restore_lease_token := NULL;
        NEW.preserve_local_artifact_on_restore_failure := FALSE;
    END IF;
    RETURN NEW;
END;
$$;
