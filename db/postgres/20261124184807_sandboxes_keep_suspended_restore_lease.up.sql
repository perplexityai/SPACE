SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- A write that keeps a row `suspended` keeps its restore lease. The memory
-- drop seals a `suspended` row with the restore lease for as long as it copies
-- the row's disk, without moving its lifecycle, and every other write to the
-- row would otherwise clear that seal. Any other write that leaves the row
-- outside `restoring` still clears the lease, as before. No other writer sets a
-- lease on a `suspended` row, so a row this keeps one on was sealed by a drop.
CREATE OR REPLACE FUNCTION public.sandboxes_clear_restore_lease() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF LOWER(TRIM(NEW.lifecycle)) <> 'restoring' THEN
        IF NOT (
            LOWER(TRIM(NEW.lifecycle)) = 'suspended'
            AND LOWER(TRIM(OLD.lifecycle)) = 'suspended'
        ) THEN
            NEW.restore_lease_expires_at := NULL;
            NEW.restore_lease_token := NULL;
        END IF;
        NEW.preserve_local_artifact_on_restore_failure := FALSE;
    END IF;
    RETURN NEW;
END;
$$;
