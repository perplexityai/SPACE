-- Roll back memory-retirement readers and writers before removing the columns.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

-- Refuse while any environment has memory_retired = TRUE: dropping the column
-- would discard an active memory retirement and let restores or state changes
-- treat the environment's memory as live. Rows with memory_retired = FALSE,
-- whether untouched or later un-retired, carry no active retirement and may be
-- rolled back (discarding only any un-retirement audit metadata).
--
-- Check once before LOCK TABLE so a refusal doesn't block traffic, then lock
-- users before runtime_environment_states (dropping memory_updated_by_user_id's
-- foreign key locks users anyway, and user deletion's ON DELETE SET NULL
-- cascade takes users first; policy state writes take runtime_environment_states
-- before users, so an overlapping state write can still deadlock on lock order)
-- and re-check under READ COMMITTED's fresh snapshot (enforced above even if
-- default_transaction_isolation is stricter) so no retirement can land between
-- the check and the drop.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM runtime_environment_states WHERE memory_retired) THEN
        RAISE EXCEPTION 'runtime_environment_states has memory_retired environments, '
            'and dropping memory_retired would discard active memory retirements; '
            'roll forward, or un-retire every environment''s memory before rolling '
            'back this migration';
    END IF;
END;
$$;

LOCK TABLE users, runtime_environment_states IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM runtime_environment_states WHERE memory_retired) THEN
        RAISE EXCEPTION 'runtime_environment_states has memory_retired environments, '
            'and dropping memory_retired would discard active memory retirements; '
            'roll forward, or un-retire every environment''s memory before rolling '
            'back this migration';
    END IF;
END;
$$;

CREATE OR REPLACE TRIGGER runtime_environment_states_set_updated_at
    BEFORE UPDATE ON runtime_environment_states
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Dropping the columns also drops their foreign key, CHECK constraints, and
-- column comments.
ALTER TABLE runtime_environment_states
    DROP COLUMN memory_retired,
    DROP COLUMN memory_updated_at,
    DROP COLUMN memory_updated_by_user_id,
    DROP COLUMN memory_reason;
