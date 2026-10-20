SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Restoring the baseline CHECK refuses while any operation of a kind outside it
-- remains, whatever its state. A `NOT VALID` CHECK still evaluates every update
-- to an existing row: the dispatcher updates queued and running operations on
-- each claim and result, and the `actor_user_id` foreign key's `ON DELETE SET
-- NULL` updates rows in any state when a user is deleted. Under the restored
-- CHECK those updates would fail, stranding the operation or blocking the
-- user's deletion. Draining such operations is the job of the change that
-- introduced their kind: they have to finish and retention has to delete them
-- before this migration can run.
--
-- The first check runs before the ACCESS EXCLUSIVE lock, so a refusal doesn't
-- block traffic. The second, identical one runs after the ALTERs, under the
-- lock the `DROP` takes, so no operation can be inserted between the check and
-- the CHECK. That relies on READ COMMITTED, Postgres's default, which the
-- migrator doesn't override: it gives the second check a fresh snapshot. A
-- failed check rolls the whole migration back.
DO $$
BEGIN
    IF EXISTS (SELECT 1
                 FROM public_operations
                WHERE kind NOT IN (
                      'create_sandbox', 'fork_sandbox', 'update_sandbox',
                      'delete_sandbox', 'start_sandbox', 'stop_sandbox',
                      'pause_sandbox', 'resume_sandbox', 'suspend_sandbox',
                      'restore_sandbox', 'wake_sandbox', 'kill_sandbox',
                      'checkpoint_sandbox', 'enable_sandbox_snapshots',
                      'disable_sandbox_snapshots', 'rollback_sandbox',
                      'delete_template', 'create_session')) THEN
        RAISE EXCEPTION 'public_operations has operations of a kind '
            'public_operations_kind_check does not admit; let them finish and '
            'retention delete them before rolling back this migration';
    END IF;
END;
$$;

ALTER TABLE public_operations
    -- squawk-ignore ban-drop-constraint
    DROP CONSTRAINT IF EXISTS public_operations_kind_check;

ALTER TABLE public_operations
    ADD CONSTRAINT public_operations_kind_check
        CHECK (kind IN (
            'create_sandbox', 'fork_sandbox', 'update_sandbox', 'delete_sandbox',
            'start_sandbox', 'stop_sandbox', 'pause_sandbox', 'resume_sandbox',
            'suspend_sandbox', 'restore_sandbox', 'wake_sandbox', 'kill_sandbox',
            'checkpoint_sandbox', 'enable_sandbox_snapshots',
            'disable_sandbox_snapshots', 'rollback_sandbox', 'delete_template',
            'create_session'
        ))
        NOT VALID;

-- The ACCESS EXCLUSIVE lock is held until commit, so bound the second check
-- tightly. It waits on no lock; a timeout rolls the migration back cleanly. No
-- index covers `kind`, so the check scans the table: one too large to scan
-- within the bound fails this migration closed on every run.
SET LOCAL statement_timeout = '1s';

DO $$
BEGIN
    IF EXISTS (SELECT 1
                 FROM public_operations
                WHERE kind NOT IN (
                      'create_sandbox', 'fork_sandbox', 'update_sandbox',
                      'delete_sandbox', 'start_sandbox', 'stop_sandbox',
                      'pause_sandbox', 'resume_sandbox', 'suspend_sandbox',
                      'restore_sandbox', 'wake_sandbox', 'kill_sandbox',
                      'checkpoint_sandbox', 'enable_sandbox_snapshots',
                      'disable_sandbox_snapshots', 'rollback_sandbox',
                      'delete_template', 'create_session')) THEN
        RAISE EXCEPTION 'public_operations has operations of a kind '
            'public_operations_kind_check does not admit; let them finish and '
            'retention delete them before rolling back this migration';
    END IF;
END;
$$;
