SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Restoring the baseline CHECK refuses while an operation of a kind outside it
-- is still queued or running: the dispatcher updates the row on every claim and
-- result, and each of those updates would fail against the restored CHECK,
-- leaving the operation with no owner. Roll back the code that queues such
-- kinds first, let the outstanding operations finish, then this migration.
--
-- Terminal rows don't block it. The restored CHECK is `NOT VALID`, as in the
-- baseline, so existing rows aren't re-checked, and nothing updates a terminal
-- operation; retention deletes them, which a CHECK doesn't evaluate.
--
-- The first check runs before the ACCESS EXCLUSIVE lock, so a refusal doesn't
-- block traffic. The second, identical one runs after the ALTERs, under that
-- lock, so no operation can be queued between the check and the CHECK.
-- A failed check rolls the whole migration back.
DO $$
BEGIN
    IF EXISTS (SELECT 1
                 FROM public_operations
                WHERE state IN ('queued', 'running')
                  AND kind NOT IN (
                      'create_sandbox', 'fork_sandbox', 'update_sandbox',
                      'delete_sandbox', 'start_sandbox', 'stop_sandbox',
                      'pause_sandbox', 'resume_sandbox', 'suspend_sandbox',
                      'restore_sandbox', 'wake_sandbox', 'kill_sandbox',
                      'checkpoint_sandbox', 'enable_sandbox_snapshots',
                      'disable_sandbox_snapshots', 'rollback_sandbox',
                      'delete_template', 'create_session')) THEN
        RAISE EXCEPTION 'public_operations has queued or running operations '
            'of a kind public_operations_kind_check does not admit; let them '
            'finish before rolling back this migration';
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

SET LOCAL statement_timeout = '1s';

DO $$
BEGIN
    IF EXISTS (SELECT 1
                 FROM public_operations
                WHERE state IN ('queued', 'running')
                  AND kind NOT IN (
                      'create_sandbox', 'fork_sandbox', 'update_sandbox',
                      'delete_sandbox', 'start_sandbox', 'stop_sandbox',
                      'pause_sandbox', 'resume_sandbox', 'suspend_sandbox',
                      'restore_sandbox', 'wake_sandbox', 'kill_sandbox',
                      'checkpoint_sandbox', 'enable_sandbox_snapshots',
                      'disable_sandbox_snapshots', 'rollback_sandbox',
                      'delete_template', 'create_session')) THEN
        RAISE EXCEPTION 'public_operations has queued or running operations '
            'of a kind public_operations_kind_check does not admit; let them '
            'finish before rolling back this migration';
    END IF;
END;
$$;
