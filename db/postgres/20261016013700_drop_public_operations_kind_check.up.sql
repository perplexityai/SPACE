SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Drop the CHECK that enumerates public operation kinds.
--
-- The application owns the set: every write binds a closed `OperationKind`
-- value, so the CHECK only duplicated that enum and had to be widened by a
-- migration each time it grew. The next addition is
-- `'compensate_create_sandbox'`, which a create queues to tear down a guest it
-- started but couldn't keep.
ALTER TABLE public_operations
    -- squawk-ignore ban-drop-constraint
    DROP CONSTRAINT IF EXISTS public_operations_kind_check;
