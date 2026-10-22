-- Roll back policy readers and writers before dropping their tables.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Refuse while the policy differs from the bootstrap seed. Dropping it would
-- silently discard it, including any blocks. The check compares what the policy
-- enforces, not its generation or audit columns, because the generation never
-- decreases and a check on it could never clear. Resetting the content needs
-- direct SQL by a cluster administrator, since the policy store will never
-- clear a default. The next up restarts the generation at 0. Lock users first:
-- dropping the tables locks it anyway, and a user deletion's ON DELETE SET NULL
-- cascade takes users before the policy tables, so taking them first could
-- deadlock with it. Holding the locks keeps any write from landing between the
-- check and the drop.
LOCK TABLE users, runtime_environment_policy, runtime_environment_states
    IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1
                 FROM runtime_environment_policy
                WHERE default_environment_ref IS NOT NULL
                   OR legacy_unbound_allowed IS NOT NULL)
       OR EXISTS (SELECT 1 FROM runtime_environment_states) THEN
        RAISE EXCEPTION 'runtime environment policy differs from the bootstrap '
            'policy, and dropping it would discard it, including any blocks; set '
            'default_environment_ref and legacy_unbound_allowed to NULL and '
            'delete every states row before rolling back this migration';
    END IF;
END;
$$;

DROP TABLE runtime_environment_policy;
DROP TABLE runtime_environment_states;
