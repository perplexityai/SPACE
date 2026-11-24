SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Durable per-environment memory-retirement policy on runtime_environment_states.
-- Retiring an environment's memory records that VM memory checkpoints produced
-- under it may no longer restore; un-retiring clears memory_retired while
-- keeping the latest actor, timestamp, and reason.
--
-- memory_retired defaults to FALSE, a catalog-only constant default, so existing
-- rows aren't rewritten and older binaries inserting (environment_ref, state,
-- updated_by_user_id, reason) record untouched memory_retired = FALSE rows.
--
-- The foreign key and CHECK constraints are NOT VALID so adding them doesn't
-- scan existing rows; every insert and update is still checked. In
-- runtime_environment_states_memory_change_check, memory_reason IS NOT NULL is
-- checked explicitly before char_length(memory_reason) so a NULL reason can't
-- evaluate to UNKNOWN and slip past CHECK's three-valued logic.
-- Take SHARE ROW EXCLUSIVE on users (the mode ADD CONSTRAINT ... REFERENCES
-- users acquires anyway) before ACCESS EXCLUSIVE on runtime_environment_states.
-- A concurrent DELETE FROM users locks users before cascading ON DELETE SET NULL
-- to runtime_environment_states, so reversing that order could deadlock (40P01,
-- which space-db-migrator doesn't retry) instead of hitting lock_timeout (55P03);
-- SHARE ROW EXCLUSIVE doesn't conflict with FOR KEY SHARE on users from state writes.
LOCK TABLE users IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE runtime_environment_states
    ADD COLUMN memory_retired BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN memory_updated_at TIMESTAMPTZ,
    ADD COLUMN memory_updated_by_user_id UUID,
    ADD COLUMN memory_reason TEXT,
    ADD CONSTRAINT runtime_environment_states_memory_updated_by_user_id_fkey
        FOREIGN KEY (memory_updated_by_user_id) REFERENCES users(id) ON DELETE SET NULL
        NOT VALID,
    ADD CONSTRAINT runtime_environment_states_memory_state_check
        CHECK (NOT memory_retired OR state <> 'allowed')
        NOT VALID,
    ADD CONSTRAINT runtime_environment_states_memory_change_check
        CHECK (
            (
                NOT memory_retired
                AND memory_updated_at IS NULL
                AND memory_updated_by_user_id IS NULL
                AND memory_reason IS NULL
            )
            OR (
                memory_updated_at IS NOT NULL
                AND memory_reason IS NOT NULL
                AND char_length(memory_reason) BETWEEN 1 AND 1024
            )
        )
        NOT VALID;

-- Narrow set_updated_at to state's own columns so retiring or un-retiring
-- memory (or an ON DELETE SET NULL cascade on memory_updated_by_user_id)
-- doesn't overwrite state's updated_at.
CREATE OR REPLACE TRIGGER runtime_environment_states_set_updated_at
    BEFORE UPDATE OF state, updated_by_user_id, reason
    ON runtime_environment_states
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMENT ON COLUMN runtime_environment_states.memory_retired IS
    'Whether checkpoint memory from this environment is retired; never TRUE while state is allowed';
COMMENT ON COLUMN runtime_environment_states.memory_updated_at IS
    'When memory_retired was last changed; NULL before any change';
COMMENT ON COLUMN runtime_environment_states.memory_updated_by_user_id IS
    'Actor recorded for the last memory_retired change; NULL when unattributed, after user deletion, or before any change';
COMMENT ON COLUMN runtime_environment_states.memory_reason IS
    'Reason (1..=1024 characters) for the last memory_retired change; NULL before any change';
