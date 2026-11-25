SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- The node assignment generation a pending round asks about, so its answer is
-- matched to the assignment it was asked for, not to whatever is current when
-- it arrives. NULL binds no generation; 0 is a generation. A round without
-- one, as every round written before this migration is, stays admitted.
--
-- The table has one row per node and ADD COLUMN takes ACCESS EXCLUSIVE anyway,
-- so the CHECK is validated here rather than left NOT VALID: every existing
-- row has a NULL generation and passes.
ALTER TABLE firewall_rules_node_observations
    ADD COLUMN pending_assignment_generation BIGINT,
    -- squawk-ignore constraint-missing-not-valid
    ADD CONSTRAINT firewall_rules_node_observations_pending_generation_check CHECK (
        pending_assignment_generation IS NULL
        OR (pending_round IS NOT NULL AND pending_assignment_generation >= 0)
    );

COMMENT ON COLUMN firewall_rules_node_observations.pending_assignment_generation IS
    'Node assignment generation the pending round asks about; set only with pending_round, NULL when the round binds none';
