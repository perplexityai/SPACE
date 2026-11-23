SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Every pending round now names the assignment generation it asks about.
-- 20261123200458 added the column and still admits a round without one.
--
-- Precondition: no unbound pending round. No release writes one; one exists
-- only after that migration's down ran and its up was reapplied, or by hand.
-- The CHECK below is validated, so this refuses with its name while one
-- remains. Replace each with a new round, numbered past both its pending and
-- accepted rounds and bound to the generation to verify, as the round opener
-- does. Never clear it or bind it under its old number: an answer to the round
-- it was before may still carry that number.
--
-- The table has one row per node, so validating holds ACCESS EXCLUSIVE only
-- briefly; NOT VALID and a VALIDATE in this transaction would hold it as long.
ALTER TABLE firewall_rules_node_observations
    -- squawk-ignore constraint-missing-not-valid
    ADD CONSTRAINT firewall_rules_node_observations_pending_bound_check CHECK (
        (pending_round IS NULL) = (pending_assignment_generation IS NULL)
    );

COMMENT ON COLUMN firewall_rules_node_observations.pending_assignment_generation IS
    'Node assignment generation the pending round asks about; set exactly when pending_round is';
