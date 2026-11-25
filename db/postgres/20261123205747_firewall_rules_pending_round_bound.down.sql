-- Runs at any time: every row this migration admits, the previous schema does.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE firewall_rules_node_observations
    DROP CONSTRAINT firewall_rules_node_observations_pending_bound_check;

COMMENT ON COLUMN firewall_rules_node_observations.pending_assignment_generation IS
    'Node assignment generation the pending round asks about; set only with pending_round, NULL when the round binds none';
