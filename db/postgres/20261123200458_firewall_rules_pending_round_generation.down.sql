-- Roll back the code that binds pending rounds to a generation, then run this.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- A bound round keeps its number and becomes unbound, a form the previous
-- release already reads and this migration admits again when reapplied.
-- Dropping the column drops its CHECK.
ALTER TABLE firewall_rules_node_observations
    DROP COLUMN pending_assignment_generation;
