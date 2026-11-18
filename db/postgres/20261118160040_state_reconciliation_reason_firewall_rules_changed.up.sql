SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- The reason that wakes a node to fetch its current firewall rules revision.
-- It carries no rules: like every reason, it only asks the node to drain its
-- queue.
--
-- Adding a value rewrites no rows and blocks no reader or writer of
-- node_reconcile_triggers. It goes after the last existing value, so it sorts
-- after every existing one and changes no existing value's position.
-- Postgres refuses to use a value in the transaction that adds it, and nothing
-- here does; a writer can cast to it once this migration commits.
--
-- IF NOT EXISTS because the down migration keeps the value: applying this
-- again after a rollback must succeed without adding a second one.
ALTER TYPE public.state_reconciliation_reason
    ADD VALUE IF NOT EXISTS 'firewall_rules_changed' AFTER 'node_template_inventory_published';
