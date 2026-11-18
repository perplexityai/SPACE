SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- The reason that wakes a node to fetch its current firewall rules. IF NOT
-- EXISTS because the down keeps the value; AFTER because Squawk requires a
-- position (require-enum-value-ordering).
ALTER TYPE public.state_reconciliation_reason
    ADD VALUE IF NOT EXISTS 'firewall_rules_changed' AFTER 'node_template_inventory_published';
