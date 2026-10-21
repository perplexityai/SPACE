SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Existing rows receive two NULLs, so they already satisfy the constraint.
-- Enforce future writes without scanning existing rows while holding the DDL lock.
ALTER TABLE node_status_snapshots
    ADD COLUMN execution_inventory_status TEXT,
    ADD COLUMN execution_inventory_received_at TIMESTAMPTZ,
    ADD CONSTRAINT node_status_execution_inventory_observation CHECK (
        (execution_inventory_status IS NULL AND execution_inventory_received_at IS NULL)
        OR (execution_inventory_status IS NOT NULL
            AND execution_inventory_received_at IS NOT NULL)
    ) NOT VALID;

COMMENT ON COLUMN node_status_snapshots.execution_inventory_status IS
    'Latest execution collection outcome; NULL means no report, not an empty inventory or readiness';
COMMENT ON COLUMN node_status_snapshots.execution_inventory_received_at IS
    'Control-plane receipt time for execution inventory only; unrelated heartbeats do not refresh it';
