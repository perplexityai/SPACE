SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE node_status_snapshots
    DROP CONSTRAINT node_status_execution_inventory_observation,
    DROP COLUMN execution_inventory_status,
    DROP COLUMN execution_inventory_received_at;
