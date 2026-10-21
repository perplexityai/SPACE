SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE node_status_snapshots
    ADD COLUMN execution_policy JSONB,
    ADD COLUMN execution_policy_received_at TIMESTAMPTZ,
    ADD CONSTRAINT node_status_execution_policy_observation CHECK (
        (execution_policy IS NULL) = (execution_policy_received_at IS NULL)
    ) NOT VALID;

COMMENT ON COLUMN node_status_snapshots.execution_policy IS
    'Validated execution eligibility observation; NULL means unknown policy, not an empty allowlist';
COMMENT ON COLUMN node_status_snapshots.execution_policy_received_at IS
    'Control-plane receipt time for this policy; inventory-only reports from older writers do not refresh it';
