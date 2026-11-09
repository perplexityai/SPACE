CREATE TABLE guest_update_operations (
    operation_id TEXT PRIMARY KEY NOT NULL,
    sandbox_id TEXT NOT NULL REFERENCES sandboxes(id),
    execution_id TEXT NOT NULL,
    request_json TEXT NOT NULL,
    phase TEXT NOT NULL,
    terminal INTEGER NOT NULL DEFAULT 0 CHECK (terminal IN (0, 1)),
    prior_identity_json TEXT,
    observed_identity_json TEXT,
    deadline INTEGER NOT NULL,
    outcome TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX guest_update_operations_by_execution
    ON guest_update_operations(sandbox_id, execution_id, terminal);
CREATE INDEX guest_update_operations_pending
    ON guest_update_operations(terminal, updated_at, operation_id);

CREATE TABLE guest_update_policy (
    sandbox_id TEXT NOT NULL REFERENCES sandboxes(id),
    execution_id TEXT NOT NULL,
    promotion_json TEXT NOT NULL,
    authorization_id TEXT NOT NULL,
    attempts INTEGER NOT NULL CHECK (attempts BETWEEN 0 AND 2),
    retry_not_before INTEGER NOT NULL,
    quarantine_reason TEXT,
    PRIMARY KEY (sandbox_id, execution_id)
);
