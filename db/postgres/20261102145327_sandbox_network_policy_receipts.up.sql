SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Receipts live for the sandbox lifetime; they contain no policy document.
CREATE TABLE sandbox_network_policy_receipts (
    sandbox_id UUID NOT NULL REFERENCES sandboxes(id) ON DELETE CASCADE,
    idempotency_digest TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    target_ref TEXT NOT NULL,
    PRIMARY KEY (sandbox_id, idempotency_digest)
);
