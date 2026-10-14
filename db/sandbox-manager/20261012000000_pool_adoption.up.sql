CREATE TABLE sandbox_pool_adoptions (
    sandbox_id TEXT PRIMARY KEY REFERENCES sandboxes(id) ON DELETE CASCADE,
    claim_id TEXT,
    org_id TEXT,
    completed INTEGER NOT NULL DEFAULT 0 CHECK (completed IN (0, 1)),
    CHECK ((claim_id IS NULL) = (org_id IS NULL)),
    CHECK (completed = 0 OR claim_id IS NOT NULL)
);
