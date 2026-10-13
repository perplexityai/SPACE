-- Requests and lifecycle live in the shared jobs table. No artifact ownership ledger.
CREATE TABLE operation_results (
    job_id TEXT PRIMARY KEY,
    phase TEXT,
    outcome TEXT,
    path TEXT NOT NULL,
    digest TEXT NOT NULL,
    updated_at_unix_ms INTEGER NOT NULL,
    CHECK ((phase IS NULL) <> (outcome IS NULL))
);
