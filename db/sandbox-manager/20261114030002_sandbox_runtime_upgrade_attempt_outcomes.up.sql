-- The final outcome of each runtime upgrade attempt, at most one per
-- operation and attempt id. The application owns the outcome vocabulary; the
-- schema doesn't check it. `sandbox_runtime_upgrade_outcomes` is keyed by
-- operation, so it holds at most one outcome per operation, whichever attempt
-- recorded it; this table holds one per attempt. `attempt_id` is NOT NULL, so
-- a row that names no attempt has no place here. There's no foreign key to
-- `sandboxes`: deleting a sandbox's row leaves its attempt outcomes in place.
-- `recorded_seq` grows with each row recorded, isn't reused while the table
-- exists, and VACUUM keeps it, so it orders the rows by when they were
-- recorded.
CREATE TABLE sandbox_runtime_upgrade_attempt_outcomes (
    recorded_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    sandbox_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    failure TEXT,
    recorded_at TEXT NOT NULL,
    UNIQUE (operation_id, attempt_id)
);

CREATE INDEX sandbox_runtime_upgrade_attempt_outcomes_by_sandbox
    ON sandbox_runtime_upgrade_attempt_outcomes (sandbox_id);
