-- Preserve cleanup work before the control-plane grant can attach a disk.
CREATE TABLE pending_block_resolutions (
    sandbox_id TEXT PRIMARY KEY NOT NULL,
    execution_id TEXT NOT NULL
);
