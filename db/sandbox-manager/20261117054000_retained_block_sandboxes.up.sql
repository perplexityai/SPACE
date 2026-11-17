-- Sticky local dependency marker: runtime cleanup must not make a disk-bearing
-- sandbox eligible for root-only snapshots, exports, or warm cloning.
CREATE TABLE retained_block_sandboxes (
    sandbox_id TEXT PRIMARY KEY NOT NULL REFERENCES sandboxes(id) ON DELETE CASCADE
);
