-- Rollback requires cleanup first: dropping a pending receipt loses authority.
CREATE TEMP TABLE require_no_pending_block_resolutions (
    pending INTEGER NOT NULL CHECK (pending = 0)
);
INSERT INTO require_no_pending_block_resolutions
SELECT COUNT(*) FROM pending_block_resolutions;
DROP TABLE require_no_pending_block_resolutions;
DROP TABLE pending_block_resolutions;
