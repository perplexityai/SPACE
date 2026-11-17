-- Abort rollback rather than discard a disk's outstanding release authority.
CREATE TEMP TABLE retained_block_bindings_rollback_guard (
    remaining INTEGER NOT NULL CHECK (remaining = 0)
);
INSERT INTO retained_block_bindings_rollback_guard
    SELECT COUNT(*) FROM retained_block_bindings;
DROP TABLE retained_block_bindings_rollback_guard;
DROP TABLE retained_block_bindings;
