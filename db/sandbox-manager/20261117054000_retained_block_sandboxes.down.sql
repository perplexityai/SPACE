CREATE TEMP TABLE retained_disk_rollback_guard (
    empty INTEGER CHECK (empty = 0)
);
INSERT INTO retained_disk_rollback_guard SELECT COUNT(*) FROM retained_block_sandboxes;
DROP TABLE retained_disk_rollback_guard;
DROP TABLE retained_block_sandboxes;
