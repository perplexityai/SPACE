-- One discovery position per control-plane database, matching the retention scan's scope.
-- No snapshot foreign key: the row at the cursor may already have been deleted.
CREATE TABLE snapshot_retention_checkpoint (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    scan_after UUID,
    revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0)
);

INSERT INTO snapshot_retention_checkpoint (singleton) VALUES (TRUE);
