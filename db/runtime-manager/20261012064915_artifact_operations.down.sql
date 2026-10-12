-- Operation intent cannot be discarded to make an older writer run again.
CREATE TEMP TABLE artifact_operations_rollback_guard (value INTEGER);
CREATE TEMP TRIGGER artifact_operations_rollback_floor
BEFORE INSERT ON artifact_operations_rollback_guard
BEGIN
    SELECT RAISE(ABORT, 'artifact operations are a rollback floor; restore a verified backup');
END;
INSERT INTO artifact_operations_rollback_guard VALUES (1);
