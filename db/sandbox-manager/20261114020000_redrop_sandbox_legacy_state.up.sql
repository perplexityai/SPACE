-- Drop the combined status again. 20261113190000 restored it while the
-- rollback target still read it; every node now runs a release that reads
-- state and cleanup_state directly (#1709, #1710, #1711), and that release
-- is the rollback target, so nothing reads legacy_state any more.
ALTER TABLE sandboxes DROP COLUMN legacy_state;
