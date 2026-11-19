-- Nothing has read the combined status since the release before this one,
-- which every node now runs, so the column the cleanup-state split kept for
-- older readers goes.
ALTER TABLE sandboxes DROP COLUMN legacy_state;
