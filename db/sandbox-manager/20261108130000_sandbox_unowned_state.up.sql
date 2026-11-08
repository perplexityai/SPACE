-- A row's lifecycle while no cleanup owns it, and NULL while one does. A
-- predicate on the lifecycle reads it, so a cleanup-owned row matches none:
-- `state` also holds the teardown transition a cleanup is over.
ALTER TABLE sandboxes ADD COLUMN unowned_state TEXT
    GENERATED ALWAYS AS (CASE WHEN cleanup_state IS NULL THEN state END) VIRTUAL;
