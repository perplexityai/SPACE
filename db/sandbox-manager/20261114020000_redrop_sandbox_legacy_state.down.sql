-- Restore the combined status a release that reads it needs.
ALTER TABLE sandboxes ADD COLUMN legacy_state TEXT
    GENERATED ALWAYS AS (
        CASE WHEN cleanup_state IS NOT NULL THEN 'cleanup_' || cleanup_state ELSE state END
    ) VIRTUAL;
