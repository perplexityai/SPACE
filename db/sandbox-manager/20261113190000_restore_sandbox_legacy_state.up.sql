-- Restore the combined status that releases before the cleanup-state reader
-- change still read. 20261109120000 dropped it before every node ran a
-- release that no longer reads it; that migration stays bundled because some
-- environments have applied it. Same definition as its down migration.
ALTER TABLE sandboxes ADD COLUMN legacy_state TEXT
    GENERATED ALWAYS AS (
        CASE WHEN cleanup_state IS NOT NULL THEN 'cleanup_' || cleanup_state ELSE state END
    ) VIRTUAL;
