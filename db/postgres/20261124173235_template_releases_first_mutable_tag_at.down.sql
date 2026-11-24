-- Roll back binaries that read or write template_releases.first_mutable_tag_at
-- before removing it. Binaries older than this column neither read nor write it.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Dropping the column loses every recorded time. Applying the up again leaves
-- every release NULL until the backfill migration runs.
ALTER TABLE template_releases
    DROP COLUMN first_mutable_tag_at;
