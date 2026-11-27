SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- When a mutable tag first landed on a release: the first time a tag other
-- than the release's own digest pointed at it. NULL until then. Every build
-- registers with its own digest as a tag, so a build registered with no other
-- tag stays NULL. The column is sticky: moving a tag off the release later
-- doesn't clear it.
--
-- It's for the warm-pool retain window, which ranks releases by their newest
-- tag. Counting only releases that have carried a mutable tag keeps a build
-- nobody tagged from taking a retain slot from a live release.
--
-- This isn't "published" in the release store's sense: release_published in
-- crates/l30_storage/template/release/store means the release has a storage
-- location and is immutable. A build registered with no mutable tags is
-- published in that sense and still NULL here.
--
-- Writer: the tag verbs in crates/l30_storage/template/store/src/release_tags.rs
-- (retarget, insert_if_absent, upsert) and scripts/bootstrap-templates.sh set it
-- in the statement that writes the tag, in a later change. Until that change is
-- deployed everywhere and the backfill below has run, nothing may read it.
-- Releases that exist before then are marked by a backfill migration that runs
-- only after no older binary is deployed or kept as a rollback target. That
-- migration replaces the column comment below with the writer list.
--
-- Nullable with no default, so adding it doesn't rewrite the table, and
-- binaries that don't know the column keep working.
ALTER TABLE template_releases
    ADD COLUMN first_mutable_tag_at TIMESTAMPTZ;

COMMENT ON COLUMN template_releases.first_mutable_tag_at IS
    'When a tag other than the release''s own digest first pointed at it; NULL until then. Not the same as release_published, which means the release has a storage location. Not written yet. Don''t read it until the backfill migration has run.';
