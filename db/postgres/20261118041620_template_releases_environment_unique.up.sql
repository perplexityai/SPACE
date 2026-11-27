SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- One template release per template, hash and execution environment. hash is
-- text, so a digest's bare and sha256:-prefixed spellings are distinct keys here,
-- as they are in the existing index.
-- A runtime upgrade campaign gives a release a twin: a second row with the same
-- template_name and hash whose execution_metadata binds it to the campaign's
-- target environment. idx_template_releases_template_name_hash_unique still
-- admits one row per (template_name, hash), so no twin can be inserted until a
-- later migration drops it. Until then both indexes hold, and this one is
-- implied by the old one, so building it can't fail on existing rows.
--
-- That drop waits until nothing that writes template_releases relies on the
-- old uniqueness, neither a deployed or rollback binary nor
-- scripts/bootstrap-templates.sh:
-- * INSERT ... ON CONFLICT (template_name, hash): Postgres picks the arbiter
--   index from that target and refuses the statement when no unique index
--   matches it.
-- * Reads keyed by template and hash that ignore the environment, whether they
--   keep one row (for example with LIMIT 1) or aggregate every match. Once a
--   twin exists, one-row reads can return the twin, so tags, pools and sandbox
--   creates resolve a release bound to the wrong environment, and aggregating
--   reads merge both twins' tags or storage locations.
-- * UPDATEs that set execution_metadata on an unbound row. They change this
--   index's key, so they fail with a unique violation when a twin bound to the
--   same environment exists.
--
-- scripts/bootstrap-templates.sh does the first two. It registers a release
-- with ON CONFLICT (template_name, hash) DO UPDATE, and its tag and
-- storage-location inserts select releases by template_name and hash alone.
-- Once a twin exists, its tag insert fails with "ON CONFLICT DO UPDATE command
-- cannot affect row a second time", and its storage-location insert attaches
-- the location to every twin.
--
-- The environment is the execution_metadata key, not a column: a stored
-- generated column would rewrite the table under ACCESS EXCLUSIVE, and a plain
-- column would stay NULL on rows that writers predating this migration insert
-- or bind. NULLS NOT DISTINCT
-- keeps one unbound release per (template_name, hash), as today; an unbound
-- release and its bound twin are distinct rows.
--
-- The key is the raw binding, so an unbound release and one bound to the
-- initial environment are distinct here, although nodes cache both in the same
-- legacy partition (TemplateCachePartition). The index can't fold them: the
-- initial environment's reference comes from the compiled runtime catalog.
-- Writers must treat an unbound release as the initial environment's and not
-- add an initial-bound row beside it.
--
-- A non-concurrent build holds SHARE on template_releases for the build. That
-- blocks every write to it (registration, filling execution_metadata, release
-- deletion) but not reads or the KEY SHARE locks of foreign keys pointing at it.
-- Releases are written by builds, publications and deletes, not by sandbox
-- traffic. A lock timeout is retried by the
-- migrator; a build past the statement timeout rolls back cleanly and fails the
-- run, leaving the schema as it was. CREATE INDEX CONCURRENTLY would need a
-- separate non-transactional migration, which the migrator doesn't retry on
-- lock contention and which can leave an invalid unique index behind.
-- squawk-ignore require-concurrent-index-creation
CREATE UNIQUE INDEX template_releases_template_name_hash_environment_key
    ON template_releases (template_name, hash, (execution_metadata->>'environment'))
    NULLS NOT DISTINCT;

COMMENT ON INDEX template_releases_template_name_hash_environment_key IS
    'One release per template, hash and execution_metadata environment; a twin of a release is bound to another environment';
