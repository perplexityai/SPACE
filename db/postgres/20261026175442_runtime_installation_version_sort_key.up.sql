SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- A sortable runtime version for each installation. Nothing writes it yet; the
-- column is nullable, so existing rows and older writers are unaffected.
--
-- node_runtime_installations and node_status_snapshots are altered in separate
-- migrations, so no transaction holds ACCESS EXCLUSIVE on one while waiting for
-- the other. Publications lock installations first, and placement queries take
-- their table locks on node_status_snapshots first, so any single order would
-- deadlock with one of them.

-- Element-wise comparison orders the key: '{1,17,0}' > '{1,15,1}', and a
-- prefix sorts first, so '{1,15}' < '{1,15,0}'. The CHECK matches the array's
-- text form, which is the same under every setting for INT elements: one
-- pattern requires a non-empty, one-dimensional array of non-negative values
-- with default bounds and no NULL. Existing rows receive NULL, which satisfies
-- it, so it's enforced on future writes without a scan.
ALTER TABLE node_runtime_installations
    -- One element per version component. Writers must keep each within INT;
    -- a wider type buys nothing for release numbers.
    -- squawk-ignore prefer-bigint-over-int
    ADD COLUMN version_sort_key INT[],
    ADD CONSTRAINT node_runtime_installations_version_sort_key_check CHECK (
        version_sort_key::TEXT ~ '^\{[0-9]+(,[0-9]+)*\}$'
    ) NOT VALID;

-- Serves operator range queries such as
-- version_sort_key >= '{1,15,1}' AND version_sort_key < '{1,18}'.
-- The table holds a few rows per node, and this transaction already holds
-- ACCESS EXCLUSIVE on it from the ALTER above, so a non-concurrent build adds
-- no lock and finishes within the statement timeout. CREATE INDEX CONCURRENTLY
-- would need a separate non-transactional migration, which the migrator
-- doesn't retry on lock contention and which can leave an invalid index behind.
-- squawk-ignore require-concurrent-index-creation
CREATE INDEX node_runtime_installations_runtime_version_sort_key_idx
    ON node_runtime_installations (runtime, version_sort_key);

COMMENT ON COLUMN node_runtime_installations.version_sort_key IS
    'Runtime version as integers in comparison order, computed by the runtime adapter; NULL means no key, because it is not computed yet or the version has no integer form, not the lowest version';
