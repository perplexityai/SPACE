SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- The environments each node reports. Placement and the upgrade reconciler read
-- only control-plane tables, so what each node actually has is stored here.
-- Nothing writes these yet; every column is nullable, so existing rows and
-- older writers are unaffected. This table is altered on its own, for the lock
-- reason in 20261026175442_runtime_installation_version_sort_key.up.sql.
--
-- An empty array is a report of no environments; NULL is no report. The
-- elements have no format CHECK: Postgres evaluates every CHECK on every
-- update of the row, including the heartbeats that leave these columns alone,
-- and an exact digest pattern costs several times the rest of that update. The
-- writer stores only parsed environment references, as with execution_policy.
--
-- The arrays have their own receipt time, as execution_policy does. Writers
-- that predate them refresh execution_inventory_received_at and
-- execution_inventory_status on every inventory report while leaving the
-- arrays alone, so neither column can date or qualify them. The writer stores
-- arrays only from a complete inventory, and readers judge them by
-- environment_refs_received_at. The CHECK requires the arrays and their
-- receipt together, and is a few NULL tests.
-- Existing rows receive three NULLs and satisfy it, so it's enforced on future
-- writes without a scan.
ALTER TABLE node_status_snapshots
    ADD COLUMN usable_environment_refs TEXT[],
    ADD COLUMN known_environment_refs TEXT[],
    ADD COLUMN environment_refs_received_at TIMESTAMPTZ,
    ADD CONSTRAINT node_status_environment_refs_observation CHECK (
        (usable_environment_refs IS NULL) = (environment_refs_received_at IS NULL)
        AND (known_environment_refs IS NULL) = (environment_refs_received_at IS NULL)
    ) NOT VALID;

-- For placement's question, which nodes can run an environment:
-- usable_environment_refs @> ARRAY[$1]. With one row per node the planner
-- prefers a sequential scan until the fleet is large. The table is small, and
-- this transaction already holds ACCESS EXCLUSIVE on it from the ALTER above,
-- so a non-concurrent build adds no lock and finishes within the statement
-- timeout. CREATE INDEX CONCURRENTLY would need a separate non-transactional
-- migration, which the migrator doesn't retry on lock contention and which can
-- leave an invalid index behind.
-- squawk-ignore require-concurrent-index-creation
CREATE INDEX node_status_snapshots_usable_environment_refs_idx
    ON node_status_snapshots USING GIN (usable_environment_refs);

COMMENT ON COLUMN node_status_snapshots.usable_environment_refs IS
    'Environments installed and verified on the node, from a complete inventory received at environment_refs_received_at; NULL means no report, not an empty set. Evidence only while that receipt is fresh';
COMMENT ON COLUMN node_status_snapshots.known_environment_refs IS
    'Environments certified in the node build''s catalog, as of environment_refs_received_at; NULL means no report, not an empty set';
COMMENT ON COLUMN node_status_snapshots.environment_refs_received_at IS
    'Control-plane receipt time for the environment arrays only; inventory reports from older writers do not refresh it';
