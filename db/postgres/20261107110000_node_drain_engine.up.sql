SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- The node drain's v2 engine. `drain_engine` names the engine a drain runs
-- under: NULL is the v1 engine, the only one before this migration, and `v2`
-- a drain the admin route started with `engine: v2`. Every v1 writer keyed on
-- the drain phase skips a `v2` row. `drain_status` is the v2 drain's own
-- record: the epoch the node is sealed under, written in the transaction that
-- starts the drain, and the node's last `DrainNode` report.
--
-- Nullable with no default, backfill or index: nothing writes either column
-- yet, so every row stays NULL and every reader behaves as before. The CHECKs
-- are NOT VALID so adding them doesn't scan `nodes`; every existing row has
-- both columns NULL, which satisfies them, and new writes are checked either
-- way.
ALTER TABLE nodes
    ADD COLUMN drain_engine text,
    ADD COLUMN drain_status jsonb,
    ADD CONSTRAINT nodes_drain_engine_check
        CHECK (drain_engine IS NULL OR drain_engine = 'v2')
        NOT VALID,
    ADD CONSTRAINT nodes_drain_status_object_check
        CHECK (drain_status IS NULL OR jsonb_typeof(drain_status) = 'object')
        NOT VALID;
