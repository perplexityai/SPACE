SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- The cluster controller's dead-node verb's last result for the node: the
-- mode it ran in, whether the node was judged lost, why, and what reconciling
-- its sandbox rows did or, in a dry run, would do. Only the verb writes it,
-- and the admin API, which runs in another process, reads it. NULL means the
-- verb has not judged the node. The value goes with the node row.
--
-- Adding a nullable column without a default changes the catalog only; no row
-- is rewritten.
ALTER TABLE nodes
    ADD COLUMN dead_node_status JSONB;

COMMENT ON COLUMN nodes.dead_node_status IS
    'Last dead-node verb result for the node, written by the cluster controller; NULL means not judged';
