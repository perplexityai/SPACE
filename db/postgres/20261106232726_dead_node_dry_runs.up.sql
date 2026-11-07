SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- The last dry run of the cluster controller's dead-node verb for each node:
-- whether the node was judged lost, why, and what reconciling its sandbox
-- rows would do. The controller writes one row per judged node and the admin
-- API, which runs in another process, reads them. Nothing else reads them,
-- and no write path acts on them.
--
-- counts maps each outcome to its number of rows; samples maps each outcome
-- to its first rows as {sandbox_id, lifecycle} objects. rows_truncated says
-- the per-node row cap cut the read short, so the counts are a lower bound.
-- verdict, reason and the outcome keys are the controller's vocabulary,
-- checked in the application.
CREATE TABLE dead_node_dry_runs (
    node_id UUID NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    verdict TEXT NOT NULL,
    reason TEXT NOT NULL,
    rows_truncated BOOLEAN NOT NULL,
    counts JSONB NOT NULL,
    samples JSONB NOT NULL,
    CONSTRAINT dead_node_dry_runs_pkey PRIMARY KEY (node_id),
    CONSTRAINT dead_node_dry_runs_node_id_fkey
        FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
);

COMMENT ON TABLE dead_node_dry_runs IS
    'Last dead-node dry run per node, written by the cluster controller and read by the admin API';
