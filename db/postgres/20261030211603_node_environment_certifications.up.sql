SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- What each node's build certifies about each environment it knows: the sort
-- key and the edges. runtime_environments keeps one edge set per environment,
-- the intersection over these rows, so the aggregate can be recomputed when a
-- node upgrades its build or withdraws its report. Without them a writer could
-- only narrow the stored set, and an edge a newer build certifies could never
-- come back.
--
-- A node votes on an edge only when its build knows both ends: a build that
-- doesn't know the target can't certify the edge, so it abstains rather than
-- vetoes. Rows are replaced from a complete inventory and deleted when a
-- complete inventory carries no observation, or when the node is deleted.
-- Nothing reads them but the writer that recomputes the aggregate.
--
-- The edge CHECKs are runtime_environments' patterns; see that migration for
-- why one pattern checks every element and the array's shape.
CREATE TABLE node_environment_certifications (
    node_id UUID NOT NULL,
    environment_ref TEXT NOT NULL,
    -- squawk-ignore prefer-bigint-over-int
    version_sort_key INT[],
    upgrade_edges TEXT[] NOT NULL,
    memory_edges TEXT[] NOT NULL,
    CONSTRAINT node_environment_certifications_pkey
        PRIMARY KEY (node_id, environment_ref),
    CONSTRAINT node_environment_certifications_node_id_fkey
        FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE,
    CONSTRAINT node_environment_certifications_environment_ref_check CHECK (
        environment_ref ~ '^(sha256|blake3):[0-9a-f]{64}$'
    ),
    CONSTRAINT node_environment_certifications_version_sort_key_check CHECK (
        version_sort_key::TEXT ~ '^\{[0-9]+(,[0-9]+)*\}$'
    ),
    CONSTRAINT node_environment_certifications_upgrade_edges_check CHECK (
        upgrade_edges::TEXT ~ '^\{((sha256|blake3):[0-9a-f]{64}(,(sha256|blake3):[0-9a-f]{64})*)?\}$'
        AND array_position(upgrade_edges, environment_ref) IS NULL
    ),
    CONSTRAINT node_environment_certifications_memory_edges_check CHECK (
        memory_edges::TEXT ~ '^\{((sha256|blake3):[0-9a-f]{64}(,(sha256|blake3):[0-9a-f]{64})*)?\}$'
        AND memory_edges <@ upgrade_edges
    )
);

-- The aggregate reads every node's row for one environment.
CREATE INDEX node_environment_certifications_environment_ref_idx
    ON node_environment_certifications (environment_ref);

COMMENT ON TABLE node_environment_certifications IS
    'Per-node certification of each known environment, from complete inventories; runtime_environments edges are recomputed from these rows';

-- The one rule for when a node's published environments are evidence, so
-- placement and the upgrade reconciler can't drift apart: the latest
-- inventory report is complete, the node is fresh by stale_after, and the
-- environment arrays were received within the node's freshness window
-- (stale_after - observed_at, two node-status intervals plus a margin). The
-- receipt has its own clock because older cluster-api writers refresh the
-- inventory columns without the arrays, so evidence they leave behind ages
-- out. NULL arrays have a NULL receipt, which is never evidence.
--
-- Call it with the row: node_environment_evidence(nss). It's an expression
-- over one row, so the planner inlines it.
CREATE FUNCTION node_environment_evidence(snapshot node_status_snapshots)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
PARALLEL SAFE
AS $$
    SELECT COALESCE(
        snapshot.execution_inventory_status = 'complete'
        AND snapshot.stale_after > now()
        AND snapshot.environment_refs_received_at
            + (snapshot.stale_after - snapshot.observed_at) > now(),
        FALSE
    )
$$;

COMMENT ON FUNCTION node_environment_evidence(node_status_snapshots) IS
    'Whether the node''s usable_environment_refs and known_environment_refs count as evidence now';
